#!/usr/bin/perl

# Copyright 2016 MOBIUS
# Author: Blake GH

# 
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# 

 use Loghandler;
 use Mobiusutil;
 use Data::Dumper;
 
		
 my $configFile = @ARGV[0];
 if(!$configFile)
 {
	print "Please specify a config file\n";
	exit;
 }

 our $mobUtil = new Mobiusutil(); 
 our $log;
 our $conf = $mobUtil->readConfFile($configFile);
 our %conf;
 
 if($conf)
 {
	%conf = %{$conf};
	if ($conf{"logfile"})
	{
	
		$log = new Loghandler($conf->{"logfile"});
		$log->addLogLine(" ---------------- Script Starting ---------------- ");
		my @reqs = ("sslconfpath","sitesavailablepath","letsencryptgitrepopath","machinestoupdate","pathtosharedcerts","pathtosharedvirtualhosts","destinationssldir","emailaddress","rootdomain");
		my $valid = 1;
		for my $i (0..$#reqs)
		{
			if(!$conf{@reqs[$i]})
			{
				$log->addLogLine("Required configuration missing from conf file");
				$log->addLogLine(@reqs[$i]." required");
				$valid = 0;
			}
		}		
		if($valid)
		{
			# make sure that we have these packages
			$log->addLine("apt-get install -y git");
			print "apt-get install -y git\n";
			system("apt-get install -y git");
			
			sslconf();
			
			checkoutletsencrypt();
			
			getcerts();
			
			my @machines = split(' ', $conf{"machinestoupdate"});
			
			
			# put ssl.conf into place
			foreach(@machines)
			{
				print "rsync -av --delete ".$conf->{"sslconfpath"}." $_:".$conf->{"sslconfpath"}."\n";
				$log->addLine("rsync -av --delete ".$conf->{"sslconfpath"}." $_:".$conf->{"sslconfpath"});
				system("rsync -av --delete ".$conf->{"sslconfpath"}." $_:".$conf->{"sslconfpath"});
			}
			
			# copy all of the certs to the shared directory
			print "cp -Rf /etc/letsencrypt/archive/* ".$conf->{"pathtosharedcerts"}."/"."\n";
			$log->addLine("cp -Rf /etc/letsencrypt/archive/* ".$conf->{"pathtosharedcerts"}."/");
			system("cp -Rf /etc/letsencrypt/archive/* ".$conf->{"pathtosharedcerts"}."/");
			
			# copy all of the certs to the local apache directory
			print "cp -Rf /etc/letsencrypt/archive/* ".$conf->{"destinationssldir"}."/"."\n";
			$log->addLine("cp -Rf /etc/letsencrypt/archive/* ".$conf->{"destinationssldir"}."/");
			system("cp -Rf /etc/letsencrypt/archive/* ".$conf->{"destinationssldir"}."/");
			
			# copy all of the certs to each of the bricks
			foreach(@machines)
			{
				print "rsync -av --delete /etc/letsencrypt/archive/* $_:".$conf->{"destinationssldir"}."\n";
				$log->addLine("rsync -av --delete /etc/letsencrypt/archive/* $_:".$conf->{"destinationssldir"});
				system("rsync -av --delete /etc/letsencrypt/archive/* $_:".$conf->{"destinationssldir"});
			}
			
			# copy all of the apache configs to local sites-available
			print "cp -Rf ".$conf->{"pathtosharedvirtualhosts"}."/* ".$conf->{"sitesavailablepath"}."/"."\n";
			$log->addLine("cp -Rf ".$conf->{"pathtosharedvirtualhosts"}."/* ".$conf->{"sitesavailablepath"}."/");
			system("cp -Rf ".$conf->{"pathtosharedvirtualhosts"}."/* ".$conf->{"sitesavailablepath"}."/");
			
			# copy all of the apache configs to remote sites-available
			foreach(@machines)
			{
				print "rsync -av --delete ".$conf->{"pathtosharedvirtualhosts"}."/* $_:".$conf->{"sitesavailablepath"}."\n";
				$log->addLine("rsync -av --delete ".$conf->{"pathtosharedvirtualhosts"}."/* $_:".$conf->{"sitesavailablepath"});
				system("rsync -av --delete ".$conf->{"pathtosharedvirtualhosts"}."/* $_:".$conf->{"sitesavailablepath"});
			}
			
		}
		$log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile'\n";
	}
}


sub sslconf
{
	# Deal with making ssl config changes and get an A rating
	if(!(-e $conf->{"pathtosharedcerts"} . '/ssl_original.conf'))
	{
		$log->addLine("cp " . $conf->{"sslconfpath"} . " " . $conf->{"sslconfpath"} . '/ssl_original.conf');
		system("cp " . $conf->{"sslconfpath"} . " " . $conf->{"pathtosharedcerts"} . '/ssl_original.conf');
	}
	my $sslconf = new Loghandler($conf->{"sslconfpath"});
	my @lines = @{$sslconf->readFile};
	my $output = '';
	
	my %configchanges = 
	(
		"SSLProtocol" => "SSLProtocol all -SSLv2 -SSLv3",
		"SSLCipherSuite" => 'SSLCipherSuite HIGH:!aNULL:!eNULL:!kECDH:!aDH:!RC4:!3DES:!CAMELLIA:!MD5:!PSK:!SRP:!KRB5:@STRENGTH'
	);
	my %foundconfs;
	foreach my $line (@lines)
	{
		if( !($line =~ m/^[\s]*#/) )
		{
			# print "not commented out\n";
			# print "$line\n";
			# Catch the configs
			while ((my $internal, my $value ) = each(%configchanges))
			{	
				if ($line =~ m/^[\s]*$internal/)
				{
					$foundconfs{$internal}=1;
					# Force mandaded config with nothing after
					$line =~ s/^([\s]*)$internal.*/$1$value/;
				}
			}
		}
		if($line =~ m/^[\s]*<\/IfModule>/)
		{
			while ((my $internal, my $value ) = each(%configchanges))
			{
				if( !$foundconfs{$internal})
				{
					$log->addLine("Config $internal not found, adding it");
					$output.=$value."\n";
				}
			}
		}
		$output.=$line;
	}
	$sslconf->truncFile($output);
	my $temp = new Loghandler($conf->{"pathtosharedcerts"} . '/ssl.conf');
	$temp->truncFile($output);
	undef $sslconf;
	undef $temp;
}

sub virtualHostConf
{
	my $confFile = @_[0];
	my %certFiles = %{@_[1]};
	my $confFileName = substr($confFile,rindex($confFile, '/')+1);
	
	my $confr = new Loghandler($confFile);
	my @lines = @{$confr->readFile};
	my $output = '';
	#my %filenames = ("cert" => \@a,"chain" => \@b,"fullchain" => \@c,"privkey" => \@d );
	
	my $sslBlock = 
	"
SSLProtocol all -SSLv3 -SSLv2
SSLHonorCipherOrder On
SSLCipherSuite HIGH:!aNULL:!eNULL:!kECDH:!aDH:!RC4:!3DES:!CAMELLIA:!MD5:!PSK:!SRP:!KRB5:@STRENGTH
SSLCertificateFile ".$certFiles{"cert"}->[1]."
SSLCertificateChainFile ".$certFiles{"fullchain"}->[1]."
SSLCertificateChainFile ".$certFiles{"chain"}->[1]."
SSLCertificateKeyFile ".$certFiles{"privkey"}->[1]."
";
	
	my %configchanges = 
	(
		"SSLProtocol" => $certFiles{"cert"}->[1],
		"SSLHonorCipherOrder" => $certFiles{"cert"}->[1],
		"SSLCipherSuite" => $certFiles{"cert"}->[1],
		"SSLCertificateFile" => $certFiles{"cert"}->[1],
		"SSLCertificateChainFile" => $certFiles{"fullchain"}->[1],
		"SSLCertificateKeyFile" => $certFiles{"privkey"}->[1]
	);
	my %foundconfs;
	$inside443Clause = 0;
	foreach my $line (@lines)
	{
		if( !($line =~ m/^[\s]*#/) )
		{
			if($inside443Clause)
			{
				while ((my $internal, my $value ) = each(%configchanges))
				{
					if ($line =~ m/^[\s]*$internal/)
					{
						# remove the line because we are going to make our own block
						#print "removing $internal\n";
						$line='';
					}
				}
			}
			elsif($line =~ m/^[\s]*<VirtualHost \*:443>/)
			{
				$inside443Clause = 1;
			}
			
			if($line =~ m/^[\s]*<\/VirtualHost>/)
			{
				if($inside443Clause)
				{
					#print "writing\n";
					$output.=$sslBlock;
					$line =~ s/^[\s]*(<\/VirtualHost>)/$1/g;
				}
				$inside443Clause = 0;
			}
		}
		
		$output.=$line if $line ne "\n";
	}
	my $temp = new Loghandler($conf->{"pathtosharedvirtualhosts"} . "/$confFileName");
	$temp->deleteFile();
	$temp->truncFile($output);
}

sub checkoutletsencrypt
{
	# Check to see if we need to clone the repo
	if(!(-e $conf{"letsencryptgitrepopath"}."/letsencrypt-auto"))
	{
		print "git clone https://github.com/letsencrypt/letsencrypt ".$conf{"letsencryptgitrepopath"}."\n";
		$log->addLine("git clone https://github.com/letsencrypt/letsencrypt ".$conf{"letsencryptgitrepopath"});
		system("git clone https://github.com/letsencrypt/letsencrypt ".$conf{"letsencryptgitrepopath"});
	}
	print"cd ".$conf{"letsencryptgitrepopath"}." && git fetch --all && git pull\n";
	$log->addLine("cd ".$conf{"letsencryptgitrepopath"}." && git fetch --all && git pull");
	system("cd ".$conf{"letsencryptgitrepopath"}." && git fetch --all && git pull");
}

sub getcerts
{
	my @files = @{getFiles($conf{"sitesavailablepath"})};
	
	# go ahead and renew if needs be
	print $conf{"letsencryptgitrepopath"}."/letsencrypt-auto renew\n";
	$log->addLine($conf{"letsencryptgitrepopath"}."/letsencrypt-auto renew");
	system($conf{"letsencryptgitrepopath"}."/letsencrypt-auto renew");
	
	# now surf for new certs
	foreach my $file(@files)
	{
		my $fileread = new Loghandler($file);
		my @lines = @{$fileread->readFile};
		undef $fileread;
		my $output = '';
		my $insideSSLClause = 0;
		my $serverName = '';
		foreach my $line (@lines)
		{
			if( !($line =~ m/^[\s]*#/) )
			{
				if($insideSSLClause)
				{
					if($line =~ m/^[\s]*ServerName/)
					{
						$serverName = $line;
						$serverName =~ s/(.*)ServerName([^\:]*).*/$2/g;
						$serverName = $mobUtil->trim($serverName);
						print "'".$serverName."'\n";
						my %certs = %{generateCert($serverName)};
						#my %certs = %{generateCert('devpolk.missourievergreen.org')};
						virtualHostConf($file,\%certs);
					}
				}
				elsif($line =~ m/^[\s]*<VirtualHost \*:443>/)
				{
					$insideSSLClause = 1;
				}
				
				if($line =~ m/^[\s]*<\/VirtualHost>/)
				{
					$insideSSLClause = 0;
				}
			}
			 
			$output.=$line if $line ne "\n";
		}
	}
}

sub generateCert
{
	my $domainName = @_[0];
	$domainName = $conf{"rootdomain"} if $domainName eq 'localhost';
	# see if this domain name already has a cert, if not make one
	if( !(-d "/etc/letsencrypt/archive/$domainName") )
	{
		
		print "/root/.local/share/letsencrypt/bin/letsencrypt certonly --webroot  --webroot-path ".$conf{"pathtosharedwebroot"}." --renew-by-default --email ".$conf{"emailaddress"}." --text --agree-tos -d $domainName"."\n";
		$log->addLine("/root/.local/share/letsencrypt/bin/letsencrypt certonly --webroot  --webroot-path ".$conf{"pathtosharedwebroot"}." --renew-by-default --email ".$conf{"emailaddress"}." --text --agree-tos  -d $domainName");
		system("/root/.local/share/letsencrypt/bin/letsencrypt certonly --webroot  --webroot-path ".$conf{"pathtosharedwebroot"}." --renew-by-default --email ".$conf{"emailaddress"}." --text --agree-tos -d $domainName");
	}
	my %ret;
	print "/etc/letsencrypt/archive/$domainName\n";
	my @certfiles = @{getFiles("/etc/letsencrypt/archive/$domainName")};
	my @a = (0,'');
	my @b = (0,'');
	my @c = (0,'');
	my @d = (0,'');
	my %filenames = ("cert" => \@a,"chain" => \@b,"fullchain" => \@c,"privkey" => \@d );
	
	foreach my $file (@certfiles)
	{
		my $thisFileName = substr($file,rindex($file, '/')+1);
		while ((my $internal, my $value ) = each(%filenames))
		{
			if($thisFileName =~ m/^$internal(.*)\..../ )
			{
				# Grab the file number off the end of the file name
				$thisFileName =~ s/^$internal(.*)\..../$1/g;
				# Compare that to previous versions on the array, bigger numbers win (presumably newer certs)
				if($filenames{$internal}[0] < $thisFileName)
				{
					$filenames{$internal}[0] = $thisFileName;
					my $thisWholeName = substr($file,rindex($file, '/')+1);
					my $path = $file;
					$path = $conf{"destinationssldir"}."/$domainName/$thisWholeName";
					$filenames{$internal}[1] = $path;
				}
			}
		}
	}
	
	# $log->addLine(Dumper(\%filenames));
	return \%filenames;
}

sub getFiles
{
	my $pwd = @_[0];
	my @files = ();
	opendir(DIR,"$pwd") or die "Cannot open $pwd\n";
	my @thisdir = readdir(DIR);
	closedir(DIR);
	foreach my $file (@thisdir) 
	{
		if(($file ne ".") and ($file ne ".."))
		{
			if (-f "$pwd/$file")
			{
				push(@files, "$pwd/$file");
			}
		}
	}
	return \@files;
}
 exit;
