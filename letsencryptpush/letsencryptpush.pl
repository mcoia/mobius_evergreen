#!/usr/bin/perl
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
		my @reqs = ("sslconfpath","sitesenabledpath","letsencryptgitrepopath","machinestoupdate","pathtosharedcerts");
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
			$log->addLine("apt-get install -y git ansible");
			print "apt-get install -y git ansible\n";
			# system("apt-get install -y git ansible");
			
			#sslconf();
			
			# checkoutletsencrypt();
			
			getcerts();
			
			
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
	if(!(-e $conf->{"pathtosharedcerts"} . '/ssl.conf'))
	{
		$log->addLine("cp " . $conf->{"sslconfpath"} . " " . $conf->{"sslconfpath"} . '/ssl.conf');
		system("cp " . $conf->{"sslconfpath"} . " " . $conf->{"pathtosharedcerts"} . '/ssl.conf');
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
	my $temp = new Loghandler($conf->{"pathtosharedcerts"} . "/test.conf");
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
	my @files = @{getFiles($conf{"sitesenabledpath"})};
	
	# go ahead and renew if needs be
	print $conf{"letsencryptgitrepopath"}."/letsencrypt-auto renew\n";
	$log->addLine($conf{"letsencryptgitrepopath"}."/letsencrypt-auto renew");
	system($conf{"letsencryptgitrepopath"}."/letsencrypt-auto renew");
	
	# now surf for new certs
	foreach my $file(@files)
	{
		my $fileread = new Loghandler($file);
		my @lines = @{$fileread->readFile};
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
						#my %certs = %{generateCert($serverName)};
						my %certs = %{generateCert('devpolk.missourievergreen.org')};
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
			$output.=$line;
		}
		my $temp = new Loghandler($conf->{"pathtosharedcerts"} . "/egconf.conf");
		$temp->truncFile($output);
		exit;
	}
}

sub generateCert
{
	my $domainName = @_[0];
	# see if this domain name already has a cert, if not make one
	if( !(-d "/etc/letsencrypt/archive/$domainName") )
	{
		print $conf{"letsencryptgitrepopath"}."/letsencrypt-auto certonly --standalone -d $domainName"."\n";
		$log->addLine($conf{"letsencryptgitrepopath"}."/letsencrypt-auto certonly --standalone -d $domainName");
		system($conf{"letsencryptgitrepopath"}."/letsencrypt-auto certonly --standalone -d $domainName");
	}
	my %ret;
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
					$filenames{$internal}[1] = $file;
				}
			}
		}
	}
	
	$log->addLine(Dumper(\%filenames));
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
