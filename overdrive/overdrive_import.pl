#!/usr/bin/perl
use lib qw(../);
use MARC::Record;
use MARC::File;
use MARC::File::XML (BinaryEncoding => 'utf8');
#use MARC::XML;
use File::Path qw(make_path remove_tree);
use strict; 
use Loghandler;
use Mobiusutil;
use DBhandler;
use recordItem;
use Data::Dumper;
use email;
use DateTime;
use utf8;
use Encode;
use DateTime;
use pQuery;
use LWP::Simple;

 my $configFile = @ARGV[0];
 if(!$configFile)
 {
	print "Please specify a config file\n";
	exit;
 }

 my $mobUtil = new Mobiusutil(); 
 my $conf = $mobUtil->readConfFile($configFile);
 
 if($conf)
 {
	my %conf = %{$conf};
	if ($conf{"logfile"})
	{
	
		my $log = new Loghandler($conf->{"logfile"});
		$log->deleteFile();
		$log->addLogLine(" ---------------- Script Starting ---------------- ");
		my @reqs = ("server","login","password","tempspace","archivefolder","dbhost","db","dbuser","dbpass","port","participants","logfile","yearstoscrape","toomanyfilesthreshold"); 
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
		my $archivefolder = $conf{"archivefolder"};
		if(!(-d $archivefolder))
		{
			$valid = 0;
			print "Sorry, the archive folder does not exist: $archivefolder\n";
		}
		
		if($valid)
		{	
			my $log = new Loghandler($conf{"logfile"});
			my @marcOutputRecords;
			my @shortnames = split(/,/,$conf{"participants"});
			for my $y(0.. $#shortnames)
			{				
				@shortnames[$y]=$mobUtil->trim(@shortnames[$y]);
			}
			my @files = @{getmarc($conf{"server"},$conf{"login"},$conf{"password"},$conf{"yearstoscrape"},$archivefolder,$log)};
			if(@files[$#files]!=-1)
			{
				for my $b(0..$#files)
				{
					$log->addLogLine("Parsing: ".$files[$b]);
					my $file = MARC::File::USMARC->in($files[$b]);
					while ( my $marc = $file->next() ) 
					{	
						$marc = add9($marc,\@shortnames);
						push(@marcOutputRecords,$marc);
					}
				}
				my $outputFile = $mobUtil->chooseNewFileName($conf{"tempspace"},"temp","mrc");
				my $marcout = new Loghandler($outputFile);
				$marcout->deleteFile();
				my $output;
				my $count=0;
				foreach(@marcOutputRecords)
				{
					my $marc = $_;
					$output.=$marc->as_usmarc();
					$count++;
				}
				$log->addLogLine("Outputting $count record(s) into $outputFile");
				$marcout->addLine($output);
				my $dbHandler;
				eval{$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});};
				if ($@) 
				{
					$log->addLogLine("Could not establish a connection to the database");
					$log->addLogLine("Deleting $outputFile");
					$marcout->deleteFile();
					foreach(@files)
					{
						my $t = new Loghandler($_);
						$log->addLogLine("Deleting $_");
						$t->deleteFile();
					}
					$valid = 0;
				}
				if($valid)
				{					
					#my @info = importMARCintoEvergreen($outputFile,$count,$log,$dbHandler);
				}
			}
			else
			{
				$log->addLogLine("There were some errors during the getmarc function, we are stopping execution. Any partially downloaded files are deleted.");
				foreach(@files)
				{
					$log->addLogLine($_);
				}
			}
		}
		
		$log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile'\n";
		
	}
}

sub getmarc
{
	my $server = @_[0];
	$server=~ s/http:\/\///gi;
	
	my $login = @_[1];
	my $password = @_[2];	
	my $yearstoscrape = @_[3];
	my $archivefolder = @_[4];
	print "Server: $server\nLogin: $login\password: $password\archivefolder: $archivefolder\n";
	my $log = @_[5];
	my @months = ("01-Jan","02-Feb","03-Mar","04-Apr","05-May","06-Jun","07-Jul","08-Aug","09-Sep","10-Oct","11-Nov","12-Dec");

	my $dt = DateTime->now(time_zone => "local");
	my $curyear = $dt->year();
	my @years = ();
	while($yearstoscrape>0)
	{
		push(@years,$curyear);
		$curyear--;
		$yearstoscrape--;
	}
	my @scrapedFileLinks;
	my @downloadedFiles;
	my @errors;
	for my $yearpos(0..$#years)
	{
		my $thisYear = @years[$yearpos];
		for my $monthpos(0..$#months)
		{
			my $thisMonth = @months[$monthpos];
			my $URL = "http://$login:$password\@$server/Overdrive/$thisYear/$thisMonth/";
			$log->addLine("Attempting to read $URL");
			
			pQuery($URL)
						->find("a")->each(sub {
									my $link = pQuery($_)->toHtml;
									my $output = "parsing: $link\n";
									my @s= split(/href=\"/,$link);
									@s = split(/\"/,@s[1]);
									$link = @s[0];
									if((index(lc($link),'.dat')>-1) || (index(lc($link),'.mrc')>-1))
									{
										## Check local archive to see if we already downloaded it
										
										my $localFile = $archivefolder."/$thisYear/$thisMonth/$link";
										if(!(-e $localFile))
										{
											if(!(-d $archivefolder."/$thisYear/$thisMonth"))
											{
												make_path($archivefolder."/$thisYear/$thisMonth", {
													verbose => 1,
													mode => 0777,
													});
											}
											$log->addLine("$output Got this: $link");
											$log->addLogLine("New: $URL$link");
											my $url = 'http://marinetraffic2.aegean.gr/ais/getkml.aspx';
											my $getsuccess = getstore($URL.$link, $localFile);
											if($getsuccess eq "200")
											{
												print "success: $getsuccess\n";
												push(@scrapedFileLinks,$URL.$link);
												push(@downloadedFiles,$localFile);
											}
											else
											{
												$log->addLogLine("COULD NOT TRANSFER $URL$link");
												$log->addLogLine("ABORTING SOON");
												push(@errors,"Unable to download: $URL$link");
											}
										}
									}
									
								}
								)
		}
	}
	if($#errors > -1)
	{
		foreach(@downloadedFiles)
		{
			my $t = new Loghandler($_);
			$t->deleteFile();
		}
		push(@errors,"-1");
		return \@errors;
	}
	$log->addLine(Dumper(\@scrapedFileLinks));
	return \@downloadedFiles;

}

sub add9
{
	my $marc = @_[0];
	my @shortnames = @{@_[1]};
	my @recID = $marc->field('856');
	if(defined @recID)
	{
		#$marc->delete_fields( @recID );
		for my $rec(0..$#recID)
		{
			#print Dumper(@recID[$rec]);
			for my $t(0.. $#shortnames)
			{
				my @subfields = @recID[$rec]->subfield( '9' );
				my $shortnameexists=0;
				for my $subs(0..$#subfields)
				{
				#print "Comparing ".@subfields[$subs]. " to ".@shortnames[$t]."\n";
					if(@subfields[$subs] eq @shortnames[$t])
					{
						print "Same!\n";
						$shortnameexists=1;
					}
				}
				#print "shortname exists: $shortnameexists\n";
				if(!$shortnameexists)
				{
					#print "adding ".@shortnames[$t]."\n";
					@recID[$rec]->add_subfields('9'=>@shortnames[$t]);
				}
			}
		}
	}
	return $marc;
}

sub importMARCintoEvergreen
{
	my $inputFile = @_[0];
	my $recordCount = @_[1];
	my $log = @_[2];
	my $dbHandler = @_[3];
	my $allmarc = MARC::XML->new($inputFile,"usmarc");
	my $dbmax=getEvergreenMax($dbHandler);
	my $query;
	
	for my $i (1..$recordCount)
	{
		if($i<2)
		{
			my $thisXML = $allmarc->output({format=>"xml",records=>[$i]});
			$thisXML=~s/'/\\'/g;
			$query = "INSERT INTO BIBLIO.RECORD_ENTRY(fingerprint,last_xact_id,marc,quality,source,tcn_source,tcn_value,owner,share_depth) VALUES(\\N,'IMPORT-1382129068.90847','$thisXML',\\N,\\N,Unknown,0000000001,\\N,\\N)";
			$log->addLine($query);
			#my $res = $dbHandler->update($query);
			#print "$res";
		}
		
	}
	
}

sub getEvergreenMax
{
	my $dbHandler = @_[0];
	
	my $query = "SELECT MAX(ID) FROM BIBLIO.RECORD_ENTRY";
	my @results = @{$dbHandler->query($query)};
	my $dbmax=0;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		$dbmax = @row[0];
	}
	print "DB Max: $dbmax\n";
	return $dbmax;
}
 exit;

 
 