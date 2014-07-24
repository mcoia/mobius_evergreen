#!/usr/bin/perl
use lib qw(../);
use MARC::Record;
use MARC::File;
use MARC::File::XML (BinaryEncoding => 'utf8');
use File::Path qw(make_path remove_tree);
use strict; 
use Loghandler;
use Mobiusutil;
use DBhandler;
use Data::Dumper;
use email;
use DateTime;
use utf8;
use Encode;
use DateTime;
use pQuery;
use LWP::Simple;
use OpenILS::Application::AppUtils;
use DateTime::Format::Duration;
use Digest::SHA1;
use XML::Simple;

 my $configFile = @ARGV[0];
my $xmlconf = "/openils/conf/opensrf.xml";
 

if(@ARGV[1])
{
	$xmlconf = @ARGV[1];
}

if(! -e $xmlconf)
{
	print "I could not find the xml config file: $xmlconf\nYou can specify the path when executing this script\n";
	exit 0;
}
 if(!$configFile)
 {
	print "Please specify a config file\n";
	exit;
 }

 my $mobUtil = new Mobiusutil(); 
 my $conf = $mobUtil->readConfFile($configFile);

 
  our $jobid=-1;
  
 if($conf)
 {
	my %conf = %{$conf};
	if ($conf{"logfile"})
	{
		my $dt = DateTime->now(time_zone => "local"); 
		my $fdate = $dt->ymd; 
		my $ftime = $dt->hms;
		my $dateString = "$fdate $ftime";
		my $log = new Loghandler($conf->{"logfile"});
		$log->truncFile("");
		$log->addLogLine(" ---------------- Script Starting ---------------- ");		
		my @reqs = ("logfile"); 
		my $valid = 1;
		my $errorMessage="";
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
			my $log = new Loghandler($conf{"logfile"});			
			my $dbHandler;
			my %dbconf = %{getDBconnects($xmlconf,$log)};
			$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
			setupSchema($dbHandler);
			print "regular cache\n";
			updateScoreCache($dbHandler,$log);
			$jobid = createNewJob($dbHandler,'processing');
			if($jobid!=-1)
			{
			}
			searchDestroyLeaders($dbHandler,$log);
		}
		
		my $afterProcess = DateTime->now(time_zone => "local");
		my $difference = $afterProcess - $dt;
		my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
		my $duration =  $format->format_duration($difference);
		my $fileList;
		my $successTitleList;
		my $successUpdateTitleList;
		my $failedTitleList;
	
		
		$log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile'\n";		
	}
}

sub searchDestroyLeaders
{
	my $dbHandler = @_[0];
	my $log = @_[1];
	my @bibs = @{findElectronicBibsMarkedAsBooks($dbHandler)};
	foreach(@bibs)
	{
		my @bibatts = @{$_};
		my $id = @bibatts[0];
		my $marc = @bibatts[1];
		if(!isScored($dbHandler, $id))
		{
			my @scorethis = ($id,$marc);
			my @st = ([@scorethis]);
			print "searchleaders cache\n";
			updateScoreCache($dbHandler,$log,\@st);
		}
		$marc =~ s/(<leader>.........)./${1}a/;			
		$marc = MARC::Record->new_from_xml($marc);
		my $electricScore = determineElectric($marc);
		$log->addLine("Electric Score: $electricScore");		
		$log->addLine("http://mig.missourievergreen.org/eg/opac/record/$id?query=yellow;qtype=keyword;locg=157;expand=marchtml#marchtml");
	}
}

sub findElectronicBibsMarkedAsBooks
{
	my $dbHandler = @_[0];	
	#my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE MARC LIKE '%\"9\">PB%' limit 14";
	my $query = "select id,marc from biblio.record_entry where id in (select record from metabib.real_full_rec where tag='856' and ind2='0') AND  marc ~* '<leader>......a' limit 100";
	my @results = @{$dbHandler->query($query)};	
	return \@results;
}

sub isScored
{
	my $dbHandler = @_[0];
	my $bibid = @_[1];
	my $query = "SELECT ID FROM SEEKDESTROY.BIB_SCORE WHERE RECORD = $bibid";
	my @results = @{$dbHandler->query($query)};
	if($#results>-1)
	{
		return 1;
	}
	return 0;
}

sub updateScoreCache
{
	
	my $dbHandler = @_[0];
	my $log = @_[1];
	my @newIDs;
	my @newAndUpdates;
	my @updateIDs;
	if(@_[2])
	{	
		@newIDs=@{@_[2]};
	}
	else
	{
		@newAndUpdates = @{identifyBibsToScore($dbHandler)};
		@newIDs = @{@newAndUpdates[0]};
	}
	print Dumper(@newIDs);
	$log->addLine("Found ".($#newIDs+1)." new Bibs to be scored");	
	if(@newAndUpdates[1])
	{
		@updateIDs = @{@newAndUpdates[1]};
		$log->addLine("Found ".($#updateIDs+1)." new Bibs to update score");	
	}
	foreach(@newIDs)
	{
		my @thisone = @{$_};
		my $bibid = @thisone[0];
		my $marc = @thisone[1];
		my $marcob = $marc;
		$marcob =~ s/(<leader>.........)./${1}a/;			
		$marcob = MARC::Record->new_from_xml($marcob);
		my $score = scoreMARC($marcob,$log);
		my $electricScore = determineElectric($marcob);
		my $query = "INSERT INTO SEEKDESTROY.BIB_SCORE(RECORD,SCORE,ELECTRONIC) VALUES($bibid,$score,$electricScore)";		
		$dbHandler->update($query);	
	}
	foreach(@updateIDs)
	{
		my @thisone = @{$_};
		my $bibid = @thisone[0];
		my $marc = @thisone[1];
		my $bibscoreid = @thisone[2];
		my $oldscore = @thisone[3];
		my $marcob = $marc;
		$marcob =~ s/(<leader>.........)./${1}a/;			
		$marcob = MARC::Record->new_from_xml($marcob);
		my $score = scoreMARC($marcob,$log);
		my $electricScore = determineElectric($marcob);
		my $improved = $score - $oldscore;
		my $query = "UPDATE SEEKDESTROY.BIB_SCORE SET IMPROVED_SCORE_AMOUNT = $improved, SCORE = $score, SCORE_TIME=NOW(), ELECTRONIC=$electricScore WHERE ID=$bibscoreid";			
		$dbHandler->update($query);	
	}
}

sub determineElectric
{
	my $marc = @_[0];
	my @e56s = $marc->field('856');
	if(!@e56s)
	{
		return 0;
	}
	my $textmarc = $marc->as_formatted();
	my $scoreTipToElectronic=3;
	my $score=0;
	my @phrases = ("electronic resource","ebook","eaudiobook","overdrive","download");
	my $has856 = 0;
	my $has245h = getsubfield($marc,'245','h');
	my $found=0;	
	foreach(@e56s)
	{
		my $field = $_;
		my $ind2 = $field->indicator(2);
		if($ind2==0) #only counts if the second indicator is 0 ("Resource") documented here: http://www.loc.gov/marc/bibliographic/bd856.html
		{	
			my @subs = $field->subfield('u');
			foreach(@subs)
			{
				#print "checking $_ for http\n";
				if(m/http/g)
				{
					$found=1;
				}
			}
		}
	}
	if($found)
	{
		$score++;
	}
	foreach(@phrases)
	{
		my $phrase = $_;
		my @c = split($phrase,lc$textmarc);
		if($#c>1) # Found more than 1 match on that phrase
		{
			$score++;
		}
	}
	#print "Electric score: $score\n";
	return $score;
}

sub identifyBibsToScore
{
	my $dbHandler = @_[0];
	my @ret;
#This query finds bibs that have not received a score at all
	my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE ID NOT IN(SELECT RECORD FROM SEEKDESTROY.BIB_SCORE) AND DELETED IS FALSE LIMIT 100";
	my @results = @{$dbHandler->query($query)};
	my @news;
	my @updates;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $id = @row[0];
		my $marc = @row[1];
		my @temp = ($id,$marc);
		push (@news, [@temp]);
	}
#This query finds bibs that have received but the marc has changed since the last score
	$query = "SELECT SBS.RECORD,BRE.MARC,SBS.ID,SCORE FROM SEEKDESTROY.BIB_SCORE SBS,BIBLIO.RECORD_ENTRY BRE WHERE SBS.score_time < BRE.EDIT_DATE AND SBS.RECORD=BRE.ID";
	@results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $rec = @row[0];
		my $marc = @row[1];
		my $id = @row[2];
		my $score = @row[3];
		my @temp = ($rec,$id,$marc,$score);
		push (@updates, [@temp]);
	}
	push(@ret,[@news]);
	push(@ret,[@updates]);
	return \@ret;
}

sub get_marc_extract
{
	my $record=@_[0];
	my $marc = populate_marc($record);	
	return normalize_marc($marc);
}
sub scoreMARC
{
	my $marc = shift;
	my $log = shift;
	
	my $score = 0;
	$score+= score($marc,2,100,400,$log,'245');
	$score+= score($marc,1,1,150,$log,'100');
	$score+= score($marc,1,1.1,150,$log,'110');
	$score+= score($marc,0,50,200,$log,'6..');
	$score+= score($marc,0,50,100,$log,'02.');
	
	$score+= score($marc,0,100,200,$log,'246');
	$score+= score($marc,0,100,100,$log,'130');
	$score+= score($marc,0,100,100,$log,'010');
	$score+= score($marc,0,100,200,$log,'490');
	$score+= score($marc,0,10,50,$log,'830');
	
	$score+= score($marc,1,.5,50,$log,'300');
	$score+= score($marc,0,1,100,$log,'7..');
	$score+= score($marc,2,2,100,$log,'50.');
	$score+= score($marc,2,2,100,$log,'52.');
	
	$score+= score($marc,2,.5,200,$log,'51.', '53.', '54.', '55.', '56.', '57.', '58.');

	return $score;
}

sub score
{
	my ($marc) = shift;
	my ($type) = shift;
	my ($weight) = shift;
	my ($cap) = shift;
	my ($log) = shift;
	my @tags = @_;
	my $ou = Dumper(@tags);
	#$log->addLine("Tags: $ou\n\nType: $type\nWeight: $weight\nCap: $cap");
	my $score = 0;			
	if($type == 0) #0 is field count
	{
		#$log->addLine("Calling count_field");
		$score = count_field($marc,$log,\@tags);
	}
	elsif($type == 1) #1 is length of field
	{
		#$log->addLine("Calling field_length");
		$score = field_length($marc,$log,\@tags);
	}
	elsif($type == 2) #2 is subfield count
	{
		#$log->addLine("Calling count_subfield");
		$score = count_subfield($marc,$log,\@tags);
	}
	$score = $score * $weight;
	if($score > $cap)
	{
		$score = $cap;
	}
	$score = int($score);
	#$log->addLine("Weight and cap applied\nScore is: $score");
	return $score;
}

sub count_subfield
{
	my ($marc) = $_[0];
	my $log = $_[1];
	my @tags = @{$_[2]};
	my $total = 0;
	#$log->addLine("Starting count_subfield");
	foreach my $tag (@tags) 
	{
		my @f = $marc->field($tag);
		foreach my $field (@f)
		{
			my @subs = $field->subfields();
			my $ou = Dumper(@subs);
			#$log->addLine($ou);
			if(@subs)
			{
				$total += scalar(@subs);
			}
		}
	}
	#$log->addLine("Total Subfields: $total");
	return $total;
	
}	

sub count_field 
{
	my ($marc) = $_[0];
	my $log = $_[1];
	my @tags = @{$_[2]};
	my $total = 0;
	foreach my $tag (@tags) 
	{
		my @f = $marc->field($tag);
		$total += scalar(@f);
	}
	return $total;
}

sub field_length 
{
	my ($marc) = $_[0];
	my $log = $_[1];
	my @tags = @{$_[2]};

	my @f = $marc->field(@tags[0]);
	return 0 unless @f;
	my $len = length($f[0]->as_string);
	my $ou = Dumper(@f);
	#$log->addLine($ou);
	#$log->addLine("Field Length: $len");
	return $len;
}

sub calcSHA1
{
	my $marc = @_[0];
	my $sha1 = Digest::SHA1->new;
	$sha1->add(  length(getsubfield($marc,'007',''))>6 ? substr( getsubfield($marc,'007',''),0,6) : '' );
	$sha1->add(getsubfield($marc,'245','h'));
	$sha1->add(getsubfield($marc,'001',''));
	$sha1->add(getsubfield($marc,'245','a'));
	return $sha1->hexdigest;
}

sub getsubfield
{
	my $marc = @_[0];
	my $tag = @_[1];
	my $subtag = @_[2];
	my $ret;
	#print "Extracting $tag $subtag\n";
	if($marc->field($tag))
	{
		if($tag<10)
		{	
			#print "It was less than 10 so getting data\n";
			$ret = $marc->field($tag)->data();
		}
		elsif($marc->field($tag)->subfield($subtag))
		{
			$ret = $marc->field($tag)->subfield($subtag);
		}
	}
	#print "got $ret\n";
	return $ret;
	
}


sub findRecord
{
	my $marcsearch = @_[0];
	my $zero01 = $marcsearch->field('001')->data();
	my $dbHandler = @_[1];
	my $sha1 = @_[2];
	my $bibsourceid = @_[3];
	my $log = @_[4];
	my $query = "SELECT bre.ID,bre.MARC FROM BIBLIO.RECORD_ENTRY bre WHERE bre.tcn_source ~ \$\$$sha1\$\$ and bre.source=$bibsourceid and bre.deleted is false";
updateJob($dbHandler,"Processing","$query");
	my @results = @{$dbHandler->query($query)};
	my @ret;
	my $none=1;
	my $foundIDs;
	my $count=0;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $id = @row[0];
		my $marc = @row[1];
		print "found matching sha1: $id\n";		
		my $prevmarc = $marc;
		$prevmarc =~ s/(<leader>.........)./${1}a/;	
		$prevmarc = MARC::Record->new_from_xml($prevmarc);
		my $score = scoreMARC($prevmarc,$log);
		my @matchedsha = ($id,$prevmarc,$score,$marc);
		$foundIDs.="$id,";
		push (@ret, [@matchedsha]);
		$none=0;
		$count++;
	}
	$foundIDs = substr($foundIDs,0,-1);
	if(length($foundIDs)<1)
	{
		$foundIDs="-1";
	}
	my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE MARC ~ \$\$$zero01\$\$ and ID not in($foundIDs) and deleted is false ";
updateJob($dbHandler,"Processing","$query");
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $id = @row[0];
		print "found matching 001: $id\n";
		my $marc = @row[1];
		my $prevmarc = $marc;
		$prevmarc =~ s/(<leader>.........)./${1}a/;	
		$prevmarc = MARC::Record->new_from_xml($prevmarc);
		my $score = scoreMARC($prevmarc,$log);
		my @matched001 = ($id,$prevmarc,$score,$marc);
		push (@ret, [@matched001]);	
		$none=0;
		$count++;
	}	
	if($none)
	{
		return -1;
	}
	print "Count matches: $count\n";
updateJob($dbHandler,"Processing","Count matches: $count");
	return \@ret;
	
}

sub readyMARCForInsertIntoME
{
	my $marc = @_[0];	
	my $lbyte6 = substr($marc->leader(),6,1);
	
	my $two45 = $marc->field('245');
	my @e856s = $marc->field('856');
	
	if($two45)
	{
		my $value = "item";
		# if($lbyte6 eq 'm' || $lbyte6 eq 'i')
		# {	
			$value = "eBook";
			if($lbyte6 eq 'i')
			{
				$value = "eAudioBook";
			}
			if($two45->subfield('h'))
			{
				$two45->update( 'h' => "[Overdrive downloadable $value] /" );
			}
			else
			{			
				$two45->add_subfields('h' => "[Overdrive downloadable $value] /");
			}
		# }
		if(@e856s)
		{
			foreach(@e856s)
			{
				my $thisfield = $_;
				my $ind2 = $thisfield->indicator(2);
				if($ind2 eq '0') #only counts if the second indicator is 0 ("Resource") documented here: http://www.loc.gov/marc/bibliographic/bd856.html
				{	
					my @sub3 = $thisfield->subfield( '3' );
					my $ignore=0;
					foreach(@sub3)
					{
						if(lc($_) eq 'excerpt')
						{
							$ignore=1;
						}
						if(lc($_) eq 'image')
						{
							$ignore=1;
						}
					}
					if(!$ignore)
					{
						$thisfield->delete_subfield(code => 'z');					
						$thisfield->add_subfields('z'=> "Click for access to the downloadable $value via Overdrive");
					}
				}
			}
		}			
	}
	return $marc;
}

sub mergeMARC856
{
	my $marc = @_[0];
	my $marc2 = @_[1];
	my $log = @_[2];
	my @eight56s = $marc->field("856");
	my @eight56s_2 = $marc2->field("856");
	my @eights;
	my $original856 = $#eight56s + 1;
	@eight56s = (@eight56s,@eight56s_2);

	my %urls;  
	foreach(@eight56s)
	{
		my $thisField = $_;
		my $ind2 = $thisField->indicator(2);
		# Just read the first $u and $z
		my $u = $thisField->subfield("u");
		my $z = $thisField->subfield("z");
		my $s7 = $thisField->subfield("7");
		
		if($u) #needs to be defined because its the key
		{
			if(!$urls{$u})
			{
				if($ind2 ne '0')
				{
					$thisField->delete_subfields('9');
					$thisField->delete_subfields('z');
				}
				$urls{$u} = $thisField;
			}
			else
			{
				my @nines = $thisField->subfield("9");
				my $otherField = $urls{$u};
				my @otherNines = $otherField->subfield("9");
				my $otherZ = $otherField->subfield("z");		
				my $other7 = $otherField->subfield("7");
				if(!$otherZ)
				{
					if($z)
					{
						$otherField->add_subfields('z'=>$z);
					}
				}
				if(!$other7)
				{
					if($s7)
					{
						$otherField->add_subfields('7'=>$s7);
					}
				}
				foreach(@nines)
				{
					my $looking = $_;
					my $found = 0;
					foreach(@otherNines)
					{
						if($looking eq $_)
						{
							$found=1;
						}
					}					
					if($found==0 && $ind2 eq '0')
					{
						$otherField->add_subfields('9' => $looking);
					}
				}
				if($ind2 ne '0')
				{
					$thisField->delete_subfields('9');
					$thisField->delete_subfields('z');
				}
				
				$urls{$u} = $otherField;
			}
		}
		
	}
	
	my $finalCount = scalar keys %urls;
	if($original856 != $finalCount)
	{
		$log->addLine("There was $original856 and now there are $finalCount");
	}
	
	my $dump1=Dumper(\%urls);
	my @remove = $marc->field('856');
	#$log->addLine("Removing ".$#remove." 856 records");
	$marc->delete_fields(@remove);


	while ((my $internal, my $mvalue ) = each(%urls))
		{	
			$marc->insert_grouped_field( $mvalue );
		}
	return $marc;
}

sub getEvergreenMax
{
	my $dbHandler = @_[0];
	
	my $query = "SELECT MAX(ID) FROM BIBLIO.RECORD_ENTRY";
	return 1000;
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

sub getTCN
{
	my $log = @_[0];
	my $dbHandler = @_[1];
	my $dbmax=getEvergreenMax($dbHandler);
	$dbmax++;
	my $result = 1;
	my $seed=0;
	my $ap="";
	my $trys = 0;
	while($result==1)
	{
		my $query = "SELECT COUNT(*) FROM BIBLIO.RECORD_ENTRY WHERE TCN_VALUE = 'od$dbmax$ap'";
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			if(@row[0]==0)
			{
				$result=0;
			}
		}
		$ap = "_".$seed;
		$seed++;
		$trys++;
	}
	if($trys>1)
	{
		$log->addLogLine("Needed to change tcn $trys times to find: 'od$dbmax$ap'");
	}
	return "od$dbmax$ap";
}

sub convertMARCtoXML
{
	my $marc = @_[0];
	my $thisXML = $marc->as_xml();			
	#this code is borrowed from marc2bre.pl
	$thisXML =~ s/\n//sog;
	$thisXML =~ s/^<\?xml.+\?\s*>//go;
	$thisXML =~ s/>\s+</></go;
	$thisXML =~ s/\p{Cc}//go;
	$thisXML = OpenILS::Application::AppUtils->entityize($thisXML);
	$thisXML =~ s/[\x00-\x1f]//go;
	#end code
	return $thisXML;
}

sub getbibsource
{
	my $dbHandler = @_[0];
	my $query = "SELECT ID FROM CONFIG.BIB_SOURCE WHERE SOURCE = 'molib2go'";
	my @results = @{$dbHandler->query($query)};
	if($#results==-1)
	{
		print "Didnt find seekdestroy in bib_source, now creating it...\n";
		$query = "INSERT INTO CONFIG.BIB_SOURCE(QUALITY,SOURCE) VALUES(90,'molib2go')";
		my $res = $dbHandler->update($query);
		print "Update results: $res\n";
		$query = "SELECT ID FROM CONFIG.BIB_SOURCE WHERE SOURCE = 'molib2go'";
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			return @row[0];
		}
	}
	else
	{
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			return @row[0];
		}
	}
}

sub createNewJob
{
	my $dbHandler = @_[0];
	my $status = @_[1];
	my $query = "INSERT INTO seekdestroy.job(status) values('$status')";
	my $results = $dbHandler->update($query);
	if($results)
	{
		$query = "SELECT max( ID ) FROM seekdestroy.job";
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			$jobid = @row[0];
			return @row[0];
		}
	}
	return -1;
}

sub updateJob
{
	my $dbHandler = @_[0];
	my $status = @_[1];
	my $action = @_[2];
	my $query = "UPDATE seekdestroy.job SET last_update_time=now(),status='$status', CURRENT_ACTION_NUM = CURRENT_ACTION_NUM+1,current_action='$action' where id=$jobid";
	my $results = $dbHandler->update($query);
	return $results;
}

sub findPBrecordInME
{
	my $dbHandler = @_[0];	
	#my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE MARC LIKE '%\"9\">PB%' limit 14";
	my $query = "select id,marc from biblio.record_entry where marc ~* '<leader>......a' AND lower(marc) like '%overdrive%' AND lower(marc) like '%ebook%'";
	my @results = @{$dbHandler->query($query)};
	my @each;
	my @ret;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $id = @row[0];
		my $marc = @row[1];
		@each = ($id,$marc);
		push(@ret,[@each]);	
	}
	return \@ret;
}

#This is borrowed from fingerprinter
sub populate_marc {
    my $record = @_[0];
    my %marc = (); $marc{isbns} = [];

    # record_type, bib_lvl
    $marc{record_type} = substr($record->leader, 6, 1);
    $marc{bib_lvl}     = substr($record->leader, 7, 1);

    # date1, date2
    my $my_008 = $record->field('008');
    $marc{tag008} = $my_008->as_string() if ($my_008);
    if (defined $marc{tag008}) {
        unless (length $marc{tag008} == 40) {
            $marc{tag008} = $marc{tag008} . ('|' x (40 - length($marc{tag008})));
            print XF ">> Short 008 padded to ",length($marc{tag008})," at rec $count\n";
        }
        $marc{date1} = substr($marc{tag008},7,4) if ($marc{tag008});
        $marc{date2} = substr($marc{tag008},11,4) if ($marc{tag008}); # UNUSED
    }
    unless ($marc{date1} and $marc{date1} =~ /\d{4}/) {
        my $my_260 = $record->field('260');
        if ($my_260 and $my_260->subfield('c')) {
            my $date1 = $my_260->subfield('c');
            $date1 =~ s/\D//g;
            if (defined $date1 and $date1 =~ /\d{4}/) {
                $marc{date1} = $date1;
                $marc{fudgedate} = 1;
                print XF ">> using 260c as date1 at rec $count\n";
            }
        }
    }

    # item_form
    if ( $marc{record_type} =~ /[gkroef]/ ) { # MAP, VIS
        $marc{item_form} = substr($marc{tag008},29,1) if ($marc{tag008});
    } else {
        $marc{item_form} = substr($marc{tag008},23,1) if ($marc{tag008});
    }

    # isbns
    my @isbns = $record->field('020') if $record->field('020');
    push @isbns, $record->field('024') if $record->field('024');
    for my $f ( @isbns ) {
        push @{ $marc{isbns} }, $1 if ( defined $f->subfield('a') and
                                        $f->subfield('a')=~/(\S+)/ );
    }

    # author
    for my $rec_field (100, 110, 111) {
        if ($record->field($rec_field)) {
            $marc{author} = $record->field($rec_field)->subfield('a');
            last;
        }
    }

    # oclc
    $marc{oclc} = [];
    push @{ $marc{oclc} }, $record->field('001')->as_string()
      if ($record->field('001') and $record->field('003') and
          $record->field('003')->as_string() =~ /OCo{0,1}LC/);
    for ($record->field('035')) {
        my $oclc = $_->subfield('a');
        push @{ $marc{oclc} }, $oclc
          if (defined $oclc and $oclc =~ /\(OCoLC\)/ and $oclc =~/([0-9]+)/);
    }

    if ($record->field('999')) {
        my $koha_bib_id = $record->field('999')->subfield('c');
        $marc{koha_bib_id} = $koha_bib_id if defined $koha_bib_id and $koha_bib_id =~ /^\d+$/;
    }

    # "Accompanying material" and check for "copy" (300)
    if ($record->field('300')) {
        $marc{accomp} = $record->field('300')->subfield('e');
        $marc{tag300a} = $record->field('300')->subfield('a');
    }

    # issn, lccn, title, desc, pages, pub, pubyear, edition
    $marc{lccn} = $record->field('010')->subfield('a') if $record->field('010');
    $marc{issn} = $record->field('022')->subfield('a') if $record->field('022');
    $marc{desc} = $record->field('300')->subfield('a') if $record->field('300');
    $marc{pages} = $1 if (defined $marc{desc} and $marc{desc} =~ /(\d+)/);
    $marc{title} = $record->field('245')->subfield('a')
      if $record->field('245');
   
    $marc{edition} = $record->field('250')->subfield('a')
      if $record->field('250');
    if ($record->field('260')) {
        $marc{publisher} = $record->field('260')->subfield('b');
        $marc{pubyear} = $record->field('260')->subfield('c');
        $marc{pubyear} =
          (defined $marc{pubyear} and $marc{pubyear} =~ /(\d{4})/) ? $1 : '';
    }
    return \%marc;
}

sub normalize_marc {
    my ($marc) = @_;

    $marc->{record_type }= 'a' if ($marc->{record_type} eq ' ');
    if ($marc->{title}) {
        $marc->{title} = NFD($marc->{title});
        $marc->{title} =~ s/[\x{80}-\x{ffff}]//go;
        $marc->{title} = lc($marc->{title});
        $marc->{title} =~ s/\W+$//go;
    }
    if ($marc->{author}) {
        $marc->{author} = NFD($marc->{author});
        $marc->{author} =~ s/[\x{80}-\x{ffff}]//go;
        $marc->{author} = lc($marc->{author});
        $marc->{author} =~ s/\W+$//go;
        if ($marc->{author} =~ /^(\w+)/) {
            $marc->{author} = $1;
        }
    }
    if ($marc->{publisher}) {
        $marc->{publisher} = NFD($marc->{publisher});
        $marc->{publisher} =~ s/[\x{80}-\x{ffff}]//go;
        $marc->{publisher} = lc($marc->{publisher});
        $marc->{publisher} =~ s/\W+$//go;
        if ($marc->{publisher} =~ /^(\w+)/) {
            $marc->{publisher} = $1;
        }
    }
    return $marc;
}

sub marc_isvalid {
    my ($marc) = @_;
    return 1 if ($marc->{item_form} and ($marc->{date1} =~ /\d{4}/) and
                 $marc->{record_type} and $marc->{bib_lvl} and $marc->{title});
    return 0;
}


sub get_fingerprints {
    my ($marc) = @_;
	my %fingerprints;
    $fingerprints->{baseline} = join("\t", 
	  $marc->{item_form}, $marc->{date1}, $marc->{record_type},
	  $marc->{bib_lvl}, $marc->{title}, $marc->{author} ? $marc->{author} : '');
}


sub setupSchema
{
	my $dbHandler = @_[0];
	my $query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'seekdestroy'";
	my @results = @{$dbHandler->query($query)};
	if($#results==-1)
	{
		$query = "CREATE SCHEMA seekdestroy";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.job
		(
		id bigserial NOT NULL,
		start_time timestamp with time zone NOT NULL DEFAULT now(),
		last_update_time timestamp with time zone NOT NULL DEFAULT now(),
		status text default 'processing',	
		current_action text,
		current_action_num bigint default 0,
		CONSTRAINT job_pkey PRIMARY KEY (id)
		  )";		  
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.bib_score(
		id serial,
		record bigint,
		score bigint,
		improved_score_amount bigint default 0,
		score_time timestamp default now(), 		
		electronic bigint)";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.item_reassignment(
		id serial,
		copy bigint,
		prev_bib bigint,
		target_bib bigint,
		change_time timestamp default now(), 
		electronic boolean default false),
		job  bigint NOT NULL,
		CONSTRAINT item_reassignment_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.bib_marc_update(
		id serial,
		record bigint,
		prev_marc text,
		changed_marc text,
		change_time timestamp default now())";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.bib_merge(
		id serial,
		leadbib bigint,
		subbib bigint,
		change_time timestamp default now(),
		job  bigint NOT NULL,
		CONSTRAINT bib_merge_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
	}
}

sub updateJob
{
	my $dbHandler = @_[0];
	my $status = @_[1];
	my $action = @_[2];
	my $query = "UPDATE seekdestroy.job SET last_update_time=now(),status='$status', CURRENT_ACTION_NUM = CURRENT_ACTION_NUM+1,current_action='$action' where id=$jobid";
	my $results = $dbHandler->update($query);
	return $results;
}

sub getDBconnects
{
	my $openilsfile = @_[0];
	my $log = @_[1];
	my $xml = new XML::Simple;
	my $data = $xml->XMLin($openilsfile);
	my %conf;
	$conf{"dbhost"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{host};
	$conf{"db"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{db};
	$conf{"dbuser"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{user};
	$conf{"dbpass"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{pw};
	$conf{"port"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{port};
	#print Dumper(\%conf);
	return \%conf;

}

 exit;

 
 