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
use LWP::Simple;
use OpenILS::Application::AppUtils;
use DateTime::Format::Duration;
use Digest::SHA1;
use XML::Simple;
use Unicode::Normalize;


#
#
#
# AUDIOBOOK has "i" in the leader:
# 01317nim a22003257  4500
# Books have "a" : 
# 01208cam a2200361 a 4500
#
#
#
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
			$jobid = createNewJob($dbHandler,'processing');
			if($jobid!=-1)
			{
				my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE id in(1320891,1044364)";
				updateScoreWithQuery($query,$dbHandler,$log);
				findPhysicalItemsOnElectronic($mobUtil,$dbHandler,$log);
				#findPossibleDups($mobUtil,$dbHandler,$log);
				#print "regular cache\n";
				#updateScoreCache($dbHandler,$log);
			}
			#searchDestroyLeaders($dbHandler,$log);
			updateJob($dbHandler,"Completed","");
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
	##print Dumper(@newIDs);
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
		#print "bibid = $bibid";
		#print "marc = $marc";
		my $query = "DELETE FROM SEEKDESTROY.BIB_SCORE WHERE RECORD = $bibid";
		$dbHandler->update($query);
		my $marcob = $marc;
		$marcob =~ s/(<leader>.........)./${1}a/;
		$marcob = MARC::Record->new_from_xml($marcob);
		my $score = scoreMARC($marcob,$log);
		my $electricScore = determineElectric($marcob);
		my %fingerprints = %{getFingerprints($marcob)};
		#$log->addLine(Dumper(%fingerprints));
		my $query = "INSERT INTO SEEKDESTROY.BIB_SCORE
		(RECORD,
		SCORE,
		ELECTRONIC,
		item_form,
		date1,
		record_type,
		bib_lvl,
		title,
		author,
		sd_fingerprint,
		eg_fingerprint) 
		VALUES($bibid,$score,$electricScore,
		\$1,\$2,\$3,\$4,\$5,\$6,\$7,(SELECT FINGERPRINT FROM BIBLIO.RECORD_ENTRY WHERE ID=$bibid)
		)";		
		my @values = (
		$fingerprints{item_form},
		$fingerprints{date1},
		$fingerprints{record_type},
		$fingerprints{bib_lvl},
		$fingerprints{title},
		$fingerprints{author},
		$fingerprints{baseline}
		);
		$dbHandler->updateWithParameters($query,\@values);
		updateBibCircs($bibid,$dbHandler);	
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
		my %fingerprints = %{getFingerprints($marcob)};		
		my $improved = $score - $oldscore;
		my $query = "UPDATE SEEKDESTROY.BIB_SCORE SET IMPROVED_SCORE_AMOUNT = $improved, SCORE = $score, SCORE_TIME=NOW(), ELECTRONIC=$electricScore ,
		item_form = \$1,
		date1 = \$2,
		record_type = \$3,
		bib_lvl = \$4,
		title = \$5,
		author = \$6,
		sd_fingerprint = \$7,
		eg_fingerprint = (SELECT FINGERPRINT FROM BIBLIO.RECORD_ENTRY WHERE ID=$bibid)
		WHERE ID=$bibscoreid";
		my @values = (
		$fingerprints{item_form},
		$fingerprints{date1},
		$fingerprints{record_type},
		$fingerprints{bib_lvl},
		$fingerprints{title},
		$fingerprints{author},
		$fingerprints{baseline},
		
		);
		$dbHandler->updateWithParameters($query,\@values);
		updateBibCircs($bibid,$dbHandler);
	}
}

sub updateBibCircs
{	
	my $bibid = @_[0];
	my $dbHandler = @_[1];
	my $query = "DELETE FROM seekdestroy.bib_item_circ_mods WHERE RECORD=$bibid";
	$dbHandler->update($query);
	
	
	$query = "CREATE TABLE seekdestroy.bib_item_circ_mods(
		id serial,
		record bigint,
		circ_modifier text,
		job  bigint NOT NULL,";
	$query = "
	select ac.circ_modifier,acn.record  from asset.copy ac,asset.call_number acn,biblio.record_entry bre where
	acn.id=ac.call_number and
	bre.id=acn.record and
	acn.record = $bibid and
	not acn.deleted and
	not bre.deleted and
	not ac.deleted
	group by ac.circ_modifier,acn.record
	order by record";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $circmod = @row[0];
		my $record = @row[1];
		my $q="INSERT INTO seekdestroy.bib_item_circ_mods(record,circ_modifier,different_circs,job)
		values
		(\$1,\$2,\$3,\$4)";
		my @values = ($record,$circmod,$#results+1,$jobid);
		$dbHandler->updateWithParameters($q,\@values);
	}
}

sub findPhysicalItemsOnElectronic
{
	my $mobUtil = @_[0];
	my $dbHandler = @_[1];
	my $log = @_[2];
	# Find Electronic bibs with physical items but and in the dedupe project
	my $query = "
	select id,marc from biblio.record_entry where not deleted and lower(marc) ~ \$\$<datafield tag=\"856\" ind1=\"4\" ind2=\"0\">\$\$
	and id in
	(
	select record from asset.call_number where not deleted and id in(select call_number from asset.copy where not deleted)
	)
	and id in
	(select lead_bibid from m_dedupe.merge_map)
	and 
	marc ~ \$\$tag=\"008\">.......................s\$\$
	limit 1000
	";
	updateJob($dbHandler,"Processing","findPhysicalItemsOnElectronic  $query");
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		my $marc = @row[1];
		my @scorethis = ($bibid,$marc);
		my @st = ([@scorethis]);
		updateScoreCache($dbHandler,$log,\@st);
		recordAssetCopyMove($bibid,$dbHandler,$log);
	}
	
	# Find Electronic bibs with physical items but not in the dedupe project
	$query = "
	select id,marc from biblio.record_entry where not deleted and lower(marc) ~ \$\$<datafield tag=\"856\" ind1=\"4\" ind2=\"0\">\$\$
	and id in
	(
	select record from asset.call_number where not deleted and id in(select call_number from asset.copy where not deleted)
	)
	and id not in
	(select lead_bibid from m_dedupe.merge_map)
	and 
	marc ~ \$\$tag=\"008\">.......................s\$\$
	limit 1000
	";
	updateJob($dbHandler,"Processing","findPhysicalItemsOnElectronic  $query");
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		my $marc = @row[1];
		my @scorethis = ($bibid,$marc);
		my @st = ([@scorethis]);
		updateScoreCache($dbHandler,$log,\@st);
		## Now find likely candidates elsewhere in the ME DB		
		$query =
		"SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE LOWER(MARC) ~ (SELECT LOWER(TITLE) FROM SEEKDESTROY.BIB_SCORE WHERE RECORD=$bibid)";
		$log->addLine($query);
		updateScoreWithQuery($query,$dbHandler,$log);
		$query =
		"SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE FINGERPRINT = (SELECT EG_FINGERPRINT FROM SEEKDESTROY.BIB_SCORE WHERE RECORD=$bibid)";
		$log->addLine($query);
		updateScoreWithQuery($query,$dbHandler,$log);

		$query = "SELECT ID,RECORD,SCORE,ELECTRONIC FROM  seekdestroy.bib_score
		WHERE 
		DATE1 = (SELECT DATE1 FROM seekdestroy.bib_score WHERE RECORD = $bibid) 
		AND RECORD_TYPE = (SELECT RECORD_TYPE FROM seekdestroy.bib_score WHERE RECORD = $bibid) 
		AND BIB_LVL = (SELECT BIB_LVL FROM seekdestroy.bib_score WHERE RECORD = $bibid) 
		AND TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = $bibid) 
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = $bibid)
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = $bibid) OR ELECTRONIC < 1)
		AND RECORD != $bibid";
		updateJob($dbHandler,"Processing","findPhysicalItemsOnElectronic  $query");
		$log->addLine($query);
		## Exact matches!
		
		my @result = @{$dbHandler->query($query)};
		for my $i (0..$#result)
		{
			my @ro = @{@result[$i]};
			my $mbibid=@ro[1];
			my $holds = findHoldsOnBib($mbibid,$dbHandler);
			$query = "INSERT INTO SEEKDESTROY.BIB_MATCH(BIB1,BIB2,MATCH_REASON,HAS_HOLDS,JOB)
			VALUES(\$1,\$2,\$3,\$4,\$5)";
			my @values = ($bibid,$mbibid,"Physical Items to Physical Bib exact",$holds,$jobid);
			$dbHandler->updateWithParameters($query,\@values);
		}
		if($#result==-1)
		{
			## Loosen the matching down to just author and title and record type
			$query = "SELECT ID,RECORD,SCORE,ELECTRONIC FROM  seekdestroy.bib_score
			WHERE TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = $bibid)
			AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = $bibid)			
			AND RECORD_TYPE = (SELECT RECORD_TYPE FROM seekdestroy.bib_score WHERE RECORD = $bibid) 
			AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = $bibid) OR ELECTRONIC < 1)
			AND RECORD != $bibid";
			updateJob($dbHandler,"Processing","findPhysicalItemsOnElectronic  $query");
			$log->addLine($query);
			my @result = @{$dbHandler->query($query)};
			for my $i (0..$#result)
			{
				my @ro = @{@result[$i]};				
				my $mbibid=@ro[1];
				my $holds = findHoldsOnBib($mbibid,$dbHandler);
				$query = "INSERT INTO SEEKDESTROY.BIB_MATCH(BIB1,BIB2,MATCH_REASON,HAS_HOLDS,JOB)	
				VALUES(\$1,\$2,\$3,\$4,\$5)";
				my @values = ($bibid,$mbibid,"Physical Items to Physical Bib loose: Author, Title, Record Type",$holds,$jobid);
				$dbHandler->updateWithParameters($query,\@values);
			}
			if($#result==-1)
			{
				## Loosen the matching down to just author and title
				$query = "SELECT ID,RECORD,SCORE,ELECTRONIC FROM  seekdestroy.bib_score
				WHERE TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = $bibid)
				AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = $bibid)
				AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = $bibid) OR ELECTRONIC < 1)
				AND RECORD != $bibid";
				updateJob($dbHandler,"Processing","findPhysicalItemsOnElectronic  $query");
				$log->addLine($query);
				my @result = @{$dbHandler->query($query)};
				for my $i (0..$#result)
				{
					my @ro = @{@result[$i]};				
					my $mbibid=@ro[1];
					my $holds = findHoldsOnBib($mbibid,$dbHandler);
					$query = "INSERT INTO SEEKDESTROY.BIB_MATCH(BIB1,BIB2,MATCH_REASON,HAS_HOLDS,JOB)	
					VALUES(\$1,\$2,\$3,\$4,\$5)";
					my @values = ($bibid,$mbibid,"Physical Items to Physical Bib loose: Author, Title",$holds,$jobid);
					$dbHandler->updateWithParameters($query,\@values);
				}
			}
		}
	}
}

sub updateScoreWithQuery
{
	my $query = @_[0];
	my $dbHandler = @_[1];
	my $log = @_[2];
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		my $marc = @row[1];
		my @scorethis = ($bibid,$marc);
		my @st = ([@scorethis]);
		updateScoreCache($dbHandler,$log,\@st);
	}
}

##
##
##UPDATE THIS QUERY TO INCLUDE EVERYTHING BEFORE GOING TO PRODUCTION
## REMOVE THIS:
## and id not in(select record from seekdestroy.bib_score)
##
sub findPossibleDups
{
	my $mobUtil = @_[0];
	my $dbHandler = @_[1];
	my $log = @_[2];
	my $query="
		select string_agg(to_char(id,'9999999999'),','),fingerprint from biblio.record_entry where fingerprint in
		(
		select fingerprint from(
		select fingerprint,count(*) \"count\" from biblio.record_entry where not deleted 
		and id not in(select record from seekdestroy.bib_score)
		group by fingerprint
		) as a
		where count>1
		)
		and not deleted
		and fingerprint != ''
		group by fingerprint
		limit 1;
		";
updateJob($dbHandler,"Processing","findPossibleDups  $query");
	my @results = @{$dbHandler->query($query)};
	my @st=();
	my %alreadycached;
	my $deleteoldscorecache="";
updateJob($dbHandler,"Processing","findPossibleDups  looping results");
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my @ids=split(',',@row[0]);
		my $fingerprint = @row[1];
		for my $i(0..$#ids)
		{
			@ids[$i]=$mobUtil->trim(@ids[$i]);
			my $id = @ids[$i];
			if(!$alreadycached{$id})
			{
				$alreadycached{$id}=1;
				my $q = "select marc from biblio.record_entry where id=$id";
				my @result = @{$dbHandler->query($q)};			
				my @r = @{@result[0]};
				my $marc = @r[0];
				my @scorethis = ($id,$marc);
				push(@st,[@scorethis]);
				$deleteoldscorecache.="$id,";
			}
		}
	}

	$deleteoldscorecache=substr($deleteoldscorecache,0,-1);
	my $q = "delete from SEEKDESTROY.BIB_SCORE where RECORD IN( $deleteoldscorecache)";
	updateJob($dbHandler,"Processing","findPossibleDups deleting old cache    $query");
	print $dbHandler->update($q);				
	$q = "delete from SEEKDESTROY.BIB_MATCH where BIB1 IN( $deleteoldscorecache) OR BIB2 IN( $deleteoldscorecache)";
	updateJob($dbHandler,"Processing","findPossibleDups deleting old cache bib_match   $query");
	print $dbHandler->update($q);	
	updateJob($dbHandler,"Processing","findPossibleDups updating scorecache selectivly");
	updateScoreCache($dbHandler,$log,\@st);
	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $q = "select record
		(select id from action.hold_request ahr where 
		ahr.target=sbs.record and
		ahr.hold_type=\$\$T\$\$ and
		ahr.capture_time is null and
		ahr.cancel_time is null limit 1
		)
		from seekdestroy.bib_score sbs
		where sbs.record in(".@row[0].")";
		
		my $fingerprint = @row[1];
		my @result = @{$dbHandler->query($q)};
		my $bib1;
		for my $i (0..$#result)
		{
			my @ro = @{@result[$i]};
			if($i==0)
			{
				$bib1 = @ro[0];
			}
			else
			{
				my @ro = @{@result[$i]};
				my $record = @ro[0];
				my $sd_fingerprint = @ro[1];
				my $hold = @ro[2];
				length($hold)>0 ? $hold=1 : $hold=0;
				
				my $q = "INSERT INTO SEEKDESTROY.BIB_MATCH(BIB1,BIB2,MATCH_REASON,HAS_HOLDS,JOB)
				VALUES(\$1,\$2,\$3,\$4,\$5)";
				my @values = ($bib1,$record,"EG Fingerprint",$hold,$jobid);
				$dbHandler->updateWithParameters($q,\@values);
			}
		}
	}
}

sub findHoldsOnBib
{
	my $bibid=@_[0];
	my $dbHandler=@_[1];	
	my $hold = 0;
	my $query = "select id from action.hold_request ahr where 
	ahr.target=$bibid and
	ahr.hold_type=\$\$T\$\$ and
	ahr.capture_time is null and
	ahr.cancel_time is null";
	updateJob($dbHandler,"Processing","findHolds $query");
	my @results = @{$dbHandler->query($query)};
	if($#results != -1)
	{
		$hold=1;
	}
	#print "returning $hold\n";
	return $hold
}

sub recordAssetCopyMove
{
	my $oldbib = @_[0];	
	my $dbHandler = @_[1];
	my $log = @_[2];
	my $query = "select id from asset.copy where call_number in(select id from asset.call_number where record in($oldbib) and label!=\$\$##URI##\$\$)";
	my @cids;
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my @row = @{$_};
		push(@cids,@row[0]);
	}
	
	if($#cids>-1)
	{		
		#attempt to put those asset.copies back onto the previously deleted bib from m_dedupe
		moveAssetCopyToPreviouslyDedupedBib($dbHandler,$oldbib,$log);		
	}
	
	#Check again after the attempt to undedupe
	@cids = ();
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my @row = @{$_};
		push(@cids,@row[0]);
	}
	
	foreach(@cids)
	{
		print "There were asset.copies on $oldbib even after attempting to put them on a deduped bib\n";
		$log->addLine("\t$oldbib\tContained physical Items");
		 $query = "
		INSERT INTO seekdestroy.item_reassignment(copy,prev_bib,target_bib,job)
		VALUES ($_,$oldbib,$oldbib,$jobid)";
		$log->addLine("$query");
updateJob($dbHandler,"Processing","recordAssetCopyMove  $query");
		$dbHandler->update($query);
	}
}

sub moveAssetCopyToPreviouslyDedupedBib
{
	my $dbHandler = @_[0];	
	my $currentBibID = @_[1];
	my $log = @_[2];
	my %possibles;	
	my $query = "select mmm.sub_bibid,bre.marc from m_dedupe.merge_map mmm, biblio.record_entry bre 
	where lead_bibid=$currentBibID and bre.id=mmm.sub_bibid";
updateJob($dbHandler,"Processing","moveAssetCopyToPreviouslyDedupedBib  $query");
	#print $query."\n";
	my @results = @{$dbHandler->query($query)};
	my $winner=0;
	my $currentWinnerElectricScore=10000;
	my $currentWinnerMARCScore=0;
	foreach(@results)
	{
		my @row = @{$_};
		my $prevmarc = @row[1];
		$prevmarc =~ s/(<leader>.........)./${1}a/;
		$prevmarc = MARC::Record->new_from_xml($prevmarc);
		my @temp=($prevmarc,determineElectric($prevmarc),scoreMARC($prevmarc,$log));
		#need to initialize the winner values
		$winner=@row[0];
		$currentWinnerElectricScore = @temp[1];
		$currentWinnerMARCScore = @temp[2];
		$possibles{@row[0]}=\@temp;
	}
	
	#choose the best deleted bib - we want the lowest electronic bib score in this case because we want to attach the 
	#items to the *most physical bib
	while ((my $bib, my $attr) = each(%possibles))
	{
		my @atts = @{$attr};
		if(@atts[1]<$currentWinnerElectricScore)
		{
			$winner=$bib;
			$currentWinnerElectricScore=@atts[1];
			$currentWinnerMARCScore=@atts[2];
		}
		elsif(@atts[1]==$currentWinnerElectricScore && @atts[2]>$currentWinnerMARCScore)
		{
			$winner=$bib;
			$currentWinnerElectricScore=@atts[1];
			$currentWinnerMARCScore=@atts[2];
		}		
	}
	if($winner!=0)
	{
		$query = "select deleted from biblio.record_entry where id=$winner";
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{	
			my $row = $_;
			my @row = @{$row};
			print "$winner - ".@row[0]."\n";
			#make sure that it is in fact deleted
			if(@row[0] eq 't' ||@row[0] == 1)
			{
				my $tcn_value = $winner;
				my $count=1;			
				#make sure that when we undelete it, it will not collide its tcn_value 
				while($count>0)
				{
					$query = "select count(*) from biblio.record_entry where tcn_value = \$\$$tcn_value\$\$ and id != $winner";
					$log->addLine($query);
updateJob($dbHandler,"Processing","moveAssetCopyToPreviouslyDedupedBib  $query");
					my @results = @{$dbHandler->query($query)};
					foreach(@results)
					{	
						my $row = $_;
						my @row = @{$row};
						$count=@row[0];
					}
					$tcn_value.="_";
				}
				#take the last tail off
				$tcn_value=substr($tcn_value,0,-1);
				#finally, undelete the bib making it available for the asset.call_number
				$query = "update biblio.record_entry set deleted='f',tcn_source='un-deduped',tcn_value = \$\$$tcn_value\$\$  where id=$winner";
				#$dbHandler->update($query);
			}
		}
		#find all of the eligible call_numbers
		$query = "SELECT ID FROM ASSET.CALL_NUMBER WHERE RECORD=$currentBibID AND LABEL!= \$\$##URI##\$\$ AND DELETED is false";
updateJob($dbHandler,"Processing","moveAssetCopyToPreviouslyDedupedBib  $query");							
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{	
			my @row = @{$_};
			my $acnid = @row[0];
			$query = 
"INSERT INTO seekdestroy.undedupe(oldleadbib,undeletedbib,undeletedbib_electronic_score,undeletedbib_marc_score,moved_call_number,job)
VALUES($currentBibID,$winner,$currentWinnerElectricScore,$currentWinnerMARCScore,$acnid,$jobid)";
updateJob($dbHandler,"Processing","moveAssetCopyToPreviouslyDedupedBib  $query");							
			$log->addLine($query);
			$dbHandler->update($query);
			$query = "UPDATE ASSET.CALL_NUMBER SET RECORD=$winner WHERE id = $acnid";
updateJob($dbHandler,"Processing","moveAssetCopyToPreviouslyDedupedBib  $query");
			$log->addLine($query);
			#$dbHandler->update($query);
		}
		moveHolds($dbHandler,$currentBibID,$winner,$log);
	}
}

sub moveHolds
{
	my $dbHandler = @_[0];	
	my $oldBib = @_[1];
	my $newBib = @_[2];
	my $log = @_[3];	
	my $query = "UPDATE ACTION.HOLD_REQUEST SET TARGET=$newBib WHERE TARGET=$oldBib AND HOLD_TYPE=\$\$T\$\$ AND current_copy IS NULL AND fulfillment_time IS NULL AND capture_time IS NULL"; 
	$log->addLine($query);
	updateJob($dbHandler,"Processing","moveHolds  $query");
	#print $query."\n";
	#$dbHandler->update($query);
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
	my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE ID NOT IN(SELECT RECORD FROM SEEKDESTROY.BIB_SCORE) AND DELETED IS FALSE LIMIT 1";
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
		my @temp = ($rec,$marc,$id,$score);
		push (@updates, [@temp]);
	}
	push(@ret,[@news]);
	push(@ret,[@updates]);
	return \@ret;
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

sub getFingerprints
{
	my $marcRecord = @_[0];
	my $marc = populate_marc($marcRecord);	
	my %marc = %{normalize_marc($marc)};    
	my %fingerprints;
    $fingerprints{baseline} = join("\t", 
	  $marc{item_form}, $marc{date1}, $marc{record_type},
	  $marc{bib_lvl}, $marc{title}, $marc{author} ? $marc{author} : '');
	$fingerprints{item_form} = $marc{item_form};
	$fingerprints{date1} = $marc{date1};
	$fingerprints{record_type} = $marc{record_type};
	$fingerprints{bib_lvl} = $marc{bib_lvl};
	$fingerprints{title} = $marc{title};
	$fingerprints{author} = $marc{author};
	#print Dumper(%fingerprints);
	return \%fingerprints;
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
#            print XF ">> Short 008 padded to ",length($marc{tag008})," at rec $count\n";
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
 #               print XF ">> using 260c as date1 at rec $count\n";
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
	#print Dumper(%marc);
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
		electronic bigint,
		item_form text,
		date1 text,
		record_type text,
		bib_lvl text,
		title text,
		author text,
		sd_fingerprint text,
		eg_fingerprint text)";		
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.item_reassignment(
		id serial,
		copy bigint,
		prev_bib bigint,
		target_bib bigint,
		change_time timestamp default now(), 
		electronic bigint default 0,
		job  bigint NOT NULL,
		CONSTRAINT item_reassignment_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";		
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
		$query = "CREATE TABLE seekdestroy.undedupe(
		id serial,
		oldleadbib bigint,
		undeletedbib bigint,
		undeletedbib_electronic_score bigint,
		undeletedbib_marc_score bigint,
		moved_call_number bigint,
		change_time timestamp default now(),
		job  bigint NOT NULL,
		CONSTRAINT undedupe_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.bib_match(
		id serial,
		bib1 bigint,
		bib2 bigint,
		match_reason text,
		merged boolean default false,
		has_holds boolean default false,
		job  bigint NOT NULL,
		CONSTRAINT bib_merge_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.bib_item_circ_mods(
		id serial,
		record bigint,
		circ_modifier text,
		different_circs bigint,
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
	##print Dumper(\%conf);
	return \%conf;

}

 exit;

 
 