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
#use email;
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



# select sbm.bib1,sbm.bib2,sbm.match_reason,merged,has_holds,job,sbs1.score,sbs2.score,sbs2.sd_fingerprint from seekdestroy.bib_match sbm,seekdestroy.bib_score sbs1,seekdestroy.bib_score sbs2
# where
# sbs1.record=sbm.bib1 and
# sbs2.record=sbm.bib2
# order by bib1,sbs1.score



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
				#my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE id in(1320891,1044364)";
				#updateScoreWithQuery($query,$dbHandler,$log);
				findPhysicalItemsOnElectronicBooksUnDedupe($mobUtil,$dbHandler,$log);
				findPhysicalItemsOnElectronicAudioBooksUnDedupe($mobUtil,$dbHandler,$log);				
				#findPhysicalItemsOnElectronicAudioBooks($mobUtil,$dbHandler,$log);
				#findInvalidElectronicMARC($dbHandler,$log);
				#matchAudioBooks($mobUtil,$dbHandler,$log);
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

sub findInvalidElectronicMARC
{
	my $dbHandler = @_[0];
	my $log = @_[1];
	my $query = "
	select id,marc from biblio.record_entry where id in (select record from metabib.real_full_rec where tag=\$\$856\$\$ and ind2=\$\$0\$\$) AND  
marc ~ \$\$<leader>......a\$\$
and
marc !~ \$\$tag=\"008\">.......................[oqs]\$\$
and
marc !~ \$\$<leader>.......p\$\$
 limit 1000";
 
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $id = @row[0];
		my $marc = @row[1];
		if(!isScored($dbHandler, $id))
		{
			my @scorethis = ($id,$marc);
			my @st = ([@scorethis]);			
			updateScoreCache($dbHandler,$log,\@st);
		}
		$query="INSERT INTO SEEKDESTROY.PROBLEM_BIBS(RECORD,PROBLEM,JOB) VALUES (\$1,\$2,\$3)";
		my @values = ($id,"MARC with E-Links but 008 tag is missing o,q,s",$jobid);
		$dbHandler->updateWithParameters($query,\@values);
	}
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
	#$log->addLine("Found ".($#newIDs+1)." new Bibs to be scored");	
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
		audioformat,
		eg_fingerprint) 
		VALUES($bibid,$score,$electricScore,
		\$1,\$2,\$3,\$4,\$5,\$6,\$7,\$8,(SELECT FINGERPRINT FROM BIBLIO.RECORD_ENTRY WHERE ID=$bibid)
		)";		
		my @values = (
		$fingerprints{item_form},
		$fingerprints{date1},
		$fingerprints{record_type},
		$fingerprints{bib_lvl},
		$fingerprints{title},
		$fingerprints{author},
		$fingerprints{baseline},
		$fingerprints{audioformat}
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
		audioformat = \$8,
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
		$fingerprints{audioformat}
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

sub findPhysicalItemsOnElectronicBooksUnDedupe
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
	(
		marc ~ \$\$tag=\"008\">.......................[oqs]\$\$
	)
	and
	(
		marc ~ \$\$<leader>......[at]\$\$
	)
	and
	(
		marc ~ \$\$<leader>.......[acdm]\$\$
	)
	";
	updateJob($dbHandler,"Processing","findPhysicalItemsOnElectronicBooksUnDedupe  $query");
	my @results = @{$dbHandler->query($query)};
	$log->addLine($#results." Bibs with physical Items attached from the dedupe");
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
}

sub addBibMatch
{
	my $dbHandler = @_[0];
	my %queries = %{@_[1]};
	my $log = @_[2];
	my $matchedSomething=0;
	my $searchQuery = $queries{'searchQuery'};
	my $problem = $queries{'problem'};
	my @matchQueries = @{$queries{'matchQueries'}};
	my @takeActionWithTheseMatchingMethods = @{$queries{'takeActionWithTheseMatchingMethods'}};	
	updateJob($dbHandler,"Processing","addBibMatch  $searchQuery");
	my @results = @{$dbHandler->query($searchQuery)};
	$log->addLine($#results." Search Query results");
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		my $marc = @row[1];
		my $extra = @row[2]?$extra:'';		
		my @scorethis = ($bibid,$marc);
		my @st = ([@scorethis]);		
		updateScoreCache($dbHandler,$log,\@st);
		my $query="INSERT INTO SEEKDESTROY.PROBLEM_BIBS(RECORD,PROBLEM,JOB) VALUES (\$1,\$2,\$3)";
		updateJob($dbHandler,"Processing","addBibMatch  $query");
		my @values = ($bibid,$problem,$jobid);
		$dbHandler->updateWithParameters($query,\@values);
		## Now find likely candidates elsewhere in the ME DB	
		addRelatedBibScores($dbHandler,$bibid,$log);
		## Now run match queries starting with tight and moving down to loose
		my $i=0;
		while(!$matchedSomething && @matchQueries[$i])
		{
			my $matchQ = @matchQueries[$i];
			$matchQ =~ s/\$bibid/$bibid/gi;
			my $matchReason = @matchQueries[$i+1];
			$i+=2;
			$log->addLine($matchQ);
			updateJob($dbHandler,"Processing","addBibMatch  $matchQ");
			my @results2 = @{$dbHandler->query($matchQ)};
			foreach(@results2)
			{
				my @ro = @{$_};
				my $mbibid=@ro[0];
				my $holds = findHoldsOnBib($mbibid,$dbHandler);
				$query = "INSERT INTO SEEKDESTROY.BIB_MATCH(BIB1,BIB2,MATCH_REASON,HAS_HOLDS,JOB)
				VALUES(\$1,\$2,\$3,\$4,\$5)";
				updateJob($dbHandler,"Processing","addBibMatch  $query");
				$log->addLine($query);
				my @values = ($bibid,$mbibid,$matchReason,$holds,$jobid);
				$dbHandler->updateWithParameters($query,\@values);
				$matchedSomething = 1;
				foreach(@takeActionWithTheseMatchingMethods)
				{
					if($_ eq $matchReason)
					{
						if($queries{'action'} eq 'movecopies')
						{
							moveCopiesOntoHighestScoringBibCandidate($dbHandler,$bibid,$matchReason,$log);
						}
						elsif($queries{'action'} eq 'mergebibs')
						{
							
						}
					}
				}
			}
		}
	}	
	return $matchedSomething;
}

sub addRelatedBibScores
{
	my $dbHandler = @_[0];
	my $rootbib = @_[1];
	my $log = @_[2];
	my $query="SELECT LOWER(TITLE) FROM SEEKDESTROY.BIB_SCORE WHERE RECORD=$rootbib";
	updateJob($dbHandler,"Processing","addRelatedBibScores  $query");
	my @results = @{$dbHandler->query($query)};		
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $title = @row[0];
		if(length($title)>5)
		{
			$query =
			"SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE LOWER(MARC) ~ (SELECT LOWER(TITLE) FROM SEEKDESTROY.BIB_SCORE WHERE RECORD=$rootbib)";
			updateJob($dbHandler,"Processing","addRelatedBibScores  $query");
			$log->addLine($query);
			updateScoreWithQuery($query,$dbHandler,$log);
		}
	}
	$query =
	"SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE FINGERPRINT = (SELECT EG_FINGERPRINT FROM SEEKDESTROY.BIB_SCORE WHERE RECORD=$rootbib)";
	updateJob($dbHandler,"Processing","addRelatedBibScores  $query");
	$log->addLine($query);
	updateScoreWithQuery($query,$dbHandler,$log);
	
}


sub attemptMovePhysicalItemsOnAnElectronicBook
{
	my $dbHandler = @_[0];
	my $oldbib = @_[1];
	my $log = @_[2];
	
	my $query;
	my %queries=();
	$queries{'action'} = 'movecopies';
	$queries{'problem'} = "Physical items attched to Electronic Bibs";
	my @okmatchingreasons=("Physical Items to Electronic Bib exact","Physical Items to Electronic Bib exact minus date1");
	$queries{'takeActionWithTheseMatchingMethods'}=\@okmatchingreasons;
	$queries{'searchQuery'} = "
	select id,marc from biblio.record_entry where not deleted and lower(marc) ~ \$\$<datafield tag=\"856\" ind1=\"4\" ind2=\"0\">\$\$
	and id in
	(
	select record from asset.call_number where not deleted and id in(select call_number from asset.copy where not deleted)
	)	
	and 
	(
		marc ~ \$\$tag=\"008\">.......................[oqs]\$\$
	)
	and
	(
		marc ~ \$\$<leader>......[at]\$\$
	)
	and
	(
		marc ~ \$\$<leader>.......[acdm]\$\$
	)
	";	
	my @results;
	if($oldbib)
	{
		$queries{'searchQuery'} = "select id,marc from biblio.record_entry where id=$oldbib";
	}
	my @matchQueries = 
	(
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE 
		DATE1 = (SELECT DATE1 FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND RECORD_TYPE = (SELECT RECORD_TYPE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND BIB_LVL = (SELECT BIB_LVL FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Bib exact",
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE 			
		RECORD_TYPE = (SELECT RECORD_TYPE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND BIB_LVL = (SELECT BIB_LVL FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Bib exact minus date1",
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)			
		AND RECORD_TYPE = (SELECT RECORD_TYPE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Bib loose: Author, Title, Record Type",
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Bib loose: Author, Title"		
	);
	
	$queries{'matchQueries'} = \@matchQueries;
	my $success = addBibMatch($dbHandler,\%queries,$log);
	return $success;
}

sub moveCopiesOntoHighestScoringBibCandidate
{
	my $dbHandler = @_[0];	
	my $oldbib = @_[1];	
	my $matchReason = @_[2];
	my $log = @_[3];
	my $query = "select sbm.bib2,sbs.score from SEEKDESTROY.BIB_MATCH sbm,seekdestroy.bib_score sbs where 
	sbm.bib1=$oldbib and
	sbm.match_reason=\$\$$matchReason\$\$ and
	sbs.record=sbm.bib2
	order by sbs.score";
	$log->addLine($query);
	updateJob($dbHandler,"Processing","moveCopiesOntoHighestScoringBibCandidate  $query");
	my @results = @{$dbHandler->query($query)};	
	$log->addLine($#results." potential bibs for destination");
	my $hscore=0;
	my $winner=0;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		my $score = @row[1];
		$log->addLine("Adding Score Possible: $score - $bibid");
		if($score>$hscore)
		{
			$winner=$bibid;
			$hscore=$score;
		}
	}
	$log->addLine("Winning Score: $hscore - $winner");
	if($winner!=0)
	{
		undeleteBIB($dbHandler,$winner,$log);
		#print "moveCopiesOntoHighestScoringBibCandidate from: $oldbib\n";
		moveAllCallNumbers($dbHandler,$oldbib,$winner,$matchReason,$log);
		moveHolds($dbHandler,$oldbib,$winner,$log);
	}
}

sub findPhysicalItemsOnElectronicBooks
{
	my $mobUtil = @_[0];
	my $dbHandler = @_[1];
	my $log = @_[2];
	my $success = 0;
	# Find Electronic bibs with physical items
	my $query = "
	select id,marc from biblio.record_entry where not deleted and lower(marc) ~ \$\$<datafield tag=\"856\" ind1=\"4\" ind2=\"0\">\$\$
	and id in
	(
	select record from asset.call_number where not deleted and id in(select call_number from asset.copy where not deleted)
	)	
	and 
	(
		marc ~ \$\$tag=\"008\">.......................[oqs]\$\$
	)
	and
	(
		marc ~ \$\$<leader>......[at]\$\$
	)
	and
	(
		marc ~ \$\$<leader>.......[acdm]\$\$
	)	
	";
	updateJob($dbHandler,"Processing","findPhysicalItemsOnElectronic  $query");
	my @results = @{$dbHandler->query($query)};	
	$log->addLine($#results." Bibs with physical Items attached");
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		$success = attemptMovePhysicalItemsOnAnElectronicBook($dbHandler,$bibid,$log);		
	}
	
	return $success;
	
}

sub findPhysicalItemsOnElectronicAudioBooksUnDedupe
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
	(
		marc ~ \$\$tag=\"008\">.......................[oqs]\$\$
	)
	and
	(
		marc ~ \$\$<leader>......i\$\$
	)	
	";
	updateJob($dbHandler,"Processing","findPhysicalItemsOnElectronicAudioBooks  $query");
	my @results = @{$dbHandler->query($query)};
	$log->addLine($#results." Bibs with physical Items attached from the dedupe");
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		my $marc = @row[1];
		my @scorethis = ($bibid,$marc);
		my @st = ([@scorethis]);
		my $q = "DELETE FROM SEEKDESTROY.BIB_SCORE WHERE RECORD=$bibid";
		$dbHandler->update($q);
		updateScoreCache($dbHandler,$log,\@st);
		recordAssetCopyMove($bibid,$dbHandler,$log);
	}

}

sub findPhysicalItemsOnElectronicAudioBooks
{
	my $mobUtil = @_[0];
	my $dbHandler = @_[1];
	my $log = @_[2];
	my $success = 0;
	# Find Electronic Audio bibs with physical items
	my $query = "
	select id,marc from biblio.record_entry where not deleted and lower(marc) ~ \$\$<datafield tag=\"856\" ind1=\"4\" ind2=\"0\">\$\$
	and id in
	(
	select record from asset.call_number where not deleted and id in(select call_number from asset.copy where not deleted)
	)	
	and 
	(
		marc ~ \$\$tag=\"008\">.......................[oqs]\$\$
	)
	and
	(
		marc ~ \$\$<leader>......i\$\$
	)
	";
	updateJob($dbHandler,"Processing","findPhysicalItemsOnElectronicAudioBooks  $query");
	my @results = @{$dbHandler->query($query)};	
	$log->addLine($#results." Audio Bibs with physical Items attached");
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		$success = attemptMovePhysicalItemsOnAnElectronicAudioBook($dbHandler,$bibid,$log);		
	}
	
	return $success;
	
}


sub attemptMovePhysicalItemsOnAnElectronicAudioBook
{
	my $dbHandler = @_[0];
	my $oldbib = @_[1];
	my $log = @_[2];
	
	my $query;
	my %queries=();
	$queries{'action'} = 'movecopies';
	$queries{'problem'} = "Physical items attched to Electronic Audio Bibs";
	my @okmatchingreasons=("Physical Items to Electronic Audio Bib exact","Physical Items to Electronic Audio Bib exact minus date1");
	$queries{'takeActionWithTheseMatchingMethods'}=\@okmatchingreasons;
	$queries{'searchQuery'} = "
	select id,marc from biblio.record_entry where not deleted and lower(marc) ~ \$\$<datafield tag=\"856\" ind1=\"4\" ind2=\"0\">\$\$
	and id in
	(
	select record from asset.call_number where not deleted and id in(select call_number from asset.copy where not deleted)
	)	
	and 
	(
		marc ~ \$\$tag=\"008\">.......................[oqs]\$\$
	)
	and
	(
		marc ~ \$\$<leader>......i\$\$
	)
	";
	my @results;
	if($oldbib)
	{
		$queries{'searchQuery'} = "select id,marc from biblio.record_entry where id=$oldbib";
	}
	my @matchQueries = 
	(
		"SELECT ID,RECORD,SCORE,ELECTRONIC FROM  seekdestroy.bib_score
		WHERE 
		DATE1 = (SELECT DATE1 FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND RECORD_TYPE = (SELECT RECORD_TYPE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND BIB_LVL = (SELECT BIB_LVL FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Audio Bib exact",		
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE 			
		RECORD_TYPE = (SELECT RECORD_TYPE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND BIB_LVL = (SELECT BIB_LVL FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Audio Bib exact minus date1",
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)			
		AND RECORD_TYPE = (SELECT RECORD_TYPE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Audio Bib loose: Author, Title, Record Type",
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Audio Bib loose: Author, Title"		
	);
	
	$queries{'matchQueries'} = \@matchQueries;
	my $success = addBibMatch($dbHandler,\%queries,$log);
	return $success;
}

sub findItemsCircedAsAudioBooksButAttachedNonAudioBib
{
	my $dbHandler = @_[0];
	my $oldbib = @_[1];
	my $log = @_[2];
	
	my $query;
	my %queries=();
	$queries{'action'} = 'movecopies';
	$queries{'problem'} = "Non-audiobook Bib with items that circulate as 'AudioBooks'";
	my @okmatchingreasons=();
	$queries{'takeActionWithTheseMatchingMethods'}=\@okmatchingreasons;
	# Find Bibs that are not Audiobooks and have physical items that are circed as audiobooks
	$queries{'searchQuery'} = "
	select bre.id,bre.marc,string_agg(ac.barcode,\$\$,\$\$) from biblio.record_entry bre, asset.copy ac, asset.call_number acn where 
bre.marc !~ \$\$<leader>......i\$\$
and
bre.id=acn.record and
acn.id=ac.call_number and
not acn.deleted and
not ac.deleted and
ac.circ_modifier=\$\$AudioBooks\$\$
group by bre.id,bre.marc
limit 1000
	";
	my @results;
	if($oldbib)
	{
		$queries{'searchQuery'} = "select id,marc from biblio.record_entry where id=$oldbib";
	}
	my @matchQueries = 
	(
		"SELECT ID,RECORD,SCORE,ELECTRONIC FROM  seekdestroy.bib_score
		WHERE 
		DATE1 = (SELECT DATE1 FROM seekdestroy.bib_score WHERE RECORD = $bibid) 
		AND RECORD_TYPE = \$\$i\$\$
		AND BIB_LVL = (SELECT BIB_LVL FROM seekdestroy.bib_score WHERE RECORD = $bibid) 
		AND TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = $bibid) 
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = $bibid)		
		AND RECORD != $bibid","AudioBooks attached to non AudioBook Bib exact",		
		
		"SELECT ID,RECORD,SCORE,ELECTRONIC FROM  seekdestroy.bib_score
		WHERE 		
		RECORD_TYPE = \$\$i\$\$
		AND BIB_LVL = (SELECT BIB_LVL FROM seekdestroy.bib_score WHERE RECORD = $bibid) 
		AND TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = $bibid) 
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = $bibid)		
		AND RECORD != $bibid","AudioBooks attached to non AudioBook Bib exact minus date1",		
		
		"SELECT ID,RECORD,SCORE,ELECTRONIC FROM  seekdestroy.bib_score
		WHERE TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = $bibid)
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = $bibid)			
		AND RECORD_TYPE = \$\$i\$\$
		AND RECORD != $bibid","AudioBooks attached to non AudioBook Bib loose"
				
	);
	
	$queries{'matchQueries'} = \@matchQueries;
	my $success = addBibMatch($dbHandler,\%queries,$log);
	return $success;
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

sub findPossibleDups
{
	my $mobUtil = @_[0];
	my $dbHandler = @_[1];
	my $log = @_[2];
	my $query="
		select string_agg(to_char(id,\$\$9999999999\$\$),\$\$,\$\$),fingerprint from biblio.record_entry where fingerprint in
		(
		select fingerprint from(
		select fingerprint,count(*) \"count\" from biblio.record_entry where not deleted 
		and id not in(select record from seekdestroy.bib_score)
		group by fingerprint
		) as a
		where count>1
		)
		and not deleted
		and fingerprint != \$\$\$\$
		group by fingerprint
		limit 1000;
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
	#print $dbHandler->update($q);				
	$q = "delete from SEEKDESTROY.BIB_MATCH where BIB1 IN( $deleteoldscorecache) OR BIB2 IN( $deleteoldscorecache)";
	updateJob($dbHandler,"Processing","findPossibleDups deleting old cache bib_match   $query");
	#print $dbHandler->update($q);	
	updateJob($dbHandler,"Processing","findPossibleDups updating scorecache selectivly");
	updateScoreCache($dbHandler,$log,\@st);
	
	
	my $query="
			select record,sd_fingerprint,score from seekdestroy.bib_score sbs2 where sd_fingerprint in(
		select sd_fingerprint from(
		select sd_fingerprint,count(*) from seekdestroy.bib_score sbs where length(btrim(regexp_replace(regexp_replace(sbs.sd_fingerprint,\$\$\t\$\$,\$\$\$\$,\$\$g\$\$),\$\$\s\$\$,\$\$\$\$,\$\$g\$\$)))>5  group by sd_fingerprint having count(*) > 1) as a 
		)
		order by sd_fingerprint,score desc
		";
updateJob($dbHandler,"Processing","findPossibleDups  $query");
	my @results = @{$dbHandler->query($query)};
	my $current_fp ='';
	my $master_record=-2;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $record=@row[0];
		my $fingerprint = @row[1];
		my $score= @row[2];
		
		if($current_fp ne $fingerprint)
		{
			$current_fp=$fingerprint;
			$master_record = $record
		}
		else
		{
			my $hold = findHoldsOnBib($record, $dbHandler);
			my $q = "INSERT INTO SEEKDESTROY.BIB_MATCH(BIB1,BIB2,MATCH_REASON,HAS_HOLDS,JOB)
			VALUES(\$1,\$2,\$3,\$4,\$5)";
			my @values = ($master_record,$record,"Duplicate SD Fingerprint",$hold,$jobid);
			$dbHandler->updateWithParameters($q,\@values);
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
	my $query = "select distinct call_number from asset.copy where call_number in(select id from asset.call_number where record in($oldbib) and label!=\$\$##URI##\$\$)";
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
	if($#cids>-1)
	{
		attemptMovePhysicalItemsOnAnElectronicBook($dbHandler,$oldbib,$log);
	}
	@cids = ();
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my @row = @{$_};
		my $callnum= @row[0];
		print "There were asset.copies on $oldbib even after attempting to put them on a deduped bib\n";
		$log->addLine("\t$oldbib\tContained physical Items");
		$query="INSERT INTO SEEKDESTROY.CALL_NUMBER_MOVE(CALL_NUMBER,FROMBIB,EXTRA,SUCCESS,JOB)
		VALUES(\$1,\$2,\$3,\$4,\$5)";	
		my @values = ($callnum,$oldbib,"FAILED",'false',$jobid);
		$log->addLine($query);				
		updateJob($dbHandler,"Processing","recordAssetCopyMove  $query");
		$dbHandler->updateWithParameters($query,\@values);		
	}	
}

sub moveAssetCopyToPreviouslyDedupedBib
{
	my $dbHandler = @_[0];	
	my $currentBibID = @_[1];
	my $log = @_[2];
	my %possibles;	
	my $query = "select mmm.sub_bibid,bre.marc from m_dedupe.merge_map mmm, biblio.record_entry bre 
	where lead_bibid=$currentBibID and bre.id=mmm.sub_bibid
	and
	bre.marc !~ \$\$tag=\"008\">.......................[oqs]\$\$
	";
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
		undeleteBIB($dbHandler,$winner,$log);
		#find all of the eligible call_numbers
		$query = "SELECT ID FROM ASSET.CALL_NUMBER WHERE RECORD=$currentBibID AND LABEL!= \$\$##URI##\$\$";
updateJob($dbHandler,"Processing","moveAssetCopyToPreviouslyDedupedBib  $query");
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{	
			my @row = @{$_};
			my $acnid = @row[0];			
			my $callNID = moveCallNumber($dbHandler,$acnid,$currentBibID,$winner,"Dedupe pool",$log);
			$query = 
			"INSERT INTO seekdestroy.undedupe(oldleadbib,undeletedbib,undeletedbib_electronic_score,undeletedbib_marc_score,moved_call_number,job)
			VALUES($currentBibID,$winner,$currentWinnerElectricScore,$currentWinnerMARCScore,$callNID,$jobid)";
updateJob($dbHandler,"Processing","moveAssetCopyToPreviouslyDedupedBib  $query");							
			$log->addLine($query);
			$dbHandler->update($query);
		}
		moveHolds($dbHandler,$currentBibID,$winner,$log);
	}
}

sub undeleteBIB
{
	my $dbHandler = @_[0];
	my $bib = @_[1];
	my $log = @_[2];
	my $query = "select deleted from biblio.record_entry where id=$bib";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{	
		my $row = $_;
		my @row = @{$row};			
		#make sure that it is in fact deleted
		if(@row[0] eq 't' || @row[0] == 1)
		{
			my $tcn_value = $bib;
			my $count=1;			
			#make sure that when we undelete it, it will not collide its tcn_value 
			while($count>0)
			{
				$query = "select count(*) from biblio.record_entry where tcn_value = \$\$$tcn_value\$\$ and id != $bib";
				$log->addLine($query);
updateJob($dbHandler,"Processing","undeleteBIB  $query");
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
			$query = "update biblio.record_entry set deleted='f',tcn_source='un-deduped',tcn_value = \$\$$tcn_value\$\$  where id=$bib";
			$dbHandler->update($query);
		}
	}
}

sub moveAllCallNumbers
{
	my $dbHandler = @_[0];	
	my $oldbib = @_[1];
	my $destbib = @_[2];
	my $matchReason = @_[3];
	my $log = @_[4];	
	my $query = "select id from asset.call_number where record=$oldbib and label!=\$\$##URI##\$\$";
	$log->addLine($query);
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my @row = @{$_};
		my $calln = @row[0];
		#print "moveAllCallNumbers from: $oldbib\n";
		moveCallNumber($dbHandler,$calln,$oldbib,$destbib,$matchReason,$log);
	}
	
}

sub recordCopyMove
{
	my $dbHandler = @_[0];	
	my $callnumberid = @_[1];
	my $destcall = @_[2];
	my $matchReason = @_[3];	
	my $log = @_[4];
	my $query = "SELECT ID FROM ASSET.COPY WHERE CALL_NUMBER=$callnumberid";
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my @row = @{$_};
		my $copy = @row[0];
		$query="INSERT INTO SEEKDESTROY.COPY_MOVE(COPY,FROMCALL,TOCALL,EXTRA,JOB) VALUES(\$1,\$2,\$3,\$4,\$5)";
		my @values = ($copy,$callnumberid,$destcall,$matchReason,$jobid);
		$log->addLine($query);
		$dbHandler->updateWithParameters($query,\@values);
	}
}

sub recordCallNumberMove
{
	my $dbHandler = @_[0];	
	my $callnumber = @_[1];
	my $record = @_[2];
	my $destrecord = @_[3];
	my $matchReason = @_[4];	
	my $log = @_[5];
	#print "recordCallNumberMove from: $record\n";
	my $query="INSERT INTO SEEKDESTROY.CALL_NUMBER_MOVE(CALL_NUMBER,FROMBIB,TOBIB,EXTRA,JOB) VALUES(\$1,\$2,\$3,\$4,\$5)";	
	my @values = ($callnumber,$record,$destrecord,$matchReason,$jobid);
	$log->addLine($query);
	$dbHandler->updateWithParameters($query,\@values);
}
	
sub moveCallNumber
{
	my $dbHandler = @_[0];	
	my $callnumberid = @_[1];
	my $frombib = @_[2];
	#print "moveCallNumber from: $frombib\n";
	my $destbib = @_[3];
	my $matchReason = @_[4];	
	my $log = @_[5];
	my $finalCallNumber=$callnumberid;
	my $query = "SELECT ID,LABEL,RECORD FROM ASSET.CALL_NUMBER WHERE RECORD = $destbib
	AND LABEL=(SELECT LABEL FROM ASSET.CALL_NUMBER WHERE ID = $callnumberid ) 
	AND OWNING_LIB=(SELECT OWNING_LIB FROM ASSET.CALL_NUMBER WHERE ID = $callnumberid ) AND NOT DELETED";
	$log->addLine($query);
	my $moveCopies=0;
	my @results = @{$dbHandler->query($query)};
	#print "about to loop the callnumber results\n";
	foreach(@results)
	{
		#print "it had a duplciate call number\n";
		## Call number already exists on that record for that 
		## owning library and label. So let's just move the 
		## copies to it instead of moving the call number			
		$moveCopies=1;
		my @row = @{$_};
		my $destcall = @row[0];
		recordCopyMove($dbHandler,$callnumberid,$destcall,$matchReason,$log);	
		$query = "UPDATE ASSET.COPY SET CALL_NUMBER=$destcall WHERE CALL_NUMBER=$callnumberid";
		updateJob($dbHandler,"Processing","moveCallNumber  $query");
		$log->addLine($query);
		$dbHandler->update($query);
		$finalCallNumber=$destcall;
	}	
	
	if(!$moveCopies)
	{	
	#print "it didnt have a duplciate call number... going into recordCallNumberMove\n";
		recordCallNumberMove($dbHandler,$callnumberid,$frombib,$destbib,$matchReason,$log);		
		#print "done with recordCallNumberMove\n";
		$query="UPDATE ASSET.CALL_NUMBER SET RECORD=$destbib WHERE ID=$callnumberid";
		#print "$query\n";
		updateJob($dbHandler,"Processing","moveCallNumber  $query");
		$log->addLine($query);
		$dbHandler->update($query);		
	}
	return $finalCallNumber;

}

sub moveHolds
{
	my $dbHandler = @_[0];	
	my $oldBib = @_[1];
	my $newBib = @_[2];
	my $log = @_[3];	
	my $query = "UPDATE ACTION.HOLD_REQUEST SET TARGET=$newBib WHERE TARGET=$oldBib AND HOLD_TYPE=\$\$T\$\$ AND fulfillment_time IS NULL AND capture_time IS NULL AND cancel_time IS NULL"; 
	$log->addLine($query);
	updateJob($dbHandler,"Processing","moveHolds  $query");
	#print $query."\n";
	$dbHandler->update($query);
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
	my $my_007 = $record->field('007');
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

	$marc{tag007} = $my_007->as_string() if ($my_007);
	if (defined $marc{tag007}) {
		$marc{audioformat} = substr($marc{tag007},3,1) unless (length $marc{tag007} < 4 );
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
	my $query = "DROP SCHEMA seekdestroy CASCADE";
	$dbHandler->update($query);
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
		audioformat text,
		eg_fingerprint text)";		
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
		$query = "CREATE TABLE seekdestroy.problem_bibs(
		id serial,
		record bigint,
		problem text,
		extra text,
		job  bigint NOT NULL,
		CONSTRAINT bib_merge_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.call_number_move(
		id serial,
		call_number bigint,
		frombib bigint,
		tobib bigint,
		extra text,
		success boolean default true,
		change_time timestamp default now(),
		job  bigint NOT NULL,
		CONSTRAINT bib_merge_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.copy_move(
		id serial,
		copy bigint,
		fromcall bigint,
		tocall bigint,
		extra text,
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
	##print Dumper(\%conf);
	return \%conf;

}

 exit;

 
 