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
use DateTime;
use utf8;
use Encode;
use DateTime;
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
		my $holdingsmove = new Loghandler($conf->{"holdingsmove"});
		$log->truncFile("");
		$holdingsmove->truncFile("");
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
			#checkbackstage("/mnt/evergreen/tmp/test/MOBIUS_LP_Perfect.oc",$dbHandler, $mobUtil, $log,"/mnt/evergreen/tmp/test/before_perfect.txt","/mnt/evergreen/tmp/test/after_perfect.txt");
			#checkbackstage("/mnt/evergreen/tmp/test/MOBIUS_LP_NonMatch.oc",$dbHandler, $mobUtil, $log,"/mnt/evergreen/tmp/test/before_nonmatch.txt","/mnt/evergreen/tmp/test/after_nonmatch.txt");
			#checkbackstage("/mnt/evergreen/tmp/test/MOBIUS_LP_NonHit.oc",$dbHandler, $mobUtil, $log,"/mnt/evergreen/tmp/test/before_nonhit.txt","/mnt/evergreen/tmp/test/after_nonhit.txt");
			#updateMARC("/mnt/evergreen/tmp/test/MOBIUS_LP_Perfect.oc",$dbHandler, $mobUtil, $log);
			alignHoldings("/mnt/evergreen/tmp/test/MOBIUS_LP_Perfect.oc",$holdingsmove,$dbHandler, $log);
		}
		
		my $afterProcess = DateTime->now(time_zone => "local");
		my $difference = $afterProcess - $dt;
		my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
		my $duration =  $format->format_duration($difference);
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

sub updateMARC
{
	my $backstagefile = @_[0];
	my $dbHandler = @_[1];
	my $mobUtil = @_[2];
	my $log = @_[3];
	my $file = MARC::File::USMARC->in($backstagefile);
	my $bibsourceid = getbibsource($dbHandler);
	my $loops=0;
	while ( my $marc = $file->next() ) 
	{	
		if(1)#$loops<1)
		{
			my $t = $marc->leader();
			my $su=substr($marc->leader(),6,1);
			#print "Leader:\n$t\n$su\n";
			my $leader = substr($marc->leader(),6,1);
			if($marc->field('901'))
			{
				if($marc->field('901')->subfield('a'))
				{
					my $bibID = $marc->field('901')->subfield('a');
					my @fields52 = $marc->field('852');
					my @fields53 = $marc->field('853');
					$marc->delete_fields(@fields52);
					$marc->delete_fields(@fields53);
					$marc = convertMARCtoXML($marc);
					my $query = "UPDATE BIBLIO.RECORD_ENTRY SET marc=\$\$$marc\$\$,tcn_source=E'backstage',source=$bibsourceid WHERE ID=$bibID";
					$log->addLine($query);
					$log->addLine("http://missourievergreen.org/eg/opac/record/$bibID?query=yellow;qtype=keyword;locg=4;expand=marchtml#marchtml\thttp://demo.missourievergreen.org/eg/opac/record/$bibID?query=yellow;qtype=keyword;locg=157;expand=marchtml#marchtml");
					my $res = $dbHandler->update($query);
				}
			}
			$loops++;
		}
	}
}

sub checkbackstage
{
	my $backstagefile = @_[0];
	my $dbHandler = @_[1];
	my $mobUtil = @_[2];
	my $log = @_[3];
	my $before = new Loghandler(@_[4]);
	my $after = new Loghandler(@_[5]);
	$before->truncFile("");
	$after->truncFile("");
	my $file = MARC::File::USMARC->in($backstagefile);
	while ( my $marc = $file->next() ) 
	{	
		my $t = $marc->leader();
		my $su=substr($marc->leader(),6,1);
		print "Leader:\n$t\n$su\n";
		my $leader = substr($marc->leader(),6,1);
		if($marc->field('901'))
		{
			my $bibID = $marc->field('901')->subfield('a');		
			if (my $memarc = getMEMARC($bibID,$dbHandler))
			{	
				$after->addLine($marc->as_formatted());
				my @oldholdingsorder;
				my @holdings = $marc->field('85.');
				foreach(@holdings)
				{
					if($_->subfield('p'))
					{
						push(@oldholdingsorder,$_->subfield('p'));
					}
				}
				#$log->addLine("$bibID");
				$memarc =~ s/(<leader>.........)./${1}a/;			
				$memarc = MARC::Record->new_from_xml($memarc);
				@oldholdingsorder = reverse @oldholdingsorder;				
				$memarc = attachHoldings($memarc,$dbHandler,\@oldholdingsorder);
				
				$before->addLine($memarc->as_formatted());
				#my @errors = @{$mobUtil->compare2MARCObjects($memarc,1,$marc,1)};
				#foreach(@errors)
				#{
			#		$log->addLine($_);
			#	}
				#$log->addLine("\r\n\r\n");
			}
		}
	}
	
}

sub alignHoldings
{
	my $backstagefile = @_[0];
	my $holdingsmove = @_[1];
	my $dbHandler = @_[2];
	my $log = @_[3];
	my $file = MARC::File::USMARC->in($backstagefile);
	my $loops=0;
	my %notmoved=();
	my $totalMarc=0;
	my $catchallBIBid = getMECatchAllBib($dbHandler,$log);
	print "catchallBIBid = $catchallBIBid\n";
	#my $a =  <STDIN>;
	while ( my $marc = $file->next() ) 
	{	
		$totalMarc++;
		if(1)#$loops<1)
		{
			if($marc->field('901'))
			{
				if($marc->field('901')->subfield('a'))
				{
					my $bibID = $marc->field('901')->subfield('a');
					my $query = "SELECT ac.barcode,acn.record from ASSET.COPY ac,asset.call_number acn
								where 
								ac.CALL_NUMBER = acn.id and
								acn.record=$bibID and						
								ac.deleted='f'";
					my @results = @{$dbHandler->query($query)};
					my %mebarcodes = ();
					my @mebarcds;
					my @backbarcodes = ();
					foreach(@results)
					{	
						my $row = $_;
						my @row = @{$row};					
						$mebarcodes{@row[0]}=@row[1];
						push(@mebarcds,@row[0]);
					}
					my @fields = $marc->field('852');
					my @fields_o = $marc->field('853');
					my @both = (@fields,@fields_o);
					foreach(@both)
					{
						my $bcode = $_->subfield('p');
						if($bcode)
						{
							push(@backbarcodes,$bcode);
						}
					}
					my $removedfromthisbib = findArrayDifference(\@mebarcds,\@backbarcodes);
					my @needsmoving = split(',',$removedfromthisbib);
					foreach(@needsmoving)
					{
						$notmoved{$_}='1';
					}
					foreach(@backbarcodes)
					{
						my $backbarcode = $_;
						if(!$mebarcodes{$backbarcode})						
						{
							my $query = "SELECT ac.barcode,acn.record from ASSET.COPY ac,asset.call_number acn
								where 
								ac.CALL_NUMBER = acn.id and
								ac.barcode='$backbarcode'";
							@results = @{$dbHandler->query($query)};
							my $oldbib='';
							foreach(@results)
							{	
								my @row = @{$_};
								$oldbib = @row[1];
							}
							if($oldbib!=$bibID)
							{
								my $result = moveAssetCopy($dbHandler,$backbarcode,$bibID,$log);
								if($result)
								{
									if($notmoved{$backbarcode})
									{
										print "removing $backbarcode from notmoved array\n";
										delete $notmoved{$backbarcode};
									}
								}
								$log->addLine("results of the asset move $backbarcode -> $bibID: $result");
								$log->addLine("link:\t$bibID\thttp://missourievergreen.org/eg/opac/record/$bibID?query=yellow;qtype=keyword;locg=4;expand=marchtml#marchtml\thttp://demo.missourievergreen.org/eg/opac/record/$bibID?query=yellow;qtype=keyword;locg=157;expand=marchtml#marchtml");							
								$holdingsmove->addLine("\"$backbarcode\",\"$oldbib\",\"$bibID\",\"$result\",$removedfromthisbib");
								$loops++;
							}
						}
					}
					
				}
			}
		}
	}
	my $output = "";
	my $count = 0;
	foreach my $bcode (keys %notmoved)
	{
		$output.="\"$bcode\",";
		print "Moving to catchall: $bcode\n";
		moveAssetCopy($dbHandler,$bcode,$catchallBIBid,$log);
		$count++;
	}
	$output = substr($output,0,-1);
	$holdingsmove->addLine("\"$totalMarc MARC / $loops Items moved from bib to bib / $count leftover to move\"");
	$holdingsmove->addLine("\"Barcodes moved to catchall: $count\",$output");
	
}

sub getMECatchAllBib
{
	my $dbHandler = @_[0];
	my $log = @_[1];
	my $catchallbibid=0;
	my $bibsourceid = getbibsource($dbHandler);
	my $query = "SELECT ID FROM BIBLIO.RECORD_ENTRY WHERE tcn_source='backstage_catchall'";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{	
		my $row = $_;
		my @row = @{$row};
		$catchallbibid=@row[0];
	}
	if($catchallbibid==0)
	{
		my $starttime = time;
		my $marc = MARC::Record->new();
		my $field =  MARC::Field->new(
			245, '1', '0',
			'a' => 'Backstage catch all holdings',
			'c' => 'MOBIUS'
			);
		my @all = ($field);
		$marc->insert_fields_ordered( @all );
		my $thisXML = convertMARCtoXML($marc);
		my $query = "INSERT INTO BIBLIO.RECORD_ENTRY(fingerprint,last_xact_id,marc,quality,source,tcn_source,owner,share_depth) VALUES(null,'IMPORT-$starttime',\$\$$thisXML\$\$,null,$bibsourceid,E'backstage_catchall',null,null)";
		$log->addLine($query);
		my $res = $dbHandler->update($query);
		my $query = "SELECT ID FROM BIBLIO.RECORD_ENTRY WHERE tcn_source='backstage_catchall'";
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{	
			my $row = $_;
			my @row = @{$row};
			$catchallbibid=@row[0];
		}
	}
	
	return $catchallbibid;
	
}

sub findArrayDifference
{
	my @mebarcodes = @{@_[0]};
	my @backbarcodes = @{@_[1]};
	my $ret='';	
	foreach(@mebarcodes)
	{
		my $found=0;
		my $mebarcode = $_;
		foreach(@backbarcodes)
		{
			if($mebarcode eq $_)
			{
				$found=1;
			}
		}
		if(!$found)
		{
			$ret.="$mebarcode,";
		}
	}
	$ret = substr($ret,0,-1);
	return $ret;
}

sub moveAssetCopy
{
	my $dbHandler = @_[0];
	my $barcode = @_[1];	
	my $newbib = @_[2];
	my $log = @_[3];
	my $query = "select deleted from biblio.record_entry where id=$newbib";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{	
		my $row = $_;
		my @row = @{$row};
		print "$newbib - ".@row[0]."\n";
		if(@row[0] eq 't' ||@row[0] == 1)
		{
			my $tcn_value = $newbib;
			my $count=1;			
			while($count>0)
			{
				$query = "select count(*) from biblio.record_entry where tcn_value='$tcn_value' and id!=$newbib";
				my @results = @{$dbHandler->query($query)};
				foreach(@results)
				{	
					my $row = $_;
					my @row = @{$row};
					$count=@row[0];
				}
				$tcn_value.="_";
			}
			$query = "update biblio.record_entry set deleted='f',tcn_source='backstage',tcn_value='$tcn_value'  where id=$newbib";
			$dbHandler->update($query);
		}
	}
	$query = "UPDATE ASSET.CALL_NUMBER SET RECORD=$newbib WHERE id =(SELECT call_number FROM ASSET.COPY WHERE BARCODE='$barcode')";
	$log->addLine($query);
	my $result = $dbHandler->update($query);
	return $result;
	
}

sub attachHoldings
{
	my $marc = @_[0];
	my $dbHandler = @_[1];
	my @oldorder = @{@_[2]};
	if($marc->field('901'))
	{
		if($marc->field('901')->subfield('a'))
		{
			my $bibID = $marc->field('901')->subfield('a');
			my $order;
			my @holdings = 
			my $query = "SELECT aou_own.shortname,aou_circ.shortname,AC.PRICE,AC.BARCODE,AC.CIRC_MODIFIER,ACN.LABEL, acl.name FROM ASSET.COPY ac,asset.call_number acn, asset.copy_location acl, actor.org_unit aou_own, actor.org_unit aou_circ
						  where 
						ac.CALL_NUMBER =acn.id and
						acn.record=$bibID and
						acn.owning_lib=aou_own.id and
						ac.circ_lib=aou_circ.id and
						acl.id=ac.location and
						ac.deleted='f'";
			my @results = @{$dbHandler->query($query)};
			my %holdings;
			my %found;
			foreach(@results)
			{	
				my $row = $_;
				my @row = @{$row};
				my $field = MARC::Field->new('852','4', '', 'b' => @row[0], 'b'=>@row[1], 'c'=>@row[6], 'j'=>@row[5], 'g'=>@row[4], 'p'=>@row[3], 'y'=>"\$".@row[2]  );
				$holdings{@row[3]}=$field;
			}
			my @all;
			foreach(@oldorder)
			{
				print "$_\n";
				if($holdings{$_})
				{
					print "Adding $_\n";
					push(@all,$holdings{$_});
					$found{$_}=1;
				}
			}
			my $bcode;
			my $field;
			while (($bcode, $field) = each(%holdings))
			{
				if(!$found{$bcode})
				{
					print "Didnt find $bcode\n";
					push(@all,$field);
				}
			}			
			$marc->insert_fields_ordered( @all );
			
		}
	}
	return $marc;
}

sub getMEMARC
{
	my $dbID = @_[0];
	my $dbHandler = @_[1];
	my $query = "SELECT MARC FROM BIBLIO.RECORD_ENTRY WHERE ID=$dbID";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $marc = @row[0];
		return $marc;
	}
	return 0;
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
	my @ids;
	if(@_[2])
	{	
		@ids=@{@_[2]};
	}
	else
	{
		@ids = @{identifyBibsToScore($dbHandler)};
	}
	$log->addLine("Found ".($#ids+1)." Bibs to be scored");
	foreach(@ids)
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
		if(@thisone[2])	#these are updates
		{
			my $bibscoreid = @thisone[2];
			my $oldscore = @thisone[3];
			my $improved = $score - $oldscore;
			$query = "UPDATE SEEKDESTROY.BIB_SCORE SET IMPROVED_SCORE_AMOUNT = $improved, SCORE = $score, SCORE_TIME=NOW(), ELECTRONIC=$electricScore WHERE ID=$bibscoreid";			
		}
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
	my @ret=();
	my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE ID NOT IN(SELECT RECORD FROM SEEKDESTROY.BIB_SCORE) LIMIT 100";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $id = @row[0];
		my $marc = @row[1];
		my @temp = ($id,$marc);
		push (@ret, [@temp]);
	}
	$query = "SELECT SBS.RECORD,BRE.MARC,SBS.ID,SCORE FROM SEEKDESTROY.BIB_SCORE SBS,BIBLIO.RECORD_ENTRY BRE WHERE SBS.score_time < BRE.EDIT_DATE AND SBS.RECORD=BRE.ID";
	@results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $rec = @row[0];
		my $id = @row[1];
		my $marc = @row[2];
		my $score = @row[3];
		my @temp = ($rec,$id,$marc,$score);
		push (@ret, [@temp]);
	}
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

sub importMARCintoEvergreen
{
	my @ret;
	my @worked;
	my @notworked;
	my $inputFile = @_[0];
	my $log = @_[1];
	my $dbHandler = @_[2];
	my $mobUtil = @_[3];
	my $bibsourceid = @_[4];
	my $file = MARC::File::USMARC->in( $inputFile );
	my $r =0;		
	my $overlay = 0;
	my $query;
	print "Working on importMARCintoEvergreen\n";
	my @updated;
	my %leadercount;
	my %z07count;
	while ( my $marc = $file->next() ) 
	{
		
		if($overlay<16)
		{
			#my $tcn = getTCN($log,$dbHandler);  #removing this because it has an auto created value in the DB
			
			my $sha1 = Digest::SHA1->new;			
			my $title;
			my $zero01;
			#see if it is already in the database
			
			my $i245h;
			my $z07;
			my $leader = substr($marc->leader(),5,3);
			if($marc->field('007'))
			{
				$z07 = substr($marc->field('007')->data(),0,6);
				$sha1->add($marc->field('007')->data());
			}
			if($marc->field('245')->subfield("h"))
			{
				$i245h =  $marc->field('245')->subfield("h");
				$sha1->add($marc->field('245')->subfield("h"));
			}
			if($marc->field('001')->data())
			{
				$zero01 =  $marc->field('001')->data();
				$sha1->add($marc->field('001')->data());
			}
			if($marc->field('245')->subfield("a"))
			{
				$title =  $marc->field('245')->subfield("a");
				$sha1->add($marc->field('245')->subfield("a"));
			}
			print "Importing $title\n";
			$sha1 = $sha1->hexdigest;			
			$marc = readyMARCForInsertIntoME($marc);
			my $bibid=-1;
			my $bibid = findRecord($marc, $dbHandler,$sha1);
			if($leadercount{$leader})
			{
				$leadercount{$leader}++;
			}
			else
			{
				$leadercount{$leader}=1;
			}
			if($z07count{$z07})
			{
				$z07count{$z07}++;
			}
			else
			{
				$z07count{$z07}=1;
			}
			if($bibid!=-1) #already exists so update the marc
			{
				my @present = @{$bibid};
				my $id = @present[0];
				my $prevmarc = @present[1];
				$prevmarc =~ s/(<leader>.........)./${1}a/;			
				$prevmarc = MARC::Record->new_from_xml($prevmarc);
				my $led = substr($prevmarc->leader(),6,1);
				my $found=0;
				if($led eq 'a')
				{
					$found=1;
				}
				if(!$found)
				{
					
				}
				else
				{	
					$prevmarc = mergeMARC856($prevmarc,$marc,$log);
					$prevmarc = fixLeader($prevmarc);
					my $thisXML = convertMARCtoXML($prevmarc);
					$query = "UPDATE BIBLIO.RECORD_ENTRY SET marc=\$\$$thisXML\$\$,tcn_source=E'script $sha1',source=$bibsourceid WHERE ID=$id";
					$log->addLine($query);
					$log->addLine("http://missourievergreen.org/eg/opac/record/$id?query=yellow;qtype=keyword;locg=4;expand=marchtml#marchtml");
					$log->addLine("http://mig.missourievergreen.org/eg/opac/record/$id?query=yellow;qtype=keyword;locg=157;expand=marchtml#marchtml");
					my $res = $dbHandler->update($query);
					print "$res";
					# my @compare = @{$mobUtil->compare2MARCObjects($marc,1,$prevmarc,1)};
					# foreach(@compare)
					# {
						# $log->addLine($_);
					# }
					if($res)
					{
						my @temp = ($id,$title);
						push @updated, [@temp];
						$overlay++;
					}
					else
					{
						push (@notworked, $id);
					}
				}
			}
			else  ##need to insert new bib instead of update
			{
				my $starttime = time;
				my $max = getEvergreenMax($dbHandler);
				my $thisXML = convertMARCtoXML($marc);
				
				$query = "INSERT INTO BIBLIO.RECORD_ENTRY(fingerprint,last_xact_id,marc,quality,source,tcn_source,owner,share_depth) VALUES(null,'IMPORT-$starttime',E'$thisXML',null,$bibsourceid,E'script ".$sha1->hexdigest.",null,null)";
				$log->addLine($query);
				#my $res = $dbHandler->update($query);
				#print "$res";
				my $newmax = getEvergreenMax($dbHandler);
				if($newmax != $max)
				{
					my @temp = ($newmax,$title);
					push @worked, [@temp];
					$log->addLine("http://mig.missourievergreen.org/eg/opac/record/$newmax?query=yellow;qtype=keyword;locg=157;expand=marchtml#marchtml");
				}
				else
				{
					push (@notworked, $marc);
				}
			}
			
			undef $sha1;
		}
		$r++;
	}
	$log->addLine("Leader Breakdown:");
	while ((my $internal, my $value ) = each(%leadercount))
	{
		$log->addLine("$internal $value");
	}
	$log->addLine("007 Breakdown:");
	while ((my $internal, my $value ) = each(%z07count))
	{
		$log->addLine("$internal $value");
	}
	
	push(@ret, (\@worked, \@notworked, \@updated));
	#print Dumper(@ret);
	return \@ret;
	
}

sub findRecord
{
	my $marcsearch = @_[0];
	my $zero01 = $marcsearch->field('001')->data();
	my $dbHandler = @_[1];
	my $sha1 = @_[2];
	my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE tcn_source LIKE '%$sha1%'";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $id = @row[0];
		my $marc = @row[1];
		print "found matching sha1: $id\n";
		my @ret = ($id,$marc);
		return \@ret;
	}
	my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE MARC LIKE '%$zero01%'";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $id = @row[0];
		print "found matching 001: $id\n";
		my $marc = @row[1];
		my @ret = ($id,$marc);
		return \@ret;
	}
	
	return -1;
}

sub readyMARCForInsertIntoME
{
	my $marc = @_[0];
	$marc = fixLeader($marc);	
	my $lbyte6 = substr($marc->leader(),6,1);
	
	my $two45 = $marc->field('245');
	my @e856s = $marc->field('856');
	
	if($two45)
	{
		$two45->delete_subfield(code => 'h');
		$two45->add_subfields('h' => "[Overdrive downloadable item] /");
		if(@e856s)
		{
			foreach(@e856s)
			{
				my $thisfield = $_;
				$thisfield->delete_subfield(code => 'z');					
				$thisfield->add_subfields('z'=> "Click for access to the downloadable item via Overdrive");
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
		
		# Just read the first $u and $z
		my $u = $thisField->subfield("u");
		my $z = $thisField->subfield("z");
		
		if($u) #needs to be defined because its the key
		{
			if(!$urls{$u})
			{
				$urls{$u} = $thisField;
			}
			else
			{
				my @nines = $thisField->subfield("9");
				my $otherField = $urls{$u};
				my @otherNines = $otherField->subfield("9");
				my $otherZ = $otherField->subfield("z");		
				if(!$otherZ)
				{
					if($z)
					{
						$otherField->add_subfields('z'=>$z);
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
					if($found==0)
					{
						$otherField->add_subfields('9' => $looking);
					}
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
	$log->addLine("Removing ".$#remove." 856 records");
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
	my $thisXML =  decode_utf8($marc->as_xml());		
	#this code is borrowed from marc2bre.pl
	$thisXML =~ s/\n//sog;
	$thisXML =~ s/^<\?xml.+\?\s*>//go;
	$thisXML =~ s/>\s+</></go;
	$thisXML =~ s/\p{Cc}//go;
	$thisXML = OpenILS::Application::AppUtils->entityize($thisXML);
	$thisXML =~ s/[\x00-\x1f]//go;
	$thisXML =~ s/^\s+//;
	$thisXML =~ s/\s+$//;
	$thisXML =~ s/<record><leader>/<leader>/;
	$thisXML =~ s/<collection/<record/;	
	$thisXML =~ s/<\/record><\/collection>/<\/record>/;
	#end code
	return $thisXML;
}

sub getbibsource
{
	my $dbHandler = @_[0];
	my $query = "SELECT ID FROM CONFIG.BIB_SOURCE WHERE SOURCE = 'backstage'";
	my @results = @{$dbHandler->query($query)};
	if($#results==-1)
	{
		print "Didnt find backstage in bib_source, now creating it...\n";
		$query = "INSERT INTO CONFIG.BIB_SOURCE(QUALITY,SOURCE) VALUES(90,'backstage')";
		my $res = $dbHandler->update($query);
		print "Update results: $res\n";
		$query = "SELECT ID FROM CONFIG.BIB_SOURCE WHERE SOURCE = 'backstage'";
		my @results = @{$dbHandler->query($query)};
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

sub findMatchInArchive
{
	my @matchList = @{@_[0]};
	my $archiveFolder = @_[1];
	my @files;
	#Get all files in the directory path
	@files = @{dirtrav(\@files,$archiveFolder)};
	for my $b(0..$#files)
	{
		my $file = MARC::File::USMARC->in($files[$b]);
		while ( my $marc = $file->next() ) 
		{	
			my $t = $marc->leader();
			my $su=substr($marc->leader(),6,1);
			print "Leader:\n$t\n$su\n";
			my $leader = substr($marc->leader(),6,1);			
			my $all = $marc->as_formatted();
			foreach(@matchList)
			{
				if($all =~ m/$_/g)
				{
					my @booya = ($files[$b]);
					print "This one: ".$files[$b]." matched $_\n";
					return \@booya;
				}
			}
		}
	}
}

sub dirtrav
{
	my @files = @{@_[0]};
	my $pwd = @_[1];
	opendir(DIR,"$pwd") or die "Cannot open $pwd\n";
	my @thisdir = readdir(DIR);
	closedir(DIR);
	foreach my $file (@thisdir) 
	{
		if(($file ne ".") and ($file ne ".."))
		{
			if (-d "$pwd/$file")
			{
				push(@files, "$pwd/$file");
				@files = @{dirtrav(\@files,"$pwd/$file")};
			}
			elsif (-f "$pwd/$file")
			{
				push(@files, "$pwd/$file");
			}
		}
	}
	return \@files;
}

sub fixLeader
{
	my $marc = @_[0];
	my $fullLeader = $marc->leader();
	if(substr($fullLeader,6,1) eq 'a')
	{
		#print "Leader has an a:\n$fullLeader";
		$fullLeader = substr($fullLeader,0,6).'m'.substr($fullLeader,7);
		$marc->leader($fullLeader);
		my $fullLeader = $marc->leader();
		#print "Changed to:\n$fullLeader";
	}
	return $marc;
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
		electronic boolean default false)";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.bib_marc_update(
		id serial,
		record bigint,
		prev_marc text,
		changed_marc text,
		change_time timestamp default now())";
		$dbHandler->update($query);
	}
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

 
 