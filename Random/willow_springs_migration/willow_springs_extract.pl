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
use Getopt::Long;
use pQuery;
use Text::Levenshtein qw(distance);


# http://webservices.amazon.com/onca/xml?Service=AWSECommerceService&AWSAccessKeyId=AKIAIZIKAPTPQA6V3WOA&Operation=ItemLookup&ItemId=0374343888&IdType=ISBN&Signature=PZK7NX2VLDPBZQYVWQWUGCZWDXSNN3JX&Timestamp=2015-04-23T12:58:44Z
# &Timestamp=[YYYY-MM-DDThh:mm:ssZ]
# &Signature=[Request Signature]


my $configFile;

	our $mobUtil = new Mobiusutil();  
	
	our $log;
	our $dbHandlerFirebird;
	our $dbHandler;
	our $jobid=-1;
	our %queries;
	our $baseTemp = "/mnt/evergreen/tmp";
	our $marcFinalOut = "/mnt/evergreen/tmp/test/willowsprings.xml";	
	our $marcFinalOutMRC = "/mnt/evergreen/tmp/test/willowsprings.mrc";	
	our @writeMARC = ();

	my $dt = DateTime->now(time_zone => "local"); 
	my $fdate = $dt->ymd; 
	my $ftime = $dt->hms;
	my $dateString = "$fdate $ftime";
	$log = new Loghandler("/mnt/evergreen/migration/willow_springs/log/extract.log");
	$log->truncFile("");
	$log->addLogLine(" ---------------- Script Starting ---------------- ");
	
	$dbHandlerFirebird = new DBhandler("c:/bla/Libsoft.gdb","192.168.12.99","SYSDBA","masterkey","3050",1);
	$dbHandler = new DBhandler("evergreen","192.168.12.176","evergreen","database","5432",0);
	
	#extractMARC();
	#breakMARC();
	#findmissing();
	
	extractPatrons();
	extractCheckouts();
	my $afterProcess = DateTime->now(time_zone => "local");
	my $difference = $afterProcess - $dt;
	my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
	my $duration =  $format->format_duration($difference);
	
	$log->addLogLine("Duration: $duration");
	$log->addLogLine(" ---------------- Script Ending ---------------- ");

sub findmissing
{
	my $query = "select ISBN,IBARCODE
		FROM itemopac 
		";
		
	$log->addLine($query);
	
	my @results = @{$dbHandlerFirebird->query($query)};
	my $inserts = "";
	print "Got: ".$#results." ISBN RECORDS\n";
	my @isbns = ();
	my %barcodes = ();
	my $file = my $file = MARC::File::USMARC->in( "/mnt/evergreen/tmp/temp/all_with51.mrc" );		
	while ( my $marc = $file->next() ) 
	{
		my @i020 = $marc->field('02.');
		foreach(@i020)
		{
			my $field = $_;
			my @subs = $field->subfield('a');
			foreach(@subs)
			{
				push(@isbns, $_);
			}
		}
		my @i020 = $marc->field('902');
		foreach(@i020)
		{
			my $field = $_;
			my @subs = $field->subfield('b');
			foreach(@subs)
			{
				my $thisb = $_;
				if($barcodes{$thisb})
				{
					$barcodes{$thisb}++;
					$log->addLine("Barcode appears more than once: $thisb");
				}
				else
				{
					$barcodes{$thisb}=1;
				}
			}
		}
	}
	$file->close();

	$log->addLine("Read ".$#isbns." isbn's from all.mrc");
	my $blankISBN = 0;
	my @found = ();
	my @notFound = ();
	my @bfound = ();
	my @bnotFound = ();
	
	foreach(@results)
	{
		my @row=@{$_};
		my $isbn = @row[0];
		my $ibcode = @row[1];
		$isbn =~ s/\D//g;
		my $finalSelection=undef;
		if(length($isbn)>0)
		{	
			if(length($isbn)>13)
			{
				$isbn = substr($isbn,0,13);
			}
			if( (length($isbn)==13) && (!(substr($isbn,0,3) =~ m/97[89]/)) )
			{
				my $out = "$isbn <- chop -> ";
				$isbn = substr($isbn,0,10);
				$log->addLine($out.$isbn);
			}
			# if($isbn eq '9780448464787')
			# {
				#$log->addLine("ISBN: ".@row[0]);
				my $notfound=1;
				my $i=0;
				while($i<$#isbns)
				{
					my $thisone = @isbns[$i];
					if($notfound)
					{	
						if(length($mobUtil->trim($thisone))>0)
						{
							#$log->addLine("comparing: $thisone to $isbn");
							if($thisone =~ m/$isbn/)
							{
								$notfound=0;
								push(@found,$isbn);
								#$log->addLine("FOUND");
							}
						}
					}
					$i++;
				}
				if($notfound)
				{
					push(@notFound, $isbn);
					$log->addLine("ISBN: ".@row[0]);
					$log->addLine("NOT FOUND");
				}
			# }
		}
		else
		{
			#$log->addLine("ISBN is blank");
			$blankISBN++;
		}
		if($barcodes{@row[1]})
		{
			push(@bfound,@row[1]);
		}
		else
		{
			push(@bnotFound,@row[1]);
		}
		
	}
	$log->addLine("Total blanks: $blankISBN");
	$log->addLine("Total found: ".$#found);
	$log->addLine("Total not found: ".$#notFound);
	$log->addLine("Total barcode found: ".$#bfound);
	$log->addLine("Total barcodes not found: ".$#bnotFound);
	my $output="";
	foreach(@bnotFound)
	{
		$output.="$_\n";
	}
	$log->addLine("Barcodes not found:\n$output");	
	my $output="";
	foreach(@notFound)
	{
		$output.="$_\n";
	}
	$log->addLine("ISBNS not found:\n$output");
	
}

sub breakMARC
{
	my $file = MARC::File::XML->in( "/mnt/evergreen/tmp/Willow Springs MARC Extract/willow_temp50.xml" );
	my $rcount=0;
	my $fmarc;
	while ( my $marc = $file->next() ) 
	{
		my $filename = $mobUtil->chooseNewFileName("/mnt/evergreen/tmp","willow_temp50_break","mrc");
		my $break = new Loghandler($filename);
		$break->appendLine($marc->as_usmarc());
	}
	$file->close();
}

sub reportMissingISBN
{
		
	###################
	# Missing ISBN
	###################
	
	my $query = "select ICHARCODE,AUTHOR1,AUTHOR2,TITLE,CALLNO,EDITION,PRICE,DUEDATE,CHECKOUTDATE FROM itemopac where trim(isbn)=''";
	
	$log->addLine($query);
	my @results = @{$dbHandlerFirebird->query($query)};
	my $inserts = "";
	print "Got: ".$#results." NON ISBN RECORDS\n";
	foreach(@results)
	{
		my @row=@{$_};
		my $insertLine = '';
		my $count=0;
		foreach(@row)
		{
			my $l = $mobUtil->trim($_);
			#$l =~ s/,/;/g;
			$insertLine.='"'.$l.'";';
			$count++;
		}
		#$log->addLine(@row[0]." had $count");
		$inserts .= substr($insertLine,0,-1)."
";
	}
	$inserts = substr($inserts,0,-1);
	my $header = "ICHARCODE,AUTHOR1,AUTHOR2,TITLE,CALLNO,EDITION,PRICE,DUEDATE,CHECKOUTDATE";
	$header =~ s/,/;/g;
	$inserts=$header."\n".$inserts;
	$log->addLine($inserts);
}

sub reportCallNumbers
{	
	my $query = "select CALLNO,COUNT(*) FROM itemopac where trim(ISBN)!='' GROUP BY CALLNO";
	
	$log->addLine($query);
	my @results = @{$dbHandlerFirebird->query($query)};
	my $inserts = "";
	print "Got: ".$#results." callnumbers\n";
	foreach(@results)
	{
		my @row=@{$_};
		my $call = @row[0];
		my $insertLine = '';
		my $count=0;
		my $format = determineFormatFromCallNumber($mobUtil->trim($call));
		foreach(@row)
		{
			my $l = $mobUtil->trim($_);
			$insertLine.='"'.$l.'";';
			$count++;
		}
		$insertLine.='"'.$format.'";';
		#$log->addLine(@row[0]." had $count");
		$inserts .= substr($insertLine,0,-1)."
";
	}
	$inserts = substr($inserts,0,-1);
	my $header = "CALLNO,COUNT,FORMAT";
	$header =~ s/,/;/g;
	$inserts=$header."\n".$inserts;
	$log->addLine($inserts);
}

sub extractMARC
{

	my $thisloopcount=1;
	my $loops = 0;
	my $count=0;
	my $notfounds = 0;
	my $chunkSize = 500;
	
	my @outputFiles = ();
	
	my %matches = ("z3950"=>0,"chopac"=>0,"evergreen"=>0,"flatMarc"=>0,"original"=>0,"notfound"=>0,"alreadyGathered"=>0,"originalnoisbn"=>0);
	populateMARCMemory();
	while($thisloopcount!=0)
	{
		$thisloopcount=0;
		my $startNumber=$loops*$chunkSize;
		my $query = "select FIRST $chunkSize SKIP $startNumber ISBN,CALLNO,IBARCODE,EDITION,TITLE,
		AUTHOR1,AUTHOR2,AUTHOR3,AUTHOR4,
		SUBJECT1,SUBJECT2,SUBJECT3,SUBJECT4,SUBJECT5,SUBJECT6,
		EDITION,
		PUBLISHER,
		PLACEPUBL,
		extract(year from PUBLDATE),
		STATUSID,
		PRICE,
		NOTE,
		SUBNOTE,
		extract(year from INVDATE)||
	'-'|| extract(month from INVDATE)||
	'-'|| extract(day from INVDATE), 
		MARC
		
		
		FROM itemopac 
		--where
--		trim(callno) NOT SIMILAR TO '_*CD_*' and
		--(
		--trim(ISBN) SIMILAR TO '9781934454039'
		--'0881664278068'
		--OR
		--TRIM(AUTHOR1) SIMILAR TO 'Collins, Suzanne'
		--)
		";
		
		$log->addLine($query);
		
		my @results = @{$dbHandlerFirebird->query($query)};
		my $inserts = "";
		print "Got: ".$#results." ISBN RECORDS\n";		
		foreach(@results)
		{
			$thisloopcount++;
			my @row=@{$_};
			my $isbn = @row[0];
			$isbn =~ s/\D//g;
			my $finalSelection=undef;
			my $desiredFormat = determineFormatFromCallNumber($mobUtil->trim(@row[1]));
			if(length($isbn)>0)
			{	
				if(length($isbn)>13)
				{
					$isbn = substr($isbn,0,13);
				}
				if( (length($isbn)==13) && (!(substr($isbn,0,3) =~ m/97[89]/)) )
				{
					my $out = "$isbn <- chop -> ";
					$isbn = substr($isbn,0,10);
					$log->addLine($out.$isbn);
				}
				$log->addLine("ISBN: ".@row[0]."\nTitle: ".$mobUtil->trim(@row[4])."\nAuthor: ".$mobUtil->trim(@row[5])."\nCall: ".$mobUtil->trim(@row[1]));
				my $alreadyGotit = lookupInGatheredMARC($isbn,\@row,$desiredFormat);
				if($alreadyGotit==-1)
				{
					my @back = @{getMARCBasedOnISBN($isbn,\@row,$desiredFormat,\%matches)};
					$finalSelection = @back[0];
					my $fromSource = @back[2];
					my $ti = '(none)';
					my $au = '(none)';
					local $@;
					eval{
						$ti = $finalSelection->field('245')->subfield('a');
					};
					local $@;
					eval{
						$au = $finalSelection->field('100')->subfield('a');					
					};
					
					my $tidistance = distance(normalizeText($ti),normalizeText($mobUtil->trim(@row[4])));
					my $audistance = distance(normalizeText($au),normalizeText($mobUtil->trim(@row[5])));
					my $added = $tidistance+$audistance;
					
					$log->addLine("\"Look at this!!\";\"".
					$tidistance."\";\"".
					$audistance."\";\"".
					$added."\";\"".
					$mobUtil->trim(@row[4])."\";\"".
					$ti."\";\"".
					$mobUtil->trim(@row[5])."\";\"".					
					$au."\";\"".
					$fromSource."\";\"".
					$isbn."\";\"".
					$mobUtil->trim(@row[1])."\";\"".
					$mobUtil->trim(@row[2])."\"");
					%matches = %{@back[1]};
				}
				else
				{
					$matches{"alreadyGathered"}++;
				}
			}
			else
			{
				$finalSelection = constructMARCFromWillowSpringsData($desiredFormat,\@row);
				if($finalSelection)
				{
					$matches{"originalnoisbn"}++;
					$log->addLine($finalSelection->as_formatted());
					$log->addLine("Orignal no ISBN: $isbn ".@row[4]);
				}
				else
				{
					$matches{"notfound"}++;
					$log->addLine("NO RECORDS FOUND $isbn ".@row[4]);
				}
			}
			if($finalSelection)
			{
				my $itemField = create902ItemField(\@row);
				$finalSelection->insert_grouped_field( $itemField );
				$finalSelection->encoding("UTF-8");
				#re-ingest the leader, im getting null values in there sometimes (z39 results)
				my $thisLeader = $finalSelection->leader();
				$thisLeader =~ s/[\x80-\x{FFFF}]/ /;
				$finalSelection->leader($thisLeader);
				push(@writeMARC,$finalSelection);
			}
			$count++;
			print "$count\n";
		}
		if(@writeMARC > 0)
		{
		print "writing to xml\n";
			my $tfile = $mobUtil->chooseNewFileName("/mnt/evergreen/tmp","willow_temp","xml");
			my $outputxmlfile = MARC::File::XML->out( $tfile );
			foreach(@writeMARC)
			{
				#$_->encoding( 'UTF-8' );
				#$marcout.=$_->as_usmarc();
				$outputxmlfile->write( $_ );
			}
			$outputxmlfile->close();
			push(@outputFiles,$tfile);
			@writeMARC=();
			$loops++;
			while ((my $internal, my $mvalue ) = each(%matches))
			{	
				$log->addLine("$internal: ".$matches{$internal});
			}
		}
	}
	$log->addLine("Total: $count");
	while ((my $internal, my $mvalue ) = each(%matches))
	{	
		$log->addLine("$internal: ".$matches{$internal});
	}
	
	my $marcoutfile = new Loghandler($marcFinalOut);
	$marcoutfile->deleteFile();
	my $outputxmlfile = MARC::File::XML->out( $marcFinalOut );
	foreach(@writeMARC)
	{
		$_->encoding( 'UTF-8' );
		#$marcout.=$_->as_usmarc();
		$outputxmlfile->write( $_ );
	}
	$outputxmlfile->close();
	#Convert to mrc just for extra options
	my $cmd = "yaz-marcdump -i marcxml -o marc /mnt/evergreen/tmp/willow_temp*.xml > $marcFinalOutMRC";
	$log->addLine("Executing:\n$cmd");	
	system($cmd);
}

sub normalizeText
{
	my $input = @_[0];
	if($input)
	{
		$input = NFD($input);
		$input =~ s/[\x{80}-\x{ffff}]//go;
		$input = lc($input);
		$input =~ s/\W+$//go;
		return $input;
	}
	return undef;
}

sub getMARCBasedOnISBN
{
	my $isbn = @_[0];
	my @row = @{@_[1]};
	my $desiredFormat = @_[2];
	my %matches = %{ @_[3] };
	my @ret;
	my $fromSource = '';
	my $finalSelection = lookupInEvergreen($isbn, $desiredFormat);
	if($finalSelection)
	{
		$matches{"evergreen"}++;
		$log->addLine("evergreen: $isbn");
		$fromSource="evergreen";
	}
	else
	{
		$finalSelection = getMARCFromZ3950($isbn, $desiredFormat);
		if($finalSelection)
		{
			$matches{"z3950"}++;
			$log->addLine("z3950: $isbn");
			$fromSource="z3950";
		}
		else
		{
			$finalSelection = chopacScrape($isbn, $desiredFormat);
			if($finalSelection)
			{
				$matches{"chopac"}++;
				$log->addLine("chopac: $isbn");
				$fromSource="chopac";
			}
			else
			{
				$finalSelection = convertFlatMARC(@row[24], $desiredFormat);
				if($finalSelection)
				{
					$matches{"flatMarc"}++;
					$log->addLine("flatMarc: $isbn");
					$fromSource="flatMarc";
				}
				else
				{
					$finalSelection = constructMARCFromWillowSpringsData($desiredFormat, \@row);
					if($finalSelection)
					{
						$matches{"original"}++;
						$log->addLine($finalSelection->as_formatted());
						$log->addLine("Orignal: $isbn");
						$fromSource="original";
					}
					else
					{
						$matches{"notfound"}++;
						$log->addLine("NO RECORDS FOUND");
						$fromSource="notfound";
					}
				}
			}
			
		}
	}
	@ret = ($finalSelection, \%matches,$fromSource);
	return \@ret;
}

sub extractPatrons
{
	
	###################
	# Patron file
	###################
	
	my $query = "DROP TABLE IF EXISTS m_wspl.patrons_file";
	$log->addLine($query);
	$dbHandler->update($query);
	my $query = "CREATE table m_wspl.patrons_file (
	NAME text,ADDRESS text,CITY text,STATE text,ZIP text,
PHONE text, EMAIL text, CONTACT text, SEX text, PATRONGROUPID text, AMTPAID text, FINE text, ALERT text, BIRTH text, MESSAGES text, PBARCODE text, PCHARCODE text,
FIRST_NAME text, LAST_NAME text, MIDDLE_NAME text,deleted boolean default false
)";
	$log->addLine($query);
	$dbHandler->update($query);
	my $query = "select NAME,ADDRESS,CITY,STATE,ZIP,PHONE,EMAIL,CONTACT,SEX,
	(select patrongroup from Patrongroup where id=a.PATRONGROUPID),AMTPAID,FINE,ALERT,
	extract(year from BIRTH)||
	'-'|| extract(month from BIRTH)||
	'-'|| extract(day from BIRTH)
,MESSAGES,PBARCODE,PCHARCODE from PATRON a";
	
	$log->addLine($query);
	my @results = @{$dbHandlerFirebird->query($query)};
	my $inserts = "";
	print "Got: ".$#results." Patrons\n";
	foreach(@results)
	{
		my @row=@{$_};
		my $insertLine = '';
		my $count=0;
		foreach(@row)
		{
			my $l = $mobUtil->trim($_);
			$l =~ s/\$\$/\\\$\\\$/g;
			#$l =~ s/#/\\#/g;
			$insertLine.='$$'.$l.'$$,';
			$count++;
		}
		#$log->addLine(@row[0]." had $count");
		$inserts .= "(".substr($insertLine,0,-1)."),
";
	}
	$inserts = substr($inserts,0,-2);
	$query = "INSERT INTO m_wspl.patrons_file (NAME,ADDRESS,CITY,STATE,ZIP,PHONE,EMAIL,CONTACT,SEX,PATRONGROUPID,AMTPAID,FINE,ALERT,BIRTH,MESSAGES,PBARCODE,PCHARCODE)
	values 
	$inserts
	";
	$log->addLine($query);
	$dbHandler->update($query);
	
}

sub extractCheckouts
{
	###################
	# Checkouts
	###################
	
	my $query = "DROP TABLE IF EXISTS m_wspl.checkouts";
	$log->addLine($query);
	$dbHandler->update($query);
	my $query = "CREATE table m_wspl.checkouts (
	PBARCODE text,IBARCODE text,CHECKOUTDATE text,OLDSTATUS text,NEWSTATUS text,ITEMCHECKOUTDATE text,
ITEMSTATUSID text, ITEMBORROWER text, deleted boolean default false
)";
	$log->addLine($query);
	$dbHandler->update($query);
	my $query = "
select h.borrower,h.ibarcode,
extract(year from h.changes_date)||
	'-'|| extract(month from h.changes_date)||
	'-'|| extract(day from h.changes_date)
	,old_status,new_status
,iop.checkoutdate,iop.statusid,iop.borrower 
from history h,
(select max(changes_date) as \"changed\" ,ibarcode from history group by ibarcode) as a
,
itemopac iop
where
h.changes_date = a.\"changed\" and
h.IBARCODE= a.ibarcode and
iop.ibarcode=h.IBARCODE and
h.new_status!=1 
--and
--h.new_status<5 and
--iop.statusid!=h.new_status
 and 
iop.borrower = h.borrower

order by iop.borrower,changes_date desc,new_status desc
";
	
	$log->addLine($query);
	my @results = @{$dbHandlerFirebird->query($query)};
	my $inserts = "";
	print "Got: ".$#results." Checkouts\n";
	foreach(@results)
	{
		my @row=@{$_};
		my $insertLine = '';
		my $count=0;
		foreach(@row)
		{
			my $l = $mobUtil->trim($_);
			$l =~ s/\$\$/\\\$\\\$/g;
			#$l =~ s/#/\\#/g;
			$insertLine.='$$'.$l.'$$,';
			$count++;
		}
		#$log->addLine(@row[0]." had $count");
		$inserts .= "(".substr($insertLine,0,-1)."),
";
	}
	$inserts = substr($inserts,0,-2);
	$query = "INSERT INTO m_wspl.checkouts (PBARCODE,IBARCODE,CHECKOUTDATE,OLDSTATUS,NEWSTATUS,ITEMCHECKOUTDATE,ITEMSTATUSID,ITEMBORROWER)
	values 
	$inserts
	";
	$log->addLine($query);
	$dbHandler->update($query);
}

sub populateMARCMemory
{
	my $file = my $file = MARC::File::XML->in( $marcFinalOut );
	@writeMARC = ();
	if($file)
	{
		while ( my $marc = $file->next() ) 
		{
			push(@writeMARC, $marc);
		}
	}
}

sub lookupInGatheredMARC
{
	my $isbn = @_[0];
	my @row = @{@_[1]};
	my $desiredFormat = @_[2];
	my $id = -1;
	my $count=0;
	foreach(@writeMARC)
	{
		$count++;
		my $thisMarc = $_;
		my @is = $thisMarc->field("020");
		foreach(@is)
		{
			my @as = $_->subfield("a");
			foreach(@as)
			{
				if($_ =~ /$isbn/)
				{
					#my $marcxml = convertMARCtoXML($thisMarc);
					my $thisMARCFormat = determineEGFormatFromMARC($thisMarc);
					if($thisMARCFormat eq $desiredFormat)
					{	
						my @itemlines = $thisMarc->field("902");
						my $foundBarcode = 0;
						foreach(@itemlines)
						{
							my $barcode = $_->subfield("b");
							if($barcode eq @row[2])
							{
							
								$foundBarcode=1;
								$id=$count;
							}
						}
						if(!$foundBarcode)
						{
							$log->addLine("Matched in collected MARC $count");
							$log->addLine("adding ".@row[2]);
							$id=$count;
							my $itemField = create902ItemField(\@row);
							$thisMarc->insert_grouped_field( $itemField );
						}
					}
				}
			}
		}
	}
	return $id;
}
sub constructMARCFromWillowSpringsData
{
	my $desiredFormat = @_[0];
	my @row = @{@_[1]};
	my $marc = MARC::Record->new();
	$marc->encoding( 'UTF-8' );
	 # ISBN,CALLNO,IBARCODE,EDITION,TITLE,
	# AUTHOR1,AUTHOR2,AUTHOR3,AUTHOR4,
	# SUBJECT1,SUBJECT2,SUBJECT3,SUBJECT4,SUBJECT5,SUBJECT6,
	# EDITION, 15 
	# PUBLISHER,16
	# PLACEPUBL, 17
	# PUBLDATE, 18
	# STATUSID, 19
	# PRICE, 20
	# NOTE, 21
	# SUBNOTE, 22
	# extract(year from INVDATE)||
# '-'|| extract(month from INVDATE)||
# '-'|| extract(day from INVDATE),  23
	# MARC 24
		

	## Author
	if(length($mobUtil->trim(@row[5]))>0)
	{
		$marc->insert_grouped_field( createField("100",'a','1','',@row[5]) );
	}
	if(length($mobUtil->trim(@row[6]))>0)
	{
		$marc->insert_grouped_field( createField("700",'a','1','',@row[6]) );
	}
	if(length($mobUtil->trim(@row[7]))>0)
	{
		$marc->insert_grouped_field( createField("700",'a','1','',@row[7]) );
	}
	if(length($mobUtil->trim(@row[8]))>0)
	{
		$marc->insert_grouped_field( createField("700",'a','1','',@row[8]) );
	}
	
	## Subject
	if(length($mobUtil->trim(@row[9]))>0)
	{
		$marc->insert_grouped_field( createField("650",'a','1','4',@row[9]) );
	}
	if(length($mobUtil->trim(@row[10]))>0)
	{
		$marc->insert_grouped_field( createField("650",'a','1','4',@row[10]) );
	}
	if(length($mobUtil->trim(@row[11]))>0)
	{
		$marc->insert_grouped_field( createField("650",'a','1','4',@row[11]) );
	}
	if(length($mobUtil->trim(@row[12]))>0)
	{
		$marc->insert_grouped_field( createField("650",'a','1','4',@row[12]) );
	}
	if(length($mobUtil->trim(@row[13]))>0)	
	{
		$marc->insert_grouped_field( createField("650",'a','1','4',@row[13]) );
	}
	if(length($mobUtil->trim(@row[14]))>0)	
	{
		$marc->insert_grouped_field( createField("650",'a','1','4',@row[14]) );
	}
	
	my $pub = createField("250",'a','','',$mobUtil->trim(@row[16]));
	$pub->add_subfields('b' => $mobUtil->trim(@row[17]), 'c' => $mobUtil->trim(@row[18]));
	$marc->insert_grouped_field( $pub );
	$marc->insert_grouped_field( createField("245",'a','1','0',@row[4]) );
	$marc->insert_grouped_field( createField("250",'a','','',@row[3]) );
	$marc->insert_grouped_field( createField("020",'a','','',@row[0]) );
	my $leader = $marc->leader();
	$marc->leader($mobUtil->insertDataIntoColumn($leader,"cam",6));
	
	$log->addLine($marc->as_formatted());
	my @marcs = ($marc);
	return matchDesiredFormat(\@marcs,$desiredFormat);
}

sub chopacScrape
{
	my $isbn = @_[0];
	my $desiredFormat = @_[1];
	my $textarea = '';
	my $url = "http://chopac.org/cgi-bin/tools/az2marc.pl?kw=$isbn&ct=com&lc=0";
	pQuery($url)->find("textarea")->each(sub {
										$textarea = pQuery($_)->toHtml;
										my $output = "parsing: $textarea\n";
										my @s = split(/textarea.*>/,$textarea);
										$textarea=@s[1];
										$textarea = substr($textarea,0,-3);
										#$log->addLine($output);
										#$log->addLine($textarea);
									}
									);
								#print length($textarea);
	#152 would be a blank marc result
	#$log->addLine("The extracted chopac:\n$textarea");
	$log->addLine("length from chopac:".length($textarea));
	if(length($textarea) > 152)
	{
		use LWP::UserAgent; 
		my $ua = LWP::UserAgent->new;		 
		my $server_endpoint = "http://chopac.org/cgi-bin/tools/az2marc.pl";
		
		# set custom HTTP request header fields
		# my $textarea =~ s/\n//g
		my $response = $ua->post($server_endpoint,[ 'full' => $textarea, 'ext' => 'mrc', save=>'Export' ]);
		my $content = $response->content();
		if($content =~ /<title>500 Internal Server Error<\/title>/)
		{
			$log->addLine("chopac - 500 Internal Server Error");
			return undef;
		}
		my $marc = MARC::Record->new_from_usmarc( $content );
		my @f856 = $marc->field("856");
		foreach(@f856)
		{
			my $field = $_;
			my @sub = $field->subfield('3');
			foreach(@sub)
			{
				if($_ =~ /Amazon/)
				{
					$marc->delete_field($field);
				}
			}
		}		
		my @marcs = ($marc);
		#$log->addLine($content);
		return matchDesiredFormat(\@marcs,$desiredFormat);
	}
	return undef;
}

sub convertFlatMARC
{
	my $text = normalizeFlatMARC(@_[0]);
	if($text)
	{
		my $desiredFormat = @_[1];
		my $outputFile = $baseTemp."/flat.out";
		if(length($mobUtil->trim($text))>0)
		{
			$outputFile = new Loghandler($outputFile);
			#print $outputFile->getFileName()."\n";
			$outputFile->deleteFile();
			$outputFile->addLineRaw($text);
			$log->addLine("Executing\nyaz-marcdump -i line -t utf-8 -o marcxml ".$baseTemp."/flat.out > ".$baseTemp."/flat.xml");
			system("yaz-marcdump -i line -t utf-8 -o marcxml ".$baseTemp."/flat.out > ".$baseTemp."/flat.xml");
			my @marcs = ();
			#print "reading xml\n";
			my $file = MARC::File::XML->in( $baseTemp."/flat.xml" );		
			if($file)
			{
				my $rcount=0;
				my $fmarc;
				eval
				{
					while ( my $marc = $file->next() ) 
					{
						
						if($rcount==0)
						{
							$fmarc=$marc;
						}
						else
						{
							my @fields = $marc->fields();
							$fmarc->insert_fields_ordered( @fields );
						}					
						$rcount++;
					}
				};
				$file->close();
				my @marcs = ($fmarc);			
				if($@)
				{
					#there was an error, so return nothing
					#print "Error reading xml\n";
					return undef;
				}
				else
				{
					if($rcount>0)
					{
						#print "Success reading xml\n";
						return matchDesiredFormat(\@marcs,$desiredFormat);
					}
				}
			}
		}
	}
	#print "returning flatmarc undef\n";
	return undef;
}

sub normalizeFlatMARC
{
	my $flat = @_[0];
	#$log->addLine("Before:\n$flat");
	$flat =~ s/^\s*//;
	$flat =~ s/^LDR\s*//;
	my $ret = "";
	my @lines = split("\n",$flat);
	foreach(@lines)
	{
		my $line = $mobUtil->trim($_);
		#$log->addLine("Looking at\n$line");
		my @words = split(" ",$line);
		if((length(@words[0])==3) && (@words[0] =~ /\d\d\d/)){$ret .= "\n$line";}
		else{$ret.=" $line";}
	}
	if(length($ret > 0))
	{
		$ret = substr($ret,1);
		#$log->addLine("After:\n$ret");	
		return $ret;
	}
	return undef;
}

sub lookupInEvergreen
{
	my $isbn = @_[0];
	my $desiredFormat = @_[1];
	my $query = 
	"select marc from biblio.record_entry where id in(
select id from reporter.materialized_simple_record where '$isbn' = any(isbn)
)
";
	$log->addLine($query);
	my @results = @{$dbHandler->query($query)};
	my $count=0;
	my @marcs = ();
	foreach(@results)
	{
		my @row = @{$_};
		my $marcob = @row[0];
		$marcob =~ s/(<leader>.........)./${1}a/;
		$marcob = MARC::Record->new_from_xml($marcob);
		push(@marcs, $marcob);
		$count++;
	}
	if($count>0)
	{
		return matchDesiredFormat(\@marcs,$desiredFormat);		
	}
	#print "returning undef evergreen\n";
	return undef;
}

sub create902ItemField
{
	my @row = @{@_[0]};
	# ISBN,CALLNO,IBARCODE,EDITION,TITLE,
	# AUTHOR1,AUTHOR2,AUTHOR3,AUTHOR4,
	# SUBJECT1,SUBJECT2,SUBJECT3,SUBJECT4,SUBJECT5,SUBJECT6,
	# EDITION, 15 
	# PUBLISHER,16
	# PLACEPUBL, 17
	# PUBLDATE, 18
	# STATUSID, 19
	# PRICE, 20
	# NOTE, 21
	# SUBNOTE, 22
	# extract(year from INVDATE)||
# '-'|| extract(month from INVDATE)||
# '-'|| extract(day from INVDATE),  23
	# MARC 24
	
	my $field = MARC::Field->new('902',' ',' ',
	'a' => $mobUtil->trim(@row[1]), #callnumber
    'b' => $mobUtil->trim(@row[2]), #barcode
	'c' => $mobUtil->trim(@row[19]), #statusid
	'd' => $mobUtil->trim(@row[20]), #price
	'e' => $mobUtil->trim(@row[21]), #note
	'f' => $mobUtil->trim(@row[22]), #note2
	'g' => $mobUtil->trim(@row[23]) #inventory date
	);
	return $field;
}

sub matchDesiredFormat
{
	my $ret;
	my @ret;
	my @marc = @{@_[0]};
	my $desiredFormat = @_[1];
	foreach(@marc)
	{	
		my $format = determineEGFormatFromMARC($_);
		if($format eq $desiredFormat)
		{
			push(@ret,$_);
		}
	}
	# Well, we have to many, let's take the highest scoring
	if($#ret>0)
	{
		$ret = getHighestScoringMARC(\@ret);
	}
	# Non of them were the right format, so, I guess we will just force the issue
	elsif($#ret=-1)
	{
		$log->addLine("Forcing the issue: $desiredFormat from ".$#marc);
		my $themarc = getHighestScoringMARC(\@marc);
		#$log->addLine($themarc->as_formatted());
		if($desiredFormat eq 'CDAUDIOBOOK')
		{
			$ret = updateMARCSetAudioBook(0,convertMARCtoXML($themarc),'f');
		}
		elsif($desiredFormat eq 'CASSAUDIOBOOK')
		{
			$ret = updateMARCSetAudioBook(0,convertMARCtoXML($themarc),'l');
		}
		elsif($desiredFormat eq 'LARGEPRINT')
		{
			$ret = updateMARCSetLargePrint(0,convertMARCtoXML($themarc));
		}
		elsif($desiredFormat eq 'DVD')
		{
			$ret = updateMARCSetVideo(0,convertMARCtoXML($themarc),'v');
		}
		elsif($desiredFormat eq 'VHS')
		{
			$ret = updateMARCSetVideo(0,convertMARCtoXML($themarc),'b');
		}
		elsif($desiredFormat eq 'BOOK')
		{
			$ret = updateMARCSetBook(0,convertMARCtoXML($themarc));
			#$ret = $themarc;
		}
	}
	#There was only 1! Yay
	else{print "there was only one\n";$ret = @ret[0];}
	my @n01 = $ret->field('901');
	$ret->delete_fields(@n01);
	return $ret;
}

sub getHighestScoringMARC
{
	my @marc = @{@_[0]};
	my $themarc = @marc[0];
	my $highScore = scoreMARC(@marc[0]);
	foreach(@marc)
	{
		my $score = scoreMARC($_);
		if($score > $highScore)
		{
			$themarc = $_;
			$highScore=$score;
		}
	}
	return $themarc;
}

	
sub determineFormatFromCallNumber
{
	my $call = @_[0];
	my $format = "BOOK";
	if($call =~ m/\sCASS/g){$format="CASSAUDIOBOOK";}
	if($call =~ m/^CASS/g){$format="CASSAUDIOBOOK";}
	if($call =~ m/\sCD/g){$format="CDAUDIOBOOK";}
	if($call =~ m/^CD/g){$format="CDAUDIOBOOK";}
	if($call =~ m/[\s\-]LP/g){$format="LARGEPRINT";}
	if($call =~ m/^LP/g){$format="LARGEPRINT";}
	if($call =~ m/[^\s\-]DVD/g){$format="DVD";}
	if($call =~ m/^DVD/g){$format="DVD";}
	if($call =~ m/[^\s\-]VHS/g){$format="VHS";}
	if($call =~ m/^VHS/g){$format="VHS";}
	return $format;
}

sub getMARCFromZ3950
{
	my @ret;
	my $isbn = @_[0];
	my $desiredFormat = @_[1];
	
	my %z3950targets = ();
	my @vals = ("210","INNOPAC","1=7");
	$z3950targets{"minerva.maine.edu"}=\@vals;	
	$z3950targets{"136.181.125.166"}=\@vals;
	$z3950targets{"mainecat.maine.edu"}=\@vals;
	$z3950targets{"olc1.ohiolink.edu"}=\@vals;
	#@vals = ("7090","Voyager","6=7");
	#$z3950targets{"z3950.loc.gov"}=\@vals;
		
	while ((my $internal, my $mvalue ) = each(%z3950targets))
	{	
		my @dets = @{$mvalue};
		local $@;
		eval{
			my @res = @{$mobUtil->getMarcFromZ3950($internal.":".@dets[0]."/".@dets[1],'@attr '.@dets[2].' "'.$isbn.'"',$log)};
			@ret = (@ret, @res);
		};
		if($@)
		{
			$log->addLine("Problem with $internal , moving on");
		}
	}
	if($#ret>-1)
	{
		return matchDesiredFormat(\@ret,$desiredFormat);
	}
	return undef;
}

sub determineEGFormatFromMARC
{

	my $marc = @_[0];
	my $marcr = populate_marc($marc);	
	my %marcr = %{normalize_marc($marcr)};
	my $format = 'BOOK';
	
	
	if( $marcr{item_form} !=~ /[abcdfoqrs]/ && $marcr{bib_lvl} =~ /[acdm]/ && $marcr{record_type} =~ /[at]/ )
	{
		$format = 'BOOK';
	}
	if($marcr{bib_lvl} =~ /[acdm]/ && $marcr{record_type} =~ /[at]/ && $marcr{item_form} =~ /[d]/)
	{
		$format = 'LARGEPRINT';
	}
	if($marcr{audioformat} =~ /[f]/ && $marcr{record_type} =~ /[i]/)
	{
		$format = 'CDAUDIOBOOK';
	}
	if($marcr{audioformat} =~ /[l]/ && $marcr{record_type} =~ /[i]/)
	{
		$format = 'CASSAUDIOBOOK';
	}
	if($marcr{videoformat} =~ /[s]/)
	{
		$format = 'BLUERAY';
	}
	if($marcr{videoformat} =~ /[b]/)
	{
		$format = 'VHS';
	}
	if($marcr{videoformat} =~ /[v]/)
	{
		$format = 'DVD';
	}
	# $marcr{record_type}
	# $marcr{videoformat}
	# $marcr{audioformat}
	# $marcr{date1}
	# $marcr{record_type}
	# $marcr{bib_lvl}
	
	return $format;
	
}
	
sub createField
{
	my $tag = @_[0];
	my $subtag = @_[1];
	my $ind1 = @_[2];
	my $ind2 = @_[3];
	my $content = @_[4];
	my $field;
	if($tag < 10)
	{
		$field = MARC::Field->new($tag, $mobUtil->trim($content));
	}
	else
	{
		$field = MARC::Field->new($tag, $ind1,$ind2,$subtag =>  $mobUtil->trim($content));
	}
	return $field;
}

sub reportResults
{
	my $newRecordCount='';
	my $updatedRecordCount='';
	my $mergedRecords='';
	my $itemsAssignedRecords='';
	my $itemsFailedAssignedRecords='';
	my $copyMoveRecords='';
	my $undedupeRecords='';
	my $largePrintItemsOnNonLargePrintBibs='';
	my $nonLargePrintItemsOnLargePrintBibs='';
	my $itemsAttachedToDeletedBibs='';
	my $DVDItemsOnNonDVDBibs='';
	my $nonDVDItemsOnDVDBibs='';
	my $AudiobookItemsOnNonAudiobookBibs='';
	my $AudiobooksPossbileEAudiobooks='';
	my @attachments=();
	
	
	#bib_marc_update table report non new bibs
	my $query = "select extra,count(*) from seekdestroy.bib_marc_update where job=$jobid and new_record is not true group by extra";
	updateJob("Processing","reportResults $query");
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		$updatedRecordCount.=@row[1]." records were updated for this reason: ".@row[0]."\r\n";
	}
	#bib_marc_update table report new bibs
	my $query = "select extra,count(*) from seekdestroy.bib_marc_update where job=$jobid and new_record is true group by extra";
	updateJob("Processing","reportResults $query");
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};		
		$newRecordCount.=@row[1]." records were created for this reason: ".@row[0]."\r\n";
	}
	
	#bib_merge table report
	$query = "select leadbib,subbib from seekdestroy.bib_merge where job=$jobid";
	updateJob("Processing","reportResults $query");
	@results = @{$dbHandler->query($query)};	
	my $count=0;	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $line=@row[0];
		$line = $mobUtil->insertDataIntoColumn($line,"<",12);
		$line = $mobUtil->insertDataIntoColumn($line,@row[1],14);
		$mergedRecords.="$line\r\n";
		$count++;
	}
	if($count>0)
	{	
		$mergedRecords = truncateOutput($mergedRecords,7000);
		$mergedRecords="$count records were merged - The left number is the winner\r\n".$mergedRecords;
		$mergedRecords."\r\n\r\n\r\n";
		my @header = ("Winning Bib","Deleted/Merged Bib");
		my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."Merged_bibs.csv");
		push(@attachments,$baseTemp."Merged_bibs.csv");
	}
	
	#call_number_move table report
	$query = "select tobib,frombib,(select label from asset.call_number where id=a.call_number),
	(select name from actor.org_unit where id=(select owning_lib from asset.call_number where id=a.call_number))
	from seekdestroy.call_number_move a where job=$jobid
	and frombib not in(select oldleadbib from seekdestroy.undedupe where job=$jobid) and tobib is not null";
	updateJob("Processing","reportResults $query");	
	@results = @{$dbHandler->query($query)};
	if($#results>-1)
	{	
		my $summary = summaryReportResults(\@results,3,"Owning Library",45,"Moved Call Numbers");
		$itemsAssignedRecords="$summary\r\n\r\n\r\n";
		my @header = ("Destination Bib","Source Bib","Call Number","Owning Library");
		my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."Moved_call_numbers.csv");
		push(@attachments,$baseTemp."Moved_call_numbers.csv");
	}
	
	#call_number_move FAILED table report
	$query = "select frombib,
	(select name from actor.org_unit where id=(select owning_lib from asset.call_number where id=a.call_number)),
	(select label from asset.call_number where id=a.call_number) from seekdestroy.call_number_move a where job=$jobid
	and frombib not in(select oldleadbib from seekdestroy.undedupe where job=$jobid) and tobib is null
	order by (select name from actor.org_unit where id=(select owning_lib from asset.call_number where id=a.call_number))";
	updateJob("Processing","reportResults $query");
	@results = @{$dbHandler->query($query)};
	if($#results>-1)
	{	
		my $summary = summaryReportResults(\@results,1,"Owning Library",45,"Call Numbers FAILED to be moved");
		$itemsFailedAssignedRecords="$summary\r\n\r\n\r\n";
		my @header = ("Source Bib","Call Number Owning Library","Call Number");
		my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."Failed_call_number_moves.csv");
		push(@attachments,$baseTemp."Failed_call_number_moves.csv");
	}
	
	#copy_move table report
	$query = "select (select barcode from asset.copy where id=a.copy),
	(select record from asset.call_number where id=a.fromcall),
	(select record from asset.call_number where id=a.tocall),
	(select label from asset.call_number where id=a.tocall),
	(select name from actor.org_unit where id=(select circ_lib from asset.copy where id=a.copy))
	from seekdestroy.copy_move a where job=$jobid";
	updateJob("Processing","reportResults $query");
	@results = @{$dbHandler->query($query)};	
	if($#results>-1)
	{	
		my $summary = summaryReportResults(\@results,4,"Circulating Library",45,"Copies moved");
		$copyMoveRecords="$summary\r\n\r\n\r\n";
		my @header = ("Barcode","Destination Bib","Source Bib","Call Number","Circulating Library");
		my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."Copy_moves.csv");
		push(@attachments,$baseTemp."Copy_moves.csv");
	}
	
	#undedupe table report
	$query = "select 
	undeletedbib,
	oldleadbib,
	(select label from asset.call_number where id=a.moved_call_number),
	(select name from actor.org_unit where id=(select owning_lib from asset.call_number where id=a.moved_call_number))
	from seekdestroy.undedupe a where job=$jobid";
	updateJob("Processing","reportResults $query");
	@results = @{$dbHandler->query($query)};	
	if($#results>-1)
	{	
		my $summary = summaryReportResults(\@results,3,"Owning Library",45,"Un-deduplicated Records");
		$undedupeRecords="$summary\r\n\r\n\r\n";				
		my @header = ("Undeleted Bib","Old Leading Bib","Call Number","Owning Library");
		my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."Undeduplicated_Bibs.csv");
		push(@attachments,$baseTemp."Undeduplicated_Bibs.csv");
	}
		
	#Audiobook Items attached to non Audiobook bibs and visa versa
	$query =  $queries{"questionable_audiobook_bib_to_item"};
	updateJob("Processing","reportResults $query");
	@results = @{$dbHandler->query($query)};
	
	if($#results>-1)
	{
		my $summary = summaryReportResults(\@results,4,"Owning Library",45,"Audiobook items/bibs mismatched");
		$AudiobookItemsOnNonAudiobookBibs="$summary\r\n\r\n\r\n";
		my @header = ("Bib ID","Barcode","Call Number","OPAC Icon","Library");
		my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."questionable_audiobook_bib_to_item.csv");
		push(@attachments,$baseTemp."questionable_audiobook_bib_to_item.csv");
	}
	
	
	#DVD Items attached to non DVD bibs
	$query =  $queries{"DVD_items_on_non_DVD_bibs"};
	updateJob("Processing","reportResults $query");
	@results = @{$dbHandler->query($query)};
	if($#results>-1)
	{
		my $summary = summaryReportResults(\@results,4,"Owning Library",45,"Items look like they are Video but they are attached to non Video bibs.");
		$DVDItemsOnNonDVDBibs="$summary\r\n\r\n\r\n";
		my @header = ("Bib ID","Barcode","Call Number","OPAC Icon","Library");
		my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."Video_items_on_non_Video_bibs.csv");
		push(@attachments,$baseTemp."Video_items_on_non_Video_bibs.csv");
	}

	
	#Non DVD Items attached to DVD bibs
	$query =  $queries{"non_DVD_items_on_DVD_bibs"};
	updateJob("Processing","reportResults $query");
	@results = @{$dbHandler->query($query)};
	if($#results>-1)
	{
		my $summary = summaryReportResults(\@results,4,"Owning Library",45,"Non Video Items attached to Video bibs.");
		$nonDVDItemsOnDVDBibs="$summary\r\n\r\n\r\n";
		my @header = ("Bib ID","Barcode","Call Number","OPAC Icon","Library");
		my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."non_Video_items_on_Video_bibs.csv");
		push(@attachments,$baseTemp."non_Video_items_on_Video_bibs.csv");
	}
	
	
	#Large print Items attached to non large print bibs
	$query =  $queries{"large_print_items_on_non_large_print_bibs"};
	updateJob("Processing","reportResults $query");
	@results = @{$dbHandler->query($query)};
	if($#results>-1)
	{
		my $summary = summaryReportResults(\@results,4,"Owning Library",45,"Items look like they are large print but attached to non large print bibs.");
		$largePrintItemsOnNonLargePrintBibs="$summary\r\n\r\n\r\n";
		my @header = ("Bib ID","Barcode","Call Number","OPAC Icon","Library");
		my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."Large_print_items_on_non_large_print_bibs.csv");
		push(@attachments,$baseTemp."Large_print_items_on_non_large_print_bibs.csv");
	}
	
	
	#non Large print Items attached to large print bibs
	$query =  $queries{"non_large_print_items_on_large_print_bibs"};
	updateJob("Processing","reportResults $query");
	@results = @{$dbHandler->query($query)};	
	if($#results>-1)
	{
		my $summary = summaryReportResults(\@results,4,"Owning Library",45,"Items do not look like large print but attached to large print bibs.");
		$nonLargePrintItemsOnLargePrintBibs="$summary\r\n\r\n\r\n";
		my @header = ("Bib ID","Barcode","Call Number","OPAC Icon","Library");
		my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."Non_large_print_items_on_large_print_bibs.csv");
		push(@attachments,$baseTemp."Non_large_print_items_on_large_print_bibs.csv");
	}
	
	# Items attached to deleted bibs
	$query =  $queries{"items_attached_to_deleted_bibs"};
	updateJob("Processing","reportResults $query");
	@results = @{$dbHandler->query($query)};	
	if($#results>-1)
	{
		my $summary = summaryReportResults(\@results,4,"Owning Library",45,"Items attached to DELETED Bibs.");
		$itemsAttachedToDeletedBibs="$summary\r\n\r\n\r\n";
		my @header = ("Bib ID","Barcode","Call Number","OPAC Icon","Library");
		my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."items_attached_to_deleted_bibs.csv");
		push(@attachments,$baseTemp."items_attached_to_deleted_bibs.csv");
	}
	
	
	my $ret=$newRecordCount."\r\n\r\n".$updatedRecordCount."\r\n\r\n".$mergedRecords.$itemsAssignedRecords.$copyMoveRecords.$undedupeRecords.$AudiobooksPossbileEAudiobooks.$itemsAttachedToDeletedBibs.$AudiobookItemsOnNonAudiobookBibs.$DVDItemsOnNonDVDBibs.$nonDVDItemsOnDVDBibs.$largePrintItemsOnNonLargePrintBibs.$nonLargePrintItemsOnLargePrintBibs;
	#print $ret;
	my @returns = ($ret,\@attachments);
	return \@returns;
}

sub summaryReportResults
{
	my @results = @{@_[0]};
	my $namecolumnpos = @_[1];
	my $nameColumnName = @_[2];
	my $nameWidth = @_[3];
	my $title = @_[4];
	my %ret = ();
	my @sorted = ();
	my $total = 0;
	my $summary='';
	foreach(@results)
	{
		my @row = @{$_};
		if($ret{@row[$namecolumnpos]})
		{
			$ret{@row[$namecolumnpos]}++;
		}
		else
		{
			$ret{@row[$namecolumnpos]}=1;
			push(@sorted, @row[$namecolumnpos]);
		}
		$total++;
	}
	my $i=1;
	while($i<$#sorted+1)
	{
		if($ret{@sorted[$i]} > $ret{@sorted[$i-1]})
		{
			my $temp = @sorted[$i];
			@sorted[$i]=@sorted[$i-1];
			@sorted[$i-1] = $temp;
			$i-=2 unless $i<2;
			$i-- unless $i<1;
		}
		$i++;
	}
	my $header = "Count";
	$header = $mobUtil->insertDataIntoColumn($header,$nameColumnName,11)."\r\n";
	foreach(@sorted)
	{
		my $line = $ret{$_};
		$line = $mobUtil->insertDataIntoColumn($line," ".$_,11);
		$summary.="$line\r\n";
	}
	my $line = $total;
	$line = $mobUtil->insertDataIntoColumn($line," Total",11);
	$summary.="$line\r\n";
	
	my $titleStars = "*";
	while(length($titleStars)<length($title)){$titleStars.="*";}
	$title="$titleStars\r\n$title\r\n$titleStars\r\n";
	$summary = $title.$header.$summary;
	return $summary;
}

sub createCSVFileFrom2DArray
{
	my @results = @{@_[0]};
	my $fileName = @_[1];
	my $fileWriter = new Loghandler($fileName);
	$fileWriter->deleteFile();
	my $output = "";
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $csvLine = $mobUtil->makeCommaFromArray(\@row,);
		$output.=$csvLine."\n";
	}
	$fileWriter->addLine($output);
	return $output;
}

sub truncateOutput
{
	my $ret = @_[0];
	my $length = @_[1];
	if(length($ret)>$length)
	{
		$ret = substr($ret,0,$length)."\r\nTRUNCATED FOR LENGTH\n\n";
	}
	return $ret;
}

sub tag902s
{
	my $query = "
		select record,extra,(select marc from biblio.record_entry where id=a.record) from SEEKDESTROY.BIB_MARC_UPDATE a";
 
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		my $reason = @row[1];
		my $marc = @row[2];
		my $note = '';
		if($reason eq "Correcting for DVD in the leader/007 rem 008_23")
		{
			$note='D V D';
		}
		elsif($reason eq "Correcting for Audiobook in the leader/007 rem 008_23")
		{
			$note='A u d i o b o o k';
		}
		elsif($reason eq "Correcting for Electronic in the 008/006")
		{
			$note='E l e c t r o n i c';
		}
		else
		{
			print "Skipping $bibid\n";
			next;
		}
		my $xmlresult = $marc;
		$xmlresult =~ s/(<leader>.........)./${1}a/;
		#$log->addLine($xmlresult);
		my $check = length($xmlresult);
		#$log->addLine($check);
		$xmlresult = fingerprintScriptMARC($xmlresult,$note);
		$xmlresult =~s/<record>//;
		$xmlresult =~s/<\/record>//;
		$xmlresult =~s/<\/collection>/<\/record>/;
		$xmlresult =~s/<collection/<record  /;
		$xmlresult =~s/XMLSchema-instance"/XMLSchema-instance\"  /;
		$xmlresult =~s/schema\/MARC21slim.xsd"/schema\/MARC21slim.xsd\"  /;
		
		#$log->addLine($xmlresult);
		#$log->addLine(length($xmlresult));
		if(length($xmlresult)!=$check)
		{		
			updateMARC($xmlresult,$bibid,'false',"Tagging 902 for $note");
		}
		else
		{
			print "Skipping $bibid - Already had the 902 for $note\n";
		}
	}
}

sub setMARCForm
{
	my $marc = @_[0];
	my $char = @_[1];
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my $marcr = populate_marc($marcob);	
	my %marcr = %{normalize_marc($marcr)};    
	my $replacement;
	my $altered=0;
	if($marcr{tag008})
	{
		my $z08 = $marcob->field('008');
		$marcob->delete_field($z08);
		#print "$marcr{tag008}\n";
		$replacement=$mobUtil->insertDataIntoColumn($marcr{tag008},$char,24);
		#print "$replacement\n";
		$z08->update($replacement);
		$marcob->insert_fields_ordered($z08);
		$altered=1;
	}
	elsif($marcr{tag006})
	{
		my $z06 = $marcob->field('006');
		$marcob->delete_fields($z06);
		#print "$marcr{tag006}\n";
		$replacement=$mobUtil->insertDataIntoColumn($marcr{tag006},$char,7);
		#print "$replacement\n";
		$z06->update($replacement);
		$marcob->insert_fields_ordered($z06);
		$altered=1;
	}
	if(!$altered && $char ne ' ')
	{
		$replacement=$mobUtil->insertDataIntoColumn("",$char,24);
		$replacement=$mobUtil->insertDataIntoColumn($replacement,' ',39);
		my $z08 = MARC::Field->new( '008', $replacement );
		#print "inserted new 008\n".$z08->data()."\n";
		$marcob->insert_fields_ordered($z08);
	}
	
	my $xmlresult = convertMARCtoXML($marcob);
	return $xmlresult;
}

sub updateMARCSetElectronic
{	
	my $bibid = @_[0];
	my $marc = @_[1];
	$marc = setMARCForm($marc,'s');
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my $marcr = populate_marc($marcob);	
	my %marcr = %{normalize_marc($marcr)};
	# we have to remove the 007s because they conflict for playaway.
	foreach(@{$marcr{tag007}})
	{		
		if(substr($_->data(),0,1) eq 's')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
		}
		elsif(substr($_->data(),0,1) eq 'v')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
		}
	}
}

sub updateMARCSetAudioBook
{	
	my $bibid = @_[0];
	my $marc = @_[1];
	my $cdOrCass = @_[2];
	$marc = setMARCForm($marc,' ');
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my $marcr = populate_marc($marcob);	
	my %marcr = %{normalize_marc($marcr)};    
	my $replacement;
	my $altered=0;
	foreach(@{$marcr{tag007}})
	{		
		if(substr($_->data(),0,1) eq 's')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
			#print $z07->data()."\n";
			$replacement=$mobUtil->insertDataIntoColumn($z07->data(),'s',1);
			$replacement=$mobUtil->insertDataIntoColumn($replacement,$cdOrCass,4);
			#print "$replacement\n";			
			$z07->update($replacement);
			$marcob->insert_fields_ordered($z07);
			$altered=1;
		}
		elsif(substr($_->data(),0,1) eq 'v')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
			#print "removed video 007\n";
		}
	}
	if(!$altered)
	{
		my $z07 = MARC::Field->new( '007', "sd $cdOrCass".'sngnnmmned' );
		#print "inserted new 007\n".$z07->data()."\n";
		$marcob->insert_fields_ordered($z07);
	}
	my $xmlresult = convertMARCtoXML($marcob);
	$xmlresult = updateMARCSetSpecifiedLeaderByte($bibid,$xmlresult,7,'i');
	$xmlresult =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($xmlresult);
	return $marcob;
}

sub updateMARCSetVideo
{	
	my $bibid = @_[0];	
	my $marc = @_[1];
	my $vhsOrDVD = @_[2];
	$marc = setMARCForm($marc,' ');
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my $marcr = populate_marc($marcob);	
	my %marcr = %{normalize_marc($marcr)};    
	my $replacement;
	my $altered=0;
	foreach(@{$marcr{tag007}})
	{		
		if(substr($_->data(),0,1) eq 'v')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
			#print $z07->data()."\n";
			$replacement=$mobUtil->insertDataIntoColumn($z07->data(),'v',1);
			$replacement=$mobUtil->insertDataIntoColumn($replacement,$vhsOrDVD,5);
			#print "$replacement\n";			
			$z07->update($replacement);
			$marcob->insert_fields_ordered($z07);
			$altered=1;
		}
		elsif(substr($_->data(),0,1) eq 's')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
			#print "removed video 007\n";
		}
	}
	if(!$altered)
	{
		my $z07 = MARC::Field->new( '007', 'vd c'.$vhsOrDVD.'aizq' );
		#print "inserted new 007\n".$z07->data()."\n";
		$marcob->insert_fields_ordered($z07);
	}
	my $xmlresult = convertMARCtoXML($marcob);
	$xmlresult = updateMARCSetSpecifiedLeaderByte($bibid,$xmlresult,7,'g');
	$xmlresult =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($xmlresult);
	return $marcob;
}

sub updateMARCSetBook
{	
	print "updating to marc\n";
	my $bibid = @_[0];	
	my $marc = @_[1];
	$marc = setMARCForm($marc,' ');
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my $marcr = populate_marc($marcob);	
	my %marcr = %{normalize_marc($marcr)};    
	my $replacement;
	foreach(@{$marcr{tag007}})
	{	
		my $z07 = $_;
		$marcob->delete_field($z07);
	}
	my $xmlresult = convertMARCtoXML($marcob);
	$xmlresult = updateMARCSetSpecifiedLeaderByte($bibid,$xmlresult,7,'a');
	$xmlresult = updateMARCSetSpecifiedLeaderByte($bibid,$xmlresult,8,'m');
	$xmlresult =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($xmlresult);
	return $marcob;
}

sub updateMARCSetLargePrint
{	
	my $bibid = @_[0];	
	my $marc = @_[1];	
	$marc = setMARCForm($marc,'d');
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my $marcr = populate_marc($marcob);	
	my %marcr = %{normalize_marc($marcr)};    
	foreach(@{$marcr{tag007}})
	{		
		if(substr($_->data(),0,1) eq 'v')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
		}
		elsif(substr($_->data(),0,1) eq 's')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
		}
	}
	
	my $xmlresult = convertMARCtoXML($marcob);
	$xmlresult = updateMARCSetSpecifiedLeaderByte($bibid,$xmlresult,7,'a');
	$xmlresult =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($xmlresult);
	return $marcob;
}

sub fingerprintScriptMARC
{
	my $marc = @_[0];
	my $note = @_[1];
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my @n902 = $marcob->field('902');
	my $altered = 0;
	my $dt = DateTime->now(time_zone => "local"); 
	my $fdate = $dt->mdy; 
	foreach(@n902)
	{
		my $field = $_;
		my $suba = $field->subfield('a');
		my $subd = $field->subfield('d');
		if($suba && $suba eq 'mobius-catalog-fix' && $subd && $subd eq "$note")
		{
			#print "Found a matching 902 for $note - updating that one\n";
			$altered = 1;
			my $new902 = MARC::Field->new( '902',' ',' ','a'=>'mobius-catalog-fix','b'=>"$fdate",'c'=>'formatted','d'=>"$note" );
			$marcob->delete_field($field);
			$marcob->append_fields($new902);
		}
	}
	if(!$altered)
	{
		my $new902 = MARC::Field->new( '902',' ',' ','a'=>'mobius-catalog-fix','b'=>"$fdate",'c'=>'formatted','d'=>"$note" );
		$marcob->append_fields($new902);
	}
	my $xmlresult = convertMARCtoXML($marcob);
	return $xmlresult
}

sub updateMARCSetSpecifiedLeaderByte  
{	
	my $bibid = @_[0];
	my $marc = @_[1];
	my $leaderByte = @_[2];		#1 based
	my $value = @_[3];
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);	
	my $leader = $marcob->leader();
	#print $leader."\n";
	$leader=$mobUtil->insertDataIntoColumn($leader,$value,$leaderByte);
	#print $leader."\n";
	$marcob->leader($leader);
	#print $marcob->leader()."\n";
	my $xmlresult = convertMARCtoXML($marcob);
	return $xmlresult;
}

sub scoreMARC
{
	my $marc = shift;	
	
	my $score = 0;
	$score+= score($marc,2,100,400,'245');
	$score+= score($marc,1,1,150,'100');
	$score+= score($marc,1,1.1,150,'110');
	$score+= score($marc,0,50,200,'6..');
	$score+= score($marc,0,50,100,'02.');
	
	$score+= score($marc,0,100,200,'246');
	$score+= score($marc,0,100,100,'130');
	$score+= score($marc,0,100,100,'010');
	$score+= score($marc,0,100,200,'490');
	$score+= score($marc,0,10,50,'830');
	
	$score+= score($marc,1,.5,50,'300');
	$score+= score($marc,0,1,100,'7..');
	$score+= score($marc,2,2,100,'50.');
	$score+= score($marc,2,2,100,'52.');
	
	$score+= score($marc,2,.5,200,'51.', '53.', '54.', '55.', '56.', '57.', '58.');

	return $score;
}

sub score
{
	my ($marc) = shift;
	my ($type) = shift;
	my ($weight) = shift;
	my ($cap) = shift;
	my @tags = @_;
	my $ou = Dumper(@tags);
	#$log->addLine("Tags: $ou\n\nType: $type\nWeight: $weight\nCap: $cap");
	my $score = 0;			
	if($type == 0) #0 is field count
	{
		#$log->addLine("Calling count_field");
		$score = count_field($marc,\@tags);
	}
	elsif($type == 1) #1 is length of field
	{
		#$log->addLine("Calling field_length");
		$score = field_length($marc,\@tags);
	}
	elsif($type == 2) #2 is subfield count
	{
		#$log->addLine("Calling count_subfield");
		$score = count_subfield($marc,\@tags);
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
	my @tags = @{$_[1]};
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
	my @tags = @{$_[1]};
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
	my @tags = @{$_[1]};

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

sub mergeMARC856
{
	my $marc = @_[0];
	my $marc2 = @_[1];	
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
	my $thisXML =  $marc->as_xml(); #decode_utf8();		
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

sub createNewJob
{
	my $status = @_[0];
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
	$fingerprints{audioformat} = $marc{audioformat};
	$fingerprints{videoformat} = $marc{videoformat};
	#print Dumper(%fingerprints);
	return \%fingerprints;
}

#This is borrowed from fingerprinter and altered a bit for the item form
sub populate_marc {
    my $record = @_[0];
    my %marc = (); $marc{isbns} = [];

    # record_type, bib_lvl
    $marc{record_type} = substr($record->leader, 6, 1);
    $marc{bib_lvl}     = substr($record->leader, 7, 1);

    # date1, date2
    my $my_008 = $record->field('008');
	my @my_007 = $record->field('007');
	my $my_006 = $record->field('006');
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
	$marc{tag006} = $my_006->as_string() if ($my_006);
	$marc{tag007} = \@my_007 if (@my_007);
	$marc{audioformat}='';
	$marc{videoformat}='';
	foreach(@my_007)
	{
		if(substr($_->data(),0,1) eq 's' && $marc{audioformat} eq '')
		{
			$marc{audioformat} = substr($_->data(),3,1) unless (length $_->data() < 4);
		}
		elsif(substr($_->data(),0,1) eq 'v' && $marc{videoformat} eq '')
		{
			$marc{videoformat} = substr($_->data(),4,1) unless (length $_->data() < 5);
		}
	}
	#print "$marc{audioformat}\n";
	#print "$marc{videoformat}\n";
	
    # item_form
    if ( $marc{record_type} =~ /[gkroef]/ ) { # MAP, VIS
        $marc{item_form} = substr($marc{tag008},29,1) if ($marc{tag008} && (length $marc{tag008} > 29 ));
    } else {
        $marc{item_form} = substr($marc{tag008},23,1) if ($marc{tag008} && (length $marc{tag008} > 23 ));
    }	
	#fall through to 006 if 008 doesn't have info for item form
	if ($marc{item_form} eq '|')
	{
		$marc{item_form} = substr($marc{tag006},6,1) if ($marc{tag006} && (length $marc{tag006} > 6 ));
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

 exit;

 
 