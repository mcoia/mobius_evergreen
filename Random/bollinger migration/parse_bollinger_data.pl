#!/usr/bin/perl
# 
# Example Configure file:
# 
use lib qw(../);
 use strict; 
 use Loghandler;
 use Mobiusutil;
 use DBhandler; 
 use Data::Dumper;
 use DateTime;
 use utf8;
 use Encode;
 
 #use warnings;
 #use diagnostics; 

my $allMarc = "/mnt/evergreen/migration/bollinger/data/AllRecords.mrc";
my $patrons = "/mnt/evergreen/migration/bollinger/data/patrons.mrc";
my $transactions = "/mnt/evergreen/migration/bollinger/data/Transactions3.csv";
my $itemsWithDates = "/mnt/evergreen/migration/bollinger/data/items_dates.csv";

 
 my $configFile = shift;
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
		my $dbHandler;			
		 eval{$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});};
		 if ($@) 
		 {
			$log->addLogLine("Could not establish a connection to the database");
			print "Could not establish a connection to the database";
		 }
		 else
		 {
			
			#$query = "CREATE SCHEMA M_BOLLINGER";
			#$dbHandler->update($query);
			
			#removedDeletedBibs($allMarc,"/mnt/evergreen/migration/bollinger/data/AllRecords_without_deleted.mrc");
			
			parsePatrons($patrons,$dbHandler);
			parseHoldingDates($itemsWithDates,$dbHandler);
			parseTransactions($transactions,$conf{"finesout"},$conf{"loanssout"},$conf{"holdsout"});
			
			#extractItems($allMarc,$dbHandler);
			#generateFieldCount($allMarc);
			
			
		 }
		
		 
		 $log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile' and 'marcoutdir'\n";
		
	}
 }
 
 sub padcode
 {
	my $string = @_[0];
	my $padchar = @_[1];
	my $start = @_[2];
	if(length($string)>9)
	{	
		$string=substr($string,-9);
	}
	while(length($string)<9)
	{
		$string=$padchar.$string;
	}
	#print "$string converted: ";
	#print $start.$string."\n";
	return $start.$string;
 }
 
 sub parseTransactions
 {
	print "Reading ".@_[0]."\n";
	my $file = new Loghandler(@_[0]);
	my $finesOut = new Loghandler(@_[1]);
	my $loansOut = new Loghandler(@_[2]);
	my $holdsOut = new Loghandler(@_[3]);
	
	$finesOut->deleteFile();
	$loansOut->deleteFile();
	$holdsOut->deleteFile();
	my $header = "CopyID,PatronID,TransDate,DateDue";
	$loansOut->addLine($header);
	$header.=",l_fines";
	$finesOut->addLine($header);
	my @con = @{$file->readFile()};
	my $lines = $#con;
	my $count=0;
	foreach(@con)
	{
		if(1)
		{
			my $line = $_;
			my @s = split(/\"/,$line);
			@s = split(/~/,@s[1]);
			if($#s>8)
			{
				my $itembcode = padcode(@s[5],"1","32713");
				my $patronbcode = padcode(@s[2],"0","22713");
				my $date1 = @s[6];
				my $date2 = @s[7];
				my $fine = @s[8];
				my $type = @s[9];
				my $year = substr($date1,0,4);
				my $month = substr($date1,4,2);
				my $day = substr($date1,6,2);
				$date1 = "$month/$day/$year";
				$year = substr($date2,0,4);
				$month = substr($date2,4,2);
				$day = substr($date2,6,2);
				$date2 = "$month/$day/$year";
				my $finalLine = "$itembcode,$patronbcode,$date1,$date2";
				#print $finalLine."\n";
				if($type eq 'Loan' || $type eq 'Renewal' )
				{
					$loansOut->addLine($finalLine);
				}
				elsif($type eq 'Hold')
				{
					$holdsOut->addLine($finalLine);
				}
				elsif($type eq 'Fine')
				{
					$finesOut->addLine($finalLine.",$fine");
				}
			}
		}
		$count++;
	}
	
 }
 
 sub generateFieldCount
 {
	print "Reading ".@_[0]."\n";
	my $file = MARC::File::USMARC->in(@_[0]);
	
		my $count=0;
		my @header;
		my @allOutCSV;
		my %typeCount;
		my %typeHCount;
		my $withoutK=0;
		while ( my $marc = $file->next())
		{
			$count++;
			my @marcOutput;
			my @fields = $marc->fields();
			my %fieldcount;
			foreach(@fields)
			{
			
				my $field = $_;
				my $tag = $field->tag();
				my $append = 1;
				while($fieldcount{$tag."_".$append})
				{
					$append++;
				}
				$fieldcount{$tag."_".$append}=1;
				$tag = $tag."_".$append;				
				$tag =~ s/ //g;
				if($field->is_control_field())
				{				
					
					
				}
				else
				{
					if($field->tag() eq '852')
					{
						my @subfields = $field->subfields();
						my %subcount;
						my $foundk=0;
						foreach(@subfields)
						{
							
							my @b = @{$_};
							if(@b[0] eq 'k')
							{
								$foundk=1;
								if($typeCount{@b[1]})
								{
									$typeCount{@b[1]}++;
								}
								else
								{
									$typeCount{@b[1]}=1;
								}
							}
						}
						if(!$foundk)
						{
							$withoutK+=1;
							foreach(@subfields)
							{
								
								my @b = @{$_};
								if(@b[0] eq 'h')
								{
									$foundk=1;
									if($typeHCount{@b[1]})
									{
										$typeHCount{@b[1]}++;
									}
									else
									{
										$typeHCount{@b[1]}=1;
									}
								}
							}
						}
					}
				}
				
			}
			
		}
		my $l = new Loghandler($conf->{"patroncsvout"});
		$l->deleteFile();
		my $txtout = "";
		while(my($k, $v) = each %typeCount) 
		{
			$txtout.="\"$k\",\"$v\"\r\n";
		}
		$l->addLine($txtout);
		my $l = new Loghandler("/tmp/run/hfields.csv");
		$l->deleteFile();
		my $txtout = "";
		while(my($k, $v) = each %typeHCount) 
		{
			$txtout.="\"$k\",\"$v\"\r\n";
		}
		$l->addLine($txtout);
			
		
		
		print "Patrons $count Records outputed: ".$conf->{"patroncsvout"}."\nWithout k: $withoutK";
 }
 
 sub removedDeletedBibs 
 {
 
	my $file = MARC::File::USMARC->in(@_[0]);
	my $outputFile = @_[1];
	my $out;
		
		my $count=0;
		my $dcount=0;
		my $acount=0;
		my @header;
		my @allOutCSV;
		while ( my $marc = $file->next())
		{
			if (1)
			{
				my $leader = $marc->leader();
				#my @s = split(//,$leader);
				#my $ex = substr($leader,5,1);
				#print "$leader\n$ex\n";
				if(substr($leader,5,1) eq 'd')
				{
					print $leader;
					$dcount++
				}
				else
				{
					$out.=$marc->as_usmarc();
					$acount++;
				}
			}
			$count++;
		}
		my $l = new Loghandler($outputFile);
		$l->deleteFile();
		$l->addLine($out);
		print "Processed $count Records\nFound $dcount deleted records\nAdded $acount Records\n";
 
 }
 
 sub parseHoldingDates
 {
	print "Reading ".@_[0]."\n";
	my $file = new Loghandler(@_[0]);
	my $dbHandler = @_[1];
	my $query = "DROP TABLE M_BOLLINGER.ASSET_DATES";
	$dbHandler->update($query);
	$query = "CREATE TABLE M_BOLLINGER.ASSET_DATES(barcode TEXT,date TEXT)";
	$dbHandler->update($query);
	$query = "INSERT INTO M_BOLLINGER.ASSET_DATES(barcode,date) VALUES ";
	my $individualUpdates;
	my $count=0;
	my @con = @{$file->readFile()};
	my $lines = $#con;
	my %ids;
	foreach(@con)
	{
		
		my $line = $_;
		my @s = split(/\"/,$line);
		@s = split(/;/,@s[1]);
		my $bcode = @s[1];
		my $date = @s[2];
		@s = split(/\//,$date);
		if(! $ids{$bcode})
		{
			$ids{$bcode}=1;
			if(($#s==2) && length($bcode>2))
			{
				my $year= @s[2];
				my $day= @s[1];
				my $month= @s[0];
				$query.="(E'$bcode',E'$month/$day/20".$year." 12:00:00 PM'),";
				$individualUpdates = "UPDATE M_BOLLINGER.ASSET_COPY_LEGACY SET L_CREATE_DATE = E'$month/$day/20".$year." 12:00:00 PM' WHERE L_BARCODE='$bcode'";
				#print "$individualUpdates\n";
				#$dbHandler->update($individualUpdates);
				$count++;
			}
		}
	}
	$query = substr($query, 0, -1);
	print "Read $lines lines and recived $count dates. Writing to DB M_BOLLINGER.ASSET_DATES\n";
	$dbHandler->update($query);
	print "Building Index M_BOLLINGER.ASSET_DATES barcode\n";
	$query = "CREATE INDEX asset_bcode on M_BOLLINGER.ASSET_DATES(barcode)";
	$dbHandler->update($query);
	$query = "UPDATE m_bollinger.asset_copy_legacy acl set  l_create_date = (select date from m_bollinger.asset_dates where barcode = acl.barcode)";
	$dbHandler->update($query);
 }
 
 
 sub parsePatrons 
 {
	print "Reading ".@_[0]."\n";
	my $file = MARC::File::USMARC->in(@_[0]);
	my $dbHandler = @_[1];
	my $query = "DROP TABLE M_BOLLINGER.PATRONS_RAW";
	$dbHandler->update($query);
		my $count=0;
		my @header;
		my @allOutCSV;
		while ( my $marc = $file->next())
		{
			$count++;
			my @marcOutput;
			my @fields = $marc->fields();
			my %fieldcount;
			foreach(@fields)
			{
			
				my $field = $_;
				my $tag = $field->tag();
				my $append = 1;
				while($fieldcount{$tag."_".$append})
				{
					$append++;
				}
				$fieldcount{$tag."_".$append}=1;
				$tag = $tag."_".$append;				
				$tag =~ s/ //g;
				if($field->is_control_field())
				{				
					my $data = $field->data();
					my $found=-1;
					my $c=0;
					foreach(@header)
					{
						if($_ eq $tag)
						{
							$found=$c;
						}
						$c++;
					}
					if($found==-1)
					{
						push(@header,$tag);	
						$found=$#header;
						print "Adding $tag\n";
					}
					while($#marcOutput<$found)
					{
						push(@marcOutput,"");
					}
					#print "found = $found\n";
					@marcOutput[$found]=$data;
					
				}
				else
				{
					my @subfields = $field->subfields();
					my %subcount;
					foreach(@subfields)
					{
						my @b = @{$_};
						my $append = 1;
						while($subcount{$tag.@b[0]."_".$append})
						{
							$append++;
						}
						$subcount{$tag.@b[0]."_".$append}=1;
						my $theader = $tag.@b[0]."_".$append;
						$theader =~ s/ //g;
						my $found=-1;
						my $c=0;
						foreach(@header)
						{
							
							if($_ eq $theader)
							{
								$found=$c;
							}
							$c++;
						}
						if($found==-1)
						{
							push(@header,$theader);						
							$found=$#header;
							print "Adding $theader\n";
						}
						while($#marcOutput<$found)
						{
							push(@marcOutput,"");
						}
						@marcOutput[$found]=@b[1];
					}
				}
				
			}
			push(@allOutCSV,[@marcOutput]);
			
		}
		my $l = new Loghandler($conf->{"patroncsvout"});
		$l->deleteFile();
		my $txtout = "";
		my $countt=0;
		my $createtable="create table m_bollinger.patrons_raw(";
		my $columnQueryPart = "";
		foreach(@header)
		{
			$txtout.="\"$_\"\t";
			$createtable.="i$_ TEXT,";
			$columnQueryPart.="i$_,";
			$countt++;
		}
		$txtout = substr($txtout, 0, -1);
		$columnQueryPart = substr($columnQueryPart, 0, -1);
		$createtable = substr($createtable, 0, -1).")";
		$l->addLine($txtout);
		$txtout = "";
		print "Creating m_bollinger.patrons_raw\n";
		print $createtable."\n";
		$dbHandler->update($createtable);
		my $insertQuery="";
		foreach(@allOutCSV)
		{
			$insertQuery.="(";
			my @line = @{$_};
			my $tc = 0;
			foreach(@line)
			{
				$txtout.="\"$_\"\t";
				my $temp = $_;
				$temp=~s/'/\\'/g;
				$insertQuery.="E'$temp',";
				$tc++;
			}
			while($tc < $countt)
			{
				$txtout.="\"\"\t";
				$insertQuery.="'',";
				$tc++;
			}
			$txtout = substr($txtout, 0, -1);
			$insertQuery = substr($insertQuery, 0, -1);
			$insertQuery.="),";
			$l->addLine($txtout);
			$txtout = "";
		}
		$insertQuery = substr($insertQuery, 0, -1);
		$insertQuery = "INSERT INTO M_BOLLINGER.PATRONS_RAW($columnQueryPart) VALUES $insertQuery";
		print "Inserting....\n";
		$dbHandler->update($insertQuery);
		
		print "Patrons $count Records outputed: ".$conf->{"patroncsvout"}."\n";
 
 }
 
 
 sub extractItems 
 {
 
	my $file = MARC::File::USMARC->in(@_[0]);
	my $dbHandler = @_[1];
		
		my $count=0;
		my @header;
		my @allOutCSV;
		while ( my $marc = $file->next())
		{
			$count++;
			
			my @fields = $marc->fields();
			my %fieldcount;
			foreach(@fields)
			{
			my @marcOutput;
				my $field = $_;
				my $tag = $field->tag();
				my $append = 1;
				#while($fieldcount{$tag."_".$append})
				#{
				#	$append++;
				#}
				#$fieldcount{$tag."_".$append}=1;
				#$tag = $tag."_".$append;
				
				if($tag =='852')
				{	
					my @subfields = $field->subfields();
					my %subcount;
					foreach(@subfields)
					{
						my @b = @{$_};
						my $append = 1;
						while($subcount{$tag.@b[0]."_".$append})
						{
							$append++;
						}
						$subcount{$tag.@b[0]."_".$append}=1;
						my $theader = $tag.@b[0]."_".$append;
						my $found=-1;
						my $c=0;
						foreach(@header)
						{
							
							if($_ eq $theader)
							{
								$found=$c;
							}
							$c++;
						}
						if($found==-1)
						{
							push(@header,$theader);						
							$found=$#header;
						}
						while($#marcOutput<$found)
						{
							push(@marcOutput,"");
						}
						@marcOutput[$found]=@b[1];
					}
					push(@allOutCSV,[@marcOutput]);
				}
				
			}
			
			
		}
		my $l = new Loghandler($conf->{"itemscsvout"});
		$l->deleteFile();
		my $txtout = "";
		my $countt=0;
		my $createtable="create table m_bollinger.items_raw(";
		my $columnQueryPart = "";
		foreach(@header)
		{
			$txtout.="\"$_\"\t";
			$createtable.="i$_ TEXT,";
			$columnQueryPart.="i$_,";
			$countt++;
		}
		$txtout = substr($txtout, 0, -1);
		$columnQueryPart = substr($columnQueryPart, 0, -1);
		$createtable = substr($createtable, 0, -1).")";
		$l->addLine($txtout);
		$txtout = "";
		print "Creating m_bollinger.items_raw\n";
		print $createtable."\n";
		$dbHandler->update($createtable);
		my $insertQuery="";
		foreach(@allOutCSV)
		{
			$insertQuery.="(";
			my @line = @{$_};
			my $tc = 0;
			foreach(@line)
			{
				$txtout.="\"$_\"\t";
				my $temp = $_;				
				$temp=~s/\\/\\\\/g;
				$temp=~s/'/\\'/g;
				$insertQuery.="E'$temp',";
				$tc++;
			}
			while($tc < $countt)
			{
				$txtout.="\"\"\t";
				$insertQuery.="'',";
				$tc++;
			}
			$txtout = substr($txtout, 0, -1);
			$insertQuery = substr($insertQuery, 0, -1);
			$insertQuery.="),";
			$l->addLine($txtout);
			$txtout = "";
		}
		$insertQuery = substr($insertQuery, 0, -1);
		$insertQuery = "INSERT INTO M_BOLLINGER.ITEMS_RAW($columnQueryPart) VALUES $insertQuery";
		print "Inserting....\n";
		$dbHandler->update($insertQuery);
		
		print "Items $count Records outputed: ".$conf->{"itemscsvout"}."\n";
 
 }
 
 exit;