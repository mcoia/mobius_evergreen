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
 use encoding 'utf8', Filter => 1;
 use MARC::Record;
 use MARC::File::XML (BinaryEncoding => 'utf8');
 
 #use warnings;
 #use diagnostics; 

my $outputdir = "/tmp/run/";
my $marcfile = "/tmp/run/conv.xml";

 
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
		my $bcodout = new Loghandler("/tmp/run/callnums.txt");
		$bcodout->deleteFile();
		my $callnumout = new Loghandler("/tmp/run/callnum_rows.txt");
		$callnumout->deleteFile();
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
			my @callNumBarcodes = @{getBarcodesWithnNoCallNum($dbHandler)};
			
			my @marcOutputRecords;
			my $count=0;
			my $matches=0;
			my $file = MARC::File::XML->in($marcfile);
			while ( my $marc = $file->next() ) 
			{	
				$count++;
				my $shortnameexists=0;
				if(1)#$matches<5)
				{
					my $hasspecial=0;
					my @all = $marc->fields();
					
					my @updates = ();					
					
					foreach(@all)
					{
						my $field = $_;
						
						if($field->tag() eq '852')
						{
							if($field->subfield('p'))
							{
								my $p = $field->subfield('p');
								foreach(@callNumBarcodes)
								{
									if($p eq $_)
									{
										if($field->subfield('h') && length($field->subfield('h'))>2)
										{
											$bcodout->addLine("\"$p\",\"".$field->subfield('h')."\"");
											updateCallNumberInDB($p,$field->subfield('h'),$dbHandler,$log);
										}
									}
								}
							}
							if(0) #Flagging bib if has special characters
							{
								my @allsubs = $_->subfields();
								foreach(@allsubs)
								{	
									my $test = @{$_}[1];
									if($test =~ m/[\x80-\x{FFFF}]/)
									{
										my $p = $field->subfield('p');
										$matches++;
										#$bcodout->addLine($field->as_formatted());
										my $callNumDB = getCallNumberRow($p,$dbHandler);
										if($callNumDB eq '"NO CALL NUMBER"')
										{
											if($field->subfield('h'))
											{
												$bcodout->addLine("\"$p\",\"".$field->subfield('h')."\"");
												updateCallNumberInDB($p,$field->subfield('h'),$dbHandler,$log);
											}
											else
											{
												print $field->subfield('h')."\n";
											}
										}
										$callnumout->addLine($callNumDB);
									}
									
								}
							}
						}							
					}
				}
				
			}
			print "Processed $count records\nMatched and updated $matches records\n";
				
		 }
		 
		
		 
		 $log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile' and 'marcoutdir'\n";
	}
 }
 
 sub getBarcodesWithnNoCallNum
 {
	my $dbHandler = @_[0];
	my $query="select DISTINCT barcode from asset.copy where call_number in(select id from asset.call_number where label='NO CALL NUMBER' AND owning_lib in(148,150))";
	my @results = @{$dbHandler->query($query)};
	my @ret;
	my $ret;
	foreach(@results)
	{
		my $ro = $_;
		my @row = @{$ro};
		push @ret,@row[0];
	}
	
	return \@ret;
	
 }
 sub updateCallNumberInDB
 {
	my $barcode = @_[0];
	my $callNum = @_[1];
	my $dbHandler = @_[2];
	my $log = @_[3];
	my $query = "UPDATE asset.call_number SET LABEL = E'$callNum' where id=(select call_number from asset.copy where barcode='$barcode');";
	$log->addLine($query);
	$dbHandler->update($query);
 }
 sub getCallNumberRow
 {
	my $barcode = @_[0];
	my $dbHandler = @_[1];
	my $query = "select label from asset.call_number where id=(select call_number from asset.copy where barcode='$barcode')";
	my @results = @{$dbHandler->query($query)};
	my $ret;
	foreach(@results)
	{
		my $ro = $_;
		my @row = @{$ro};
		foreach(@row)
		{
			$ret.="\"$_\",";
		}
		$ret=substr($ret,0,-1);
		#$ret.="\n";
	}
	
	return $ret;
 }
 
 sub getXMLfromDB
 {
	my $barcode = @_[0];
	my $dbHandler = @_[1];
	my $log = @_[2];
	my $query = "select id,marc from biblio.record_entry where id in(select record from asset.call_number where id=(select call_number from asset.copy where barcode='$barcode'))";
	#print $query."\n";
	my @results = @{$dbHandler->query($query)};
	my $dbID;
	my $marcd;
	#print $#results."\n";
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		$dbID = @row[0];
		my $xml = @row[1];		
		eval 
		{
			$marcd = MARC::Record->new_from_xml($xml);
		};
		if ($@) 
		{
			$log->addLine("could not parse $dbID: $@");
			#elog("could not parse $dbID: $@\n");
			import MARC::File::XML (BinaryEncoding => 'utf8');
			print "could not parse $dbID: $@\n";
		}
		if(length($dbID)<2)
		{	
			print "HEY! $dbID - $barcode\n";
		}
		return ($dbID,$marcd);
	}
	
 }
 
 sub replaceField
 {
	my $field = @_[0];
	my $marc = @_[1];
	my $log = @_[2];
	if(ref($field) eq "ARRAY")
	{
		my @fields = @{$field};
		my $l = $#fields;
		my $tag = @fields[0]->tag();
		#print "New number: $l\n";
		my @oldfields = $marc->field($tag);
		$log->addLine(Dumper(@oldfields));
		$log->addLine("Moving to:");
		$log->addLine(Dumper(@fields));
		$marc->delete_fields(@oldfields);
		$marc->insert_fields_ordered( @fields );
	}
	else
	{
		my $title = $marc->field('245')->subfield('a');
		my $tag = $field->tag();
		if($field->is_control_field())
		{
			my $oldfield = $marc->field($tag);
			if(1)#$oldfield !=~ m/[\x80-\xFF]/)
			{
				my @add=($field);
				$marc->delete_field($oldfield);
				$marc->insert_fields_ordered( @add );
			}
			
		}
		else
		{
			my @allsubs = $field->subfields();
			foreach(@allsubs)
			{
				my $subtag = @{$_}[0];
				my $data = @{$_}[1];
				#print "$tag Subtag: $subtag\n";
				my @oldfields = $marc->field($tag);
				if($#oldfields>1)
				{
					print "More than 1 $tag for $title\n";
				}
				if($marc->field($tag))
				{
					if($marc->field($tag)->subfield($subtag))
					{
						my $testorg = $marc->field($tag)->subfield($subtag);
						#my $hasit = $testorg =~ m/[\x80-\xFFFF]/;
						#print "ORG: $hasit\n";
						if(1)#$testorg !=~ m/[\x80-\xFFFF]/  &&  $data =~ m/[\x80-\xFFFF]/ )
						{
							#print "Original did not have specials and current did\n";
							my $oldfield = $marc->field($tag);
							$log->addLine(Dumper($oldfield));
							$marc->delete_field($oldfield);
							#print(Dumper($oldfield));
							$oldfield->delete_subfield(code => $subtag);
							#print(Dumper($oldfield));
							$oldfield->add_subfields( $subtag => $data );
							#print(Dumper($oldfield));
							$log->addLine("Moving to:\n".Dumper($oldfield));
							my @add = ($oldfield);
							$marc->insert_fields_ordered( @add );
							#$log->addLine("Marc:\n".Dumper($marc));
						}
					}
					else
					{
						my $oldfield=$marc->field($tag);
						$marc->delete_field($oldfield);
						$oldfield->delete_subfield(code => $subtag);
						$oldfield->add_subfields($subtag => $data);
						my @add = ($oldfield);
						$marc->insert_fields_ordered( @add );				
						
					}
				}
				else
				{
					print "DB did not have $tag - so I added it\n";
					my @add = ($field);
					$marc->insert_fields_ordered( @add );
				}
			}
		}
	}
	return $marc;
 }
 
 sub makeXMLfromMARC
 {
	my $marcd = @_[0];
	my $outputxml = $marcd->as_xml();
	$outputxml =~ s/\\/\\\\/g;
	$outputxml =~ s/\'/\\\'/g;
	$outputxml =~ s/\n//g;
	$outputxml =~ s/\r//g;
	return $outputxml;
 }
 
 exit;