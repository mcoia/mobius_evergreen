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
		my $bcodout = new Loghandler("/tmp/run/diacritics.txt");
		my $before = new Loghandler("/tmp/run/before.txt");
		my $after = new Loghandler("/tmp/run/after.txt");
		my $error = new Loghandler("/tmp/run/NON852.txt");
		$before->deleteFile();
		$error->deleteFile();
		$after->deleteFile();
		$bcodout->deleteFile();
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
			#print Dumper(@barcodes);
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
						if($_->is_control_field())
						{
							my $test = $_->data();
							if($test =~ m/[\x80-\x{FFFF}]/)
							{
								#print "$test\n";
								$hasspecial=1;
								push @updates,$field;
							}
						}
						else
						{
							if($field->tag() ne '852' && $field->tag() ne '029' && $field->tag() ne '049' && $field->tag() ne '551' && $field->tag() ne '590' 
							&& $field->tag() ne '945' && $field->tag() ne '948' && $field->tag() ne '949' && $field->tag() ne '985' && $field->tag() ne '988' 
							&& $field->tag() ne '989' && $field->tag() ne '995' && $field->tag() ne '998' && $field->tag() ne '999')
							{
								#print $field->tag()." - checking\n";
								my @allsubs = $_->subfields();
								foreach(@allsubs)
								{
									my $test = @{$_}[1];
									if($test =~ m/[\x80-\x{FFFF}]/)
									{
										#print "$test\n";
										$hasspecial=1;
										my @morethan1 = $marc->field($field->tag());
										if($#morethan1>0)
										{
											#print "Tag: ".$field->tag()."\n";
											#print $#morethan1."\nAdding hash\n";
											push @updates,[@morethan1];
										}
										else
										{
											push @updates,$field;
										}
									}
								}
							}
						}								
					}
					if($hasspecial)
					{	
						my $bb;
						my @recID = $marc->field('852');
						for my $rec(0..$#recID)
						{
							
							#print Dumper(@recID[$rec]);
							my @subfields = @recID[$rec]->subfield( 'p' );
							
							if(@subfields)
							{
								for my $subs(0..$#subfields)
								{
									if(length(@subfields[$subs])>3)
									{
										$bb = @subfields[$subs];
									}
								}								
							}						
						}
						if(length($bb)>3)
						{
							$matches++;
							$bcodout->addLine("'$bb',");
							my @stuff = getXMLfromDB($bb,$dbHandler);
							my $marcd = @stuff[1];
							my $dbID = @stuff[0];							
							$before->addLine($marcd->as_formatted());
							foreach(@updates)
							{
								my $field = $_;
								$marcd = replaceField($field, $marcd, $log);
							}
							
							my $outputxml = makeXMLfromMARC($marcd);
							$after->addLine($marcd->as_formatted());		
							my $query = "update biblio.record_entry set marc=E'$outputxml' where id=$dbID";
							#$log->addLine($query);
							#$dbHandler->update($query);
							#print "updated $dbID\n";
							#$log->addLine("updated $dbID");
							#print "Same!\n";
							my $field = MARC::Field->new('901','','','c' => "$dbID");
							$marc->append_fields($field);
						}
						else
						{
							my $title;
							my $e;
							if($marc->field('245'))
							{
								if($marc->field('245')->subfield('a'))
								{
									$title=$marc->field('245')->subfield('a');
								}
							}
							$e="245a = $title";
							if($marc->field('001'))
							{
								$e.= "   001= \"".$marc->field('001')->data()."\"";
							}
							$error->addLine($e);
							#print "Could not get 852p for $count 245a = $title\n";
						}
					}
					
					
					
				}
				
			}
			print "Processed $count records\nMatched and updated $matches records\n";
			my $marcout = new Loghandler("/tmp/run/carthagetitles.mrc");
			$marcout->deleteFile();
			my $output;
			foreach(@marcOutputRecords)
			{
				my $marc = $_;
				$output.=$marc->as_usmarc();
			}
			$marcout->addLine($output);
				
		 }
		 
		
		 
		 $log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile' and 'marcoutdir'\n";
	}
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