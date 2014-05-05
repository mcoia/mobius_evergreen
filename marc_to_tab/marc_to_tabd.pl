#!/usr/bin/perl
# 
# Example Configure file:
# 
# logfile = /tmp/log.log
# csvout = /tmp/run/marc_to_tab_extract.csv

 use strict; 
 use Loghandler;
 use Mobiusutil;
 use DBhandler;
 use Data::Dumper;
 use DateTime;
 use utf8;
 use Encode;
 use DateTime::Format::Duration;
 
 #use warnings;
 #use diagnostics; 
		 
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
		$log->addLogLine(" ---------------- Script Starting ---------------- ");
		my $file = MARC::File::USMARC->in('/tmp/run/marc.mrc');
		my @final = ();
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
			@marcOutput=();
				my $field = $_;
				my $tag = $field->tag();
				# my $append = 1;
				# while($fieldcount{$tag."_".$append})
				# {
					# $append++;
				# }
				# $fieldcount{$tag."_".$append}=1;
				# $tag = $tag."_".$append;
				
				if($tag =='852')
				{
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
						print "not found so creating header: $found\n";
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
				}
				
				}
				
			}
			push(@allOutCSV,[@marcOutput]);
			
		}
		my $l = new Loghandler($conf->{"csvout"});
		$l->deleteFile();
		my $txtout = "";
		my $countt=0;
		my $createtable="create table(";
		my $copytext="";
		foreach(@header)
		{
			$txtout.="\"$_\"\t";
			$createtable.="i$_ TEXT,";
			$copytext.="i$_,";
			$countt++;
		}
		$txtout = substr($txtout, 0, -1);
		$createtable = substr($createtable, 0, -1).")";
		$copytext = substr($copytext, 0, -1);
		$l->addLine($txtout);
		$txtout = "";
		my $tfile = new Loghandler("/tmp/run/querys.txt");
		$tfile->deleteFile();
		$tfile->addLine($createtable);
		$tfile->addLine($copytext);
		foreach(@allOutCSV)
		{
			my @line = @{$_};
			my $tc = 0;
			foreach(@line)
			{
				if(length($_)>0)
				{
					$txtout.="\"$_\"\t";					
				}
				else
				{
					$txtout.="\"$_\"\t";
				}
				$tc++;
			}
			while($tc < $countt)
			{
				$txtout.="\"\"\t";
				$tc++;
			}
			$txtout = substr($txtout, 0, -1);
			$l->addLine($txtout);
			$txtout = "";
		}
		
		
		print "Found $count Records outputed: ".$conf->{"csvout"}."\n";
		
		 
		 $log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile' and 'marcoutdir'\n";
		
	}
 }
 
 exit;