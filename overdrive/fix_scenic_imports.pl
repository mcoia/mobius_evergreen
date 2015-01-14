#!/usr/bin/perl

# These Perl modules are required:
# install pQuery
# install Email::MIME
# install Email::Sender::Simple
# install Digest::SHA1

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
use LWP::Simple;
use OpenILS::Application::AppUtils;
use DateTime::Format::Duration;
use Digest::SHA1;


 my $configFile = @ARGV[0];
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
		my @reqs = ("server","login","password","tempspace","archivefolder","dbhost","db","dbuser","dbpass","port","participants","logfile","yearstoscrape"); 
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
		my $archivefolder = $conf{"archivefolder"};
		if(!(-d $archivefolder))
		{
			$valid = 0;
			print "Sorry, the archive folder does not exist: $archivefolder\n";
			$errorMessage = "Sorry, the archive folder does not exist: $archivefolder";
		}
		
		my $finalImport = 0;
		my @info;
		my $count=0;
		my @files;
		my $dbHandler;
		if($valid)
		{	
			my @marcOutputRecords;
			my @shortnames = split(/,/,$conf{"participants"});
			my @shortnamesScenic = ('SRLWA');
			for my $y(0.. $#shortnames)
			{				
				@shortnames[$y]=$mobUtil->trim(@shortnames[$y]);
			}			
			$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});
			 my $query = "select id,marc,(select changed_marc from 
			 molib2go.bib_marc_update where record=a.id order by change_time desc limit 1)
			 from biblio.record_entry a where marc ~ \$\$scenicregional.lib.overdrive\$\$ and create_date > \$\$01-12-2014\$\$::date order by id";
			my @results = @{$dbHandler->query($query)};	
			my $count=0;
			foreach(@results)
			{
				my $hadboth=0;
				my $row = $_;
				my @row = @{$row};
				my $bibid = @row[0];
				my $bibmarc = @row[1];
				my $molibmarc = @row[2];				
				$bibmarc =~ s/(<leader>.........)./${1}a/;
				$bibmarc = MARC::Record->new_from_xml($bibmarc);
				$bibmarc = add9($bibmarc,\@shortnames);
				$bibmarc = add9sceniconly($bibmarc,\@shortnamesScenic);
				$bibmarc = readyMARCForInsertIntoME($bibmarc);
				removeOldCallNumberURI($bibid,$dbHandler);
				my $finalMARC;				
				if(length($mobUtil->trim($molibmarc)) && length($mobUtil->trim($molibmarc))>0)
				{	
					$hadboth=1;
					$molibmarc =~ s/(<leader>.........)./${1}a/;
					$molibmarc = MARC::Record->new_from_xml($molibmarc);
					$molibmarc = add9($molibmarc,\@shortnames);
					$finalMARC = mergeMARC856($molibmarc, $bibmarc, $log);
					$finalMARC = readyMARCForInsertIntoME($finalMARC);
					$finalMARC = convertMARCtoXML($finalMARC,$log);
				}
				else
				{
					$finalMARC = convertMARCtoXML($bibmarc,$log);
				}				
				$query = "UPDATE BIBLIO.RECORD_ENTRY SET MARC=\$1,edit_date=now() WHERE ID=$bibid";
				my @values = ($finalMARC);
				$log->addLine("$count\t$bibid\thttp://missourievergreen.org/eg/opac/record/$bibid?query=yellow;qtype=keyword;locg=4;expand=marchtml#marchtml\thttp://mig3.missourievergreen.org/eg/opac/record/$bibid?query=yellow;qtype=keyword;locg=157;expand=marchtml#marchtml");
				my $res = $dbHandler->updateWithParameters($query,\@values);
				
				$count++;
			}
			
		}
		$log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile'\n";		
	}
}

sub add9
{
	my $marc = @_[0];
	my @shortnames = @{@_[1]};
	my @recID = $marc->field('856');
	if(@recID)
	{
		#$marc->delete_fields( @recID );
		for my $rec(0..$#recID)
		{
			#print Dumper(@recID[$rec]);
			for my $t(0.. $#shortnames)
			{
				my @sub3 = @recID[$rec]->subfield( '3' );
				my @subu = @recID[$rec]->subfield( 'u' );
				my $ind2 = @recID[$rec]->indicator(2);
				my $ignore=0;
				foreach(@sub3)
				{
					if(lc($_) eq 'excerpt')
					{
						$ignore=1;
					}
				}
				if($ind2 ne '0')
				{
					$ignore=1;
				}
				foreach(@subu)
				{
					my $test = lc($_);
					if($test =~ m/scenic/g){$ignore=1;}
				}
				if(!$ignore)
				{
					my @s7 = @recID[$rec]->subfield( '7' );
					if($#s7==-1)
					{
						@recID[$rec]->add_subfields('7'=>'molib2go');
					}
					my @subfields = @recID[$rec]->subfield( '9' );
					my $shortnameexists=0;
					for my $subs(0..$#subfields)
					{
					#print "Comparing ".@subfields[$subs]. " to ".@shortnames[$t]."\n";
						if(@subfields[$subs] eq @shortnames[$t])
						{
							#print "Same!\n";
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
	}
	return $marc;
}


sub add9sceniconly
{
	my $marc = @_[0];
	my @shortnames = @{@_[1]};
	my @recID = $marc->field('856');
	if(@recID)
	{
		#$marc->delete_fields( @recID );
		for my $rec(0..$#recID)
		{
			#print Dumper(@recID[$rec]);
			for my $t(0.. $#shortnames)
			{
				my @subu = @recID[$rec]->subfield( 'u' );
				my $ind2 = @recID[$rec]->indicator(2);
				my $ignore=0;
				foreach(@subu)
				{
					my $test = lc($_);
					if($test=~ m/scenic/g){}
					else{$ignore=1;}
				}				
				if(!$ignore)
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
	}
	return $marc;
}

sub removeOldCallNumberURI
{
	my $bibid = @_[0];
	my $dbHandler = @_[1];
	my $query = "
	DELETE FROM asset.uri_call_number_map WHERE call_number in 
	(
		SELECT id from asset.call_number WHERE record = $bibid AND label = \$\$##URI##\$\$
	)
	";

	$dbHandler->update($query);
	$query = "
	DELETE FROM asset.uri_call_number_map WHERE call_number in 
	(
		SELECT id from asset.call_number WHERE  record = $bibid AND label = \$\$##URI##\$\$
	)";

	$dbHandler->update($query);
	$query = "
	DELETE FROM asset.uri WHERE id not in
	(
		SELECT uri FROM asset.uri_call_number_map
	)";

	$dbHandler->update($query);
	$query = "
	DELETE FROM asset.call_number WHERE  record = $bibid AND label = \$\$##URI##\$\$
	";

	$dbHandler->update($query);
	$query = "
	DELETE FROM asset.call_number WHERE  record = $bibid AND label = \$\$##URI##\$\$
	";

	$dbHandler->update($query);

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
	my $log = @_[1];
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

 exit;

 
 