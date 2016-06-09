#!/usr/bin/perl

use lib qw(../);
use Loghandler;
use Mobiusutil;
use Data::Dumper;
use XML::Simple;
use XML::TreeBuilder;
use Getopt::Long;
use DBhandler;
use Encode;
use MARC::Record;
use MARC::File;
use MARC::File::XML (BinaryEncoding => 'utf8');
use OpenILS::Application::AppUtils;


our $wordnikapikey='';
our $mobUtil = new Mobiusutil();
our $log;
our $dbHandler;
our $dt;

our @columns;
our @allRows;
our @allcomps;
our @changed;
our @notchanged;
our $changedmarcxml = new Loghandler("/mnt/evergreen/tmp/oclcchangedmarc.xml");
our $before = new Loghandler("/mnt/evergreen/tmp/before.txt");
our $after = new Loghandler("/mnt/evergreen/tmp/after.txt");

my $xmlconf = "/openils/conf/opensrf.xml";
GetOptions (
"logfile=s" => \$logFile,
"xmlconfig=s" => \$xmlconf
)
or die("Error in command line arguments\nYou can specify
--logfile configfilename (required)
--xmlconfig pathtoevergreenopensrf.xml (default /opensrf/conf/opensrf.xml)
\n");

if(! -e $xmlconf)
{
	print "I could not find the xml config file: $xmlconf\nYou can specify the path when executing this script --xmlconfig configfilelocation\n";
	exit 0;
}
if(!$logFile)
{
	print "Please specify a log file\n";
	exit;
}

	$log = new Loghandler($logFile);
	$log->truncFile('');
	$log->addLogLine(" ---------------- Script Starting ---------------- ");

	my %dbconf = %{getDBconnects($xmlconf)};
	$changedmarcxml->truncFile('<collection xmlns="http://www.loc.gov/MARC21/slim">');
	$before->truncFile('');
	$after->truncFile('');
	
	$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
	my @files = ("/mnt/evergreen/tmp/D160301.R777010.XREFRPT.txt","/mnt/evergreen/tmp/D160303.R777009.XREFRPT.txt");
	$log->addLogLine("Reading files");
	foreach(@files)
	{
		my $file = new Loghandler($_);
		my @lines = @{$file->readFile()};
		foreach(@lines)
		{
			my $line = $_;
			if( $line =~ m/[^\d]*(\d*)[^\d]*(\d*)/ )
			{
				# $log->addLine("First number:".$1." second: ".$2);
				my @temp = ($1,$2);
				if( (length($1)>0) && ( length($2)>0) )
				{
					push(@allcomps,[@temp]);
				}
				# if($count>15){ $log->addLine(Dumper(\@allcomps)); exit;}
			}
		}
		$log->addLine("Read $count from ". $file->getFileName());
	}
	#$log->addLine(Dumper(\@allcomps));
	
	my $progress = 1;
	foreach(@allcomps)
	{
		my @thispair = @{$_};
		my $oclcnum = @thispair[0];
		my $egnum = @thispair[1];
		my $query = "select id,marc from biblio.record_entry where 
		marc ~\$\$<datafield tag=\"035\" ind1=\".\" ind2=\".\"><subfield code=\"a\">[^1]*$oclcnum\$\$
		and id=$egnum";
		my @results = @{$dbHandler->query($query)};
		if($#results == -1)
		{
			my $query = "select marc from biblio.record_entry where id=$egnum";
			my @marcresults = @{$dbHandler->query($query)};
			$log->addLogLine("$progress / $#allcomps");
			if($#marcresults == 0)
			{
				my @line = @{@marcresults[0]};
				my $newmarc = updateMARC( @line[0], $oclcnum );
				updateMARCDB($newmarc, $egnum);
				$changedmarcxml->addLine( $newmarc );
				my $marcob = MARC::Record->new_from_xml(@line[0]);
				$before->addLine($marcob->as_formatted());
				push(@changed, "$egnum, $oclcnum"); 
				undef $marcob;
			}
			else
			{
				$log->addLine("Bib $egnum doesn't exist in EG!");
			}
		}
		else
		{
			push(@notchanged, "$egnum, $oclcnum");
		}
		$progress++;
		#if($progress>50){last;}
	}
	
	foreach(@changed)
	{
		$log->addLine("changed ".$_);
	}
	
	foreach(@notchanged)
	{
		$log->addLine("not changed ".$_);
	}
	
	my $progress = 1;
	# oops - we have to undelete the deleted bibs in order for the ingest to work and add those 035s to the metabib.real_full_rec
	foreach(@allcomps)
	{
		my @thispair = @{$_};
		my $oclcnum = @thispair[0];
		my $egnum = @thispair[1];
		my $query = "select id,marc from biblio.record_entry where id=$egnum and deleted";
		my @results = @{$dbHandler->query($query)};
		if($#results == 0)
		{
			my $query = "update biblio.record_entry set deleted=false where id=$egnum";
			$dbHandler->update($query);
			$query = "update biblio.record_entry set deleted=true where id=$egnum";
			$dbHandler->update($query);
			$log->addLogLine("$egnum - $progress / $#allcomps");
		}
		$progress++;
	}
	
	$log->addLine("Changed: $#changed\nNot Changed: $#notchanged\nTotal: $#allcomps");
	$log->addLogLine(" ---------------- Script End ---------------- ");


sub updateMARC
{
	my $oldmarc = MARC::Record->new_from_xml(shift);
	my $oclcnum = shift;
	
	my $field = MARC::Field->new( '035',' ',' ','a'=>'(OCoLC)'.$oclcnum );
	
	my @zero35s = $oldmarc->field('035');
	foreach(@zero35s)
	{
		my $this035 = $_;
		my $data = $this035->subfield('a');
		if( lc( $data ) =~ m/oc[ol][lc]/ )
		{
			#$log->addLine("$data needs to be removed!");
			$oldmarc->delete_field($this035);
		}
	}
	my @inserts = ($field);
	$oldmarc->insert_fields_ordered(@inserts);
	$after->addLine($oldmarc->as_formatted());
	return convertMARCtoXML($oldmarc);
}

sub updateMARCDB
{
	my $newmarc = @_[0];
	my $bibid = @_[1];
	my $query = "UPDATE BIBLIO.RECORD_ENTRY SET MARC=\$1 WHERE ID=\$2";
	my @values = ($newmarc,$bibid);
	$dbHandler->updateWithParameters($query,\@values);
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

sub clockStart
{
	$dt = DateTime->now(time_zone => "local");
}

sub clockEnd
{
	my $afterProcess = DateTime->now(time_zone => "local");
	my $difference = $afterProcess - $dt;
	return $difference;
}

sub getDBconnects
{
	my $openilsfile = @_[0];
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