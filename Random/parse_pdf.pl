#!/usr/bin/perl
# 

use strict; 
use Loghandler;
use Mobiusutil;
use DBhandler;
use Data::Dumper;
use XML::Simple;
use XML::TreeBuilder;
use Getopt::Long;
use utf8;
use Encode;
use MARC::File::XML ( BinaryEncoding => 'utf8', RecordFormat => 'UNIMARC' );
MARC::File::XML->default_record_format('USMARC');
 
 my $xmlconf = "/openils/conf/opensrf.xml";
 our $dbHandler;
 our $tablename = 'checkouts';
 our $marcfile;
 our $manualqueryfile;
 our $manualcsvfile;
 our $holdingfield;
 our $barcodesubfield;
 our $logfile;
 our $schema = 'm_seymour';
 our $count = 0;
 
our $mobUtil = new Mobiusutil(); 

our $log = new Loghandler($logfile);
$log->addLogLine(" ---------------- Script Starting ---------------- ");
my $file = "/mnt/evergreen/migration/seymour/data/patrons_with_items.parse";

my %dbconf = %{getDBconnects($xmlconf)};
$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});

my $readFile = new Loghandler($file);    
my @lines = @{$readFile->readFile($file)};

my %allOutCSV = ();

my $readingPatron = 0;
my @values = ();
foreach (@lines)
{
   
    my $line = $_;
    
    if($line =~ m/^\d{5,500}$/)  ## New patron encountered, close the last one out
    {
        $count++;
        if ($readingPatron)
        {
            my @c = ();
            foreach(@values)
            {
                push(@c,[@{$_}]);
            }
            $allOutCSV{$readingPatron} = \@c;
            # print Dumper(%allOutCSV);
            @values = ();
            print "reading patron $count\n" if ($count % 50 == 0);
        }
        $readingPatron = $line;
        $readingPatron =~ s/[\n|\r]//g;
    }
    else  ## data
    {
        if($line =~ m/\d*?\s\d\d\/\d\d\/\d\d\d\d\s\$\s.*$/)
        {
            # print "found item data\n$line\n";
            my ($item,$date) = $line =~ /(\d*?)\s(\d\d\/\d\d\/\d\d\d\d)\s\$\s.*$/;
            
            push(@values,[($item,$date)]);
            
        }
    }
}
my $l = new Loghandler('/mnt/evergreen/migration/seymour/data/patrons_with_items.insert');
$l->deleteFile();
my @header = ("patron_barcode","item_barcode","due_date");
# 
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
my $tfile = new Loghandler('/mnt/evergreen/migration/seymour/data/patrons_with_items.query');
$tfile->deleteFile();
$tfile->addLine($createtable);
$tfile->addLine($copytext);

my @creates = ();

while ((my $patron, my $values) = each(%allOutCSV))
{
    my @dataline = @{$values};
    foreach(@dataline)
    {
        my ($item,$date) = @{$_};
        $txtout.="$item\t$date";
        push(@creates, [($patron,$item,$date)]);
    }
    $txtout .="\n";
}
$txtout = $mobUtil->trim($txtout);
$l->addLine($txtout);


print "Now inserting into DB\n";
setupTable(\@header,\@creates);
 
 $log->addLogLine(" ---------------- Script Ending ---------------- ");

 
 
 
sub setupTable
{
	my @header = @{@_[0]};
	my @lines = @{@_[1]};
    
    $log->appendLine($_) foreach(@header);
    
	print "Gathering $tablename....";
	$log->addLine(Dumper(\@header));
	print scalar @lines." rows\n";
	
	
	#drop the table
	my $query = "DROP TABLE IF EXISTS $schema.$tablename";
	$log->addLine($query);
	$dbHandler->update($query);
	
	#create the table
	$query = "CREATE TABLE $schema.$tablename (";
	$query.="i".$_." TEXT," for @header;
	$query=substr($query,0,-1).")";
	$log->addLine($query);
	$dbHandler->update($query);
	
	if($#lines > -1)
	{
		#insert the data
		$query = "INSERT INTO $schema.$tablename (";
		$query.="i".$_."," for @header;
		$query=substr($query,0,-1).")\nVALUES\n";
        my $count = 0;
		foreach(@lines)
		{
            my @thisrow = @{$_};
            my $thisline = '';
            my $valcount = 0;
           
            $query.="(";
            for(@thisrow)
            {
                if($valcount < scalar @header)
                {
                    my $value = $_;
                    #add period on trialing $ signs
                    #print "$value -> ";
                    $value =~ s/\$$/\$\./;
                    $value =~ s/\n//;
                    $value =~ s/\r//;
                    # $value = NFD($value);
                    $value =~ s/[\x{80}-\x{ffff}]//go;
                    $thisline.=$value;
                    #print "$value\n";
                    $query.='$data$'.$value.'$data$,';
                    $valcount++;
                }
            }
            # pad columns for lines that are too short
            my $pad = $#header - $#thisrow - 1;
            for my $i (0..$pad)
            {
                $thisline.='$data$$data$,';
                $query.='$data$$data$,';
            }
            # $log->addLine( "final line $thisline");
            $query=substr($query,0,-1)."),\n";
            $count++;
            if( $count % 5000 == 0)
            {
                $query=substr($query,0,-2)."\n";
                print "Inserted ".$count." Rows into $schema.$tablename\n";
                $log->addLine($query);
                $dbHandler->update($query);
                $query = "INSERT INTO $schema.$tablename (";
                $query.="i".$_."," for @header;
                $query=substr($query,0,-1).")\nVALUES\n";
            }
            
		}
		$query=substr($query,0,-2)."\n";
		print "Inserted ".$count." Rows into $schema.$tablename\n";
        
		$log->addLine($query);
		$dbHandler->update($query);
	}
	else
	{
		print "Empty dataset for $tablename \n";
		$log->addLine("Empty dataset for $tablename");
	}
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