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
 our $tablename;
 our $marcfile;
 our $manualqueryfile;
 our $manualcsvfile;
 our $holdingfield;
 our $barcodesubfield;
 our $logfile;
 our $schema;
 
 
GetOptions (
"logfile=s" => \$logfile,
"xmlconfig=s" => \$xmlconf,
"marcfile=s" => \$marcfile,
"manualqueryfile=s" => \$manualqueryfile,
"manualcsvfile=s" => \$manualcsvfile,
"holding_field=s" => \$holdingfield,
"barcode_subfield=s" => \$barcodesubfield,
"tablename=s" => \$tablename,
"schema=s" => \$schema,
)
or die("Error in command line arguments\nYou can specify
--logfile configfilename (required)
--xmlconfig pathtoevergreenopensrf.xml (default /opensrf/conf/opensrf.xml)
--marcfile path to the marc file (XML File)
--holding_field AKA 852
--barcode_subfield AKA p
--tablename AKA subfield_holdings
--schema AKA m_seymour
\n");

our $mobUtil = new Mobiusutil(); 

our $log = new Loghandler($logfile);
$log->addLogLine(" ---------------- Script Starting ---------------- ");
my $file = MARC::File::XML->in($marcfile);
my @final = ();
my $count=0;
my @header;
my @allOutCSV;
my @marcOutput;

my %dbconf = %{getDBconnects($xmlconf)};
$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
    
while ( my $marc = $file->next())
{
    $count++;			
    my @fields = $marc->fields();			
    foreach(@fields)
    {
    
         my $field = $_;
         my $tag = $field->tag();
        
        
        if($tag eq $holdingfield)
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
            @marcOutput=();
        }
        
    }
}
my $l = new Loghandler($manualcsvfile);
$l->deleteFile();
my $txtout = "";
my $countt=0;
my $createtable="create table(";
my $copytext="";
for my $i (0.. $#header)
{
    @header[$i] =~ s/[\.\/\s\$!\-\(\)]/_/g;
    @header[$i] =~ s/\_{2,50}/_/g;
    @header[$i] =~ s/\_$//g;
}
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
my $tfile = new Loghandler($manualqueryfile);
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
            $txtout.="$_\t";					
        }
        else
        {
            $txtout.="$_\t";
        }
        $tc++;
    }
    while($tc < $countt)
    {
        $txtout.="\t";
        $tc++;
    }
    $txtout = substr($txtout, 0, -1)."\n";
}
$txtout = $mobUtil->trim($txtout);
$l->addLine($txtout);


print "Found $count Records outputed: $manualcsvfile\n";
print "Now inserting into DB\n";
setupTable(\@header,\@allOutCSV);
 
 $log->addLogLine(" ---------------- Script Ending ---------------- ");

 
 
 
sub setupTable
{
	my @header = @{@_[0]};
	my @lines = @{@_[1]};
    
    $log->appendLine($_) foreach(@header);
    
	print "Gathering $tablename....";
	$log->addLine(Dumper(\@header));
	print $#lines." rows\n";
	
	
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
                    $query.='$$'.$value.'$$,';
                    $valcount++;
                }
            }
            # pad columns for lines that are too short
            my $pad = $#header - $#thisrow - 1;
            for my $i (0..$pad)
            {
                $thisline.='$$$$,';
                $query.='$$$$,';
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