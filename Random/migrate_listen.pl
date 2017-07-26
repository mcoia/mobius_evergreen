#!/usr/bin/perl


use Loghandler;
use Mobiusutil;
use Data::Dumper;
use XML::Simple;
use XML::TreeBuilder;
use Getopt::Long;
use DBhandler;
use Encode;

our $schema;
our $mobUtil = new Mobiusutil();
our $log;
our $dbHandler;
our $data_dir;
our $sample;
our $loginvestigationoutput;
our @columns;
our @allRows;

my $xmlconf = "/openils/conf/opensrf.xml";
GetOptions (
"logfile=s" => \$logFile,
"xmlconfig=s" => \$xmlconf,
"data_dir=s" => \$data_dir,
"sample=i" => \$sample,
"schema=s" => \$schema,
)
or die("Error in command line arguments\nYou can specify
--logfile configfilename (required)
--xmlconfig pathtoevergreenopensrf.xml (default /opensrf/conf/opensrf.xml)
--data_dir path to the text file data directory
--schema (eg. m_slmpl)
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
if(!$schema)
{
	print "Please specify an Evergreen DB schema to dump the data to\n";
	exit;
}

	$log = new Loghandler($logFile);
	$log->truncFile("");
	$log->addLogLine(" ---------------- Script Starting ---------------- ");		

	my %dbconf = %{getDBconnects($xmlconf)};
	$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
    my $dh;
    opendir($dh, $data_dir) || die "Dan't open the directory $data_dir";
    print "opened folder $data_dir\n";
    my @dots;
    while (my $file = readdir($dh)) 
    {
        print "Checking $file\n";
        push @dots, $file if ( !( $file =~ m/^\./) && -f "$data_dir$file" && ( $file =~ m/\.txt$/i) )
    }
    closedir $dh;
    
    foreach(@dots)
    {
        print $_;
        my $tablename = $_;
        $tablename =~ s/\.txt//gi;
        $tablename =~ s/\s/_/g;
        my $file = new Loghandler($data_dir.''.$_);
        my $insertFile = new Loghandler($data_dir.''.$_.'.insert');
        $insertFile->deleteFile();
        my @lines = @{$file->readFile()};
        setupTable(\@lines,$tablename,$insertFile);
    }
    
	
	
	$log->addLogLine(" ---------------- Script End ---------------- ");

sub setupTable
{
	my @lines = @{@_[0]};
	my $tablename = @_[1];
    my $insertFile = @_[2];
    my $insertString = '';
	
    my $header = shift @lines;
    $log->addLine($header);
    my @cols = split(/\t/,$header);
    $log->appendLine($_) foreach(@cols);
    for my $i (0.. $#cols)
	{
        @cols[$i] =~ s/[\.\/\s\$!\-\(\)]/_/g;
        @cols[$i] =~ s/\_{2,50}/_/g;
        @cols[$i] =~ s/\_$//g;
	}
	print "Gathering $tablename....";
	$log->addLine(Dumper(\@cols));
    $insertString.= join("\t",@cols);
    $insertString.="\n";
	print $#lines." rows\n";
	
	
	#drop the table
	my $query = "DROP TABLE IF EXISTS $schema.$tablename";
	$log->addLine($query);
	$dbHandler->update($query);
	
	#create the table
	$query = "CREATE TABLE $schema.$tablename (";
	$query.=$_." TEXT," for @cols;
	$query=substr($query,0,-1).")";
	$log->addLine($query);
	$dbHandler->update($query);
	
	if($#lines > -1)
	{
		#insert the data
		$query = "INSERT INTO $schema.$tablename (";
		$query.=$_."," for @cols;
		$query=substr($query,0,-1).")\nVALUES\n";
        my $count = 0;
		foreach(@lines)
		{
            last if ( $sample && ($count > $sample) );
            # ensure that there is at least one tab
            if($_ =~ m/\t/)
            {
                my @thisrow = split(/\t/,$_);
                my $thisline = '';
                my $valcount = 0;
                # if(@thisrow[0] =~ m/2203721731/)
                # {
                $query.="(";
                for(@thisrow)
                {
                    if($valcount < scalar @cols)
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
                        $insertString.=$value."\t";
                        #print "$value\n";
                        $query.='$$'.$value.'$$,';
                        $valcount++;
                    }
                }
                # pad columns for lines that are too short
                my $pad = $#cols - $#thisrow - 1;
                for my $i (0..$pad)
                {
                    $thisline.='$$$$,';
                    $query.='$$$$,';
                    $insertString.="\t";
                }
                $insertString = substr($insertString,0,-1)."\n";
                # $log->addLine( "final line $thisline");
                $query=substr($query,0,-1)."),\n";
                $count++;
                if( $count % 5000 == 0)
                {
                    $insertFile->addLine($insertString);
                    $insertString='';
                    $query=substr($query,0,-2)."\n";
                    $loginvestigationoutput.="select count(*),$_ from $schema.$tablename group by $_ order by $_\n" for @cols;
                    print "Inserted ".$count." Rows into $schema.$tablename\n";
                    $log->addLine($query);
                    $dbHandler->update($query);
                    $query = "INSERT INTO $schema.$tablename (";
                    $query.=$_."," for @cols;
                    $query=substr($query,0,-1).")\nVALUES\n";
                }
                # }
            }
		}
		$query=substr($query,0,-2)."\n";
		$loginvestigationoutput.="select count(*),$_ from $schema.$tablename group by $_ order by $_\n" for @cols;
		print "Inserted ".$count." Rows into $schema.$tablename\n";
		$log->addLine($query);
		$dbHandler->update($query);
        $insertFile->addLine($insertString);
	}
	else
	{
		print "Empty dataset for $tablename \n";
		$log->addLine("Empty dataset for $tablename");
	}
}

sub calcCheckDigit
{
	my $seed =@_[1];
	$seed = reverse($seed);
	my @chars = split("", $seed);
	my $checkDigit = 0;
	for my $i (0.. $#chars)
	{
		$checkDigit += @chars[$i] * ($i+2);
	}
	$checkDigit =$checkDigit%11;
	if($checkDigit>9)
	{
		$checkDigit='x';
	}
	return $checkDigit;
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