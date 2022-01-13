#!/usr/bin/perl
# /production/sites/default/settings.php

use lib qw(./ ../); 

use Data::Dumper;
use File::Path qw(make_path remove_tree);
use File::Copy;
use Encode;
use Text::CSV;
use DateTime;
use DateTime::Format::Duration;
use DateTime::Span;
use JSON;
use Selenium::Remote::Driver;
use Selenium::Firefox;
use Selenium::Remote::WebElement;
use pQuery;
use Getopt::Long;
use Cwd;
use Mobiusutil;
use Loghandler;
use DBhandler;

use TLCWebController;
use TLCWebReport;

our $schema;
our $pidfile = "/tmp/tlc_import.pl.pid";
our $xmlconf = "/openils/conf/opensrf.xml";
our $configFile;
our $mobUtil = new Mobiusutil();

our $driver;
our $dbHandler;
our $log;
our $debug = 0;
our %conf;
our $branches;
our $processfile;
our $fileconfig;

GetOptions (
"log=s" => \$log,
"opensrfconf=s" => \$xmlconf,
"config=s" => \$configFile,
"processfile=s" => \$processfile,
"manual_process_fileconfig=s" => \$fileconfig,
)
or die("Error in command line arguments\nYou can specify
--log path_to_log_output.log                  [Path to the log output file - required]
--opensrfconf /openils/conf/opensrf.xml       [Path to the Evergreen Config file (used to get DB access, defaults to /openils/conf/opensrf.xml ]
--config path_to_config.conf                  [Path to this script's config, required]
--processfile path_to_raw_csv_from_TLC        [Path to the TLC downloaded report]
--manual_process_fileconfig name_of_config_report_match      [When specifying a manually downloaded file, you need to provide the configuration map that it belongs to, AKA 'report_3']
\n");


if(!$log)
{
    print "Please specify a logfile \n";
    exit;
}

if(!$configFile)
{
    print "Please specify a config file \n";
    exit;
}

my $conf = $mobUtil->readConfFile($configFile);
if($conf)
{
    %conf = %{$conf};
}
else
{
    print "Something wrong with the config file: $configFile\n";
    exit;
}

$log = new Loghandler($log);

figurePIDFileStuff();

$log->truncFile("");
$log->addLogLine("****************** Starting ******************");

my %dbconf = %{getDBconnects($xmlconf)};
$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});

initializeBrowser();

my $writePid = new Loghandler($pidfile);
$writePid->truncFile("running");

my $cwd = getcwd();
$cwd .= "/screenshots";
mkdir $cwd unless -d $cwd;

my $dataDir = %conf{"report_save_folder"};
mkdir $dataDir unless -d $dataDir;

figureBranches();

if(scalar @{$branches} == 0)
{
    print "You've not defined any branches in the config file\n";
    exit;
}

my %reports = %{figureReportConfigs()};
my %map = %{$reports{"map"}};
my $reportNum = 0;
my $finished = 0;
my $reportCount = keys %map;
my $failed = 0;

print "Found $reportCount report(s) in config\n";
while($finished < $reportCount)
{
    if($map{$reportNum})
    {
        print $mobUtil->boxText("Running '" . $map{$reportNum} ."'","#","|",4);
        my %props = %{$reports{$reportNum}};
        my $attr = $props{"attr"};
        my $outFileName = $props{"migname"};
        my $colRemoves = $props{"colrem"};
        # print "sending\n".Dumper($branches) if($finished > 0);

        my $rep = new TLCWebReport($map{$reportNum}, $driver, $cwd, $log, $debug, $conf{"url"}, $conf{"login"}, $conf{"pass"}, $branches, $attr, $conf{"output_folder"}, $outFileName, $colRemoves);
        local $@;
        eval
        {
            if(!$fileconfig)
            {
                $rep->scrape();
            }
            elsif($fileconfig && $processfile)
            {
                $rep->processDownloadedFile($processfile, 1);
            }
            $props{"file"} = $rep->getResultFile();
        };
        if( $@ )
        {
            $failed++;
            print $mobUtil->boxText("We have a failure: '" . $map{$reportNum} ."'","#","|",4);
            print "Press Enter to continue to the next report\n";
            my $answer = <STDIN>;
            $props{"error"} = $rep->getError();
        }
        else
        {
            $props{"error"} = $rep->getError();
        }
        
       
        $finished++;
        $reports{$reportNum} = \%props;
        undef $rep;
    }
    $reportNum++;
}

undef $writePid;
closeBrowser();

my $summary = $mobUtil->boxText("Completed: $finished","#","|",1);
$summary .=  $mobUtil->boxText("Failed: $failed","#","|",1);

$reportNum = 0;
$finished = 0;
while($finished < $reportCount)
{
    if($map{$reportNum})
    {
        my %props = %{$reports{$reportNum}};
        my $file = $props{"file"};
        my $error = $props{"error"};
        print "---- $map{$reportNum} ----\n";
        print "->   $file\n" if !$error;
        print "->   $error\n" if $error;
        $finished++;
        undef $rep;
    }
    $reportNum++;
}

print $summary;

if($conf{"import"} =~/yes/i && $conf{"import_schema"})
{
    print "You've elected to automatically import these files into the database\nPress enter to continue:";
    my $answer = <STDIN>;
    my $data_dir = $conf{"output_folder"};
    $data_dir .= "/" if!($data_dir =~ /\/$/);
    my $dh;
    opendir($dh, $data_dir) || die "Can't open the directory $data_dir";
    my @dots;
    while (my $file = readdir($dh)) 
    {
        push @dots, $file if ( !( $file =~ m/^\./) && -f "$data_dir$file" && ( $file =~ m/\.migdat$/i) )
    }
    closedir $dh;

    foreach(@dots)
    {
        print $_."\n";
        my $tablename = $_;
        $tablename =~ s/\.migdat//gi;
        $tablename =~ s/\s/_/g;
        my $file = new Loghandler($data_dir.''.$_);
        my $insertFile = new Loghandler($data_dir.''.$_.'.insert');
        $insertFile->deleteFile();
        my @lines = @{$file->readFile()};
        setupTable(\@lines,$tablename,$insertFile,$conf{"import_schema"});
    }
    
}


$log->addLogLine("****************** Ending ******************");


sub setupTable
{
	my @lines = @{@_[0]};
	my $tablename = @_[1];
    my $insertFile = @_[2];
    my $schema = @_[3];
    my $insertString = '';
	
    my $emptyHeaderName = 'ghost';
    my $header = shift @lines;
    $log->addLine($header);
    my @cols = split(/\t/,$header);
    $log->appendLine($_) foreach(@cols);
    my %colTracker = ();
    for my $i (0.. $#cols)
	{
        @cols[$i] =~ s/[\.\/\s\$!\-\(\)]/_/g;
        @cols[$i] =~ s/\_{2,50}/_/g;
        @cols[$i] =~ s/\_$//g;
        @cols[$i] =~ s/,//g;
        @cols[$i] =~ s/\*//g;
        
        # Catch those naughty columns that don't have anything left to give
        $emptyHeaderName.='t' if(length(@cols[$i]) == 0);
        @cols[$i]=$emptyHeaderName if(length(@cols[$i]) == 0);
        my $int = 1;
        my $base = @cols[$i];
        while($colTracker{@cols[$i]}) #Fix duplicate column names
        {
            @cols[$i] = $base."_".$int;
            $int++;
        }
        $colTracker{@cols[$i]} = 1;
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
                # $log->appendLine($_) if $count > 15000;
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
                        $query.='$data$'.$value.'$data$,';
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


sub figureBranches
{
    my $b = $conf{"branches"};
    my @branchs = split(/,/,$b);
    foreach my $i (0 .. $#branchs)
    {
        @branchs[$i] = trim(@branchs[$i]);
    }
    $branches = \@branchs;
}

sub figureReportConfigs
{
    my %reports = ();
    my %reportNumbers = ();
    my @reportTypes = ("tlcname","migname","colrem","attr");
    while ((my $key, my $val) = each(%conf))
    {
        if($key =~ m/report/)
        {
            # print "$key\n";
            my $thisNum = $key;
            $thisNum =~ s/[^_]*_([^_]*)_.*/$1/g;
            # print $thisNum ."\n";
            my $process = 0;
            if(!$processfile) #if bash arguments specified a specific file to process, then we only want the matching report config, otherwise, we do them all
            {
                $process = 1 if( ($key eq 'report_'.$thisNum.'_tlcname') && (%conf{'report_'.$thisNum.'_migname'}) );
            }
            else
            {
                $process = 1 if( ($key eq $fileconfig.'_tlcname') && (%conf{$fileconfig.'_migname'}) );
            }
            if( $process )
            {
                my %repProp = ();
                foreach(@reportTypes)
                {
                    $repProp{$_} = $conf{"report_".$thisNum."_$_"};
                }
                $reports{$thisNum} = \%repProp;
                $reportNumbers{$thisNum} = $val;
            }
        }
    }
    $reports{"map"} = \%reportNumbers;
    return \%reports;
}

sub escapeData
{
    my $d = shift;
    $d =~ s/'/\\'/g;
    return $d;
}

sub unEscapeData
{
    my $d = shift;
    $d =~ s/\\'/'/g;
    return $d;
}

sub initializeBrowser
{
    $Selenium::Remote::Driver::FORCE_WD3=1;
    print "Setting Download folder:\n ".$conf{"output_folder"} . "\n";
    # my $driver = Selenium::Firefox->new();
    my $profile = Selenium::Firefox::Profile->new;
    $profile->set_preference('browser.download.folderList' => '2');
    $profile->set_preference('browser.download.dir' => $conf{"output_folder"});
    # $profile->set_preference('browser.helperApps.neverAsk.saveToDisk' => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;application/pdf;text/plain;application/text;text/xml;application/xml;application/xls;text/csv;application/xlsx");
    $profile->set_preference('browser.helperApps.neverAsk.saveToDisk' => "application/vnd.ms-excel; charset=UTF-16LE");
    # $profile->set_preference('browser.helperApps.neverAsk.saveToDisk' => "");
    $profile->set_preference("browser.helperApps.neverAsk.openFile" =>"application/vnd.ms-excel; charset=UTF-16LE");
    # $profile->set_preference("browser.helperApps.neverAsk.openFile" => "");
    $profile->set_boolean_preference('browser.download.manager.showWhenStarting' => 0);
    $profile->set_boolean_preference('pdfjs.disabled' => 1);
    $profile->set_boolean_preference('browser.helperApps.alwaysAsk.force' => 0);
    $profile->set_boolean_preference('browser.download.manager.useWindow' => 0);
    $profile->set_boolean_preference("browser.download.manager.focusWhenStarting" => 0);
    $profile->set_boolean_preference("browser.download.manager.showAlertOnComplete" => 0);
    $profile->set_boolean_preference("browser.download.manager.closeWhenDone" => 1);
    $profile->set_boolean_preference("browser.download.manager.alertOnEXEOpen" => 0);

    $driver = Selenium::Remote::Driver->new
    (
        binary => '/usr/bin/geckodriver',
        # binary => '/usr/bin/firefox',
        browser_name  => 'firefox',
        firefox_profile => $profile
    );
    $driver->set_window_size(1200,1500);
}

sub closeBrowser
{
    $driver->quit;

    # $driver->shutdown_binary;
}

sub trim
{
    my $st = shift;
    $st =~ s/^[\s\t]*(.*)/$1/;
    $st =~ s/(.*)[\s\t]*$/$1/;
    return $st;
}

sub dirtrav
{
    my @files = @{@_[0]};
    my $pwd = @_[1];
    opendir(DIR,"$pwd") or die "Cannot open $pwd\n";
    my @thisdir = readdir(DIR);
    closedir(DIR);
    foreach my $file (@thisdir) 
    {
        if(($file ne ".") and ($file ne ".."))
        {
            if (-d "$pwd/$file")
            {
                push(@files, "$pwd/$file");
                @files = @{dirtrav(\@files,"$pwd/$file")};
            }
            elsif (-f "$pwd/$file")
            {           
                push(@files, "$pwd/$file");         
            }
        }
    }
    return \@files;
}

sub figurePIDFileStuff
{
    if (-e $pidfile)
    {
        #Check the processes and see if there is a copy running:
        my $thisScriptName = $0;
        my $numberOfNonMeProcesses = scalar grep /$thisScriptName/, (split /\n/, `ps -aef`);
        print "$thisScriptName has $numberOfNonMeProcesses running\n" if $debug;
        # The number of processes running in the grep statement will include this process,
        # if there is another one the count will be greater than 1
        if($numberOfNonMeProcesses > 1)
        {
            print "Sorry, it looks like I am already running.\nIf you know that I am not, please delete $pidfile\n";
            exit;
        }
        else
        {
            #I'm really not running
            unlink $pidFile;
        }
    }
}


sub getDBconnects
{
    my $openilsfile = @_[0];
    my $log = @_[1];
    my $xml = new XML::Simple;
    my $data = $xml->XMLin($openilsfile);
    my %conf;
    $conf{"dbhost"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{host};
    $conf{"db"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{db};
    $conf{"dbuser"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{user};
    $conf{"dbpass"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{pw};
    $conf{"port"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{port};
    #print Dumper(\%conf);
    return \%conf;

}

sub DESTROY
{
    print "I'm dying, deleting PID file $pidFile\n";
    unlink $pidFile;
}

exit;
