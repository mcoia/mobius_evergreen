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

GetOptions (
"log=s" => \$log,
"opensrfconf=s" => \$xmlconf,
"config=s" => \$configFile,
)
or die("Error in command line arguments\nYou can specify
--log path_to_log_output.log                  [Path to the log output file - required]
--opensrfconf /openils/conf/opensrf.xml                 [Path to the Evergreen Config file (used to get DB access, defaults to /openils/conf/opensrf.xml ]
--config path_to_config.conf              [Path to this script's config, required]
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

print "Found $reportCount report(s) in config\n";
while($finished < $reportCount)
{
    if($map{$reportNum})
    {
        print "Running '" . $map{$reportNum} ."'\n";
        # name => shift,
        # dbHandler => shift,
        # driver => shift,
        # screenshotDIR => shift,
        # log => shift,
        # debug => shift,
        # webURL => shift,
        # webLogin => shift,
        # webPass => shift,
        
        my $rep = new TLCWebReport($map{$reportNum},$dbHandler,$driver,$cwd,$log,$debug,$conf{"url"},$conf{"login"},$conf{"pass"}, $branches);
        $rep->scrape();
        $finished++;
    }
    $reportNum++;
}


undef $writePid;
closeBrowser();
$log->addLogLine("****************** Ending ******************");


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
            if( ($key eq 'report_'.$thisNum.'_tlcname') && (%conf{'report_'.$thisNum.'_migname'}) )
            {
                my %repProp = ();
                foreach(@reportTypes)
                {
                    $repProp{$_} = $conf{"report_".$thisNum."_$_"};
                }
                $reports{$val} = \%repProp;
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
    # my $driver = Selenium::Firefox->new();
    my $profile = Selenium::Firefox::Profile->new;
    $profile->set_preference(
        'browser.download.folderList' => '2',
        'browser.download.manager.showWhenStarting' => false,
        'browser.download.dir' => '/tmp',
        'browser.helperApps.neverAsk.saveToDisk' => "application/xls;text/csv"
    );
    $driver = Selenium::Remote::Driver->new
    (
        binary => '/usr/bin/geckodriver',
        browser_name  => 'firefox',
        'firefox_profile' => $profile
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
