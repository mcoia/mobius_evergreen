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
--opensrfconf /openils/conf/opensrf.xml       [Path to the Evergreen Config file (used to get DB access, defaults to /openils/conf/opensrf.xml ]
--config path_to_config.conf                  [Path to this script's config, required]
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
        print Dumper($branches) if($finished > 0);

        my $rep = new TLCWebReport($map{$reportNum}, $dbHandler, $driver, $cwd, $log, $debug, $conf{"url"}, $conf{"login"}, $conf{"pass"}, $branches, $attr, $conf{"output_folder"}, $outFileName);
        local $@;
        eval
        {
            $rep->scrape();
        };
        if( $@ )
        {
            $failed++;
            print $mobUtil->boxText("We have a failure: '" . $map{$reportNum} ."'","#","|",4);
            print "Press Enter to continue to the next report\n";
            my $answer = <STDIN>;
        }
        else
        {
            $finished++;
        }
        $rep = undef;
    }
    $reportNum++;
}


undef $writePid;
closeBrowser();
$log->addLogLine("****************** Ending ******************");

print $mobUtil->boxText("Completed: $finished","#","|",1);
print $mobUtil->boxText("Failed: $failed","#","|",1);



sub importFileIntoDB
{
    my $file = $_;
    print "Processing $file\n";
    my $path;
    my @sp = split('/',$file);
    $path=substr($file,0,( (length(@sp[$#sp]))*-1) );
    my $bareFilename =  pop @sp;
    @sp = split(/\./,$bareFilename);
    $bareFilename =  shift @sp;
    $bareFilename =~ s/^\s+//;
    $bareFilename =~ s/^\t+//;
    $bareFilename =~ s/\s+$//;
    $bareFilename =~ s/\t+$//;
    $bareFilename =~ s/^_+//;
    $bareFilename =~ s/_+$//;
    
    my $tableName = $tablePrefix."_".$bareFilename;
    
    $inputFileFriendly .= "\r\n" . $bareFilename;


    checkFileReady($file);
    
    my @colPositions = (); # two dimension array with number pairs [position, length]
    my $lineCount = -1;
    my @columnNames;
    my $baseInsertHeader = "INSERT INTO $schema.$tableName (";
    my $queryByHand = '';
    my $queryInserts = '';
    my @queryValues = ();
    my $success = 0;
    my $accumulatedTotal = 0;
    my $parameterCount = 0;
    
    open(my $fh,"<:encoding(UTF-16)",$file) || die "error $!\n";
    while(<$fh>) 
    {
        $lineCount++;
        my $line = $_;
        
        if($lineCount == 0) #first line contains the column header names
        {
            # For now, we will just push the whole line because we need the second line to tell us the divisions.
            push(@columnNames, $line);
            next;
        }
        if($lineCount == 1) #second line contains the column division clues
        {
            my @chars = split('', $line);
            @colPositions = @{figureColumnPositions(\@chars)};
            @columnNames = @{getDataFromLine(\@colPositions, @columnNames[0])};
            my $query = "DROP TABLE IF EXISTS $schema.$tableName";
            $dbHandler->update($query);
            $query = "CREATE TABLE $schema.$tableName (";
            $query.="id bigserial," if ($primarykey);
            $query .= "$_ TEXT," foreach(@columnNames);
            $baseInsertHeader .= "$_," foreach(@columnNames);
            $query = substr($query,0,-1);
            $baseInsertHeader = substr($baseInsertHeader,0,-1);
            $query .= ")";
            $baseInsertHeader .= ")\nVALUES\n";
            $queryByHand = $baseInsertHeader;
            $queryInserts = $baseInsertHeader;
            $log->addLine($query);
            $dbHandler->update($query);
            next;
        }
        
        
        my @lineLength = split('', $line);
        # print $#lineLength."\n";
        my @lastCol = @{@colPositions[$#colPositions]};
        my $m = @lastCol[0] + @lastCol[1];
        # print "Needs to be:\n$m";
        next if ($#lineLength < (@lastCol[0] + @lastCol[1])); # Line is not long enough to get all columns
        my @data = @{getDataFromLine(\@colPositions, $line)};
        $queryInserts.="(" if ($#data > -1);
        $queryByHand.="(" if ($#data > -1);
        foreach(@data)
        {
            $parameterCount++ if (lc($_) ne 'null');
            push(@queryValues, $_) if (lc($_) ne 'null');
            $queryInserts .= "null, "  if (lc($_) eq 'null');
            $queryInserts .= "\$$parameterCount, "  if (lc($_) ne 'null');
            $queryByHand .= "null, " if (lc($_) eq 'null');
            $queryByHand .= "\$data\$$_\$data\$, " if (lc($_) ne 'null');
        }
        $queryInserts = substr($queryInserts,0,-2) if ($#data > -1);
        $queryByHand = substr($queryByHand,0,-2) if ($#data > -1);
        $queryInserts.="),\n" if ($#data > -1);
        $queryByHand.="),\n" if ($#data > -1);
        $success++ if ($#data > -1);
    
        if( ($success % 500 == 0) && ($success != 0) )
        {
            $accumulatedTotal+=$success;
            $queryInserts = substr($queryInserts,0,-2);
            $queryByHand = substr($queryByHand,0,-2);
            $log->addLine($queryByHand);
            # print ("Importing $success\n");
            $log->addLine("Importing $accumulatedTotal / $lineCount");
            $dbHandler->updateWithParameters($queryInserts,\@queryValues);
            $success = 0;
            @queryValues = ();
            $queryByHand = $baseInsertHeader;
            $queryInserts = $baseInsertHeader;
            $parameterCount = 0;
        }

    }
    close($fh);
    
    $queryInserts = substr($queryInserts,0,-2) if $success;
    $queryByHand = substr($queryByHand,0,-2) if $success;
    
    # Handle the case when there is only one row inserted
    if($success == 1)
    {
        $queryInserts =~ s/VALUES \(/VALUES /;            
        $queryInserts = substr($queryInserts,0,-1);
    }

    # $log->addLine($queryInserts);
    $log->addLine($queryByHand);
    # $log->addLine(Dumper(\@queryValues));
    
    $accumulatedTotal+=$success;
    $log->addLine("Importing $accumulatedTotal / $lineCount") if $success;
    
    $dbHandler->updateWithParameters($queryInserts,\@queryValues) if $success;
    
    # # delete the file so we don't read it again
    # Disabled because we are going to let bash do this 
    # so that we don't halt execution of this script in case of errors
    # unlink $file;
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
            if( ($key eq 'report_'.$thisNum.'_tlcname') && (%conf{'report_'.$thisNum.'_migname'}) )
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
