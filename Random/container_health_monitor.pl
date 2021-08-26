#!/usr/bin/perl
#
#
#

use strict; use warnings;
use lib qw(../);
use OpenILS::Utils::TestUtils;
use OpenILS::Utils::CStoreEditor qw/:funcs/;
use OpenILS::Utils::Fieldmapper;
use DateTime::Format::Duration;
use Loghandler;
use Mobiusutil;
use Data::Dumper;
use XML::Simple;
use DBhandler;
use File::Grep qw(fgrep);
use Sys::Hostname qw(hostname);

our $e;
our $script;
our %conf;
our $dbHandler;
our $log;
our $deathOutputFolder;
our $dt;
our %apacheTracking = ();
our $randomNumberMachine;

my $xmlconf = "/openils/conf/opensrf.xml";
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
    %conf = %{$conf};
    if ($conf{"logfile"})
    {
        $log = new Loghandler($conf->{"logfile"});
        $log->addLogLine(" ---------------- Script Starting ---------------- ");
        $deathOutputFolder = $conf{'death_output_folder'} || '/mnt/evergreen';

        my %checkFunctionMap = (
        "check_opensrf" => "doOpenSRFChecks()",
        "check_primary_grep" => "doGrepPhrases('primary')",
        "check_secondary_grep" => "doGrepPhrases('second')",
        "check_websocketd" => "doWebsocketdChecks()",
        "check_cpu" => "doCPUChecks()",
        "check_diskspace" => "doDiskFullChecks()",
        "check_apache" => "doApacheChecks()"
        );

        print "apache_soft_level = $conf{'apache_soft_level'}. \n";
        print "apache_hard_level = $conf{'apache_hard_level'}. \n";
        print "apache_soft_period = $conf{'apache_soft_period'}. \n";
        print "apache_hard_period = $conf{'apache_hard_period'}. \n";

        # Setup timestamp
        $dt = DateTime->now(time_zone => "local");
        $apacheTracking{"dt_hardlimit"} = $dt;
        $apacheTracking{"dt_softlimit"} = $dt;
        $apacheTracking{"hardlimit_exceeded"} = 0;
        $apacheTracking{"softlimit_exceeded"} = 0;

        # setup workstation and login
        # -------------
        # All OpenSRF checks are DISABLED NOW Because it's not really working right.
        # -------------
        # setupLogin( 1 ); # 1 = Call the create user function
        $|++;
        while(1)
        {
            while ((my $checkVar, my $runFunction) = each(%checkFunctionMap))
            {
                if( $conf{$checkVar} && $conf{$checkVar} ne 'no' && $conf{$checkVar} ne 'false' )
                {
                    eval $runFunction;
                }
            }

            my $duration = getDuration();
            print "It's been $duration minutes\n\r";
            undef $duration;

            # Sleep for awhile and let's do it again
            # Make sure that more than 1 app server isn't running these exact checks at the exact time
            # introduce some randomness
            # -------------
            # sleep 1;
            sleepRandomly();

        } # end while loop

        $log->addLogLine(" ---------------- Script Ending ---------------- ");
    }
    else
    {
        print "Config file does not define 'logfile' and 'marcoutdir'\n";
    }
}

sub doCPUChecks
{
    # Make sure the CPU is not out of control.
    # Exit if 15-min CPU load is greater than allowed cpu_percent_threshold.
    if ( $conf{'cpu_percent_threshold'} && $conf{'cpu_file_path'}  && -e $conf{'cpu_file_path'} )
    {
        my @load = split (" ", `cat $conf{'cpu_file_path'}`);

        my $load15 = @load[2] + 0;

        my $printWarning = $conf{'cpu_percent_threshold'} - 1;
        print "15-MIN load: $load15 \n" if($load15 > $printWarning);
        undef $printWarning;

        if ($load15 > $conf{'cpu_percent_threshold'})
        {
            killme("Exiting due to high 15-minute CPU load\nload $load15 is higher than allowed " . $conf{'cpu_percent_threshold'}, "CPU_load_$load15");
        }
        undef @load;
        undef $load15;
    }
}

sub doDiskFullChecks
{
    # Make sure the root volume has space.
    # Exit if the / file system's percent of free disk space is less than allowed diskspace_percent_free.
    if( $conf{'diskspace_percent_free'} )
    {
        my @list;
        foreach (`/bin/df /`)
        {
            if ($_ =~ /\/\n/)
            {
                @list = split(/\s+/, $_);
            }
        }

        # Example df output
        # Filesystem     1K-blocks     Used Available Use% Mounted on
        # /dev/sda2      927339848 97529236 782634628  12% /
        # $list[2] is "Used"
        # $list[3] is "Available"
        # percent free formula: (Available / (Used + Available)) * 100
        my $diskfree = (($list[3]) / ($list[2]+$list[3])) * 100.00;
        $diskfree = sprintf("%.2f", $diskfree);
        # print "Disk free percentage: $diskfree\n";
        # print Dumper(@list);
        my $threshold = $conf{'diskspace_percent_free'} + 0; # converts to numeric
        if ($diskfree < $threshold)
        {
            killme("exiting due to disk space problem. \n/ free space ${diskfree}% is less than allowed $conf{'diskspace_percent_free'}%", "disk_check");
        }
        undef @list;
    }
}

sub doGrepPhrases
{
    my $type = shift; # soft or hard
    my $grepFile = $conf{$type . "_file_log_path"};
    my $grepPhrasesVar = $type . "_file_log_grep";

    if($grepFile && (-e $grepFile) )
    {
        my @grepPhrases = ();
        @grepPhrases = split(/,/,$conf{$grepPhrasesVar}) if $conf{$grepPhrasesVar};

        foreach(@grepPhrases)
        {
            my $thisPhrase = $_;
            my $found = 0;
            $thisPhrase =~ s/^\s+|\s+$//g;
            # print "grepping '".$grepFile."' for '$thisPhrase'\n";
            $found = 1 if ( fgrep { /$thisPhrase/ } $grepFile );
            if( $found )
            {
                killme( "Exiting due to presence of $thisPhrase in " . $grepFile, "grep_$type","/bin/grep -C 10000 '$thisPhrase' '".$grepFile."'" );
            }
            undef $found;
        }
        undef @grepPhrases;
    }
    undef $type;
    undef $grepFile;
    undef $grepPhrasesVar;
}

sub doWebsocketdChecks
{

    if($conf{'check_websocketd'} && ($conf{'check_websocketd'} ne 'no') && ($conf{'check_websocketd'} ne 'false') )
    {

        if(`ps -aef | grep -v grep | grep websocketd`)
        {
            # print "websocketd is running!\n";
        }
        else
        {
            killme("websocketd is NOT running!", "websocketd_died", "/bin/echo websocketd died");
        }
    }
}

sub doApacheChecks
{
    # Make sure Apache children limits are fine.
    # Exit if the soft limit apache_soft_level(i.e. 30) -- in whole minutes --  is exceeded longer than apache_soft_period (i.e. 15 minutes).
    # Exit if the hard limit apache_hard_level(i.e. 100) -- in whole minutres -- is exceeded longer than apache_hard_period (i.e. 2 minutes).

    my $total_apache = getApacheProcNum();
    if(!$total_apache)
    {
         killme("Apache is not running\n", "apache_not_running");
    }
    my @types = qw/soft hard/;
    foreach(@types)
    {
        if ($conf{'apache_' . $_ . '_level'} && $conf{'apache_' . $_ . '_period'})
        {
            checkApacheLimit($_, $total_apache);
            checkApacheDuration($_);
        }
    }
    undef $total_apache;
}

sub checkApacheLimit
{
    my $type = shift; # soft or hard
    my $total_apache = shift;
    my $apacheTrackingExceedVar = $type . "limit_exceeded";
    my $apacheTrackingDTVar = "dt" . $type . "limit";
    my $confVar = "apache_" . $type ."_level";

    if (!$apacheTracking{$apacheTrackingExceedVar} && ($total_apache > $conf{$confVar}) )
    {
        $apacheTracking{$apacheTrackingDTVar} = DateTime->now(time_zone => "local");
        $apacheTracking{$apacheTrackingExceedVar} = 1;
    }
    elsif ($total_apache < $conf{$confVar})
    {
        # Stop paying attention, we've gone below the threshold
        $apacheTracking{$apacheTrackingExceedVar} = 0
    }
    undef $apacheTrackingExceedVar;
    undef $apacheTrackingDTVar;
    undef $confVar;
    undef $type;
    undef $total_apache;
}

sub checkApacheDuration
{
    my $type = shift; # soft or hard
    my $apacheTrackingExceedVar = $type . "limit_exceeded";
    my $apacheTrackingDTVar = "dt" . $type . "limit";
    my $confVar = "apache_" . $type ."_period";

    if ($apacheTracking{$apacheTrackingExceedVar})
    {
        my $duration = getDuration(0, $apacheTracking{$apacheTrackingDTVar});
        print "apache $type true for $duration minutes\n";
        if ($duration > $conf{$confVar})
        {
            killme("Apache $apacheTrackingExceedVar true for $duration minutes\n", "apache_$type", "/bin/echo apache $apacheTrackingExceedVar true for $duration minutes");
        }
        undef $duration;
    }
    undef $apacheTrackingExceedVar;
    undef $apacheTrackingDTVar;
    undef $confVar;
    undef $type;
}

sub getApacheProcNum
{
    my @numApacheProcs = `pgrep -f sbin/apache2 | wc -l`;
    my $numApacheProcs = $numApacheProcs[0] - 1;
    return $numApacheProcs;
}

sub doOpenSRFChecks
{
    # my $duration = getDuration();
    # refresh login every 5 minutes
    # if($duration > 5 )
    # {
        # undef $e;
        # undef $dbHandler;
        # $dt = DateTime->now(time_zone => "local");
        # $script->logout();
        # undef $script;
        # setupLogin( 0 ); # 0 = Don't bother calling the create user function
    # }
    # Begin basic testing interacting with storage
    # Find a bib without parts
    # -------------
    # my $sdestbib = $e->search_biblio_record_entry([
    # {
    # id =>
        # {
            # 'not in' =>
                # { "from" => 'bmp',
                    # 'select' =>  { "bmp" => [ 'record' ] }
                # }
        # },
    # deleted => 'f' },
    # { limit => 3 }

    # ]);

    # my $destbib;
    # foreach(@{$sdestbib}) {
        # if ($_->id > -1) {
            # $destbib = $_;
            # last;
        # }
    # }

    # if(!$destbib)
    # {
        # $log->addLogLine("Couldn't find a bib\n Test is officially fail");
        # exit 1;
    # }

    # $log->addLogLine("Got bib ". $destbib->id);

    # # Load the holds shelf so that we interact with OpenSRF
    # # -------------

    # # First, we need an OU ID number, best to get a branch instead of a system or consortium
    # my @test_ou = @{$e->search_actor_org_unit([
    # {
        # ou_type => 3,
        # opac_visible => 't'
    # }
    # ])};
    # # @test_ou = @{@test_ou[0]};
    # print "Max array ".$#test_ou."\n";
    # my $random_number = int(rand($#test_ou));
    # print "Random - $random_number\n";
    # my $test_ou = @test_ou[$random_number];

    # print $test_ou->id." ".$test_ou->shortname ."\n";
    # my $storage = $script->session('open-ils.circ');
    # my $req = $storage->request(
        # 'open-ils.circ.holds.id_list.retrieve_by_pickup_lib', 0, $test_ou->id )->gather(1);
    # $log->addLine(Dumper($req));

    # Clean memory
    # -------------

    # undef $sdestbib;
    # undef $destbib;
    # undef @test_ou;
    # undef $test_ou;

}

sub getDuration
{
    my $toDT = shift;
    my $fromDT = shift || $dt;

    $toDT = DateTime->now(time_zone => "local") if !$toDT;
    # print "dt to:   $toDT\n";
    # print "dt from: $fromDT\n";

    my $difference = $toDT - $fromDT;
    my $format = DateTime::Format::Duration->new(pattern => '%M');
    my $duration =  $format->format_duration($difference);
    return $duration;
}

sub sleepRandomly
{
    my $randomNumber = getRandomNumber();
    sleep ( $conf{'sleep_interval'} + $randomNumber );
    undef $randomNumber;
}

sub getMachineName
{
    my $name = hostname();
    $name =~ s/\./_/g;
    $randomNumberMachine = getRandomNumber(100000) if !$randomNumberMachine;
    return $name . "-$randomNumberMachine";
}

sub killme
{
    my $printErrorMessage = shift;
    my $type = shift;
    my $systemEcho = shift;
    print "$printErrorMessage\n";
    writeDeathLog($type, $systemEcho);
    writeDeathLog("ejabber_log", "/bin/cat /var/log/ejabberd/ejabberd.log /var/log/ejabberd/error.log");
    exit 1;
}

sub setupLogin
{
    my $attempt_create_usr = shift;

    # print "Received '$attempt_create_usr' from setupLogin call\n";
    my %dbconf = %{getDBconnects($xmlconf)};
    eval{$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});};
    if ($@)
    {
        $log->addLogLine("Could not establish a connection to the database");
        print "Could not establish a connection to the database";
        exit 1;
    }

    if($attempt_create_usr)
    {
         # Make sure the user exists, if not, create it!
        createDBUser($dbHandler, $mobUtil, $conf{'workstation_lib'}, $conf{'usrname'},
            $conf{'workstation'}, $conf{'passwd'}, $conf{'profile'}, $conf{'ident'},
            $conf{'first'}, $conf{'last'});
    }

    # Setup the vars for everyone
    # -------------
    undef $script if $script;
    $e->reset() if $e;
    $script = OpenILS::Utils::TestUtils->new();
    $script->bootstrap;
    $e = new_editor(xact => 1);
    $e->init;

    my $workstation = $e->search_actor_workstation([ {name => $conf{'workstation'}, owning_lib => $conf{'workstation_lib'} } ])->[0];

    if( !$workstation )
    {
        $script->authenticate({
            username => $conf{'usrname'},
            password => $conf{'passwd'},
            type => 'staff'});
        my $ws = $script->register_workstation($conf{'workstation'},$conf{'workstation_lib'});
        $script->logout();
    }

    $script->authenticate({
        username => $conf{'usrname'},
        password => $conf{'passwd'},
        workstation => $conf{'workstation'}
    });
}

sub getRandomNumber
{
    my $seed = shift || 50;
    return int(rand($seed));
}

sub writeDeathLog
{
    my $type = shift || "death_log";
    $type =~ s/[\s\\\/\."']/_/g;
    my $systemEcho = shift || "/bin/echo brick died";
    my $machineName = getMachineName();
    my $fullPath = $deathOutputFolder . "/$machineName" . "_$type" .".log";
    my $exec = $systemEcho . " > '$fullPath'";
    print "Writing death log: $fullPath\n";
    print "Exec: $exec\n";
    system($exec);
}

sub createDBUser
{
    my $dbHandler = shift;
    my $mobiusUtil = shift;
    my $org_unit_id = shift;
    my $usr = shift;
    my $workstation = shift;
    my $pass = shift;
    my $profile = shift;
    my $ident = shift;
    my $first = shift;
    my $last = shift;

    print "Creating User\n";

    my $query = "select id from actor.usr where upper(usrname) = upper('$usr')";
    my @results = @{$dbHandler->query($query)};
    my $result = 1;
    if($#results==-1)
    {
        #print "inserting user\n";
        $query = "INSERT INTO actor.usr (profile, usrname, passwd, ident_type, first_given_name, family_name, home_ou) VALUES (\$\$$profile\$\$, \$\$$usr\$\$, \$\$$pass\$\$, \$\$$ident\$\$, \$\$$first\$\$, \$\$S$last\$\$, \$\$$org_unit_id\$\$)";
        $result = $dbHandler->update($query);
    }
    else
    {
        #print "updating user\n";
        my @row = @{@results[0]};
        my $usrid = @row[0];
        $query = "select * from actor.create_salt('main')";
        my @results = @{$dbHandler->query($query)};
        @row = @{@results[0]};
        my $salt = @row[0];
        $query = "select * from actor.set_passwd($usrid,'main',
        md5(\$salt\$$salt\$salt\$||md5(\$pass\$$pass\$pass\$)),
        \$\$$salt\$\$
        )";
        $result = $dbHandler->update($query);
        $query = "UPDATE actor.usr SET home_ou=\$\$$org_unit_id\$\$,ident_type=\$\$$ident\$\$,profile=\$\$$profile\$\$,active='t',super_user='t',deleted='f' where id=$usrid";
        $result = $dbHandler->update($query);
    }
    if($result)
    {
        $query = "select id from actor.workstation where upper(name) = upper('$workstation')";
        my @results = @{$dbHandler->query($query)};
        if($#results==-1)
        {
        #print "inserting workstation\n";
            $query = "INSERT INTO actor.workstation (name, owning_lib) VALUES (\$\$$workstation\$\$, \$\$$org_unit_id\$\$)";
            $result = $dbHandler->update($query);
        }
        else
        {
        #print "updating workstation\n";
            my @row = @{@results[0]};
            $query = "UPDATE actor.workstation SET name=\$\$$workstation\$\$, owning_lib= \$\$$org_unit_id\$\$ WHERE ID=".@row[0];
            $result = $dbHandler->update($query);
        }
    }
    #print "User: $usr\npass: $pass\nWorkstation: $workstation";

    my @ret = ($usr, $pass, $workstation, $result);
    return \@ret;
}

sub getDBconnects
{

    my $openilsfile = shift;
    my $xml = new XML::Simple;
    my $data = $xml->XMLin($openilsfile);
    my %dbconf;
    $dbconf{"dbhost"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{host};
    $dbconf{"db"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{db};
    $dbconf{"dbuser"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{user};
    $dbconf{"dbpass"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{pw};
    $dbconf{"port"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{port};
    ##print Dumper(\%dbconf);
    return \%dbconf;
}

 exit;
