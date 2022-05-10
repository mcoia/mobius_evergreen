#!/usr/bin/perl


use lib qw(./ ../); 
use Loghandler;
use Mobiusutil;
use Data::Dumper;
use DateTime;
use Getopt::Long;
use Cwd;
use email;


our $log;
our $configFile;

GetOptions (
"config=s" => \$configFile,
)
or die("Error in command line arguments\nYou can specify
--config                                      [Path to the config file]
\n");

if(!$configFile || !(-e $configFile) )
{
    print "Please specify a valid path to a config file\n";
    exit;
}


our $mobUtil = new Mobiusutil();
our $conf = $mobUtil->readConfFile($configFile);


if($conf)
{
    %conf = %{$conf};
    if ($conf{"logfile"})
    {
        checkConfig();
        $log = new Loghandler($conf->{"logfile"});
        $log->truncFile("");
        $log->addLogLine("****************** Starting ******************");

        my @oldIPs = @{ readOldIPFile($conf->{"previous_ip_file"}) };
        my @newIPs = @{ getCurrentIPs() };
        if(validateIPs(\@newIPs))
        {
            if ($#oldIPs < 0) # catch the scenario where this script is run for the first time, and there is no history
            {
                recordIPs(\@newIPs, $conf->{"previous_ip_file"});
                exit;
            }
            my %diff = %{compareOldToNew(\@oldIPs, \@newIPs)};
            print Dumper(\%diff);
            my @new = @{$diff{'new'}};
            my @removals = @{$diff{'removals'}};
            if($#removals == -1)
            {
                $log->addLogLine("IP's are the same, nothing to do");
            }
            else
            {
                $log->addLogLine("IP's are different... sending email!");
                recordIPs(\@newIPs, $conf->{"previous_ip_file"});
                sendSuccess(\@removals, \@new, \@newIPs);
            }
        }
        else
        {
            sendError("NOBLE IP check - Bad data from kubectl - couldn't determine current brick IPs");
        }
        $log->addLogLine("****************** Ending ******************");
    }
    else
    {
        print "Your config file needs to specify a logfile\n";
    }
}
else
{
    print "Something went wrong with the config\n";
    exit;
}

sub sendError
{
    my $subject = shift;
    my @tolist = ($conf{"alwaysemail"});
    my $email = new email($conf{"fromemail"},\@tolist,1,0,\%conf);
    $email->send($subject,"This email was generated from the gcp_kub_vm_ip_collector script on the NOBLE server.\r\nThe log file:\r\n".$conf->{"logfile"}."\r\n\r\n-MOBIUS Perl Squad-");
}

sub sendSuccess
{
    my $removalsRef = shift;
    my $newIPRef = shift;
    my $completeIPRef = shift;
    my $body = readEmailTextFile($conf{"email_body_text_file"}, $removalsRef, $newIPRef, $completeIPRef);
    $log->addLine($conf{"email_subject"});
    $log->addLine($body);

    my @tolist = ($conf{"alwaysemail"});
    $email = new email($conf{"fromemail"}, \@tolist, 0, 1, \%conf);

    $email->send($conf{"email_subject"}, $body);

}

sub readEmailTextFile
{
    my $emailTemplate = shift;
    my $removalsRef = shift;
    my $newIPRef = shift;
    my $completeIPRef = shift;
    my @oldIPs = @{$removalsRef};
    my @newIPs = @{$newIPRef};
    my @completeIPs = @{$completeIPRef};
    my $oldIPsFlat = "";
    $oldIPsFlat .= $_."\r\n" foreach(@oldIPs);
    my $newIPsFlat = "";
    $newIPsFlat .= $_."\r\n" foreach(@newIPs);
    my $completeIPsFlat = "";
    $completeIPsFlat .= $_."\r\n" foreach(@completeIPs);
    my $reader = new Loghandler($emailTemplate);
    my @lines = @{$reader->readFile()};
    my $out = "";
    $out .= $_ foreach(@lines);
    $out =~ s/!!!oldIPs/$oldIPsFlat/g;
    $out =~ s/!!!newIPs/$newIPsFlat/g;
    $out =~ s/!!!completeIPs/$completeIPsFlat/g;
    return $out;
}

sub validateIPs
{
    my $IPRef = shift;
    my @IPs = @{$IPRef};
    return 0 if ($#IPs == -1);
    foreach(@IPs)
    {
        if( !($_ =~ m/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/) )
        {
            $log->addLogLine("Found invalid IP: ".$_);
            return 0;
        }
    }
    return 1;
}

sub getCurrentIPs
{
    my $cmd = "kubectl describe node|grep ExternalIP|awk '{print \$2}'| uniq | sort";
    my $IPs = execSystemCMDWithReturn($cmd);
    my @ips = split(/\n/,$IPs);
    $log->addLogLine($_) foreach @ips;
    return \@ips;
}
        
sub compareOldToNew
{
    # return empty array when they are equal
    # returns a differencing HASH when there are differences
    my $oldIPRef = shift;
    my $newIPRef = shift;
    my @oldIPs = @{$oldIPRef};
    my @newIPs = @{$newIPRef};
    my @removals = ();
    my @new = ();
    my %newMatches = ();
    my %ret = ();
    foreach(@oldIPs)
    {
        my $thisOldIP = $_;
        my $found = 0;
        foreach(@newIPs)
        {
            my $thisNewIP = $_;
            if($thisOldIP eq $thisNewIP)
            {
                $newMatches{$thisNewIP} = 1;
                $found = 1;
            }
        }
        if(!$found)
        {
            push (@removals, $thisOldIP);
        }
    }
    foreach(@newIPs)
    {
        my $thisNewIP = $_;
        push (@new, $thisNewIP) if(!$newMatches{$thisNewIP});
    }
    $ret{'new'} = \@new;
    $ret{'removals'} = \@removals;

    return \%ret;
}

sub readOldIPFile
{
    my $file = shift;
    my @ret = ();
    $log->addLogLine( "Reading old IP's from: $file");
    if( -e $file)
    {
        my $oldfile = new Loghandler($file);
        my @read = @{$oldfile->readFile()};
        for my $i(0..$#read) # the file can have extra line endings that wind up in the data
        {
            @read[$i] = trim(@read[$i]);
            push @ret, @read[$i] if length(@read[$i]) > 7;
        }
        $log->addLogLine($_) foreach(@ret);
    }
    else
    {
        my $oldfile = new Loghandler($file);
        $oldfile->truncFile("");
    }
    return \@ret;
}

sub recordIPs
{
    my $ipsRef = shift;
    my $outFile = shift;
    my @ips = @{$ipsRef};
    my $out = "";
    $out.="$_\n" foreach(@ips);
    $out = substr($out,0,-1);
    my $fileWrite = new Loghandler($outFile);
    $fileWrite->truncFile($out);
}

sub escapeData
{
    my $d = shift;
    $d =~ s/'/\\'/g;   # ' => \'
    $d =~ s/\\/\\\\/g; # \ => \\
    return $d;
}

sub checkConfig
{
    my @reqs = ("logfile","previous_ip_file","successemaillist","fromemail","email_body_text_file");
    my $valid = 1;
    print Dumper(\%conf);
    for my $i (0..$#reqs)
    {
        if(!$conf{$reqs[$i]})
        {
            print "Required configuration missing from conf file: ".$reqs[$i]."\n";
            exit;
        }
    }
}

sub execSystemCMDWithReturn
{
    my $cmd = shift;
    my $dont_trim = shift;
    my $ret;
    $log->addLogLine("executing '$cmd'");
    $log->addLogLine($cmd);
    open(DATA, $cmd.'|');
    my $read;
    while($read = <DATA>)
    {
        $ret .= $read;
    }
    close(DATA);
    return 0 unless $ret;
    $ret = substr($ret,0,-1) unless $dont_trim; #remove the last character of output.
    return $ret;
}

sub trim
{
    my $st = shift;
    $st =~ s/^[\s\t]*(.*)/$1/;
    $st =~ s/(.*)[\s\t]*$/$1/;
    return $st;
}

exit;
