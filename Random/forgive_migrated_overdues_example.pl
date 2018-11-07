#!/usr/bin/perl


#use strict; use warnings;

use lib qw(../);
use LWP;
use Getopt::Std;
use JSON::XS;
use Text::CSV;
use Data::Dumper;
use OpenILS::Utils::Cronscript;
use OpenILS::Utils::Fieldmapper;
use OpenILS::Utils::CStoreEditor qw/:funcs/;
use OpenILS::Const qw(:const);
use OpenSRF::AppSession;
use OpenSRF::Utils::SettingsClient;
use OpenSRF::MultiSession;
use OpenSRF::EX qw(:try);
use Encode;
use Scalar::Util qw(blessed);
use Loghandler;
use DBhandler;
use Mobiusutil;
use XML::Simple;

bootstrap;
loadIDL;


my $logfile = @ARGV[0];
my $xmlconf = "/openils/conf/opensrf.xml";
our $lastou = ""; 
our $lastxact = 0; 
our $authtoken;
our $log;
our $dbHandler;
our $output = "";
our $config = "/openils/conf/opensrf_core.xml";
our $script = OpenILS::Utils::Cronscript->new;
our @failedcheckout;
our @failedfinegen;
our @failedlost;
our @failedpay;
our @failedcheckin;
our @failedforgive;
our @successcheckout;
our @successfinegen;
our @successlost;
our @successpay;
our @successcheckin;
our @successforgive;


if(@ARGV[1])
{
    $xmlconf = @ARGV[1];
}

if(! -e $xmlconf)
{
    print "I could not find the xml config file: $xmlconf\nYou can specify the path when executing this script\n";
    exit 0;
}
 if(!$logfile)
 {
    print "Please specify a log file\n";
    print "usage: ./03run_checkouts.pl /tmp/logfile.log [optional /path/to/xml/config/opensrf.xml]\n";
    exit;
 }

$log = new Loghandler($logfile);
$log->truncFile(" ---------------- Script Starting ---------------- ");

my %conf = %{getDBconnects($xmlconf,$log)};
#print Dumper(\%conf);
my @reqs = ("dbhost","db","dbuser","dbpass","port"); 
my $valid = 1;
for my $i (0..$#reqs)
{
    if(!$conf{@reqs[$i]})
    {
        $log->addLogLine("Required configuration missing from conf file");
        $log->addLogLine(@reqs[$i]." required");
        $valid = 0;
    }
}
if($valid)
{    
    eval{$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});};
    if ($@) 
    {
        $log->addLogLine("Could not establish a connection to the database");
        print "Could not establish a connection to the database";
    }
    else
    {
        my $mobutil = new Mobiusutil();
        my $query = "
select mb.xact,sum(mb.amount),ac.barcode,string_agg(mb.note,'
'), mmbxs.balance_owed,au.home_ou
from 
money.billing mb,
money.grocery mg,
actor.usr au,
actor.card ac,
money.materialized_billable_xact_summary mmbxs

where 
mmbxs.id=mg.id and
not mb.voided and
mb.xact=mg.id and
mg.usr=au.id and
ac.id=au.card and
mb.xact in(select id from money.materialized_billable_xact_summary where usr in (select id from actor.usr where home_ou in(select id from actor.org_unit where lower(name) ~ 'cass')) and balance_owed > 0 )
and
mb.btype=102 and
-- ac.barcode='20022001099993' and
lower(mb.note)~'overdue'
group by 1,3,5,6
order by 1
limit 1000 offset 14000
";
$log->addLogLine("$query");
        my @results = @{$dbHandler->query($query)};
        my $total = $#results+1;
        my $loops = 0;
        $log->addLogLine("$total circs");
        
        my $stop=0;
        my $lastitem = '';
        my @markLost = ();
        foreach(@results)
        {
            my $row = $_;
            my @row = @{$row};
            my $xactid = @row[0];
            my $totalOwed = @row[1];
            my $patronBarcode = @row[2];
            my $note = @row[3];
            my $balanceOwed = @row[4];
            my $ou = @row[5];
            if($lastxact != $xactid)
            {
                $log->addLogLine("xact $xactid");
                $lastxact = $xactid;
                
                if($lastou ne $ou)
                {
                    $log->addLogLine("Changing OU");
                    my @usrcreds = @{createDBUser($dbHandler,$mobutil,$ou)};
                    #$log->addLogLine(@usrcreds[0].' '.@usrcreds[1].' '.@usrcreds[2]);
                    if(@usrcreds[3])
                    {
                        $authtoken = $script->authenticate(
                            {
                                username => @usrcreds[0],
                                password => @usrcreds[1],
                                workstation => @usrcreds[2]
                            }
                        );
                        $lastou=$ou;
                    }
                    else
                    {
                        $stop=1;
                        $log->addLogLine("Failed creating the user/workstation in the DB\nusr:".@usrcreds[0]." pass: ".@usrcreds[1]." workstation: ".@usrcreds[2]);
                        print "Failed creating the user/workstation in the DB\nusr:".@usrcreds[0]." pass: ".@usrcreds[1]." workstation: ".@usrcreds[2];
                    }
                }
                
                if(!$stop)
                {
                    
                     try 
                     {
                        my $forgiveAmount = $totalOwed;
                        my $continue = 1;
                        if( ($note =~ m/PROCESSFEE/) || ($note =~ m/REFERRAL/) || ($note =~ m/DAMAGE/) || ($note =~ m/LOST/) )
                        {
                            # print $note."\ntotal owed".$totalOwed."\n";
                            # This billing was one of those that were combined into a single bill. Need to parse the note in order to figure out how much of the bill was OVERDUES
                            my @s = split(/\n/,$note);
                            $continue = 0 if $#s < 1;
                            # example of a parseable note:
                            # "OVERDUE 05/23/2018 0.05 Outlander (Television program). Season 3"
                            
                            my %typeMap = ();
                            foreach(@s)
                            {
                                my $type = $_;
                                $type =~ s/^([^\s]*?)\s.*/$1/;
                                $typeMap{$type} = 0.00 if (!$typeMap{$type});
                                
                                my $amount = $_;
                                $amount =~ s/^([^\s]*)\s*([^\s]*)\s*([^\s]*)\s.*/$3/;
                                my $testnumeric = $amount;
                                $testnumeric =~ s/\.//g;
                                # any characters that are non-numeric apart from the period would make this a non-numeric value
                                if ( $testnumeric =~ m/\D/ ) {}
                                else
                                {
                                    $amount += 0.00;
                                    $typeMap{$type} += 0.00 + $amount;
                                }
                            }
                            $log->addLine(Dumper(\%typeMap));
                            $forgiveAmount = 0.00;
                            while ((my $internal, my $value ) = each(%typeMap))
                            {
                                $forgiveAmount += $value if( lc($internal) =~ m/overdu/ );
                            }
                            $forgiveAmount = $balanceOwed if $forgiveAmount > $balanceOwed;
                        }
                        my $diffamount = $balanceOwed - $forgiveAmount;
                        
                        
                        # it's possible that some of these bills are not formed correctly and connot be properly parsed. Those get ignored
                        if($continue && ($forgiveAmount > 0) )
                        {
                            $log->addLine("stats, $xactid, $patronBarcode, $totalOwed, $balanceOwed, $forgiveAmount, $diffamount");
                            my $paid = do_payment($xactid, $forgiveAmount, $patronBarcode, "Forgive migrated OVERDUE", "forgive_payment");
                            if(!$paid)
                            {
                                push (@failedpay,[$xactid, $forgiveAmount, $patronBarcode, "Forgive migrated OVERDUE", "forgive_payment"]);
                                # failing is bad
                                $problem = 1;
                            }
                            else
                            {
                                push(@successforgive, [$xactid, $forgiveAmount, $patronBarcode, "Forgive migrated OVERDUE", "forgive_payment"]);
                            }
                        }
                        else
                        {
                            $log->addLine("stats, $xactid, $patronBarcode, $totalOwed, $balanceOwed, $forgiveAmount, $diffamount, NOT CHANGED");
                        }
                            
                            
                    }
                    catch Error with 
                    {
                        my $err = shift;
                        $log->addLine("An overall FAILED $err");
                    }
                }
           
            $loops++;
            }
        }
        # $log->addLine(Dumper(@failedcheckout));
        # $log->addLine(Dumper(@failedfinegen));
        # $log->addLine(Dumper(@failedlost));
        $log->addLine(Dumper(@failedpay));
        # $log->addLine(Dumper(@failedforgive));
        # $log->addLine(Dumper(@successcheckout));
        # $log->addLine(Dumper(@successfinegen));
        # $log->addLine(Dumper(@successlost));
        # $log->addLine(Dumper(@successpay));
        
        # $log->addLine("failedcheckout ".$#failedcheckout);
        # $log->addLine("failedfinegen ".$#failedfinegen);
        # $log->addLine("failedlost ".$#failedlost);
        # $log->addLine("failedfines ".$#failedpay);
        # $log->addLine("failed Grocery Bills ".$#failedforgive);
        # $log->addLine("successcheckout ".$#successcheckout);
        # $log->addLine("successfinegen ".$#successfinegen);
        # $log->addLine("successlost ".$#successlost);
        # $log->addLine("successfines ".$#successpay);
        $log->addLine("successforgive ".$#successforgive);
        
        
    }
}


$log->addLogLine(" ---------------- Script Ending ---------------- ");

sub getxacttotals
{
    my $userID = shift;
    my $query = "select mmbxs.id, mmbxs.xact_type,mmbxs.balance_owed from money.materialized_billable_xact_summary mmbxs where 
    mmbxs.xact_finish is null and
    mmbxs.balance_owed != 0 and
    mmbxs.usr=(select id from actor.usr where usrname=\$\$$userID\$\$)
    order by mmbxs.xact_type desc,mmbxs.balance_owed desc";
    my @results = @{$dbHandler->query($query)};
    return \@results;
}

sub findCircID
{
    my $userID = shift;
    my $itemid = shift;
    
    my $query = "select id from action.circulation where usr = 
    (select id from actor.usr where usrname=\$\$$userID\$\$) and
    target_copy = (select id from asset.copy where barcode=\$\$$itemid\$\$)
    ";
    my @results = @{$dbHandler->query($query)};
    if($#results>-1)
    {
        my @row = @{@results[0]};
        return @row[0];
    }
    return undef;
}

sub do_checkout
{
    my $userID = shift;
    my $itemid = shift;
    my $chdate = shift;
    my $duedate = shift;

    my $args = 
    {
        patron_barcode => $userID ,
        barcode => $itemid,
        checkout_time => $chdate,
        due_date => $duedate,
        permit_override => 1
    };
    my $before = getCircCount($dbHandler,$userID);
    my $r;
    $log->addLogLine("Running patron_barcode => $userID ,barcode => $itemid,checkout_time => $chdate,due_date => $duedate,permit_override => 1");
    $r = OpenSRF::AppSession->create('open-ils.circ')
        ->request('open-ils.circ.checkout', $authtoken, $args)
            ->gather(1);
    #print Dumper $r;
    my $after = getCircCount($dbHandler,$userID);
    $after = $after-$before;
    my $temp = Dumper $r;
    if($after>0)
    {
        $log->addLogLine("Success: user: $userID");
    }
    else
    {
        $log->addLogLine("FAILED: user: $userID barcode: $itemid");
        $log->addLogLine(Dumper $r);
        return undef;
    }
    return figurecircID($userID,$itemid);
    
        
# CHECKOUT END
}

sub do_fines
{
    my $circID = shift;
    my $before = getFineCount($dbHandler,$circID);
    $log->addLogLine("Running fines => $circID");        
    my $args = 
    {
        client => $authtoken ,
        circ_id => $circID
    };    
    my $r;
    $r = OpenSRF::AppSession->create('open-ils.storage')
        ->request('open-ils.storage.action.circulation.overdue.generate_fines', $circID , $args)
            ->gather(1);
    
    my $after = getFineCount($dbHandler,$circID);
    $after = $after-$before;
    
    if($after>0)
    {
        $log->addLogLine("Success: circ: $circID");
    }
    else
    {
        $log->addLogLine("No fines: circ: $circID");
    }            
    return 1;
}

sub do_lost
{
    my $barcode = shift;
    my $xactid = shift;
    my $userID = shift;
    my $args = {barcode=>$barcode};
    my $before = getFineCount($dbHandler,$xactid);
    my $r;
    $log->addLogLine("Marking lost patronID => $userID ,itembarcode => $barcode");
    $r = OpenSRF::AppSession->create('open-ils.circ')->request('open-ils.circ.circulation.set_lost',  $authtoken,
         $args)->gather(1);                    
         
    #print Dumper $r;
    my $after = getFineCount($dbHandler,$xactid);
    $after = $after-$before;
    if($after>0)
    {
        $log->addLogLine("Success: Transid: $xactid, user: $userID itembarcode: $barcode");
    }
    else
    {
        $log->addLogLine("Error marking lost: Transid: $xactid, user: $userID itembarcode: $barcode");
        $log->addLine(Dumper $r);
        return undef;
    }
    
    return 1;
}

sub do_checkin
{
    my $barcode = shift;
    my $xactid = shift;
    my $checkindate = shift;
    #print "Starting checkin\n";
    my $args = 
    {
        barcode=>$barcode,
        force => 1,
        noop => 1,
        backdate => $checkindate
    };
    #print "just assigned args\n";
    my $r;
    #print "About to log something\n";
    $log->addLogLine("Checking in itembarcode => $barcode");
    #print "Logged it\n";
    $r = OpenSRF::AppSession->create('open-ils.circ')->request('open-ils.circ.checkin.override',  $authtoken,
         $args)->gather(1);                    
         
    #print Dumper $r;
    #print "Sending off to getCheckinCheck\n";
    my $checkincheck = getCheckinCheck($xactid);
    if($checkincheck)
    {
        $log->addLogLine("Checkin success: Transid: $xactid");
    }
    else
    {
        $log->addLogLine("Error checking in: Transid: $xactid itembarcode: $barcode");
        $log->addLine(Dumper $r);
        return undef;
    }

    return 1;
}

sub do_payment
{
# return 1;
    my $circID = shift;
    my $amount = shift;
    my $userBarcode = shift;
    my $note = shift;
    my $type = shift || 'cash_payment';
    $userBarcode = getActorUsrID($userBarcode);
    return undef if(!$userBarcode);
 
    my @payment;
    push(@payment, [$circID, $amount]);
    my $args = 
    {
        payment_type => $type,
        userid=>$userBarcode,
        note=> $note,
        payments=>\@payment
    };
    my $lastxact = getlastxact($userBarcode);
    my $before = getPaymentCount($circID);
    my $r;
    $log->addLogLine("Running payment patronID => $userBarcode, xactid => $circID, amount => $amount");
    
       
    $r = OpenSRF::AppSession->create('open-ils.circ')->request('open-ils.circ.money.payment',  $authtoken,
         $args, $lastxact)->gather(1);                    
         
    #print Dumper $r;
    my $after = getPaymentCount($circID);
    $after = $after-$before;
    my $temp = Dumper $r;
    if($after>0)
    {
        $log->addLogLine("Success");
        return 1;
    }
    else
    {
        print Dumper $r;
        $log->addLogLine("FAILED Error making payment: user: $userBarcode type: $type amount: $amount");
        return undef;
    }
    
    
}

sub getActorUsrID
{
    my $usrbarcode = shift;
    my $query = "select usr from actor.card where barcode=\$\$$usrbarcode\$\$";
    my @results = @{$dbHandler->query($query)};
    if($#results>-1)
    {
        my @row = @{@results[0]};
        return @row[0];
    }
    return undef;
}

sub figurecircID
{
    my $patronbarcode = shift;
    my $itembarcode = shift;
    
    my $query = "select id from action.circulation where usr =
    (select id from actor.usr where usrname=\$\$$patronbarcode\$\$)
    and
    target_copy = (select id from asset.copy where barcode=\$\$$itembarcode\$\$ and not deleted)
    ";
    my @results = @{$dbHandler->query($query)};
    if($#results>-1)
    {
        my @row = @{@results[0]};
        return @row[0];
    }
    return undef;
}

sub figureBalanceOwed
{
    my $xactid = shift;
    
    my $query = "select balance_owed from money.materialized_billable_xact_summary where id= $xactid";
    my @results = @{$dbHandler->query($query)};
    if($#results>-1)
    {
        my @row = @{@results[0]};
        return @row[0];
    }
    return undef;
}

sub figureUserTotalOwed
{
    my $userid = shift;
    
    my $query = "select sum(balance_owed) from money.materialized_billable_xact_summary where usr = (select id from actor.usr where usrname=\$\$$userid\$\$)";
    my @results = @{$dbHandler->query($query)};
    if($#results>-1)
    {
        my @row = @{@results[0]};
        return @row[0];
    }
    return 0;
}


sub parseOutput
{
    my $string = @_[0];    
    my $output = "circ,".getSection($string,"circ");
    #$output.= ",volume,";
    #$output.=getSection($string,"volume");
    #$output.= ",record,";
    #$output.=getSection($string,"record");
    $output.= ",copy,";
    $output.=getSection($string,"copy");
    $output.= ",textcode,";
    $output.=getSection($string,"textcode");
    $output.="\n";
    #print $output;
    return $output;
    
}

sub getSection
{
    my $wholeString = @_[0];
    my $section = @_[1];
    my @s = split(/$section/,$wholeString);
    @s = split(/\],/,@s[1]);
    my @circ = split(/,/,@s[0]);
    my $output;
    foreach my $i (0..$#circ)
    {
        $output.="\"".@circ[$i]."\",";
    }
    $output=substr($output,0,-1);        
    $output=~s/\n//g;
    $output=~s/\r//g;
    $output=~s/\t//g;
    $output=~s/\s{2,}//g;
    $output=~s/'//g;
    $output=~s/\=\>bless\(\[//g;
    #print "$section :\n";
    #print $output."\n";
    return $output;
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

sub createDBUser
{
    my $dbHandler = @_[0];
    my $mobiusUtil = @_[1];
    my $org_unit_id = @_[2];
    my $usr = "migrate";
    my $workstation = "migrate-script";
    my $pass = $mobiusUtil->generateRandomString(10);
    
    my $query = "select id from actor.usr where upper(usrname) = upper('$usr')";
    my @results = @{$dbHandler->query($query)};
    my $result = 1;
    if($#results==-1)
    {
        #print "inserting user\n";
        $query = "INSERT INTO actor.usr (profile, usrname, passwd, ident_type, first_given_name, family_name, home_ou) VALUES ('25', E'$usr', E'$pass', '3', 'Script', 'Script User', E'$org_unit_id')";
        $result = $dbHandler->update($query);
    }
    else
    {
        #print "updating user\n";
        my @row = @{@results[0]};
        my $usrid = @row[0];
        $query = "select * from actor.create_salt('main')";
        my @results = @{$dbHandler->query($query)};
        my @row = @{@results[0]};
        my $salt = @row[0];
        $query = "select * from actor.set_passwd($usrid,'main',
        md5(\$salt\$$salt\$salt\$||md5(\$pass\$$pass\$pass\$)),
        \$\$$salt\$\$
        )";
        $result = $dbHandler->update($query);
        $query = "UPDATE actor.usr SET home_ou=E'$org_unit_id',ident_type=3,profile=25,active='t',super_user='t',deleted='f' where id=$usrid";
        $result = $dbHandler->update($query);
    }
    if($result)
    {
        $query = "select id from actor.workstation where upper(name) = upper('$workstation')";
        my @results = @{$dbHandler->query($query)};
        if($#results==-1)
        {
        #print "inserting workstation\n";
            $query = "INSERT INTO actor.workstation (name, owning_lib) VALUES (E'$workstation', E'$org_unit_id')";        
            $result = $dbHandler->update($query);
        }
        else
        {
        #print "updating workstation\n";
            my @row = @{@results[0]};
            $query = "UPDATE actor.workstation SET name=E'$workstation', owning_lib= E'$org_unit_id' WHERE ID=".@row[0];    
            $result = $dbHandler->update($query);
        }
    }
    #print "User: $usr\npass: $pass\nWorkstation: $workstation";
    
    my @ret = ($usr, $pass, $workstation, $result);
    return \@ret;
}

sub deleteDBUser
{
    #This code is not used. DB triggers prevents the deletion of actor.usr.
    #I left this function as informational.
    my $dbHandler = @_[0];
    my @usrcreds = @{@_[1]};
    my $query = "delete from actor.usr where usrname='".@usrcreds[0]."'";
    print $query."\n";
    $dbHandler->update($query);    
    $query = "delete from actor.workstation where name='".@usrcreds[2]."'";
    print $query."\n";
    $dbHandler->update($query);
}

sub getCircCount
{
    my $dbHandler = @_[0];    
    my $userid = @_[1];    
    my $query = "select count(*) from action.circulation where usr=(select id from actor.usr where usrname='$userid')";
    #print $query."\n";
    my @count = @{$dbHandler->query($query)};
    my $before=0;
    if($#count>-1)
    {
        my @t = @{@count[0]};
        $before = @t[0];
    }
    
    return $before;
}

sub getFineCount
{

    my $dbHandler = @_[0];    
    my $circID = @_[1];    
    my $query = "select count(*) from money.billing where xact=$circID";
    #print $query."\n";
    my @count = @{$dbHandler->query($query)};
    my $before=0;
    if($#count>-1)
    {
        my @t = @{@count[0]};
        $before = @t[0];
    }
    
    return $before;
}

sub getCheckinCheck
{
    my $xactid = shift;
    my $query = "select id from action.circulation where id= $xactid and checkin_time is not null";
    #print $query."\n";
    my @count = @{$dbHandler->query($query)};
    if($#count>-1)
    {
        return 1;
    }
    return undef;
}

sub getPaymentCount
{
    my $xactid = shift;
    my $query = "select count(*) from money.payment where xact=$xactid";
    #print $query."\n";
    my @count = @{$dbHandler->query($query)};
    my $before=0;
    if($#count>-1)
    {
        my @t = @{@count[0]};
        $before = @t[0];
    }
    return $before;
}

sub getlastxact
{
    my $usrid = shift;
    my $ret='none';
    my $query = "select last_xact_id from actor.usr where id=$usrid";
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        #print Dumper(@results);
        my $row = $_;
        my @row = @{$row};
        $ret = @row[0];
        if(length($ret)<5)
        {
            $ret = 'none';
        }
    }
                #print $ret;
    return $ret;
}

sub removeAutoAdjustments
{
    my $xactid = shift;
    $log->addLine("Removing money.account_adjustment where  xact = $xactid");
    my $query = "delete from money.account_adjustment where  xact = $xactid";
    #print $query."\n";
    $dbHandler->update($query);
}




