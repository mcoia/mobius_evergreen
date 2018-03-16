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
# use OpenSRF::EX qw(:try);
use Encode;
use Scalar::Util qw(blessed);
use Loghandler;
use DBhandler;
use Mobiusutil;
use XML::Simple;
use Try::Tiny;

bootstrap;
loadIDL;


my $logfile = @ARGV[0];
my $xmlconf = "/openils/conf/opensrf.xml";
our $logoutput = ""; 
our $authtoken;
our $log;
our $dbHandler;
our $config = "/openils/conf/opensrf_core.xml";
our $script = OpenILS::Utils::Cronscript->new;


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
        my @usrcreds = @{createDBUser($dbHandler,$mobutil,1)};
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
        }
        else
        {
            $log->addLogLine("Failed creating the user/workstation in the DB\nusr:".@usrcreds[0]." pass: ".@usrcreds[1]." workstation: ".@usrcreds[2]);
            print "Failed creating the user/workstation in the DB\nusr:".@usrcreds[0]." pass: ".@usrcreds[1]." workstation: ".@usrcreds[2];
            exit;
        }

        print Dumper($authtoken);
        my $loops = 0;
		my $tquery = "
select 
acirc.id,
due_date
from action.circulation acirc
where 
xact_finish is null and
xact_start::date between '2018-02-25'::date and  '2018-03-13'::date and
due_date::date != (xact_start::date + duration) and 
due_date > '2018-03-10' and
date_part('hour',due_date)::numeric = 0 and
acirc.duration > '23 hours'::interval
-- and
-- acirc.id = 53117840
order by xact_start
limit 200
";

$logoutput.=$tquery."\n";
		my @oresults = @{$dbHandler->query($tquery)};
		
        while($#oresults > -1)
        {
		
            my $stop=0;
# print Dumper(\@oresults);
            foreach(@oresults)
            {
                my $row = $_;
                my @row = @{$row};
                my $circID = @row[0];
                my $dueDate = @row[1];
# print "circID = $circID, dueDate = $dueDate\n";
                 try
                 {
                    $logoutput.="trans $loops    $circID"."\n";
print "trans $loops    $circID"."\n";
                    my $query = "insert into m_circ_time.acirc(acircid,original_time) values($circID, \$\$$dueDate\$\$)";
                    $dbHandler->update($query);
                    correct_due_date($circID);
                    correct_fines($circID);
                }
                catch 
                {
                    my $err = shift;
                    print "An overall FAILED $err\n";
                    $logoutput.="An overall FAILED $err"."\n";
                };
                $loops++;
            }
            @oresults = ();
#print "About to $tquery\n";
            @oresults = @{$dbHandler->query($tquery)};
            $log->addLine($logoutput);
            $logoutput='';
        }
	}
}


$log->addLogLine(" ---------------- Script Ending ---------------- ");


sub correct_due_date
{
    my $circID = @_[0];
    my $days = 0;
    
    my $count = 0;
    
    while (!$count)
    {
    print "finding a good due date with $days days for $circID\n";
        my $query = "select distinct (due_date + '11 hours'::interval + '$days days'::interval) from 
        action.circulation acirc,
        (
            select 
            id,
            string_agg(
            (case 
            when dow_0_open = dow_0_close then '1'
            when dow_1_open = dow_1_close then '2'
            when dow_2_open = dow_2_close then '3'
            when dow_3_open = dow_3_close then '4'
            when dow_4_open = dow_4_close then '5'
            when dow_5_open = dow_5_close then '6'
            when dow_6_open = dow_6_close then '0'
            else ''
            end),' ') as alldays
            from
            actor.hours_of_operation
            group by 1
            ) as dowclosed
                where
                dowclosed.id=acirc.circ_lib and
                extract(dow from (due_date + '11 hours'::interval + '$days days'::interval))||'' not in(dowclosed.alldays) and
                coalesce((select false from actor.org_unit_closed aouc where acirc.circ_lib=aouc.org_unit and (acirc.due_date + '11 hours'::interval + '$days days'::interval) between aouc.close_start and aouc.close_end),true) and
                acirc.id = $circID
            ";
        
        my @results = @{$dbHandler->query($query)};
        if($#results > -1)
        {
            my @c = @{@results[0]};
            $count = @c[0];
        }
        $days++;
    }
    
    $query = "update action.circulation set due_date = '$count' where id = $circID";
    $logoutput.=$query."\n";
    $dbHandler->update($query);
}



sub find_money_payment
{
    my $circID = shift;
    my $ret = 0;
    my $query = "select count(*) from money.payment where xact=$circID";
    my @results = @{$dbHandler->query($query)};
    my @count = @{@results[0]};
    
    # return positive - meaning there are 0 payments and we can proceed with dealing with messed up billing.
    $ret = 1 if @count[0] == 0;
    my $opposite = 1;
    $opposite = 0 if $ret;
    $query = "update m_circ_time.acirc set had_payments = '$opposite' where acircid=$circID";
    $logoutput.=$query."\n";
    $dbHandler->update($query);
    print "payment count = $ret\n";
    return $ret;
}

sub find_voided_billing
{
    my $circID = shift;
    my $ret = 0;
    my $query = "select count(*) from money.billing where xact=$circID";
# print "About to $query\n";
    my @results = @{$dbHandler->query($query)};
# print Dumper(\@results);
    my @count = @{@results[0]};
# print Dumper(\@count);
    # abort to save time, there are 0 bills anyway.
# print "value = ".@count[0]."\n";
# my $temp = @count[0] == 0;
# print "temp = $temp\n";
    return '0' if @count[0] == 0;
# print "just passed the return 0 abort\n";
    my $query = "select count(*) from money.billing where xact=$circID and (voided is true or note !='System Generated Overdue Fine')";
    my @results = @{$dbHandler->query($query)};
    my @count = @{@results[0]};
    # return positive - meaning there are 0 billing lines that prevent us from with dealing with messed up billing.
    $ret = 1 if @count[0] == 0;
# print "just passed the  ret = 1 if\n";
    my $opposite = 1;
    $opposite = 0 if $ret;    
    $query = "update m_circ_time.acirc set had_bills = \$\$$opposite\$\$ where acircid=$circID";
    $logoutput.=$query."\n";
# print "just passed the update m_circ_time.acirc set had_bills\n";
    $dbHandler->update($query);
# print "just passed dbHandler->update\n";    
    print "billing count = $ret\n";
    return $ret;
}

sub correct_fines
{
    my $circID = shift;
    if( find_money_payment($circID) eq '1' && find_voided_billing($circID)  eq '1' )
    {
        my $query = "delete from money.billing where xact=$circID";
        $log->addLine($query);
        $dbHandler->update($query);
        $query = "update money.materialized_billable_xact_summary set last_billing_ts = null where id=$circID";
        $log->addLine($query);
        $dbHandler->update($query);
        return do_fines($circID);
    }
    else
    {
        $query = "update m_circ_time.acirc set couldnt_correct_bills = \$\$1\$\$ where acircid=$circID";
# print "About to $query \n";
        $dbHandler->update($query);
    }
    return 0;
}

sub do_fines
{
	my $circID = shift;
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
	return 1;
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
	my $usr = "migrate-script";
	my $workstation = "migrate-script";
	my $pass = $mobiusUtil->generateRandomString(10);
	
	my $query = "select id from actor.usr where upper(usrname) = upper('$usr')";
	my @results = @{$dbHandler->query($query)};
	my $result = 1;
	if($#results==-1)
	{
		#print "inserting user\n";
		$query = "INSERT INTO actor.usr (profile, usrname, passwd, ident_type, first_given_name, family_name, home_ou) VALUES ('13', E'$usr', E'$pass', '3', 'Script', 'Script User', E'$org_unit_id')";
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
		$query = "UPDATE actor.usr SET home_ou=E'$org_unit_id',ident_type=3,profile=13,active='t',super_user='t',deleted='f' where id=$usrid";
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




