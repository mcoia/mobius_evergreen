#!/usr/bin/perl


#use strict; use warnings;

use lib qw(../);
use LWP;
use Getopt::Std;
use Data::Dumper;
use Encode;
use Scalar::Util qw(blessed);
use Loghandler;
use DBhandler;
use Mobiusutil;
use XML::Simple;


my $logfile = @ARGV[0];
my $xmlconf = "/openils/conf/opensrf.xml";
my $success = 0;
our $authtoken;
our $log;
our $dbHandler;
our $output = "";
our $config = "/openils/conf/opensrf_core.xml";


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
    print "usage: ./delete_patrons.pl /tmp/logfile.log [optional /path/to/xml/config/opensrf.xml]\n";
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

select 
au.id,
au.usrname,
au.family_name,
au.first_given_name,
au.expire_date::date,
aou.name,
(select name from permission.grp_tree where id=au.profile),
(select sum(balance_owed) from money.materialized_billable_xact_summary where balance_owed > 0 and usr=au.id)
from 
actor.usr au,
actor.org_unit aou
where
aou.id=au.home_ou and
-- include only jcl patrons
au.home_ou in(select id from actor.org_unit where lower(name)~'jefferson') and
-- must be expired before 1/1/2017
expire_date < '2017-01-01' and
-- remove internet only from the selection
profile not in(select id from permission.grp_tree where lower(name)~'internet on') and
-- remove people with more than \$20
au.id not in(select usr from (select usr,sum(balance_owed) as tsum from money.materialized_billable_xact_summary where balance_owed > 0 group by 1 ) as a where tsum > 19.99) and
-- remove those who still have items checked out
au.id not in(select usr from action.circulation where xact_finish is null and usr in(select id from actor.usr where home_ou in(select id from actor.org_unit where lower(name)~'jefferson'))) and
-- no need to delete an already deleted patron
not au.deleted
-- group by 1
";
$log->addLogLine("$query");
        my @results = @{$dbHandler->query($query)};
        my $total = $#results+1;
        my $loops = 0;
        $log->addLogLine("$total patrons");
        
        foreach(@results)
        {
            my $row = $_;
            my @row = @{$row};
            my $patronid = @row[0];
            
            $success++;
            $query = "select * from actor.usr_delete($patronid, null)";
            $log->addLine($query);
            my @res = @{$dbHandler->query($query)};
            $log->addLine($res[0][0]);
            # Just do one for testing
            # exit;
            $loops++;
        }
        
        $log->addLine("success Delete ".$#successforgive);
        
        
    }
}


$log->addLogLine(" ---------------- Script Ending ---------------- ");



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




