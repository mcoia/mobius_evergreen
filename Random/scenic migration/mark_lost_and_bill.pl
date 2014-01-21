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
	print "usage: ./pay_bills.pl /tmp/logfile.log [optional /path/to/xml/config/opensrf.xml]\n";
	exit;
 }

my $log = new Loghandler($logfile);
$log->deleteFile();
$log->addLogLine(" ---------------- Script Starting ---------------- ");

my %conf = %{getDBconnects($xmlconf,$log)};
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
	my $dbHandler;
	
	eval{$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});};
	if ($@) 
	{
		$log->addLogLine("Could not establish a connection to the database");
		print "Could not establish a connection to the database";
	}
	else
	{
		my $mobutil = new Mobiusutil();
		my $updatecount=0;
		my $script = OpenILS::Utils::Cronscript->new;
		my $query = "
select mmbxs.id,mmbxs.usr,ac.barcode,acirc.circ_lib,mmbxs.xact_start,mmbxs.xact_finish,acirc.due_date,mmbxs.total_owed,mmbxs.total_paid,mmbxs.balance_owed,macl.l_status,ccs.name,acirc.target_copy from
config.copy_status ccs,
m_scenic.asset_copy_legacy macl,
asset.copy ac,
action.circulation acirc,
money.materialized_billable_xact_summary mmbxs,
actor.usr au
where
mmbxs.id=acirc.id and
acirc.target_copy=ac.id and
ac.barcode=macl.l_barcode and 
(macl.l_status='Lost & Paid' or macl.l_status='Long Term Overdue -Cl.Ret' or macl.l_status='Long Term Forgiven' or macl.l_status='Lost') and
mmbxs.usr=au.id and
au.home_ou between 154 and 164 and
mmbxs.total_owed=0 and 
ccs.id=ac.status
order by acirc.circ_lib
";
		my @results = @{$dbHandler->query($query)};
		my $total = $#results+1;
		my $count=0;
		$log->addLogLine("$total circulations that need to be marked lost");
		my $lastou = "";
		my $output = "";
		my $authtoken;
		my $stop=0;
		my $formattedOutput="";
		foreach(@results)
		{
			if($count>-1)
			{
				my $row = $_;
				my @row = @{$row};
				my $lstatus = @row[10];
				my $copyid = @row[12];
				my $barcode = @row[2];
				my $userID = @row[1];			
				my $ou = @row[3];
				my $xactid=@row[0];
				if($lastou!=$ou)
				{
					$log->addLogLine("Changing OU");
					my @usrcreds = @{createDBUser($dbHandler,$mobutil,$ou)};
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
					 
						my $args = {barcode=>$barcode};
						
						$formattedOutput.="$barcode,$ou,$userID,";	
						my $before = getCount($dbHandler,$xactid);
						my $r;
						$log->addLogLine("Marking lost patronID => $userID ,itembarcode => $barcode");
						
						   
						 my $r = OpenSRF::AppSession->create('open-ils.circ')->request('open-ils.circ.circulation.set_lost',  $authtoken,
							 $args)->gather(1);					
							 
						#print Dumper $r;
						my $after = getCount($dbHandler,$xactid);
						$after = $after-$before;
						my $temp = Dumper $r;
						if($after>0)
						{
							$updatecount++;
							$log->addLogLine("Success: Transid: $xactid, user: $userID home: $ou itembarcode: $barcode");
						}
						else
						{
							print Dumper $r;
							$log->addLogLine("FAILED: Transid: $xactid, user: $userID home: $ou itembarcode: $barcode");
							$log->addLine(Dumper $r);
						}
						$formattedOutput.=parseOutput($temp);
					} 
					catch Error with 
					{
						my $err = shift;
						$formattedOutput.=$err."\n";
					}
				}
			}
			$count++;
			
		}
		$output=substr($output,0,-1);
		$log->addLogLine($output);
		$log->addLogLine("Updated $updatecount / $total mark lost");
		$log->addLogLine("$formattedOutput");
		
## this section is removed because you cannot mark an item lost without a circulation.		
		my $updatecount=0;
		my $query = "
select ac.barcode,ac.circ_lib,macl.l_status,ccs.name from
config.copy_status ccs,
m_scenic.asset_copy_legacy macl,
asset.copy ac
where
ac.barcode=macl.l_barcode and 
(macl.l_status='Lost & Paid' or macl.l_status='Long Term Forgiven' or macl.l_status='Lost') and
ccs.id=ac.status and
ac.id not in(select target_copy from action.circulation where circ_lib between 154 and 164) and
ac.status!=3
order by ac.circ_lib
";
		my @results = @{$dbHandler->query($query)};
		my $total = $#results+1;
		my $count=0;
		$log->addLogLine("$total circulations that need to be marked lost");
		my $lastou = "";
		my $output = "";
		my $authtoken;
		my $stop=0;
		my $formattedOutput="";
		foreach(@results)
		{
			if($count<-1)
			{
				my $row = $_;
				my @row = @{$row};
				my $lstatus = @row[2];
				my $barcode = @row[0];							
				my $ou = @row[1];
				if($lastou!=$ou)
				{
					$log->addLogLine("Changing OU");
					my @usrcreds = @{createDBUser($dbHandler,$mobutil,$ou)};
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
					 
						my $args = {barcode=>$barcode};
						
						$formattedOutput.="$barcode,$ou,";	
						my $before = getCount($dbHandler,$xactid);
						my $r;
						$log->addLogLine("Marking lost itembarcode => $barcode");
						   
						 my $r = OpenSRF::AppSession->create('open-ils.circ')->request('open-ils.circ.circulation.set_lost',  $authtoken,
							 $args)->gather(1);					
							 
						#print Dumper $r;
						my $after = getCount($dbHandler,$xactid);
						$after = $after-$before;
						my $temp = Dumper $r;
						if($after>0)
						{
							$updatecount++;
							$log->addLogLine("Success: home: $ou itembarcode: $barcode");
						}
						else
						{
							print Dumper $r;
							$log->addLogLine("FAILED: home: $ou itembarcode: $barcode");
							$log->addLine(Dumper $r);
						}
						$formattedOutput.=parseOutput($temp);
					} 
					catch Error with 
					{
						my $err = shift;
						$formattedOutput.=$err."\n";
					}
				}
			}
			$count++;
			
		}
		$output=substr($output,0,-1);
		$log->addLogLine($output);
		$log->addLogLine("Marked $updatecount / $total lost");
		$log->addLogLine("$formattedOutput");
		
		
		
	}
}


$log->addLogLine(" ---------------- Script Ending ---------------- ");


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
	my $usr = "scenic-migrate";
	my $workstation = "scenic-migrate-script";
	my $pass = $mobiusUtil->generateRandomString(10);
	
	my %params = map { $_ => 1 } @results;
	
	my $query = "select id from actor.usr where upper(usrname) = upper('$usr')";
	my @results = @{$dbHandler->query($query)};
	my $result = 1;
	if($#results==-1)
	{
		$query = "INSERT INTO actor.usr (profile, usrname, passwd, ident_type, first_given_name, family_name, home_ou) VALUES ('25', E'$usr', E'$pass', '3', 'Script', 'Script User', E'$org_unit_id')";
		$result = $dbHandler->update($query);
	}
	else
	{
		my @row = @{@results[0]};
		$query = "UPDATE actor.usr SET PASSWD=E'$pass', home_ou=E'$org_unit_id' where id=".@row[0];
		$result = $dbHandler->update($query);
	}
	if($result)
	{
		$query = "select id from actor.workstation where upper(name) = upper('$workstation')";
		my @results = @{$dbHandler->query($query)};
		if($#results==-1)
		{
			$query = "INSERT INTO actor.workstation (name, owning_lib) VALUES (E'$workstation', E'$org_unit_id')";		
			$result = $dbHandler->update($query);
		}
		else
		{
			my @row = @{@results[0]};
			$query = "UPDATE actor.workstation SET name=E'$workstation', owning_lib= E'$org_unit_id' WHERE ID=".@row[0];	
			$result = $dbHandler->update($query);
		}
	}
	#print "User: $usr\npass: $pass\nWorkstation: $workstation";
	
	@ret = ($usr, $pass, $workstation, $result);
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

sub getCount
{
	my $dbHandler = @_[0];		
	my $xactid = @_[1];
	my $query = "select count(*) from money.billing where xact=$xactid";
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
