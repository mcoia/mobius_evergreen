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
select mmbxs.id,mmbxs.usr,ac.barcode,acirc.circ_lib,mmbxs.xact_start,mmbxs.xact_finish,acirc.due_date,ac.price,mmbxs.total_owed,mmbxs.total_paid,
mmbxs.balance_owed,mpfines.assessment_amount,mpfines.adjustment_to_date,mpfines.total_paid_to_date,mpfines.balanced_owed,mmbxs.total_owed-(mpfines.assessment_amount::float),ac.price-(mpfines.assessment_amount::float),macl.l_status,ccs.name,acirc.target_copy from
config.copy_status ccs,
m_scenic.asset_copy_legacy macl,
asset.copy ac,
action.circulation acirc,
money.materialized_billable_xact_summary mmbxs,
actor.usr au,
m_scenic.patron_fines mpfines,
m_scenic.patron_file mpfile
where
mpfines.patronid=mpfile.patronid and
mpfile.patron_barcode=au.usrname and
mpfines.item_barcode=ac.barcode and
mmbxs.id=acirc.id and
acirc.target_copy=ac.id and
mpfines.item_barcode=macl.l_barcode and 
macl.l_status='Damaged - Total' and
mmbxs.usr=au.id and
au.home_ou between 154 and 164 and
ccs.id=ac.status and 

ac.price=mpfines.assessment_amount::float
order by acirc.circ_lib

";
#ac.status!=14 and
		my @results = @{$dbHandler->query($query)};
		my $total = $#results+1;
		my $count=0;
		$log->addLogLine("$total damaged items that are checked out");
		my $lastou = "";
		my $output = "";
		my $authtoken;
		my $stop=0;
		my $errors=0;
		my $forgivecount=0;
		my $forgivelostcount=0;
		my $checkincount=0;
		my $paycount=0;
		foreach(@results)
		{
			if($count>-1)
			{
				my $row = $_;
				my @row = @{$row};
				my $xactid = @row[0];
				my $userID = @row[1];
				my $ibarcode = @row[2];
				my $ou = @row[3];
				my $assessdif = @row[16];
				my $lstat = @row[17];
				my $curstat = @row[18];
				my $copyid = @row[19];
				my $ogpay = @row[13];
				my $ogforgive = @row[12];
				my $copyprice = @row[7];
				my $totalowed = @row[8];
				
				
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
						 #Check it in if it is checked out
						 $valid=1;
						 print "Current Status = $curstat\n";
						if($curstat eq "Checked out")
						{	
							print "Looks like it is checked out - need to check it in\n";
							my $before = getCountCheckedin($dbHandler,$xactid);
							my $args = 
							{
								barcode => $ibarcode
							};
							
							$log->addLogLine("Checking in patronID => $userID ,xactid => $xactid, item => $ibarcode");

							my $r = OpenSRF::AppSession->create('open-ils.circ')->request('open-ils.circ.checkin',  $authtoken,$args)->gather(1);
							my $after = getCountCheckedin($dbHandler,$xactid);
							if($after>0)
							{
								$log->addLogLine("Success: checkin user: $userID home: $ou itembarcode: $ibarcode");
								$valid=1;
								$checkincount++;
							}
							else
							{
								print Dumper $r;
								$log->addLogLine("FAILED: checkin user: $userID home: $ou itembarcode: $ibarcode");
								$valid=0;
								$errors++;
							}
						}
						elsif($curstat eq "Lost")
						{	
							print "Looks like it is Lost - Need to void Lost bills\n";
							my $lastxact = getlastxact($dbHandler,$userID);
							my @payment;
							push(@payment, [$xactid, $totalowed]);
							 my $args = 
							 {
								payment_type => "forgive_payment",
								userid=>$userID,
								note=>'Migration Lost Convert to Damage Forgive Lost Bill',
								payments=>\@payment
							};
							$formattedOutput.="$userID,$xactid,$totalowed,$ou,";	
							my $before = getforgivelostCount($dbHandler,$xactid);
							my $r;
							$log->addLogLine("Forgiving lost bill patronID => $userID ,xactid => $xactid,amount => $totalowed");
							
							   
							 my $r = OpenSRF::AppSession->create('open-ils.circ')->request('open-ils.circ.money.payment',  $authtoken,
								 $args, $lastxact)->gather(1);	
							my $after = getforgivelostCount($dbHandler,$xactid);
							$after = $after-$before;
							my $temp = Dumper $r;
							if($after>0)
							{
								$valid=1;
								$log->addLogLine("Success (forgive payment): user: $userID home: $ou amount: $totalowed");
								$forgivelostcount++;
							}
							else
							{
								print Dumper $r;
								$log->addLogLine("FAILED: Could not forgive Lost bill user: $userID home: $ou amount: $totalowed");
								$valid=0;
								$errors++;
							}
							$formattedOutput.=parseOutput($temp);
						}
						if($valid)
						{
						
						
							## MARK DAMAGED
							my $args = 
							{
								apply_fines => 1
							};
							my $before = getDamagedCount($dbHandler,$copyid);
							my $r;
							$log->addLogLine("Damaging item patronID => $userID ,xactid => $xactid, itembarcode => $ibarcode");
							   
							 my $r = OpenSRF::AppSession->create('open-ils.circ')->request('open-ils.circ.mark_item_damaged',  $authtoken, $copyid, $args)->gather(1);
								 
							#print Dumper $r;
							my $after = getDamagedCount($dbHandler,$copyid);
							if($after>0)
							{
								$log->addLogLine("Success: Damage user: $userID home: $ou itembarcode: $ibarcode");
								$valid=1;
								$updatecount++;
							}
							else
							{
								print Dumper $r;
								$log->addLogLine("FAILED: Damage user: $userID home: $ou itembarcode: $ibarcode");
								$valid=0;
								$errors++;
							}
							
							if($valid)
							{
							
								## FINALLY PAY if patron paid before
								if($assessdif>1 || $assessdif<-1)
								{
									$log->addLogLine("ERROR - EVERGREEN BILL IS THAN 1 DOLLAR DIFFERENT THAN LISTEN. WILL NOT PAY. Running patronID => $userID ,xactid => $xactid,copy id => $copyid");
									print "ERROR - EVERGREEN BILL IS THAN 1 DOLLAR OFF FROM LISTEN. WILL NOT PAY. Running patronID => $userID ,xactid => $xactid,copy id => $copyid";
								}
								else
								{
									if($ogpay>0)
									{
										my @payment;
										push(@payment, [$xactid, $ogpay]);
										 my $args = 
										 {
											payment_type => "cash_payment",
											userid=>$userID,
											note=>'Migration Pay Damaged',
											payments=>\@payment
										};
										my $lastxact = getlastxact($dbHandler,$userID);
										$formattedOutput.="$userID,$xactid,$ogpay,$ou,";	
										my $before = getCount($dbHandler,$xactid);
										my $r;
										$log->addLogLine("Paying patronID => $userID,xactid => $xactid, Evergreenbill=> $copyprice, payamount => $ogpay");
										
										   
										 my $r = OpenSRF::AppSession->create('open-ils.circ')->request('open-ils.circ.money.payment',  $authtoken,
											 $args, $lastxact)->gather(1);					
											 
										#print Dumper $r;
										my $after = getCount($dbHandler,$xactid);
										$after = $after-$before;
										my $temp = Dumper $r;
										if($after>0)
										{
											$paycount++;
											$log->addLogLine("Success: user: $userID home: $ou amount: $ogpay xactid: $xactid");
										}
										else
										{
											print Dumper $r;
											$log->addLogLine("FAILED: user: $userID home: $ou amount: $ogpay xactid: $xactid");
											$errors++;
										}
										$formattedOutput.=parseOutput($temp);										
									}
									if($ogforgive>0)
									{
										my @payment;
										my $lastxact = getlastxact($dbHandler,$userID);
										push(@payment, [$xactid, $ogforgive]);
										 my $args = 
										 {
											payment_type => "forgive_payment",
											userid=>$userID,
											note=>'Migration Forgive',
											payments=>\@payment
										};
										$formattedOutput.="$userID,$xactid,$ogforgive,$ou,";	
										my $before = getCount($dbHandler,$xactid);
										my $r;
										$log->addLogLine("Forgiving patronID => $userID ,xactid => $xactid,amount => $ogforgive");
										
										   
										 my $r = OpenSRF::AppSession->create('open-ils.circ')->request('open-ils.circ.money.payment',  $authtoken,
											 $args, $lastxact)->gather(1);					
											 
										#print Dumper $r;
										my $after = getCount($dbHandler,$xactid);
										$after = $after-$before;
										my $temp = Dumper $r;
										if($after>0)
										{
											$forgivecount++;
											$log->addLogLine("Success: user: $userID home: $ou amount: $amount");
										}
										else
										{
											print Dumper $r;
											$log->addLogLine("FAILED: user: $userID home: $ou amount: $amount");
										}
										$formattedOutput.=parseOutput($temp);
										
									}
								}
							}
						}
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
		$log->addLogLine("Checked in $checkincount items");
		$log->addLogLine("Paid $paycount items");
		$log->addLogLine("Forgave $forgivecount items");
		$log->addLogLine("Forgave Lost Bills $forgivelostcount items");
		$log->addLogLine("Damaged $updatecount / $total damaged");
		$log->addLogLine("$errors errors");
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


sub getCountCheckedin
{
	my $dbHandler = @_[0];		
	my $xactid = @_[1];
	my $query = "select count(*) from action.circulation where id=$xactid and checkin_time is not null";
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

sub getDamagedCount
{
	my $dbHandler = @_[0];		
	my $itembarcode = @_[1];
	my $query = "select count(*) from asset.copy where id='$itembarcode' and status = 14";
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

sub getforgivelostCount
{
	my $dbHandler = @_[0];		
	my $xactid = @_[1];
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
	my $dbHandler = @_[0];		
	my $usrid = @_[1];
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
