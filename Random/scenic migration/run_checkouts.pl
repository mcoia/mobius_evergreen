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
	print "usage: ./scenic_run_checkouts.pl /tmp/logfile.log [optional /path/to/xml/config/opensrf.xml]\n";
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
select au.usrname,mic.item_barcode,mic.Original_Check_Out_Date,mic.Latest_Due_Date,au.home_ou from
m_scenic.items_checkedout mic,
actor.usr au,
m_scenic.patrons msp
where au.usrname=msp.usrname
and
mic.patronid=msp.patronid
and length(concat(btrim(mic.Original_Check_Out_Date),btrim(mic.Latest_Due_Date)))>8 order by au.home_ou
";
		my @results = @{$dbHandler->query($query)};
		my $total = $#results+1;
		$log->addLogLine("$total circs");
		my $lastou = "";
		my $output = "";
		my $authtoken;
		my $stop=0;
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			my $userID = @row[0];
			my $itemid = @row[1];
			my $chdate = @row[2];
			my $duedate = @row[3];			
			my $ou = @row[4];
			#correct blank dates based upon the other date
			if(length($mobutil->trim($chdate))<8 && length($mobutil->trim($duedate))>8)
			{
				#$log->addLogLine("$duedate");
				my @sp = split(/\//,$duedate);
				#$log->addLogLine(Dumper(@sp));
				@sp[0]=int(@sp[0])-2;
				@sp[0]=$mobutil->padLeft(@sp[0],2,'0');
				if(@sp[0]<1)
				{
					@sp[0]=12;
					@sp[2]=@sp[2]-1;
				}
				$chdate = @sp[0].'/'.@sp[1].'/'.@sp[2];
				#$log->addLogLine("$chdate");				
			}
			if(length($mobutil->trim($duedate))<8 && length($mobutil->trim($chdate))>8)
			{
				my @sp = split(/\//,$duedate);
				@sp[0]=@sp[0]+2;
				if(@sp[0]>12)
				{
					@sp[0]=1;
					@sp[2]=@sp[2]+1
				}
				$duedate = @sp[0].'/'.@sp[1].'/'.@sp[2];
			}
			
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
					 my $args = 
					 {
						patron_barcode => $userID ,
						barcode => $itemid,
						checkout_time => $chdate,
						due_date => $duedate,
						permit_override => 1
					};
					$formattedOutput.="$userID,$itemid,$chdate,$duedate,";	
					my $before = getCount($dbHandler,$userID);
					my $r;
					$log->addLogLine("Running patron_barcode => $userID ,barcode => $itemid,checkout_time => $chdate,due_date => $duedate,permit_override => 1");
					my $r = OpenSRF::AppSession->create('open-ils.circ')
						->request('open-ils.circ.checkout', $authtoken, $args)
							->gather(1);
					#print Dumper $r;
					my $after = getCount($dbHandler,$userID);
					$after = $after-$before;
					my $temp = Dumper $r;
					if($after>0)
					{
						$updatecount++;
						$log->addLogLine("Success: user: $userID home: $ou");
					}
					else
					{
						$log->addLogLine("FAILED: user: $userID home: $ou barcode: $itemid");
						$log->addLogLine(Dumper $r);
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
		$output=substr($output,0,-1);
		$log->addLogLine($output);
		$log->addLogLine("Updated $updatecount / $total circs");
		$log->addLogLine("$formattedOutput");
		my $percent = ($updatecount / $total)*100;
		
		if(0)#$percent > 95) #more than 95 percent of the circulations were successful, then go ahead and fix the statuses
		{
			#Fix "Bindery" status
			$query = "
			 update asset.copy set status=2 where id in(
			 select ac.id  from
			 config.copy_status ccs,
			 m_scenic.asset_copy_legacy macl,
			 asset.copy ac
			 where
			 ac.barcode=macl.l_barcode and 
			 (btrim(macl.l_status)!='') and
			 ccs.id=ac.status and
			 ac.id not in(select target_copy from action.circulation where circ_lib between 154 and 164) and
			 (macl.l_status = 'At Bindery')
			 and
			 ac.status=0
			 )";
$dbHandler->update($query);
			#Fix "Repair in Branch" status
			$query = "
			 update asset.copy set status=105 where id in(
			 select ac.id  from
			 config.copy_status ccs,
			 m_scenic.asset_copy_legacy macl,
			 asset.copy ac
			 where
			 ac.barcode=macl.l_barcode and 
			 (btrim(macl.l_status)!='') and
			 ccs.id=ac.status and
			 ac.id not in(select target_copy from action.circulation where circ_lib between 154 and 164) and
			 (macl.l_status = 'Repair in Branch')
			 and
			 ac.status=0
			 )
			";
$dbHandler->update($query);
			 #Fix Everything else to missing
			 $query = "
			update asset.copy set status=4 where id in(
			select ac.id  from
			config.copy_status ccs,
			m_scenic.asset_copy_legacy macl,
			asset.copy ac
			where
			ac.barcode=macl.l_barcode and 
			(btrim(macl.l_status)!='') and
			ccs.id=ac.status and
			ac.id not in(select target_copy from action.circulation where circ_lib between 154 and 164) and
			(macl.l_status != 'Long Term Overdue -Cl.Ret' and btrim(macl.l_status) != '')
			and
			ac.status=0
			)";
$dbHandler->update($query);
		}
		
		
		
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
	my $userid = @_[1];	
	$query = "select count(*) from action.circulation where usr=(select id from actor.usr where usrname='$userid')";
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

