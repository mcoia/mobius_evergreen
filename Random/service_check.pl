#!/usr/bin/perl


# Testing Evergreen services
# Blake GH
# MOBIUS


# placing holds via OpenSRF Examples from Dyrcona
# http://git.evergreen-ils.org/?p=working/NCIPServer.git;a=blob;f=lib/NCIP/ILS/Evergreen.pm;h=85aa4407536e8d86c8ca551ee4831e460f9b5f71;hb=b7d7ab764a76b07fd2a853c504813ccc076b5aba
# sub place_hold {


use strict; 
#use warnings;
#no strict 'refs';

use lib qw(../);
use LWP;
use Getopt::Std;
use JSON::XS;
use DateTime::Format::Duration;
use Text::CSV;
use Data::Dumper;
use OpenILS::Utils::Cronscript;
use OpenILS::Utils::Fieldmapper;
use OpenILS::Utils::CStoreEditor qw/:funcs/;
use OpenILS::Const qw(:const);
use OpenILS::Utils::Configure;
use OpenSRF::AppSession;
use OpenSRF::EX qw(:try);
use Encode;
use Scalar::Util qw(blessed);
use Loghandler;
use DBhandler;
use Mobiusutil;
use XML::Simple;
use Getopt::Long;

my $logfile = "servicecheck.log";
our $xmlconf = "/openils/conf/opensrf.xml";

our $dryrun = 0;
our $server = "";
our $notificationemail = "";
our $testOU = "147";
our $authtoken = "";
our $script;
our $output;
our $dt;
our $tempspace = "/tmp";
our $authusername;
our $authpassword;
our $verbose = 0;
our $onlytest;
our %testBattery =
(
PatronEdit => \&PatronEdit,
Circulation => \&Circulation,
CopyHold => \&CopyHold,
Autogen => \&Autogen,
CreateReport => \&CreateReport,
OPACSearch => \&OPACSearch,
SIPLogin => \&SIPLogin
);

our $mobutil = new Mobiusutil();	
our $dbHandler;	


GetOptions (
"server=s" => \$server,
"xmlconfig=s" => \$xmlconf,
"log=s" => \$logfile,
"ou=s" => \$testOU,
"email=s" => \$notificationemail,
"tempspace=s" => \$tempspace,
"v" => \$verbose,
"onlytest=s" => \$onlytest,
)
or die("Error in command line arguments\nYou can specify
--server specificappserver (optional)
--xmlconfig pathto_opensrf.xml (optional)
--log pathtodesiredlog.log (default working_directory/servicecheck.log)
--email notify emailaddress (optional)
--ou designated testing Org Unit ID (default 147)
--tempspace writable temp space (default /tmp)
-v (verbose)
\n");
 
if(! -e $xmlconf)
{
	print "I could not find the xml config file: $xmlconf\nYou can specify the path when executing this script\n";
	exit 0;
}
 
our $log = new Loghandler($logfile);
$log->deleteFile();
$log->addLogLine(" ---------------- Script Starting ---------------- ");

my %conf = %{getDBconnects()};
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

my $error = 0;

if($valid)
{
	$conf{"dbhost"} = $server if length($server)>0;
	
	eval{$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});};
	if ($@) 
	{
		$log->addLogLine("Could not establish a connection to the database");
		print "Could not establish a connection to the database";
	}
	else
	{
		# clear the log for debuging
		system "echo '' > /openils/var/log/osrfsys.log";
		# Access to the OpenSRF Router network is restricted to localhost,
		# therefore this must be run on each app server.
		my $result=0;
		$script = OpenILS::Utils::Cronscript->new;
		$log->addLogLine("Setup user in DB and grabbing auth token from it");
		$result = setupUser();
		print "0\n" unless $result;
		print "fail!\n" unless ($result && !$verbose);
		exit 1 unless $result;
		
		
		while ( (my $key, my $testFunction) = each %testBattery )
		{
			next if($onlytest && ($onlytest ne $key) );
			$log->addLogLine("Testing $testFunction");
			clockStart();
			$result = $testBattery{$key}->();
			my $time = clockEnd();
			$log->addLogLine("result $result - $time $testFunction");
			$error=1 unless $result eq '0';
		}
		system "cp /openils/var/log/osrfsys.log /mnt/evergreen/";
	}
}

$log->addLogLine(" ---------------- Script Ending ---------------- ");

print $error."\n";
exit $error;

# This is pretty much required in order to start OpenSRF testing. So, this will test the staff client's
# ability to login at the same time
sub setupUser
{
	my @usrcreds = @{createDBUser($testOU)};
	if(@usrcreds[3])
	{
		$authtoken = $script->authenticate(
			{
				username => @usrcreds[0],
				password => @usrcreds[1],
				workstation => @usrcreds[2]
			}
		);
		$authusername = @usrcreds[0];
		$authpassword = @usrcreds[1];
	}
	else
	{
		$log->addLogLine("Failed creating the user/workstation in the DB\nusr:".@usrcreds[0]." pass: ".@usrcreds[1]." workstation: ".@usrcreds[2]);
		return 0;
	}
	$log->addLogLine("$authusername,$authpassword");
	return 1;
}

sub PatronEdit
{
	my $r = getIDForThisPatron();
	if(!(ref($r) eq 'HASH')) # HASH will return if the patron is not found, otherwise an integer
	{
		$r = OpenSRF::AppSession->create('open-ils.actor')->request('open-ils.actor.user.fleshed.retrieve', $authtoken, $r, 0)->gather(1);
		# $log->addLogLine(Dumper($r));
		# Now we have the patron object stored in $r
		
		# Make a change to the user		
		my $random = $mobutil->generateRandomString(5);
		#dollar signs make for weird things, let's get rid of them if it really doesnt matter what we change it to.
		$random =~ s/\$/_/g;
		$r->day_phone( $random );
		#Update it
		$r = OpenSRF::AppSession->create('open-ils.actor')->request('open-ils.actor.patron.update', $authtoken, $r); #->gather(1);
		my $query = "select id from actor.usr where usrname=E'$authusername' and day_phone = \$\$$random\$\$";
		my @results = @{$dbHandler->query($query)};
		return 0 if($#results == 0) # 1 result is array position 0
	}
	return 1;
}

sub Circulation
{
	my $r = getIDForThisPatron();
	if(!(ref($r) eq 'HASH')) # HASH will return if the patron is not found, otherwise an integer
	{
		my @item = @{getAvailableItem($r)};
		my $itembarcode = @item[0];
		my $numbercircs = getCircTotal();
		my %args = (
		patron => $r,
		barcode => $itembarcode,
		permit_override => 1
		);
		$r = OpenSRF::AppSession->create('open-ils.circ')->request('open-ils.circ.checkout.full.override', $authtoken, \%args)->gather(1);
		#$log->addLogLine(Dumper($r));
		
		if(getCircTotal() > $numbercircs)
		{
			$log->addLine("Doing Checkin $itembarcode");
			my %args = (barcode => $itembarcode);
			my $r = OpenSRF::AppSession->create('open-ils.circ')->request('open-ils.circ.checkin', $authtoken, \%args)->gather(1);
			# $log->addLogLine(Dumper($r));
			return 0;
		}
	}
	return 1;
}

sub CopyHold
{
	my $r = getIDForThisPatron();
	if(!(ref($r) eq 'HASH')) # HASH will return if the patron is not found, otherwise an integer
	{
		my $patronid=$r;
		my @item = @{getAvailableItem($r)};
		my $item = @item[0];
		my $numberholds = getHoldTotal();
		
		my %args = (
		pickup_lib => $testOU,
		selection_ou => $testOU,
		depth => 2,
		patronid => $patronid,
		target => @item[1],
		hold_type => 'C'
		);
		$r = OpenSRF::AppSession->create('open-ils.circ')->request('open-ils.circ.holds.test_and_create.batch.override', $authtoken, \%args, [@item[1]])->gather(1);
		$log->addLogLine(Dumper($r));
		
		if(getHoldTotal() > $numberholds)
		{
			my $query = "update action.hold_request set cancel_time=now() where cancel_time is null and usr=$patronid";
			$log->addLine("$query");
			$dbHandler->update($query);
			return 0;
		}
	}
	return 1;
}

sub Autogen
{
	my $ret = 0;
	system "mkdir $tempspace/autogen";
	my $file = "autogentest.js";
	OpenILS::Utils::Configure::org_tree_js($tempspace."/autogen/", $file);
	my $originalFile = new Loghandler("/openils/var/web/opac/common/js/en-US/OrgTree.js");
	my $newFile = new Loghandler($tempspace."/autogen/"."en-US/$file");
	my @oldlines = @{$originalFile->readFile()};
	my @newlines = @{$newFile->readFile()};
	for my $i(0..$#oldlines)
	{
		$log->addLine(@oldlines[$i]."\n".@newlines[$i]);
		$ret = 1 if( @oldlines[$i] ne @newlines[$i] );
	}
	# lets cleanup
	system "rm -Rf $tempspace/autogen";
	return $ret;
}

sub clockStart
{
	$dt = DateTime->now(time_zone => "local");
}

sub clockEnd
{
	my $afterProcess = DateTime->now(time_zone => "local");
	my $difference = $afterProcess - $dt;
	my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
	return $format->format_duration($difference);
}

sub getIDForThisPatron
{
	# First, we need the ID of our patron/admin,
	# might as well test with the account we are using here $authusername
	my $r = OpenSRF::AppSession->create('open-ils.actor')->request
	('open-ils.actor.user.retrieve_id_by_barcode_or_username', $authtoken, 0, $authusername)->gather(1);
	# Now we have the patron object stored in $r
	return $r;
}

sub getCircTotal()
{
	my $query = "select id from action.circulation where usr = (select id from actor.usr where usrname=E'$authusername')";
	my @results = @{$dbHandler->query($query)};
	return $#results;
}

sub getHoldTotal()
{
	my $query = "select id from action.hold_request where usr = (select id from actor.usr where usrname=E'$authusername')";
	my @results = @{$dbHandler->query($query)};
	return $#results;
}

# Not used
sub createNewConfig
{
	my $readConfig = new Loghandler($xmlconf);
	my @lines = @{$readConfig->readFile()};
	my $newconfig = new Loghandler($tempspace."/temptestconfig.xml");
	$newconfig->deleteFile();
	foreach(@lines)
	{
		my $line = $_;
		$line =~ s/(<host>)([^<]*)(<\/host>)/$1$server$3/g;
		$newconfig->appendLine($line);
	}
	$xmlconf = $tempspace."/temptestconfig.xml";
}

sub getAvailableItem
{
	my $userid = @_[0];
	my $query = "select barcode,status,id from asset.copy where circ_lib = $testOU and not deleted and call_number > 0";
	$log->addLine($query);
	my @results = @{$dbHandler->query($query)};
	my $found = 0;
	my $itemid = '';
	my $itembarcode = '';
	for (my $i..$#results)
	{
		my @row = @{@results[$i]};
		if(@row[1] eq '0') # available status
		{
			$found = @row[0];
			$itemid = @row[2];
			last;
		}
	}
	if(!$found)
	{
		# save us a trip to the db
		my @lastrow = @{@results[$#results]} unless $#results == -1;
		$itembarcode = $#results > -1 ? @lastrow[0] : -1;
		$itemid = $#results > -1 ? @lastrow[2] : -1;
		$log->addLine("itembarcode = $itembarcode");
		if($itembarcode == -1) # go ahead and create a dummy item
		{
			my $query = "insert into asset.call_number(creator,editor,label,owning_lib,record) values ($userid,$userid,E'mobiustest',$testOU,
			
			(select id from biblio.record_entry where id not in(select id from biblio.record_entry where
					lower(marc) ~ \$\$<datafield tag=\"856\" ind1=\"4\" ind2=\"0\">\$\$ AND

					(
							marc ~ \$\$tag=\"008\">.......................[oqs]\$\$
							or
							marc ~ \$\$tag=\"006\">......[oqs]\$\$
					)
					and
					(
							marc ~ \$\$<leader>......[at]\$\$
					)
					and
					(
							marc ~ \$\$<leader>.......[acdm]\$\$
					)
)and not deleted limit 1))";
			$log->addLine($query);
			$dbHandler->update($query);
			$query = "select id from asset.call_number where label=E'mobiustest' and owning_lib=$testOU limit 1";
			my @results = @{$dbHandler->query($query)};
			@results=@{@results[0]};
			my $callnumber = @results[0];
			my $randombarcode = $mobutil->generateRandomString(8);
			$randombarcode =~ s/\$/1/g;
			$randombarcode =~ s/&/1/g;
			$randombarcode =~ s/#/1/g;
			$randombarcode =~ s/@/1/g;
			$query = "INSERT INTO asset.copy (circ_lib,creator,call_number,editor,loan_duration,fine_level,barcode,dummy_title,dummy_author,holdable)
			VALUES ($testOU,E'$userid',$callnumber,E'$userid',2,2,E'$randombarcode',E'mobius-test',E'mobius-test',false)";
			$log->addLine($query);
			$dbHandler->update($query);
			$query = "select barcode,id from asset.copy where id in(select max(id) from asset.copy where circ_lib=$testOU and status=0 and not deleted and call_number=$callnumber)";
			my @results = @{$dbHandler->query($query)};
			my @row = @{@results[0]};			
			$itembarcode = @row[0];
			$itemid = @row[1];
		}
		# Let's do a checkin
		$log->addLine("Doing Checkin $itembarcode");
		my %args = (barcode => $itembarcode);
		my $r = OpenSRF::AppSession->create('open-ils.circ')->request
			('open-ils.circ.checkin', $authtoken, \%args)->gather(1);
		$found = $itembarcode;
	}
	my @found = ($found, $itemid);
	return \@found;
	
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
	my $xml = new XML::Simple;
	my $data = $xml->XMLin($xmlconf);
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
	my $org_unit_id = @_[0];
	my $usr = "mobius-test";
	my $workstation = "mobius-test";
	my $pass = $mobutil->generateRandomString(10);
	
	my $query = "select id from actor.usr where upper(usrname) = upper(E'$usr')";
	my @results = @{$dbHandler->query($query)};
	my $result = 1;
	my $userid = -1;
	
	if($#results==-1)
	{
		#print "inserting user\n";
		$query = "INSERT INTO actor.usr (profile, usrname, passwd, ident_type, first_given_name, family_name, home_ou) VALUES (25, E'$usr', E'$pass', '3', 'Script', 'Script User', $org_unit_id)";
		$result = $dbHandler->update($query);
	}
	else
	{
		my @userid = @{@results[0]};
		$userid = @userid[0];
		#print "updating user\n";
		my @row = @{@results[0]};
		$query = "UPDATE actor.usr SET PASSWD=E'$pass', home_ou=E'$org_unit_id',ident_type=3,profile=25,active=E't',super_user=E't',deleted=E'f' where id=".@row[0];
		$result = $dbHandler->update($query);
	}
	if($result)
	{
		$query = "select id from actor.workstation where upper(name) = upper(E'$workstation')";
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
		$query = "select id from actor.usr_address where usr = $userid";
		my @results = @{$dbHandler->query($query)};
		if($#results==-1)
		{
			$query = "INSERT INTO actor.usr_address (usr,street1,city,country,post_code) values ($userid,'test','test','test','test')";
			$log->addLine($query);
			$result = $dbHandler->update($query);
			$query = "select id from actor.usr_address where usr=$userid and street1=\$\$test\$\$";
			$log->addLine($query);
			my @results = @{$dbHandler->query($query)};
			@results = @{@results[0]};
			$query = "update actor.usr set mailing_address=".@results[0]." where id=$userid";
			$log->addLine($query);
			$result = $dbHandler->update($query);
			
		}
		$query = "select id from actor.card where usr = $userid";
		my @results = @{$dbHandler->query($query)};
		if($#results==-1)
		{
			$query = "INSERT INTO actor.card (usr,barcode) values ($userid,E'$usr')";
			$log->addLine($query);
			$result = $dbHandler->update($query);
			$query = "select id from actor.card where usr=$userid and barcode=\$\$$usr\$\$";
			$log->addLine($query);
			my @results = @{$dbHandler->query($query)};
			@results = @{@results[0]};
			$query = "update actor.usr set card=".@results[0]." where id=$userid";
			$log->addLine($query);
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
	my @usrcreds = @{@_[1]};
	my $query = "delete from actor.usr where usrname='".@usrcreds[0]."'";
	print $query."\n";
	$dbHandler->update($query);	
	$query = "delete from actor.workstation where name='".@usrcreds[2]."'";
	print $query."\n";
	$dbHandler->update($query);
}

