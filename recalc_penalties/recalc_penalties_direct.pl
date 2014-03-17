#!/usr/bin/perl

# Copyright 2011 Traverse Area District Library
# Author: Blake Graham-Henderson

# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
# 

# Script to do a mass re-calculation of system standing penalies 
# Useful for updating penalties after policy/config changes

# This script will create a user in actor.usr and a workstation in actor.workstation 
# for opensrf authorization. If the script runs more than once, it will use those 
# already-created rows in the DB. 

# It will not run on the whole database. It only looks at patrons who have overdue items 
# inside the last 48 hour period AND who do not have penalties 1,2,3,4

# Feel free to alter the DB query on line 114 to meet your search needs.


# Example usage:
# ./recalc_penalties_direct.pl log.log
#

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
	print "usage: ./recalc_penalties_direct.pl /tmp/logfile.log [optional /path/to/xml/config/opensrf.xml]\n";
	exit;
 }

my $log = new Loghandler($logfile);
#$log->deleteFile();
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
		#print "Ok - I am connected to:\n".$conf{"dbhost"}."\n".$conf{"db"}."\n".$conf{"dbuser"}."\n".$conf{"dbpass"}."\n".$conf{"port"}."\n";
		OpenSRF::System->bootstrap_client(config_file => '/openils/conf/opensrf_core.xml'); 
		my $script = OpenILS::Utils::Cronscript->new;
		
		my $query = "select distinct usr,(select home_ou from actor.usr where id=a.usr) from action.circulation a where due_date> (now()-('48 hours'::interval)) and due_date<now() and xact_finish is null and checkin_time is null and usr not in (select usr from actor.usr_standing_penalty) order by (select home_ou from actor.usr where id=a.usr)";
		my @results = @{$dbHandler->query($query)};
		my $total = $#results+1;
		$log->addLogLine("$total users to check");
		my $lastou = "";
		my $output = "";
		my $authtoken;
		my $stop=0;
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			my $userID = @row[0];
			
			my $ou = @row[1];
			
			if($lastou!=$ou)
			{
				#$log->addLogLine("Changing OU");
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
					my $before = getCountPenalty($dbHandler);
					my $r = OpenSRF::AppSession->create('open-ils.actor')->request('open-ils.actor.user.penalties.update', $authtoken,  $userID )->gather(1);					
					my $after = getCountPenalty($dbHandler);
					$after = $after-$before;
					if($after>0)
					{
						$output.=$userID.",";
						$updatecount++;
						$log->addLogLine("user: $userID home: $ou");
						$log->addLogLine("Added $after row(s) to actor.usr_standing_penalty\n results:\n");
						$log->addLine(Dumper($r));
					}
					#print Dumper $r;
		
				} 
				catch Error with 
				{
					my $err = shift;
					$log->addLogLine("Error: $err");
				}
			}
			
		}
		$output=substr($output,0,-1);
		$log->addLogLine($output);
		$log->addLogLine("Updated $updatecount / $total users");
		
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

sub createDBUser
{
	my $dbHandler = @_[0];
	my $mobiusUtil = @_[1];
	my $org_unit_id = @_[2];
	my $usr = "recalc-penalty";
	my $workstation = "recalc-penalty-script";
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

sub getCountPenalty
{
	my $dbHandler = @_[0];	
	$query = "select count(*) from actor.usr_standing_penalty";
	my @count = @{$dbHandler->query($query)};
	my $before=0;
	if($#count>-1)
	{
		my @t = @{@count[0]};
		$before = @t[0];
	}
	
	return $before;
}

