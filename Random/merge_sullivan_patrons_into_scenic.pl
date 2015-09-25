#!/usr/bin/perl
use lib qw(../);
use File::Path qw(make_path remove_tree);
use strict; 
use Loghandler;
use Mobiusutil;
use DBhandler;
use Data::Dumper;
use DateTime;
use utf8;
use DateTime;
use OpenILS::Application::AppUtils;
use DateTime::Format::Duration;
use XML::Simple;


my $xmlconf = "/openils/conf/opensrf.xml";

our $mobUtil = new Mobiusutil();
our $dbHandler;	
	
my $dt = DateTime->now(time_zone => "local"); 
my $fdate = $dt->ymd; 
my $ftime = $dt->hms;
my $dateString = "$fdate $ftime";
my %dbconf = %{getDBconnects($xmlconf)};
$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});

print "Removing email addresses\n";
my $query = "update actor.usr set email = ''";
$dbHandler->update($query);

scramblePatronNames();
scrambleAddresses();


sub scramblePatronNames
{
	print "Gathering Patron DB count\n";

	my $query = "select count(*) from actor.usr";
	my @results = @{$dbHandler->query($query)};

	my $row = @results[0];
	my @row = @{$row};
	my $total = @row[0];

	my $current = 1;
	my $thisid = 1;
	my $offset = 0;
	my $query = "select id,first_given_name,second_given_name,family_name from actor.usr order by id limit 1 offset $offset";
	my @results = @{$dbHandler->query($query)};
	my @thisset = ();
	if ($#results > -1)
	{
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			@thisset = (@row[1],@row[2],@row[3]);
			$thisid = @row[0];
		}
	}
	else
	{
		$thisid=0;
	}
	while ($thisid > 0)
	{	
		$offset++;
		print "$current / $total\t";
		print "$thisid,".@thisset[0].",".@thisset[1].",".@thisset[2]."\t";
		my $query = "
	update actor.usr au set first_given_name = coalesce((select first_given_name from actor.usr where id=(select floor(random()*((select max(id) from actor.usr)+1)-1) where au.id=au.id)),'random'),
	family_name = coalesce((select family_name from actor.usr where id=(select floor(random()*((select max(id) from actor.usr)+1)-1) where au.id=au.id)),'random'),
	second_given_name = coalesce((select second_given_name from actor.usr where id=(select floor(random()*((select max(id) from actor.usr)+1)-1) where au.id=au.id)),'random')
	where au.id = $thisid
	";
		$dbHandler->update($query);
		my $query = "select id,first_given_name,second_given_name,family_name from actor.usr where id=$thisid";
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			my @ranresults = (@row[1],@row[2],@row[3]);
			print @ranresults[0].",".@ranresults[1].",".@ranresults[2]."\n";
		}
		
		my $query = "select id,first_given_name,second_given_name,family_name from actor.usr order by id limit 1 offset $offset";
		my @results = @{$dbHandler->query($query)};
		if ($#results > -1)
		{
			foreach(@results)
			{
				my $row = $_;
				my @row = @{$row};
				@thisset = (@row[1],@row[2],@row[3]);
				$thisid = @row[0];	
			}
		}
		else
		{
			$thisid=0;
		}
		$current++;
	}
	print "Done\n";
}

sub scrambleAddresses
{
	print "Gathering Address DB Counts\n";

	my $query = "select count(*) from actor.usr_address";
	my @results = @{$dbHandler->query($query)};

	my $row = @results[0];
	my @row = @{$row};
	my $total = @row[0];

	my $current = 1;
	my $thisid = 1;
	my $offset = 0;
	my $query = "select id,street1,street2,city,state,post_code from actor.usr_address order by id limit 1 offset $offset";
	my @results = @{$dbHandler->query($query)};
	my @thisset = ();
	if ($#results > -1)
	{
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			@thisset = (@row[1],@row[2],@row[3],@row[4],@row[5]);
			$thisid = @row[0];
		}
	}
	else
	{
		$thisid=0;
	}
	while ($thisid != 0)
	{	
		$offset++;
		print "$current / $total\t";
		print "$thisid,".@thisset[0].",".@thisset[1].",".@thisset[2].",".@thisset[3].",".@thisset[4]."\t";
		my $query = "
	update actor.usr_address au set 
	street1 = coalesce((select street1 from actor.usr_address where id=(select floor(random()*((select max(id) from actor.usr_address)+1)-1) where au.id=au.id)),'random'),
	street2 = coalesce((select street2 from actor.usr_address where id=(select floor(random()*((select max(id) from actor.usr_address)+1)-1) where au.id=au.id)),'random'),
	city = coalesce((select city from actor.usr_address where id=(select floor(random()*((select max(id) from actor.usr_address)+1)-1) where au.id=au.id)),'random'),
	state = coalesce((select state from actor.usr_address where id=(select floor(random()*((select max(id) from actor.usr_address)+1)-1) where au.id=au.id)),'random'),
	post_code = coalesce((select post_code from actor.usr_address where id=(select floor(random()*((select max(id) from actor.usr_address)+1)-1) where au.id=au.id)),'random')
	where au.id = $thisid
	";
		$dbHandler->update($query);
		my $query = "select id,street1,street2,city,state,post_code from actor.usr_address where id=$thisid";
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			my @ranresults = (@row[1],@row[2],@row[3],@row[4],@row[5]);
			print @ranresults[0].",".@ranresults[1].",".@ranresults[2].",".@ranresults[3].",".@ranresults[4]."\n";
		}
		
		my $query = "select id,street1,street2,city,state,post_code from actor.usr_address order by id limit 1 offset $offset";
		my @results = @{$dbHandler->query($query)};
		if ($#results > -1)
		{
			foreach(@results)
			{
				my $row = $_;
				my @row = @{$row};
				@thisset = (@row[1],@row[2],@row[3],@row[4],@row[5]);
				$thisid = @row[0];	
			}
		}
		else
		{
			$thisid=0;
		}
		$current++;
	}
}


sub getDBconnects
{
	my $openilsfile = @_[0];
	my $xml = new XML::Simple;
	my $data = $xml->XMLin($openilsfile);
	my %conf;
	$conf{"dbhost"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{host};
	$conf{"db"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{db};
	$conf{"dbuser"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{user};
	$conf{"dbpass"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{pw};
	$conf{"port"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{port};
	##print Dumper(\%conf);
	return \%conf;

}

 exit;

 
 