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
our $log = @ARGV[0];

$log = new Loghandler($log);
$log->truncFile("");
	
my $dt = DateTime->now(time_zone => "local"); 
my $fdate = $dt->ymd; 
my $ftime = $dt->hms;
my $dateString = "$fdate $ftime";
my %dbconf = %{getDBconnects($xmlconf)};
$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});


# add sullivan as a working location for all of the scenic staff user accounts:

# insert into permission.usr_work_ou_map usr,work_ou
# (
# select distinct usr,179 from permission.usr_work_ou_map where usr in(
# select id from actor.usr where home_ou in(select id from actor.org_unit where lower(name)~'sceni')
# and profile in(select id from permission.grp_tree where parent=3 union select id from permission.grp_tree where id=3)
# ) and work_ou not in(select id from actor.org_unit where lower(name)~'sulliv')
# )

	my $query = "
	select id,home_ou,upper(first_given_name),upper(family_name),dob,(select barcode from actor.card where usr=au.id and active limit 1) from actor.usr au where
lower(first_given_name||family_name||dob) in(
select lower(first_given_name||family_name||dob)
from
(
select home_ou,first_given_name,family_name,dob,count(*) from actor.usr where
lower(first_given_name||family_name||dob) in(
(
select lower(first_given_name||family_name||dob) from 
(
select lower(first_given_name) \"first_given_name\",lower(family_name) \"family_name\",dob,count(*) 
	from actor.usr where 
	home_ou in(select id from actor.org_unit where lower(name)~'sulliv' or lower(name)~'sceni') and
	lower(first_given_name) !='sullivan' 
	group by lower(first_given_name),lower(family_name),dob having count(*)>1
	) as a
	)
	)
	group by home_ou,first_given_name,family_name,dob
	having count(*)=1
) as b
)
AND upper(first_given_name) !='DEBBIE' AND upper(family_name)!='LUCHENBILL'
and home_ou in(select id from actor.org_unit where lower(name)~'sulliv' or lower(name)~'sceni')
	order by lower(first_given_name),lower(family_name),dob,home_ou desc";
	my @results = @{$dbHandler->query($query)};
	my %map;
	my %barcodemap;
	my %namemap;
	my $prevpat = '';
	$log->addLine($#results." Results");
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $patronsig = @row[2].@row[3].@row[4];
		if( ($patronsig ne $prevpat) && ($prevpat ne '') )
		{
			#$log->addLine("($patronsig ne $prevpat)");
			if($map{179})  #just make sure that sullivan has a patron in the group, otherwise what are we doing here?
			{
				my $master = -1;
				my $masterbarcode = -1;
				my @merges;
				while ((my $ou, my $patronida ) = each(%map))
				{
					foreach(@{$patronida})
					{
						my $patronid = $_;
						if($ou != 179)
						{
							if($master == -1)
							{
								$master = $patronid;
							}
							else  # weird - Means that there are multiple scenic patrons in this group
							{
								$log->addLine("$patronid;$ou;$master;".$barcodemap{$patronid}.";".$barcodemap{$master}.";".$namemap{$patronid}.";".$namemap{$master}.";Multiple Scenic patrons");
								# Lets not merge scenic accounts together for this project.
								#push @merges, $patronid;
							}
						}
						else
						{
							push @merges, $patronid;
						}
					}
				}
				if($master > -1)
				{
					foreach(@merges)
					{
						$log->addLine("$_;;$master;".$barcodemap{$_}.";".$barcodemap{$master}.";".$namemap{$_}.";".$namemap{$master});
						$query = "select actor.usr_merge($_,$master,true,true,true);";
						print $dbHandler->update($query);
						$log->addLine($query);
					}
				}
				else
				{
					$log->addLine("Master was never set".Dumper(@merges));
				}
			}
			else{$log->addLine("Sullivan doesnt have a patron here!");}
			$log->addLine("Clearing Map");
			%map = ();
			$prevpat = $patronsig;
		}
		if($prevpat eq ''){$prevpat = $patronsig;}
		
		$log->addLine("Pushing ".@row[0]."  ".@row[1]);
		if(ref $map{@row[1]} eq 'ARRAY')
		{
			push $map{@row[1]},@row[0];
		}
		else
		{
			my @temp = (@row[0]);
			$map{@row[1]} = \@temp;
		}
		$barcodemap{@row[0]} = @row[5];
		$namemap{@row[0]} = @row[2]." ".@row[3];
		
	}
	print "Done\n";


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

 
 