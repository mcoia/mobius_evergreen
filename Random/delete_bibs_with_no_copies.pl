#!/usr/bin/perl
use lib qw(../);
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



	my $query = "
	select id from biblio.record_entry where not deleted and id not in(select record from asset.call_number where not deleted and id in(select call_number from asset.copy where not deleted))
and id not in
(select record from asset.call_number where label=\$\$##URI##\$\$ and not deleted)
and lower(marc) !~ \$\$<datafield tag=\"856\" ind1=\"4\" ind2=\"0\">\$\$
order by id
	";
	my @results = @{$dbHandler->query($query)};
	$log->addLine($#results." Results");
	my $total = $#results;
	my $current=0;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		my $duration = calcTimeDiff($dt);
		my $speed = $current / $duration;
		$speed = 1 if $speed < .1;
		my $eta = ($total - $current) / $speed / 60;
		$eta = substr($eta,0,index($eta,'.')+3);
		$duration = $duration / 60;
		$duration = substr($duration,0,index($duration,'.')+3);
		
		print "$current / $total id $bibid\telapsed/remaining $duration/$eta\n";
		$query = "delete from biblio.record_entry where id=$bibid";
		$dbHandler->update($query);
		$log->addLine("$bibid\t$query");
		$current++;
	}
	print "Done\n";

	
	
sub calcTimeDiff
{
	my $previousTime = @_[0];
	my $currentTime=DateTime->now;
	my $difference = $currentTime - $previousTime;#
	my $format = DateTime::Format::Duration->new(pattern => '%M');
	my $minutes = $format->format_duration($difference);
	$format = DateTime::Format::Duration->new(pattern => '%S');
	my $seconds = $format->format_duration($difference);
	my $duration = ($minutes * 60) + $seconds;
	if($duration<.1)
	{
		$duration=.1;
	}
	return $duration;
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

 
 