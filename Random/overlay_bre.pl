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
my $brefile = "/mnt/evergreen/migration/jeffco/out/pg_loader-output.bre.sql";

our $mobUtil = new Mobiusutil();
our $dbHandler;	
	
my $dt = DateTime->now(time_zone => "local"); 
my $fdate = $dt->ymd; 
my $ftime = $dt->hms;
my $dateString = "$fdate $ftime";
my %dbconf = %{getDBconnects($xmlconf)};
$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});



print "Reading $brefile\n";
my $brereader = new Loghandler($brefile);
my @brelines = @{$brereader->readFile()};
foreach(@brelines)
{
    my @tabbed = split(/\t/,$_);
    my $id = @tabbed[7];
    my $marc = @tabbed[9];
    print "updating $id\n";
    my $query = "UPDATE biblio.record_entry set marc = \$1 where id=\$2";
    my @vals = ($marc,$id);
    $dbHandler->updateWithParameters($query,\@vals);
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

 
 