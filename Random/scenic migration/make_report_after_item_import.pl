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
	print "usage: ./make_report.pl /tmp/report.txt [optional /path/to/xml/config/opensrf.xml]\n";
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
		my %reports=
		(
		'Report how many are "Withdrawn"' ,
					 "select count(*) from m_scenic.asset_copy_legacy where l_status='Withdrawn'",
		'Report how many are "On-line"',
					"select count(*) from m_scenic.asset_copy_legacy where l_owning_b='On-Line';",
		'Report Duplicate Barcodes' ,
					 "select l_barcode,count from(select l_barcode,count(*) as \"count\" from m_scenic.asset_copy_legacy group by l_barcode) as a where count>1;",
		'Holding Totals per branch' ,
					 "select l_owning_b,count(*) from m_scenic.asset_copy_legacy group by l_owning_b order by count(*);",
		'Report Date lengths' ,
					 "select length(l_bcode_date),count(*) from m_scenic.asset_copy_legacy group by length(l_bcode_date) order by count(*)",
		'Report legacy locations' ,
					 "select l_shelf_loc,count(*) from m_scenic.asset_copy_legacy group by l_shelf_loc order by count(*);",
		'Report legacy location descriptions' ,
					 "select l_shelf_des,count(*) from m_scenic.asset_copy_legacy group by l_shelf_des order by count(*);",
		'Report barcode conflicts with production' ,
					 "select barcode from asset.copy where barcode in(select l_barcode from m_scenic.asset_copy_legacy);",
		'Report prices that are not digits.digitdigit',
					 'select l_price from m_scenic.asset_copy_legacy where length(regexp_replace(l_price, \'^\d+\.\d\d$\', \'\'))>0;',
		'Report Item Statuses',
					 "select l_status,count(*) from m_scenic.asset_copy_legacy group by l_status order by count(*);",
		'Report Item Material Types' ,
					"select l_mat_type,count(*) from m_scenic.asset_copy_legacy group by l_mat_type order by count(*);",
		'Report patrons that will be removed because they are deceased' ,
					"select count(*) from m_scenic.patron_file where patronid in(select patronid from m_scenic.invalid_patron where Reason_Card_Invalidated='Deceased');",
		'Report circulations that will be removed because the patron is deceased' ,
					"select count(*) from m_scenic.items_checkedout where patronid in(select patronid from m_scenic.invalid_patron where Reason_Card_Invalidated='Deceased');",
		'Report patrons that will be removed because they do not have circulations and are older than 2 years' ,
					"select count(*) from m_scenic.patron_file where to_date(Card_Expiration_Date,'MM/DD/YYYY')+'2 years'::interval < now() and Card_Expiration_Date is not null and Card_Expiration_Date!=''
and patronid not in(select patronid from m_scenic.items_checkedout);",
		'Report patrons that will be removed because they have not renewed in 5 years and still have circulations' ,
					"select count(*) from m_scenic.patron_file where to_date(Card_Expiration_Date,'MM/DD/YYYY')+'5 years'::interval < now() and Card_Expiration_Date is not null and Card_Expiration_Date!=''
and patronid in(select patronid from m_scenic.items_checkedout);",
		'Report items that will be deleted because they are checked out by patrons that are 5 years old' ,
					"select * from m_scenic.asset_copy_legacy where barcode in
(
select item_barcode from m_scenic.items_checkedout where patronid in(select patronid from m_scenic.patron_file where to_date(Card_Expiration_Date,'MM/DD/YYYY')+'5 years'::interval < now() and Card_Expiration_Date is not null and Card_Expiration_Date!='')
) 
and create_date+'5 years'::interval < now()
order by create_date",
		'Report circs that will be removed because the item is damaged and resulting bill has been canceled in Listen' ,
					"select item_barcode from m_scenic.patron_fines where adjustment_to_date::float >0 and total_paid_to_date::float = 0 and item_barcode in(select l_barcode from m_scenic.asset_copy_legacy where l_status = 'Damaged - Total')",
		'Report item statuses that will be deleted because they are checked out by patrons that are 5 years old' ,
					"select l_status,count(*) from m_scenic.asset_copy_legacy where barcode in
(
select item_barcode from m_scenic.items_checkedout where patronid in(select patronid from m_scenic.patron_file where to_date(Card_Expiration_Date,'MM/DD/YYYY')+'5 years'::interval < now() and Card_Expiration_Date is not null and Card_Expiration_Date!='')
) 
and create_date+'5 years'::interval < now()
group by l_status;"
			
		);

		while (($title, $query) = each(%reports))
		{
			$log->addLine("\n");
			$log->addLine($title);
			#print "$title\n";
			my @results = @{$dbHandler->query($query)};
			
			my @tabs = @{gettabs(\@results)};
			#print Dumper(@tabs);
			foreach(@results)
			{
				my $row = $_;
				my @row = @{$row};
				my $line;
				my $colnum=0;
				foreach(@row)
				{
					#print "Getting width: ".@tabs[$colnum]."\n";
					$thisval = $mobutil->makeEvenWidth($_,@tabs[$colnum]+3);
					$line.=$thisval;
					$colnum++;
				}
				$log->addLine($line);
			}
			
		}

	}
}

$log->addLogLine(" ---------------- Script Ending ---------------- ");

sub gettabs
{
	my @results = @{@_[0]};
	my @cols;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $line;
		my $colnum=0;
		foreach(@row)
		{
			if(!@cols[$colnum])
			{
				@cols[$colnum]=0;
			}
			$stringlen=length($_);
			if(@cols[$colnum]<$stringlen)
			{
				@cols[$colnum]=$stringlen;
			}
			$colnum++;
		}
	}
	return \@cols;
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
