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
		'Number of bibs in Missouri Evergreen' ,
					 "select count(*) from biblio.record_entry where id>1027390 and deleted='f'",
		'Report Holding Totals per branch',
					"select (select name from actor.org_unit where id=a.circ_lib),count(*) from asset.copy a where circ_lib between 154 and 164 and deleted='f' group by (select name from actor.org_unit where id=a.circ_lib) order by count(*)",
		'Number of physical non dulplicate items in Missouri Evergreen' ,
					 "select count(*) from asset.call_number where owning_lib between 154 and 164 and deleted='f' and label!='##URI##'",
		'Number of physical items in Missouri Evergreen including duplicates' ,
					 "select count(*) from asset.copy where call_number in(select id from asset.call_number where owning_lib between 154 and 164 and deleted='f' and label!='##URI##')",
		'Number of electronic clickable items in Missouri Evergreen' ,
					 "select count(*) from asset.call_number where owning_lib between 154 and 164 and deleted='f' and label='##URI##'",
		'Number of bibs that have electronic clickable items in Missouri Evergreen' ,
					 "select count(*) from biblio.record_entry where id in( select record from asset.call_number where owning_lib between 154 and 164 and deleted='f' and label='##URI##')",
		'Samples of bibs that have electronic clickable items in Missouri Evergreen' ,
					 "select distinct mrfr.value ,u.label,u.href
						from 
						metabib.real_full_rec mrfr,
						biblio.record_entry bre,
						asset.call_number acn,
						asset.uri_call_number_map ucnm,
						asset.uri u
						where
						ucnm.call_number=acn.id and
						u.id=ucnm.uri and 
						acn.label='##URI##' and
						bre.id=acn.record and
						acn.deleted='f' and
						mrfr.record=bre.id and
						mrfr.tag='245' and
						mrfr.subfield='a' and
						acn.owning_lib between 154 and 164
						limit 20",
		'Number of items that have parts' ,
					 "select count(*) from asset.copy where circ_lib between 154 and 164 and deleted='f' and id in(select target_copy from asset.copy_part_map)",
		'Samples of items with parts',
					 "select barcode from asset.copy where circ_lib between 154 and 164 and deleted='f' and id in(select target_copy from asset.copy_part_map) limit 10",
		'Item Status Counts in Missouri Evergreen',
					 "select (select name from config.copy_status where id=a.status),count(*) from asset.copy a where circ_lib between 154 and 164 and deleted='f' group by (select name from config.copy_status where id=a.status)",
		'Item Statuses from Listen' ,
					"select l_status,count(*) from m_scenic.asset_copy_legacy group by l_status order by count(*)",
		'Item Status conversion map' ,
					"select l_status,count(*) from m_scenic.asset_copy_legacy group by l_status order by count(*)",
		'Report the circ mods compared to material types for scenic - What is was -> Converted to in Evergreen' ,
					"select concat(l_mat_type,' -> ',l_circ_mod),count(*) from m_scenic.asset_copy_legacy where circ_lib not in(156) group by concat(l_mat_type,' -> ',l_circ_mod) order by count(*) desc",
		'Report the circ mods compared to material types for washington - What is was -> Converted to in Evergreen' ,
					"select concat(l_mat_type,' -> ',l_circ_mod),count(*) from m_scenic.asset_copy_legacy where circ_lib in(156) group by concat(l_mat_type,' -> ',l_circ_mod) order by count(*) desc",
		'Items that did not get a Circ Mod' ,
					"select l_mat_type,count(*) from m_scenic.asset_copy_legacy where l_circ_mod is null group by l_mat_type order by count(*) desc",
		'Report numbers on circ mods' ,
					"select l_circ_mod,count(*) from m_scenic.asset_copy_legacy group by l_circ_mod order by count(*) desc",
		'Report old mat type compared to new circ mod for both scenic and washington' ,
					"select concat(l_mat_type,' -> ',l_circ_mod),count(*) from m_scenic.asset_copy_legacy group by concat(l_mat_type,' -> ',l_circ_mod) order by count(*) desc",
		'Report evergreen status vs Listen status' ,
					"select concat(ccs.name,' <-> ',ccs2.name),count(*)
						from
						asset.copy ac,
						m_scenic.asset_copy_legacy macl,
						config.copy_status ccs,
						config.copy_status ccs2
						where macl.l_status_stage!=ac.status and
						macl.l_barcode=ac.barcode
						and macl.l_status_stage!=0
						and macl.l_status_stage!=1
						and ccs.id=ac.status
						and ccs2.id= macl.l_status_stage
						group by concat(ccs.name,' <-> ',ccs2.name)
						order by
						concat(ccs.name,' <-> ',ccs2.name)",
		'Listen fines that have more than 1 fine per patron,item combo (blank patron barcodes are those that were removed for migration rules)' ,
					"select patronid,(select patron_barcode from m_scenic.patron_file where patronid=a.patronid),item_barcode,assessment_amount,adjustment_to_date,total_paid_to_date,balanced_owed,description,fine_or_fee_description from m_scenic.patron_fines a where concat(patronid,'_',item_barcode) in
						(
						select item from 
						(select concat(patronid,'_',item_barcode) \"item\",count(*) \"count\" from 
						 m_scenic.patron_fines where length(btrim(item_barcode))>4 and item_barcode in(select barcode from asset.copy where circ_lib between 154 and 164) group by  concat(patronid,'_',item_barcode) order by count(*) desc
						 ) as b where count>1
						) and patronid in(select patronid from m_scenic.patron_file)
						order by patronid,item_barcode
						",
		'Number of circulations in Missouri Evergreen' ,
					 "select count(*) from action.circulation where usr in(select id from actor.usr where home_ou between 154 and 164)",
		'Number of circulations in Listen' ,
					 "select count(*) from m_scenic.items_checkedout",
		'Circulations that did not migrate into Missouri Evergreen\nPatron Barcode,Item Barcode,Check out date, Branch, Due Date' ,
					 "select (select patron_barcode from m_scenic.patron_file where patronid=a.patronid),item_barcode,Original_Check_Out_Date,Original_Check_Out_Branch,Latest_Due_Date from m_scenic.items_checkedout a
						where item_barcode not in
						(
						select barcode from asset.copy where id in(select target_copy from action.circulation where usr in(select id from actor.usr where home_ou between 154 and 164))
						)",
		'Number of patrons in Evergreen' ,
					 "select count(*) from actor.usr where home_ou between 154 and 164",
			'Invalid patrons by reason counts' ,
					 "select Reason_Card_Invalidated,count(*) from m_scenic.invalid_patron group by Reason_Card_Invalidated",
			'Lengths of barcodes',
					"select length(l_barcode),count(*) from m_scenic.asset_copy_legacy group by length(l_barcode)",
			'Report Duplicate Barcodes',
					"select l_barcode,count from(select l_barcode,count(*) as \"count\" from m_scenic.asset_copy_legacy group by l_barcode) as a where count>1",
			
			'Report legacy locations' ,
					"select l_shelf_loc,count(*) from m_scenic.asset_copy_legacy group by l_shelf_loc order by count(*)",
			'Report legacy location descriptions' ,
					"select l_shelf_des,count(*) from m_scenic.asset_copy_legacy group by l_shelf_des order by count(*)",
			'Report prices that are not digits.digitdigit' ,
					"select l_price from m_scenic.asset_copy_legacy where length(regexp_replace(l_price, '^\d+\.\d\d$', ''))>0",			
			'Report Item Material Types' ,
					"select l_mat_type,count(*) from m_scenic.asset_copy_legacy group by l_mat_type order by count(*)",
			'Report patrons that will be removed because they are deceased' ,
					"select count(*) from m_scenic.invalid_patron where Reason_Card_Invalidated='Deceased'",
			'Report circulations that will be removed because the patron is deceased' ,
					"select count(*) from m_scenic.items_checkedout where patronid in(select patronid from m_scenic.invalid_patron where Reason_Card_Invalidated='Deceased')",
			'Report patrons that will be removed because they do not have circulations and are older than 2 years' ,
					"select count(*) from m_scenic.patron_file where to_date(Card_Expiration_Date,'MM/DD/YYYY')+'2 years'::interval < now() and Card_Expiration_Date is not null and Card_Expiration_Date!=''
			and patronid not in(select patronid from m_scenic.items_checkedout)",
			'Report patrons that will be removed because they have not renewed in 5 years and still have circulations' ,
					"select count(*) from m_scenic.patron_file where to_date(Card_Expiration_Date,'MM/DD/YYYY')+'5 years'::interval < now() and Card_Expiration_Date is not null and Card_Expiration_Date!=''
			and patronid in(select patronid from m_scenic.items_checkedout)",
			'Report Item Statuses that are checked out - the resulting number is the number of circulations for that status' ,
					"select a.l_status,count(*) from m_scenic.items_checkedout b, m_scenic.asset_copy_legacy a where a.l_barcode=b.item_barcode group by a.l_status order by count(*)",
			'Report Item statuses that are checked out by old patrons' ,
					"select l_status,count(*) from m_scenic.asset_copy_legacy where 
			l_barcode in 
				(
				select Item_Barcode from m_scenic.items_checkedout  where patronid in
					(
						select patronid from m_scenic.patron_file where to_date(Card_Expiration_Date,'MM/DD/YYYY') < now()-'2 years'::interval and Card_Expiration_Date is not null and Card_Expiration_Date!=''
					)		
				)
			 group by l_status order by count(*)"
			
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
