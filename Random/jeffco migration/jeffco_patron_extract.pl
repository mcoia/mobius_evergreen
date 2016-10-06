#!/usr/bin/perl
use lib qw(../);
use MARC::Record;
use MARC::File;
use MARC::File::XML (BinaryEncoding => 'utf8');
use File::Path qw(make_path remove_tree);
use strict; 
use Loghandler;
use Mobiusutil;
use DBhandler;
use Data::Dumper;
use DateTime;
use utf8;
use Encode;
use LWP::Simple;
use OpenILS::Application::AppUtils;
use DateTime::Format::Duration;
use Digest::SHA1;
use XML::Simple;
use Unicode::Normalize;
use Getopt::Long;



	our $mobUtil = new Mobiusutil();  
	my $xmlconf = "/openils/conf/opensrf.xml";
	our $log;
	our $dbHandler;
	our $jobid=-1;
	our %queries;
	our $baseTemp = "/mnt/evergreen/tmp";
	our @writeMARC = ();

	my $dt = DateTime->now(time_zone => "local"); 
	my $fdate = $dt->ymd; 
	my $ftime = $dt->hms;
	my $dateString = "$fdate $ftime";
	my $file = "/mnt/evergreen/migration/jeffco/data/patrons.xml";
	$log = new Loghandler("/mnt/evergreen/migration/jeffco/log/extract.log");
	$log->truncFile("");
	$log->addLogLine(" ---------------- Script Starting ---------------- ");
	
	my %dbconf = %{getDBconnects($xmlconf)};
	$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
	my $xml = new XML::Simple;
	my $data = $xml->XMLin($file);
	#$log->addLine( Dumper($data) );
	my @users = ();
	my %columns = ();
	while ( (my $user, my $userdata) = each(%{$data}))
	{
		foreach(@{$userdata})
		{
			my %s;
			my $all = xmlLoop($_, "", \%s);
			#$log->addLine("Done Looping");
			#$log->addLine(Dumper($all));
			while ((my $internal, my $value ) = each(%{$all}))
			{
				$columns{$internal} = 0;
			}
			push(@users, $all);
		}
	}
	#$log->addLine( Dumper(@users) );
	
	my %loopvars = 
	(
	"patron_file" => "\$answer = ( !(\$internal =~ m/^bill/) && !(\$internal =~ m/^charge/) )", 
	"patron_bill_file" => "\$answer = \$internal =~ m/^bill/",
	"patron_charge_file" => "\$answer = \$internal =~ m/^charge/"
	);
	
	while ((my $table, my $search ) = each(%loopvars))
	{
		my @cols = ();
		my $userid_included=0;
		while ((my $internal, my $value ) = each(%columns))
		{
			my $answer;
			eval($search);
			#$log->addLine("answer = $answer");
			if( $answer ) #saving the bill section for another table
			{
				#$log->addLine("Adding $internal column to $table");
				push(@cols, $internal);
			}
			# Always include the userid
			elsif( $internal =~ m/userid/g )
			{
				if(!$userid_included)
				{
					push(@cols, $internal);
					$userid_included=1;
				}
			}
		}
		# Make the postgres table pretty
		@cols = sort ( @cols );
		
		my $makeTable = "CREATE TABLE m_jeffco.$table (";
		my @billfinal = ();
		my @billsections = ();
		
		if($table ne "patron_file")
		{
			my %billcols = ();
			my %billsects = ();
			#$log->addLine(Dumper(\@cols));
			foreach(@cols)
			{
				my $t = $_;
				if(!($t =~ m/userid/g))
				{
					$t =~ s/[^\d]*(\d*)(.*)/$2/;
					#$log->addLine("billcols is getting $t");
					#$t = "noname" if length($mobUtil->trim($t)) == 0;
					$billcols{$t}=0;
					$t = $_;
					$t =~ s/([^\d]*\d*)(.*)/$1/;
				}
				else{$log->addLine("billsect is getting $t")};
				#$log->addLine("billsects is getting $t");
				$billsects{$t}=0;
			}
			while ((my $billcol, my $tvalue ) = each(%billcols))
			{
				push(@billfinal,$billcol);
			}
			while ((my $billcol, my $tvalue ) = each(%billsects))
			{
				push(@billsections,$billcol);
			}
			@billfinal = sort ( @billfinal );
			$makeTable = $makeTable."$_ TEXT," for @billfinal;
		}
		else
		{
			$makeTable = $makeTable."$_ TEXT," for @cols;
		}
		
		$makeTable = substr($makeTable,0,-1);
		$makeTable.=")";
		$log->addLine($makeTable);
		$dbHandler->update("DROP TABLE IF EXISTS m_jeffco.$table");
		$dbHandler->update($makeTable);
		my $query = "INSERT INTO m_jeffco.$table (";
		if ($table ne "patron_file"){$query.="$_,"  for @billfinal;}
		if ($table eq "patron_file"){$query.="$_,"  for @cols;}
		$query = substr($query,0,-1);
		$query .= ")\n VALUES\n";
			
		foreach(@users)
		{
			my %attr = %{$_};
			if($table ne "patron_file")
			{	
				foreach(@billsections)
				{
					my $row = "(";
					my $hadData = 0;
					my $billsect = $_;
					foreach(@billfinal)
					{
						my $key = $billsect.$_;
						if($key =~ m/userid/g)
						{
							$key = $billsect;
						}
						# if($_ =~ m/noname/)
						# {
							# my $temp = $_;
							# $temp =~ s/noname//;
							# $key = $billsect.$temp;
						# }
						if($attr{$key})
						{
							my $data = $attr{$key};
							# data that ends with a $ will mess up the query. So we need to put a space character at the end				if(
							$data =~ s/\$$/\$ /g;
							$row.="\$\$$data\$\$,";
							$hadData = 1 if !($key =~ m/userid/g);
						}
						else
						{
							$row.="null,";
						}
						# $log->addLine("next billfinal");
					}
					$row = substr($row,0,-1);
					$row .= "),\n";
					$query .= $row if $hadData;
					# $log->addLine("next billsection");
				}
			}
			else
			{
				$query.="(";
				foreach(@cols)
				{
					if($attr{$_})
					{
						my $data = $attr{$_};
						# data that ends with a $ will mess up the query. So we need to put a space character at the end				if(
						$data =~ s/\$$/\$ /g;
						$query.="\$\$$data\$\$,";
					}
					else
					{
						$query.="null,";
					}
				}
				$query = substr($query,0,-1);
				$query .= "),\n";
			}
			# $log->addLine("next user");
		}
		$query = substr($query,0,-2);
		$log->addLine($query);
		
		#insert the patron data
		$dbHandler->update($query);
		undef @cols;
	}
	
	
	my $afterProcess = DateTime->now(time_zone => "local");
	my $difference = $afterProcess - $dt;
	my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
	my $duration =  $format->format_duration($difference);
	
	$log->addLogLine("Duration: $duration");
	$log->addLogLine(" ---------------- Script Ending ---------------- ");
	
	
sub xmlLoop
{
	my $xmlscrap = @_[0];
	my $parent = lc @_[1];
	my $compiled = @_[2];
	#$log->addLine(Dumper($compiled));
	if(ref($xmlscrap) eq 'ARRAY')
	{
		#$log->addLine("It's an Array");
		my $i=0;
		foreach (@{$xmlscrap})
		{
			#$log->addLine("Recursing $_");
			$compiled = xmlLoop($_, $parent.$i, $compiled);
			$i++;
		}
	}
	elsif(ref($xmlscrap) eq 'HASH')
	{
		#$log->addLine("It's an HASH");
		while ((my $internal, my $value ) = each(%{$xmlscrap}))
		{
			#$log->addLine("Recursing $value");
			$compiled = xmlLoop($value, $parent.$internal, $compiled);
		}
	}
	else
	{
		#$log->addLine("It's data");
		my $i=0;
		my %compiled = %{$compiled};
		my $key = $parent;
		$key =~ s/[\s,]/_/g;
		while( ($compiled{$key."_".$i} ) )
		{
			$i++;
		}
		#$log->addLine("Assigning ".$key."_".$i." = $xmlscrap");
		$compiled{$key."_".$i} = $xmlscrap;
		$compiled = \%compiled;
	}
	return $compiled;
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

 
 