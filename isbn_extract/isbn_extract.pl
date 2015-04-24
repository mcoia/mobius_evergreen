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
use email;
use DateTime;
use utf8;
use Encode;
use DateTime;
use LWP::Simple;
use OpenILS::Application::AppUtils;
use DateTime::Format::Duration;
use Digest::SHA1;
use XML::Simple;
use Unicode::Normalize;



my $configFile = @ARGV[0];
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
 if(!$configFile)
 {
	print "Please specify a config file\n";
	exit;
 }

	our $mobUtil = new Mobiusutil();
	our $dbHandler;
	my $conf = $mobUtil->readConfFile($configFile);
	our $jobid=DateTime->now;
	our $log;
	our %conf;
	our $baseTemp;
	our $libraryname;
	
  
 if($conf)
 {
	%conf = %{$conf};	
	
	if ($conf{"logfile"})
	{
		my $dt = DateTime->now(time_zone => "local"); 
		my $fdate = $dt->ymd; 
		my $ftime = $dt->hms;
		my $dateString = "$fdate $ftime";
		$log = new Loghandler($conf->{"logfile"});
		$log->truncFile("");
		$log->addLogLine(" ---------------- Script Starting ---------------- ");
		print "Executing job tail the log for information (".$conf{"logfile"}.")\n";		
		my @reqs = ("logfile","tempdir","libraryname","ftplogin","ftppass","ftphost","remote_directory","displayname","emailsubjectline"); 
		my $valid = 1;
		my $errorMessage="";
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
			my %dbconf = %{getDBconnects($xmlconf)};
			$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
			
			$baseTemp = $conf{"tempdir"};
			$baseTemp =~ s/\/$//;
			$baseTemp.='/';
			$libraryname = $mobUtil->trim($conf{"libraryname"});
			my $subject = $mobUtil->trim($conf{"emailsubjectline"});
			
			my $displayname = $mobUtil->trim($conf{"displayname"});
			my @isbns = @{getList($libraryname)};
			my $file = $mobUtil->chooseNewFileName($baseTemp,$displayname."_ISBN_extract_".$fdate."_","csv");
			my $output='';
			my $count = 0;
			foreach(@isbns)
			{
				my $row = $_;
				my @row = @{$row};
				$output.=@row[0]."\n";
				$count++;
			}
			my $outputFile = new Loghandler($file);
			$outputFile->truncFile($output);
			my @files = ($file);
			#my @files = ("/mnt/evergreen/tmp/2015-04-16_.csv");
			
			$mobUtil->sendftp($conf{"ftphost"},$conf{"ftplogin"},$conf{"ftppass"},$conf{"remote_directory"}, \@files, $log);
			
			my @tolist = ($conf{"alwaysemail"});
			my $email = new email($conf{"fromemail"},\@tolist,$valid,1,\%conf);
			my $afterProcess = DateTime->now(time_zone => "local");
			my $difference = $afterProcess - $dt;
			my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
			my $duration =  $format->format_duration($difference);
			my @s = split(/\//,$file);
			my $displayFilename = @s[$#s];
			$email->send("$subject","Duration: $duration\r\nTotal Extracted: $count\r\nFilename: $displayFilename\r\nFTP Directory: ".$conf{"remote_directory"}."\r\nThis is a full replacement\r\n-Evergreen Perl Squad-");
			foreach(@files)
			{
				unlink $_ or warn "Could not remove $_\n";
			}
		}
		$log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile'\n";		
	}
}

sub getList
{
	my $libname = lc(@_[0]);
	my $query = "
	select distinct isbn from
(
select record,regexp_replace(value,\$\$\\D\$\$,\$\$\$\$,\$\$g\$\$) \"isbn\",value from metabib.real_full_rec where 
(
record in
(
select record from asset.call_number where 
owning_lib in(select id from actor.org_unit where lower(name)~\$\$$libname\$\$) and 
not deleted and 
id in(select call_number from asset.copy where not deleted and circ_lib in(select id from actor.org_unit where lower(name)~\$\$$libname\$\$) )
)
or
record in
(
select record from asset.call_number where 
owning_lib in(select id from actor.org_unit where lower(name)~\$\$$libname\$\$) and 
not deleted and 
label=\$\$##URI##\$\$
)
)
and
tag=\$\$020\$\$
and
record not in(select id from biblio.record_entry where deleted)
) as a
where length(isbn) in(10,13)
order by 1";
	$log->addLine($query);
	my @results = @{$dbHandler->query($query)};
	
	return \@results;
	
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

 
 