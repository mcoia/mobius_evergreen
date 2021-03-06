#!/usr/bin/perl
use lib qw(../);
use strict; 
use Loghandler;
use Mobiusutil;
use DBhandler;
use Data::Dumper;
use email;
use DateTime;
use utf8;
use Encode;
use XML::Simple;
use DateTime::Format::Duration;


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
	our $fullDB;
	
  
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
            $log->addLogLine("Valid Config");
			my %dbconf = %{getDBconnects($xmlconf)};
            $log->addLogLine("got XML db connections");
			$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
            
            $log->addLogLine("Getting Max ID");
			my $maxID = getMaxID();            
            $log->addLogLine("Max ID = $maxID");
            
			$baseTemp = $conf{"tempdir"};
			$baseTemp =~ s/\/$//;
			$baseTemp.='/';
			$libraryname = $mobUtil->trim($conf{"libraryname"});
			$fullDB = $conf{'fullDB'} if($conf{'fullDB'});
			my $subject = $mobUtil->trim($conf{"emailsubjectline"});
			
			my $displayname = $mobUtil->trim($conf{"displayname"});
            
            my $limit = $conf{'chunksize'} || 10000;
            my $offset = 1;
			my @isbns = ('data');
            my $file = $mobUtil->chooseNewFileName($baseTemp,$displayname."_ISBN_extract_".$fdate."_","csv");
            
            my @header=("isbn","itemid","bibrecordcallno","bibrecordid","title");
            my @rows = ([@header]);
            writeData($file,\@rows,1);
            my $count = 0;
            while( $offset < $maxID )
            {
                @isbns = @{getList($libraryname,$limit,$offset)};
                $count+=(scalar @isbns);
                writeData($file,\@isbns);
                $offset+=$limit;
            }
			$log->addLogLine("Received $count rows from database - writing to $file");
            $log->addLogLine("finished $file");
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

sub writeData
{
    my $file = @_[0];
    my @isbns = @{@_[1]};
    my $overwriteFile = @_[2] || 0;
    my $outputFile = new Loghandler($file);
    my $output = '';
    foreach(@isbns)
    {
        my $row = $_;
        my @row = @{$row};
        $output.=join("\t",@row);
        $output.="\n";        
    }
    $output = substr($output,0,-1) if $overwriteFile;
    $outputFile->appendLine($output) if !$overwriteFile;
    $outputFile->truncFile($output) if $overwriteFile;
    undef $output;
    undef $outputFile;
    my $lines = scalar @isbns;
    $log->addLogLine("wrote $lines lines to $file");
}


sub getList
{
	my $libname = lc(@_[0]);
    my $limit = @_[1];
    my $offset = @_[2];
    
	my $query = "
select isbn,
ac.barcode,
concat(
    (case when acn.prefix > -1 then (select label||\$\$ \$\$ from asset.call_number_prefix where id=acn.prefix) else \$\$\$\$ end),
    acn.label,
    (case when acn.suffix > -1 then (select label||\$\$ \$\$ from asset.call_number_prefix where id=acn.suffix) else \$\$\$\$ end)
),
a.record,
string_agg(mtfe.value,\$\$ / \$\$ order by mtfe.value)
from
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
record 
in(select id from biblio.record_entry where not deleted order by id limit $limit offset $offset)
) as a
left join metabib.title_field_entry mtfe on a.record=mtfe.source
left join asset.call_number acn on(mtfe.source=acn.record and not acn.deleted)
left join asset.copy ac on (ac.call_number=acn.id and not ac.deleted)
where
length(a.isbn) in(10,13)
group by 1,2,3,4
order by 1";

	if($fullDB)
	{
		$query = "
select isbn,
ac.barcode,
concat(
    (case when acn.prefix > -1 then (select label||\$\$ \$\$ from asset.call_number_prefix where id=acn.prefix) else \$\$\$\$ end),
    acn.label,
    (case when acn.suffix > -1 then (select label||\$\$ \$\$ from asset.call_number_prefix where id=acn.suffix) else \$\$\$\$ end)
),
a.record,
string_agg(mtfe.value,\$\$ / \$\$ order by mtfe.value)
from
(
select record,regexp_replace(value,\$\$\\D\$\$,\$\$\$\$,\$\$g\$\$) \"isbn\",value from metabib.real_full_rec where 
(
record in
(
select id from biblio.record_entry where not deleted order by id limit $limit offset $offset
)
and
tag=\$\$020\$\$
)
) as a
left join metabib.title_field_entry mtfe on a.record=mtfe.source
left join asset.call_number acn on(mtfe.source=acn.record and not acn.deleted)
left join asset.copy ac on (ac.call_number=acn.id and not ac.deleted)
where
length(a.isbn) in(10,13)
group by 1,2,3,4
order by 1";
	}
	$log->addLine($query);
	my @results = @{$dbHandler->query($query)};
	
	return \@results;
	
}

sub getMaxID
{
    my @results = @{$dbHandler->query("select max(id) from biblio.record_entry")};
    my @row = @{@results[0]};
    return @row[0];
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

 
 