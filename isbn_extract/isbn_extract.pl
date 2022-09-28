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
    our $fileOutputBuffer = '';
    our $fileOutputBufferRecordCount = 0;
	
  
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
                $fileOutputBufferRecordCount = 30000 if($offset+$limit > $maxID); # Need to ensure that we write all of this last loop's data to the file
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

    foreach(@isbns)
    {
        my $row = $_;
        my @row = @{$row};
        $fileOutputBuffer .= join("\t",@row);
        $fileOutputBuffer .= "\n";
        $fileOutputBufferRecordCount++;
    }
    if($overwriteFile)
    {
        my $outputFile = new Loghandler($file);
        $fileOutputBuffer = substr($fileOutputBuffer,0,-1);
        $outputFile->truncFile($fileOutputBuffer);
        $fileOutputBuffer = '';
        $fileOutputBufferRecordCount = 0;
        undef $outputFile;
    }
    # write to disk at a reasonable pace, 20000 lines at a time.
    if($fileOutputBufferRecordCount > 20000)
    {
        my $outputFile = new Loghandler($file);
        $outputFile->appendLine($fileOutputBuffer);
        $log->addLogLine("wrote $fileOutputBufferRecordCount lines to $file");
        $fileOutputBuffer = '';
        $fileOutputBufferRecordCount = 0;
        undef $outputFile;
    }
}


sub getList
{
	my $libname = lc(@_[0]);
    my $limit = @_[1];
    my $offset = @_[2];
    my $range = $offset + $limit - 1; # the between sql statement includes the ending number in the return
    
    my @sp = split(/,/,$libname);
    my $libs = join ( '$$,$$', @sp);
    $libs = '$$' . $libs . '$$';

	my $query = "
select isbn,
ac.barcode,
concat(
    (case when acn.prefix > -1 then (select label||\$\$ \$\$ from asset.call_number_prefix where id=acn.prefix) else \$\$\$\$ end),
    acn.label,
    (case when acn.suffix > -1 then (select label||\$\$ \$\$ from asset.call_number_prefix where id=acn.suffix) else \$\$\$\$ end)
),
a.record,
string_agg(distinct mtfe.value,\$\$ / \$\$ order by mtfe.value)
from
(
select record,regexp_replace(value,\$\$\\D\$\$,\$\$\$\$,\$\$g\$\$) \"isbn\",value
from
metabib.real_full_rec mrfr
join biblio.record_entry bre on(bre.id=mrfr.record and not bre.deleted and mrfr.record between $offset and $range)
where
(
record in
(
select record from asset.call_number where
record between $offset and $range and
owning_lib in(select id from actor.org_unit where lower(shortname) in ($libs)) and 
not deleted and 
id in(select call_number from asset.copy where not deleted and circ_lib in(select id from actor.org_unit where lower(shortname) in ($libs)) )
)
or
record in
(
select record from asset.call_number where
record between $offset and $range and
owning_lib in(select id from actor.org_unit where lower(shortname) in ($libs)) and 
not deleted and 
label=\$\$##URI##\$\$
)
)
) as a
join metabib.real_full_rec mrfr on(mrfr.record=a.record and mrfr.tag=\$\$020\$\$)
left join metabib.title_field_entry mtfe on a.record=mtfe.source
left join asset.call_number acn on(mtfe.source=acn.record and not acn.deleted)
left join asset.copy ac on (ac.call_number=acn.id and not ac.deleted and ac.circ_lib in(select id from actor.org_unit where lower(shortname) in ($libs)))
where
length(a.isbn) in(10,13) and
ac.id is not null
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
string_agg(distinct mtfe.value,\$\$ / \$\$ order by mtfe.value)
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
	$log->addLine($query) if( ( int($offset / $limit) % 10) == 0 );  # only logging the query sometimes, it takes too much time;
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

 
 