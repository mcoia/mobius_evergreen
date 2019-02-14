#!/usr/bin/perl


use Loghandler;
use Mobiusutil;
use Data::Dumper;
use XML::Simple;
use XML::TreeBuilder;
use Getopt::Long;
use DBhandler;


our $schema;
our $mobUtil = new Mobiusutil();
our $log;
our $dbHandler;
our $sierradbHandler;
our $sierrahost;
our $sierraport;
our $sierralogin;
our $sierrapass;
our $sierralocationcodes;
our $loginvestigationoutput;
our $sample;
our @columns;
our @allRows;

my $xmlconf = "/openils/conf/opensrf.xml";
GetOptions (
"logfile=s" => \$logFile,
"xmlconfig=s" => \$xmlconf,
"schema=s" => \$schema,
"sierrahost=s" => \$sierrahost,
"sierraport=s" => \$sierraport,
"sierralogin=s" => \$sierralogin,
"sierrapass=s" => \$sierrapass,
"sierralocationcodes=s" => \$sierralocationcodes,
"sample=s" => \$sample
)
or die("Error in command line arguments\nYou can specify
--logfile configfilename (required)
--xmlconfig pathtoevergreenopensrf.xml (default /opensrf/conf/opensrf.xml)
--sierrahost IP/domain
--sierraport DB Port
--sierralogin DB user
--sierrapass DB password
--sierralocationcodes sierra location codes regex accepted (comma separated)
--schema (eg. m_slmpl)
--sample (number of rows to fetch eg --sample 100)
\n");

if(! -e $xmlconf)
{
	print "I could not find the xml config file: $xmlconf\nYou can specify the path when executing this script --xmlconfig configfilelocation\n";
	exit 0;
}
if(!$logFile)
{
	print "Please specify a log file\n";
	exit;
}
if(!$schema)
{
	print "Please specify an Evergreen DB schema to dump the data to\n";
	exit;
}

	$log = new Loghandler($logFile);
	$log->truncFile("");
	$log->addLogLine(" ---------------- Script Starting ---------------- ");		

	my %dbconf = %{getDBconnects($xmlconf)};
	$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
	$sierradbHandler = new DBhandler("iii",$sierrahost,$sierralogin,$sierrapass,$sierraport);
	
	
	my @sp = split(',',$sierralocationcodes);
	$sierralocationcodes='';
	$sierralocationcodes.="LOCATION_CODE~\$\$$_\$\$ OR " for @sp;
	$sierralocationcodes=substr($sierralocationcodes,0,-4);
	
	my $patronlocationcodes = $sierralocationcodes;
	$patronlocationcodes =~ s/LOCATION_CODE/home_library_code/g;
  
  
  #get itype meanings
	my $query = "
		select * from sierra_view.itype_property_myuser";
	setupEGTable($query,"itype_property_myuser");
	
    
    
  #get location/branches
	my $query = "
		select * from 
(
select svl.code as location_code,svl.is_public,svl.is_requestable,svln.name as location_name,svb.address,svb.code_num,svbm.name as branch_name from 
sierra_view.location svl,
sierra_view.location_name svln,
sierra_view.branch svb,
sierra_view.branch_myuser svbm
where
svbm.code=svb.code_num and
svb.code_num=svl.branch_code_num and
svln.location_id=svl.id
)
as b
where
($sierralocationcodes)
	";
	setupEGTable($query,"location_branch_info");
	
  
	#get patrons
	my $query = "
		select * from sierra_view.patron_view where ($patronlocationcodes) 
	";
	setupEGTable($query,"patron_view");
	
  
  
	#get patron addresses	
	my $query = "
		select * from sierra_view.patron_record_address where 
		patron_record_id in
		(
		select id from sierra_view.patron_view where ($patronlocationcodes)
		) 
	";
	setupEGTable($query,"patron_record_address");
	
	#get patron names	
	my $query = "
		select * from sierra_view.patron_record_fullname where 
		patron_record_id in
		(
		select id from sierra_view.patron_view where ($patronlocationcodes)
		) 
	";
	setupEGTable($query,"patron_record_fullname");
	
	#get patron phone numbers	
	my $query = "
		select * from sierra_view.patron_record_phone where 
		patron_record_id in
		(
		select id from sierra_view.patron_view where ($patronlocationcodes)
		) 
	";
	setupEGTable($query,"patron_record_phone");
	
	#get patron checkouts
	my $query = "
		select * from sierra_view.checkout where 
		patron_record_id in
		(
		select id from sierra_view.patron_view where ($patronlocationcodes)
		) 
	";
	setupEGTable($query,"checkout");
	
	# get patron fines
	my $query = "
		select * from sierra_view.fine where 
		patron_record_id in
		(
		select id from sierra_view.patron_view where ($patronlocationcodes)
		) 
	";
	setupEGTable($query,"fine");
	
	#get patron fines paid
	my $query = "
		select * from sierra_view.fines_paid where 
		patron_record_metadata_id in
		(
		select id from sierra_view.patron_view where ($patronlocationcodes)
		) 
	";
	setupEGTable($query,"fines_paid");
	
	#get bibs
	my $query = "select * from sierra_view.bib_view where id in
	(
		SELECT BIB_RECORD_ID FROM SIERRA_VIEW.BIB_RECORD_LOCATION WHERE 
		($sierralocationcodes)
	)
	";
	setupEGTable($query,"bib_view");
	
	#get items
	my $query = "select * from sierra_view.item_view where id in
	(
		select item_record_id from sierra_view.bib_record_item_record_link where bib_record_id
		in
		(
			select id from sierra_view.bib_view where id in
			(
				SELECT BIB_RECORD_ID FROM SIERRA_VIEW.BIB_RECORD_LOCATION WHERE 
				($sierralocationcodes)
			)
		)
	) 
	";
	setupEGTable($query,"item_view");
	
	#get items bib links
	my $query = "
		select * from sierra_view.bib_record_item_record_link where bib_record_id
		in
		(
			select id from sierra_view.bib_view where id in
			(
				SELECT BIB_RECORD_ID FROM SIERRA_VIEW.BIB_RECORD_LOCATION WHERE 
				($sierralocationcodes)
			)
		)
	";
	setupEGTable($query,"bib_record_item_record_link");
	
	#get patron types
	my $query = "
		select * from sierra_view.ptype_property_myuser
	";
	setupEGTable($query,"ptype_property_myuser");
	
	#get patron types
	my $query = "
		select * from sierra_view.user_defined_pcode1_myuser
	";
	setupEGTable($query,"user_defined_pcode1_myuser");
	
	#get patron types
	my $query = "
		select * from sierra_view.user_defined_pcode2_myuser
	";
	setupEGTable($query,"user_defined_pcode2_myuser");
	
	#get patron types
	my $query = "
		select * from sierra_view.user_defined_pcode3_myuser
	";
	setupEGTable($query,"user_defined_pcode3_myuser");
	
	#get patron messages
	my $query = "
		select * from sierra_view.varfield_view where record_type_code='p' and
		record_id in
		(
			select id from sierra_view.patron_view where ($patronlocationcodes)
		)
	";
	setupEGTable($query,"patron_varfield_view");
	
	#get item extra
	my $query = "
		select * from sierra_view.varfield_view where record_type_code='i' and varfield_type_code='y' and
		record_id in
		(
			select item_record_id from sierra_view.bib_record_item_record_link where bib_record_id
			in
			(
				select id from sierra_view.bib_view where id in
				(
					SELECT BIB_RECORD_ID FROM SIERRA_VIEW.BIB_RECORD_LOCATION WHERE 
					($sierralocationcodes)
				)
			)
		)
	";
	setupEGTable($query,"item_varfield_view");
  
	#get holds
	my $query = "
        select * from 
        sierra_view.hold
        where patron_record_id in
        (
            select id from sierra_view.patron_view where ($patronlocationcodes)
        )
	";
	setupEGTable($query,"patron_holds");
	
    #get holds metadata
	my $query = "
        select * from 
        sierra_view.record_metadata
        where id in
        (
            select record_id from 
                sierra_view.hold
                where patron_record_id in
                (
                    select id from sierra_view.patron_view where ($patronlocationcodes)
                )
        )
	";
	setupEGTable($query,"record_metadata");	
    
    #get Item Status
	my $query = "
        select * from 
        sierra_view.item_status_property_myuser
	";
	setupEGTable($query,"item_status_property_myuser");	
    
    #get Item Status
	my $query = "
        select id,item_status_code from 
        sierra_view.item_record
        where
        id in
        (
            select item_record_id from sierra_view.bib_record_item_record_link where bib_record_id
            in
            (
                select id from sierra_view.bib_view where id in
                (
                    SELECT BIB_RECORD_ID FROM SIERRA_VIEW.BIB_RECORD_LOCATION WHERE 
                    ($sierralocationcodes)
                )
            )
        )
        and
        item_status_code !='-'
	";
	setupEGTable($query,"non_available_item");	
	
	
	$log->addLogLine(" ---------------- Script End ---------------- ");
	
	
sub setupEGTable
{
	my $query = @_[0];
	my $tablename = @_[1];
    
    my $insertChunkSize = 500;
	
	print "Gathering $tablename....";
	
    my @ret = @{getRemoteSierraData($query)};
    
	my @allRows = @{@ret[0]};
	my @cols = @{@ret[1]};
	print $#allRows." rows\n";
	
	
	#drop the table
	my $query = "DROP TABLE IF EXISTS $schema.$tablename";
	$log->addLine($query);
	$dbHandler->update($query);
	
	#create the table
	$query = "CREATE TABLE $schema.$tablename (";
	$query.=$_." TEXT," for @cols;
	$query=substr($query,0,-1).")";
	$log->addLine($query);
	$dbHandler->update($query);
    my @vals = ();
    my $valpos = 1;
    my $totalInserted = 0;
	
	if($#allRows > -1)
	{
		#insert the data
        my $rowcount = 0;
		$query = "INSERT INTO $schema.$tablename (";
		$query.=$_."," for @cols;
		$query=substr($query,0,-1).")\nVALUES\n";
        my $queryTemplate = $query;
		foreach(@allRows)
		{
			$query.="(";
			my @thisrow = @{$_};
			$query.= "\$" . $valpos++ . "," for(@thisrow);
            push @vals, @thisrow;
            # for(@thisrow)
			# {
				# my $value = $_;
				# #add period on trialing $ signs
				# #print "$value -> ";
				# $value =~ s/\$$/\$\./;
                # #add period on head $ signs
                # $value =~ s/^\$/\.\$/;
				# #print "$value\n";
				# $query.='$data$'.$value.'$data$,'
			# }
			$query=substr($query,0,-1)."),\n";
            $rowcount++;
            if($rowcount % $insertChunkSize == 0)
            {
                $totalInserted+=$insertChunkSize;
                $query=substr($query,0,-2)."\n";
                $loginvestigationoutput.="select count(*),$_ from $schema.$tablename group by $_ order by $_\n" for @cols;
                print "Inserted ".$totalInserted." Rows into $schema.$tablename\n";
                $log->addLine($query);
                $dbHandler->updateWithParameters($query, \@vals);
                $query = $queryTemplate;
                $rowcount=0;
                @vals = ();
                $valpos = 1;
            }
		}
        
        if($valpos > 1)
        {
            $query=substr($query,0,-2)."\n";
            $loginvestigationoutput.="select count(*),$_ from $schema.$tablename group by $_ order by $_\n" for @cols;
            print "Inserted ".$#allRows." Rows into $schema.$tablename\n";
            $log->addLine($query);
            $dbHandler->updateWithParameters($query, \@vals);
        }

	}
	else
	{
		print "Empty dataset for $tablename \n";
		$log->addLine("Empty dataset for $tablename");
	}
}

sub getRemoteSierraData
{
    my $queryTemplate = @_[0];
    my $offset = 0;
    my @ret = ();
    my $limit = 10000;
    $limit = $sample if $sample;
    $queryTemplate.="\nORDER BY 1\n LIMIT $limit OFFSET !OFFSET!";
    my $loops = 0;
    my @cols;
    my $data = 1;
    my @allRows = ();
    
    while($data)
    {
        my $query = $queryTemplate;
        $query =~ s/!OFFSET!/$offset/g;
        $log->addLine($query);
        my @theseRows = @{$sierradbHandler->query($query)};
        @cols = @{$sierradbHandler->getColumnNames()} if !(@cols);
        $data = 0 if($#theseRows < 0 );
        push @allRows, @theseRows if ($#theseRows > -1 );
        $loops++;
        $offset = ($loops * $limit) + 1;
        $data = 0 if $sample;
        undef @theseRows;
    }


    push @ret, [@allRows];
    push @ret, [@cols];
    return \@ret;
}

sub calcCheckDigit
{
	my $seed =@_[1];
	$seed = reverse($seed);
	my @chars = split("", $seed);
	my $checkDigit = 0;
	for my $i (0.. $#chars)
	{
		$checkDigit += @chars[$i] * ($i+2);
	}
	$checkDigit =$checkDigit%11;
	if($checkDigit>9)
	{
		$checkDigit='x';
	}
	return $checkDigit;
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