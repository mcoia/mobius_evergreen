#!/usr/bin/perl


use lib qw(../); 
use Loghandler;
use Data::Dumper;
use File::Path qw(make_path remove_tree);
use File::Copy;
use DBhandler;
use Encode;
use Text::CSV;
use DateTime;
use DateTime::Format::Duration;
use DateTime::Span;
use Getopt::Long;
use XML::Simple;



    my $xmlconf = "/openils/conf/opensrf.xml";
    our $log;
    our $dbHandler;
    our $inputFiles;
    our $primarykey;
    our $schema;
    my $logFile;
    
    GetOptions (
    "logfile=s" => \$logFile,
    "xmlconfig=s" => \$xmlconf,
    "schema=s" => \$schema,
    "primarykey" => \$primarykey,
    "files=s@" => \$inputFiles
    )
    or die("Error in command line arguments\nYou can specify
    --logfile configfilename (required)
    --xmlconfig  pathto_opensrf.xmlml
    --schema (eg. m_slmpl)
    --primarykey (create id column)
    --files list of space separated files
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
        print "Please specify a DB schema\n";
        exit;
    }
    $log = new Loghandler($logFile);
    $log->truncFile("");
    $log->addLogLine(" ---------------- Script Starting ---------------- ");		

    
	my $dt = DateTime->now(time_zone => "local"); 
	my $fdate = $dt->ymd; 
	my $ftime = $dt->hms;
	my $dateString = "$fdate $ftime";
    
    my %dbconf = %{getDBconnects($xmlconf)};
	$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
    
    #Student Number,Last Name,First Name,Middle Name,Date of Birth ,Street,City,State,Zip,Guardian,Email Address,Phone Number,Gender,School ,Home Library,County
    my %colmap = (
    0 => 'studentID',
    1 => 'lastName',
    2 => 'firstName',
    3 => 'middleName',
    4 => 'dob',
    5 => 'street1',
    6 => 'city',
    7 => 'state',
    8 => 'zip',
    9 => 'guardian',
    10 => 'email',
    11 => 'phone',
    12 => 'gender',
    13 => 'school',
    14 => 'library',
    15 => 'county'
    );
    
    setupSchema(\%colmap);
  
    foreach(@{$inputFiles})
    {
        my $file = $_;
        my $path;
        my @sp = split('/',$file);
       
        $path=substr($file,0,( (length(@sp[$#sp]))*-1) );
                
        checkFileReady($file);
        my $csv = Text::CSV->new ( )
            or die "Cannot use CSV: ".Text::CSV->error_diag ();
        open my $fh, "<:encoding(utf8)", $file or die "$file: $!";
        my $rownum = 0;
        my $success = 0;
        my $queryByHand = '';
        my $parameterCount = 1;
        
        my $queryInserts = "INSERT INTO $schema.patron_import(";
        $queryByHand = "INSERT INTO $schema.patron_import(";
        my @order = ();
        my $sanitycheckcolumnnums = 0;
        my @queryValues = ();
        while ( (my $key, my $value) = each(%colmap) )
        {
            $queryInserts .= $value.",";
            $queryByHand .= $value.",";
            push @order, $key;
            $sanitycheckcolumnnums++
        }
        $log->addLine("Expected columns: $sanitycheckcolumnnums");
        $queryInserts = substr($queryInserts,0,-1);
        $queryByHand = substr($queryByHand,0,-1);
        
        $queryInserts .= ")\nVALUES \n";
        $queryByHand  .= ")\nVALUES \n";
        
        while ( my $row = $csv->getline( $fh ) )
        {
           
            my @rowarray = @{$row};
            if(scalar @rowarray != $sanitycheckcolumnnums )
            {
                $log->addLine("Error parsing line $rownum\nIncorrect number of columns: ". scalar @rowarray);
            }
            else
            {
             
                my $valid = 1;
                
                my $thisLineInsert = '';
                my $thisLineInsertByHand = '';
                my @thisLineVals = ();
                
                foreach(@order)
                {
                    my $colpos = $_;
                    # print "reading $colpos\n";
                    $thisLineInsert .= '$'.$parameterCount.',';
                    $parameterCount++;
                    # Trim whitespace off the data
                    @rowarray[$colpos] =~ s/^[\s\t]*(.*)/$1/;
                    @rowarray[$colpos] =~ s/(.*)[\s\t]*$/$1/;
                                        
                    $thisLineInsertByHand.="\$data\$".@rowarray[$colpos]."\$data\$,";
                    push (@thisLineVals, @rowarray[$colpos]);
                    # $log->addLine(Dumper(\@thisLineVals));
                }
                
                if($valid)
                {
                    $thisLineInsert = substr($thisLineInsert,0,-1);
                    $thisLineInsertByHand = substr($thisLineInsertByHand,0,-1);
                    $queryInserts .= '(' . $thisLineInsert . "),\n";
                    $queryByHand .= '(' . $thisLineInsertByHand . "),\n";
                    foreach(@thisLineVals)
                    {
                        # print "pushing $_\n";
                        push (@queryValues, $_);
                    }
                    $success++;
                }
                undef @thisLineVals;
            }
            $rownum++;
        }
        
        $queryInserts = substr($queryInserts,0,-2) if $success;
        $queryByHand = substr($queryByHand,0,-2) if $success;
        
        # Handle the case when there is only one row inserted
        if($success == 1)
        {
            $queryInserts =~ s/VALUES \(/VALUES /;            
            $queryInserts = substr($queryInserts,0,-1);
        }

        $log->addLine($queryInserts);
        $log->addLine($queryByHand);
        $log->addLine(Dumper(\@queryValues));
        
        close $fh;
        $log->addLine("Importing $success / $rownum");
        
        $dbHandler->updateWithParameters($queryInserts,\@queryValues) if $success;
        
        # # Clean out duplicate barcodes
        # my $queryClean = "DELETE from dtdata where
        # number in(
        # select thenumber from 
        # (
            # select min(number) as thenumber,item from dtdata
            # WHERE
            # item IN
            # (
            # select item FROM
            # (
            # select item,count(*) from dtdata group by 1 having count(*)>1
            # ) as dups
            # )
            # group by 2
        # ) as dupclear
        # )
        # ";
        # $dbHandler->update($queryClean);
        # # delete the file so we don't read it again
        # unlink $file;
    }
    
    ## Now scrub data and compare data to production and make logic decisions. 
    
    # First, remove blank student ID's/
    my $query = "delete from $schema.patron_import where btrim(studentID)=\$\$\$\$";
    $log->addLine($query);
    $dbHandler->update($query);
    
    # Next 

sub checkFileReady
{
    my $file = @_[0];
    my $worked = open (inputfile, '< '. $file);
    my $trys=0;
    if(!$worked)
    {
        print "******************$file not ready *************\n";
    }
    while (!(open (inputfile, '< '. $file)) && $trys<100)
    {
        print "Trying again attempt $trys\n";
        $trys++;
        sleep(1);
    }
    close(inputfile);
}


sub setupSchema
{
    my $columns = shift;
    my %columns = %{$columns};
	my $query = "select * from INFORMATION_SCHEMA.COLUMNS where table_name = 'patron_import' and table_schema='$schema'";
	my @results = @{$dbHandler->query($query)};
	if($#results==-1)
	{
		$query = "CREATE SCHEMA $schema";
		$dbHandler->update($query);
		$query = "CREATE TABLE $schema.patron_import
		(
		id bigserial NOT NULL,
        ";
         while ( (my $key, my $value) = each(%columns) ) { $query.= $value." text,"; }
        $query.="
        imported boolean default false,
        error_message text,
        insert_date timestamp with time zone NOT NULL DEFAULT now()
        )";
        
		$dbHandler->update($query);
        $log->addLine($query);
	}
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


sub DESTROY
{
    print "I'm dying, deleting PID file $pidFile\n";
    unlink $pidFile;
}

exit;
