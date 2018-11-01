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
use email;


    my $xmlconf = "/openils/conf/opensrf.xml";
    our $log;
    our $dbHandler;
    our @inputFiles;
    our $primarykey;
    our $schema;
    our $inputDirectory;
    my $logFile;
    my $tablePrefix;
    my $inputFileFriendly;
    
    
    GetOptions (
    "logfile=s" => \$logFile,
    "xmlconfig=s" => \$xmlconf,
    "schema=s" => \$schema,
    "primarykey" => \$primarykey,
    "tableprefix=s" => \$tablePrefix,
    "directory=s" => \$inputDirectory
    )
    or die("Error in command line arguments\nYou can specify
    --logfile configfilename (required)
    --xmlconfig  pathto_opensrf.xml
    --schema (eg. m_slmpl)
    --primarykey (create id column)
    --directory where to find the input files
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
    $tablePrefix = "polaris" if ( (!$tablePrefix) || ($tablePrefix eq "" ) );
    @inputFiles = @{getFiles($inputDirectory)};
    
        
    $log = new Loghandler($logFile);
    # $log->truncFile("");
    $log->truncFile(" ---------------- Script Starting ---------------- ");		

    
	my $dt = DateTime->now(time_zone => "local"); 
	my $fdate = $dt->ymd; 
	my $ftime = $dt->hms;
	my $dateString = "$fdate $ftime";
    
    my %dbconf = %{getDBconnects($xmlconf)};
    $dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"}); #$dbconf{"dbhost"}
    
    foreach(@inputFiles)
    {
        my $file = $_;
        print "Processing $file\n";
        my $path;
        my @sp = split('/',$file);
        $path=substr($file,0,( (length(@sp[$#sp]))*-1) );
        my $bareFilename =  pop @sp;
        @sp = split(/\./,$bareFilename);
        $bareFilename =  shift @sp;
        $bareFilename =~ s/^\s+//;
        $bareFilename =~ s/^\t+//;
        $bareFilename =~ s/\s+$//;
        $bareFilename =~ s/\t+$//;
        $bareFilename =~ s/^_+//;
        $bareFilename =~ s/_+$//;
        
        my $tableName = $tablePrefix."_".$bareFilename;
        
        $inputFileFriendly .= "\r\n" . $bareFilename;


        checkFileReady($file);
        
        my @colPositions = (); # two dimension array with number pairs [position, length]
        my $lineCount = -1;
        my @columnNames;
        my $baseInsertHeader = "INSERT INTO $schema.$tableName (";
        my $queryByHand = '';
        my $queryInserts = '';
        my @queryValues = ();
        my $success = 0;
        my $accumulatedTotal = 0;
        my $parameterCount = 0;
        
        open(my $fh,"<:encoding(UTF-16)",$file) || die "error $!\n";
        while(<$fh>) 
        {
            $lineCount++;
            my $line = $_;
            
            if($lineCount == 0) #first line contains the column header names
            {
                # For now, we will just push the whole line because we need the second line to tell us the divisions.
                push(@columnNames, $line);
                next;
            }
            if($lineCount == 1) #second line contains the column division clues
            {
                my @chars = split('', $line);
                @colPositions = @{figureColumnPositions(\@chars)};
                @columnNames = @{getDataFromLine(\@colPositions, @columnNames[0])};
                my $query = "DROP TABLE IF EXISTS $schema.$tableName";
                $dbHandler->update($query);
                $query = "CREATE TABLE $schema.$tableName (";
                $query.="id bigserial," if ($primarykey);
                $query .= "$_ TEXT," foreach(@columnNames);
                $baseInsertHeader .= "$_," foreach(@columnNames);
                $query = substr($query,0,-1);
                $baseInsertHeader = substr($baseInsertHeader,0,-1);
                $query .= ")";
                $baseInsertHeader .= ")\nVALUES\n";
                $queryByHand = $baseInsertHeader;
                $queryInserts = $baseInsertHeader;
                $log->addLine($query);
                $dbHandler->update($query);
                next;
            }
            
            
            my @lineLength = split('', $line);
            # print $#lineLength."\n";
            my @lastCol = @{@colPositions[$#colPositions]};
            my $m = @lastCol[0] + @lastCol[1];
            # print "Needs to be:\n$m";
            next if ($#lineLength < (@lastCol[0] + @lastCol[1])); # Line is not long enough to get all columns
            my @data = @{getDataFromLine(\@colPositions, $line)};
            $queryInserts.="(" if ($#data > -1);
            $queryByHand.="(" if ($#data > -1);
            foreach(@data)
            {
                $parameterCount++ if (lc($_) ne 'null');
                push(@queryValues, $_) if (lc($_) ne 'null');
                $queryInserts .= "null, "  if (lc($_) eq 'null');
                $queryInserts .= "\$$parameterCount, "  if (lc($_) ne 'null');
                $queryByHand .= "null, " if (lc($_) eq 'null');
                $queryByHand .= "\$data\$$_\$data\$, " if (lc($_) ne 'null');
            }
            $queryInserts = substr($queryInserts,0,-2) if ($#data > -1);
            $queryByHand = substr($queryByHand,0,-2) if ($#data > -1);
            $queryInserts.="),\n" if ($#data > -1);
            $queryByHand.="),\n" if ($#data > -1);
            $success++ if ($#data > -1);
        
            if( ($success % 500 == 0) && ($success != 0) )
            {
                $accumulatedTotal+=$success;
                $queryInserts = substr($queryInserts,0,-2);
                $queryByHand = substr($queryByHand,0,-2);
                $log->addLine($queryByHand);
                # print ("Importing $success\n");
                $log->addLine("Importing $accumulatedTotal / $lineCount");
                $dbHandler->updateWithParameters($queryInserts,\@queryValues);
                $success = 0;
                @queryValues = ();
                $queryByHand = $baseInsertHeader;
                $queryInserts = $baseInsertHeader;
                $parameterCount = 0;
            }

        }
        close($fh);
        
        $queryInserts = substr($queryInserts,0,-2) if $success;
        $queryByHand = substr($queryByHand,0,-2) if $success;
        
        # Handle the case when there is only one row inserted
        if($success == 1)
        {
            $queryInserts =~ s/VALUES \(/VALUES /;            
            $queryInserts = substr($queryInserts,0,-1);
        }

        # $log->addLine($queryInserts);
        $log->addLine($queryByHand);
        # $log->addLine(Dumper(\@queryValues));
        
        $accumulatedTotal+=$success;
        $log->addLine("Importing $accumulatedTotal / $lineCount") if $success;
        
        $dbHandler->updateWithParameters($queryInserts,\@queryValues) if $success;
        
        # # delete the file so we don't read it again
        # Disabled because we are going to let bash do this 
        # so that we don't halt execution of this script in case of errors
        # unlink $file;
    }
    
    
    $log->addLogLine(" ---------------- Script Ending ---------------- ");

    
sub figureColumnPositions
{
    my @chars = @{@_[0]};
    my @colPositions = ();
    my $pos = 0;
    my $length = 0;
    my $currentPos = 0;
    # $log->addLine("chars length = ". $#chars);
    foreach(@chars)
    {
        last if($currentPos == $#chars);
        if($_ =~ m/\s/) # found a break, this is a column divider
        {
            push (@colPositions, [$pos, $length]);
            $currentPos++;
            $pos = $currentPos;
            $length = 0;
            next;
        }
        $length++;
        $currentPos++;
    }
    # $log->addLine(Dumper(\@colPositions));
    return \@colPositions;
}

sub getDataFromLine
{
    my @colPositions = @{@_[0]};
    my $line = @_[1];
    my @ret = ();
    # $log->addLine("Line = '$line'");
    foreach(@colPositions)
    {
        my @pair = @{$_};
        my $starting = @pair[0];
        my $length = @pair[1];
        my $data = substr($line,$starting,$length);
        # $log->addLine("data = '$data'");
        # Trim whitespace off the data
        $data =~ s/^\s+//;
        $data =~ s/^\t+//;
        $data =~ s/\s+$//;
        $data =~ s/\t+$//;
        # $log->addLine("Trimmed data = '$data'");
        push (@ret, $data);
    }
    return \@ret;
}


sub checkFileReady
{
    my $file = shift;
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

sub getFiles
{
	my $path = shift;
    my @ret = ();
	opendir(DIR, $path) or die $!;
	while (my $file = readdir(DIR)) 
	{
		if ( (-e "$path/$file") && !($file =~ m/^\./) )
		{
            # print "pushing $path/$file\n";
			push @ret, "$path/$file"
		}
	}
    
	return \@ret;
}

sub setupSchema
{
    my $columns = shift;
    my %columns = %{$columns};
	my $query = "select * from INFORMATION_SCHEMA.COLUMNS where table_name = 'patron_import' and table_schema='$schema'";
	my @results = @{$dbHandler->query($query)};
	if($#results==-1)
	{
        $query = "select schema_name from information_schema.schemata where schema_name='mymig'";
        my @results = @{$dbHandler->query($query)};
        if($#results <  0)
        {
            $query = "CREATE SCHEMA $schema";
            $dbHandler->update($query);
        }
		
		$query = "CREATE TABLE $schema.patron_import
		(
		id bigserial NOT NULL,
        ";
        while ( (my $key, my $value) = each(%columns) ) { $query.= $value." text,"; }
        $query.="
        home_ou bigint,
        usr_id bigint,
        imported boolean default false,
        error_message text DEFAULT \$\$\$\$,        
        dealt_with boolean default false,
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


exit;
