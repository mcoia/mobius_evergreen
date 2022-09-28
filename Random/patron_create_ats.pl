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
    our $schema;
    our $table;
    our $fromEmail;
    our $toEmail;
    our $inputDirectory;
    our $defaultProfileID = 2;
    our $ensureAlreadyExistingPatronsAreTagged = 0;
    our $patronTagString = "Student ID Project";
    my $logFile;
    my $inputFileFriendly;
    my %fileParsingReport = ();


    GetOptions (
    "logfile=s" => \$logFile,
    "xmlconfig=s" => \$xmlconf,
    "schema=s" => \$schema,
    "table=s" => \$table,
    "fromemail=s" => \$fromEmail,
    "toemail=s" => \$toEmail,
    "defaultprofile:s" => \$defaultProfileID,
    "directory=s" => \$inputDirectory,
    "ensure-tagged" => \$ensureAlreadyExistingPatronsAreTagged,
    "patron-tag:s" => \$patronTagString
    )
    or die("Error in command line arguments\nYou can specify
    --logfile Path to write a log (required)
    --xmlconfig  pathto_opensrf.xml
    --schema (database schema name eg. mymig)
    --table (database table name eg. patron_import)
    --fromemail spoofed from email address
    --toemail comma separated email addresses for the report
    --defaultprofile an ID number coorisponding to permission.grp_tree.id
    --directory where to find the input files
    --ensure-tagged flag to enable error checking when overlaying patrons with the same barcode
    --patron-tag A string to tag the created patrons ident_value with (quote the string when spaces are present)
                 ( Default is: 'Student ID Project' )
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

    @inputFiles = @{getFiles($inputDirectory)};


    $log = new Loghandler($logFile);
    # $log->truncFile("");
    $log->addLogLine(" ---------------- Script Starting ---------------- ");


	my $dt = DateTime->now(time_zone => "local");
	my $fdate = $dt->ymd;
	my $ftime = $dt->hms;
	my $dateString = "$fdate $ftime";

    my %dbconf = %{getDBconnects($xmlconf)};
    $dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"}); #$dbconf{"dbhost"}

    #"studentID",
    #"lastName",
    #"firstName",
    #"middleName",
    #"dob",
    #"street1",
    #"city",
    #"state",
    #"zip",
    #"email",
    #"phone",
    #"library",
    #"county",
    #"profileid",
    #"suffix",
    #"alias",
    #"country",
    #"expirationDate",
    #"au_statcat1","au_statcat2","au_statcat3","au_statcat4","au_statcat5","au_statcat6","au_statcat7","au_statcat8","au_statcat9","au_statcat10","au_statcat11","au_statcat12",
    #"ats_statcat1","ats_statcat2","ats_statcat3","ats_statcat4","ats_statcat5","ats_statcat6","ats_statcat7","ats_statcat8","ats_statcat9","ats_statcat10","ats_statcat11","ats_statcat12"

    my %colmap = (
    0 => 'studentid',
    1 => 'lastname',
    2 => 'firstname',
    3 => 'middlename',
    4 => 'dob',
    5 => 'street1',
    6 => 'city',
    7 => 'state',
    8 => 'zip',
    9 => 'email',
    10 => 'phone',
    11 => 'library',
    12 => 'county',
    13 => 'profileid',
    14 => 'suffix',
    15 => 'alias',
    16 => 'country',
    17 => 'expirationdate'
    );

    our %statcatmap = (
    18 => 'Import Source',
    19 => 'Gender',
    20 => 'School',
    21 => 'Program',
    22 => 'Grade Level',
    23 => 'Department',
    24 => 'Major',
    25 => 'Graduation Year/Month',
    26 => 'au_statcat9',
    27 => 'au_statcat10',
    28 => 'au_statcat11',
    29 => 'au_statcat12',
    30 => 'User Type',
    31 => 'School Status',
    32 => 'Degree Program',
    33 => 'Program Version',
    34 => 'ats_statcat5',
    35 => 'ats_statcat6',
    36 => 'ats_statcat7',
    37 => 'ats_statcat8',
    38 => 'ats_statcat9',
    39 => 'ats_statcat10',
    40 => 'ats_statcat11',
    41 => 'ats_statcat12'
    );

    setupSchema(\%colmap);

    foreach(@inputFiles)
    {
        my $file = $_;
        print "Processing $file\n";
        my $path;
        my @sp = split('/',$file);
        $path=substr($file,0,( (length(@sp[$#sp]))*-1) );
        my $bareFilename =  pop @sp;
        $fileParsingReport{"*** $bareFilename ***"} = "\r\n";
        $inputFileFriendly .= "\r\n" . $bareFilename;
        my $expireDateString = getDateFromFile($bareFilename);

        # last;  # short circut when debugging  and data is already imported


        checkFileReady($file);
        my $csv = Text::CSV->new ( )
            or die "Cannot use CSV: ".Text::CSV->error_diag ();
        open my $fh, "<:encoding(utf8)", $file or die "$file: $!";
        my $rownum = 0;
        my $success = 0;
        my $accumulatedTotal = 0;
        my $queryByHand = '';
        my $parameterCount = 1;

        my $queryInserts = "INSERT INTO $schema.$table(";
        $queryByHand = "INSERT INTO $schema.$table(";
        my @order = ();
        my $sanitycheckcolumnnums = 0;
        my @queryValues = ();
        while ( (my $key, my $value) = each(%colmap) )
        {
            my $colName = createDBFriendlyName($value);
            $queryInserts .= $colName.",";
            $queryByHand .= $colName.",";
            push @order, $key;
            $sanitycheckcolumnnums = $key if $key > $sanitycheckcolumnnums;
        }
        while ( (my $key, my $value) = each(%statcatmap) )
        {
            my $colName = createDBFriendlyName($value);
            $queryInserts .= $colName.",";
            $queryByHand .= $colName.",";
            push @order, $key;
            $sanitycheckcolumnnums = $key if $key > $sanitycheckcolumnnums;
        }
        $sanitycheckcolumnnums++; # 0-based converted to 1-based
        $log->addLine("Expected columns: $sanitycheckcolumnnums");
        $queryInserts = substr($queryInserts,0,-1);
        $queryByHand = substr($queryByHand,0,-1);

        $queryInserts .= ")\nVALUES \n";
        $queryByHand  .= ")\nVALUES \n";

        my $queryInsertsHead = $queryInserts;
        my $queryByHandHead = $queryByHand;

        while ( my $row = $csv->getline( $fh ) )
        {

            my $valid = 0;
            my @rowarray = @{$row};
            if(scalar @rowarray != $sanitycheckcolumnnums )
            {
                $log->addLine("Error parsing line $rownum\nIncorrect number of columns: ". scalar @rowarray);
                $fileParsingReport{"*** $bareFilename ***"} .= "Error parsing line $rownum\nIncorrect number of columns: ". scalar @rowarray . "\r\n";
            }
            else
            {
                $valid = 1;
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
                    @rowarray[$colpos] =~ s/^\s+//;
                    @rowarray[$colpos] =~ s/^\t+//;
                    @rowarray[$colpos] =~ s/\s+$//;
                    @rowarray[$colpos] =~ s/\t+$//;
                    # Some bad characters can mess with some processes later. Excel loves these \xA0
                    @rowarray[$colpos] =~ s/\x{A0}//g;

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

            if( ($success % 500 == 0) && ($success != 0) )
            {
                $accumulatedTotal+=$success;
                $queryInserts = substr($queryInserts,0,-2);
                $queryByHand = substr($queryByHand,0,-2);
                $log->addLine($queryByHand);
                # print ("Importing $success\n");
                $fileParsingReport{"*** $bareFilename ***"} .= "Importing $accumulatedTotal / $rownum\r\n";
                $log->addLine("Importing $accumulatedTotal / $rownum");
                $dbHandler->updateWithParameters($queryInserts,\@queryValues);
                $success = 0;
                $parameterCount = 1;
                @queryValues = ();
                $queryInserts = $queryInsertsHead;
                $queryByHand = $queryByHandHead;
            }
        }

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

        close $fh;
        $accumulatedTotal+=$success;
        $fileParsingReport{"*** $bareFilename ***"} .= "\r\nImporting $accumulatedTotal / $rownum"  if $success;
        $log->addLine("Importing $accumulatedTotal / $rownum") if $success;

        $dbHandler->updateWithParameters($queryInserts,\@queryValues) if $success;

        my $query = "UPDATE $schema.$table set file_name = \$data\$$bareFilename\$data\$ WHERE file_name is null";
        $log->addLine($query) if $success;
        $dbHandler->update($query) if $success;

        # # delete the file so we don't read it again
        # Disabled because we are going to let bash do this
        # so that we don't halt execution of this script in case of errors
        # unlink $file;
    }

    ## Now scrub data and compare data to production and apply logic

    my %reporting = ();

    # First, remove blank student ID's/
    my $query = "select count(*) from $schema.$table where btrim(studentid)=\$\$\$\$ and not dealt_with";
    @results = @{$dbHandler->query($query)};
    $reporting{"Blank Student ID"} = $results[0][0] || 0;

    $query = "delete from $schema.$table where btrim(studentid)=\$\$\$\$";
    $log->addLine($query);
    $dbHandler->update($query);

    # Delete file header rows
    $query = "delete from $schema.$table where lower(btrim(studentid))~\$\$studentid\$\$";
    $log->addLine($query);
    $dbHandler->update($query);

    # Kick out lines that do not match actor.org_unit
    $query = "update $schema.$table
    set
    error_message = \$\$Library '\$\$||library||\$\$' does not match anything in the database\$\$
    where not dealt_with and not imported and lower(library) not in(select lower(shortname) from actor.org_unit)";
    $log->addLine($query);
    $dbHandler->update($query);

    # Default DOB to 1/1/1990
    $query = "select count(*) from $schema.$table where btrim(dob)=\$\$\$\$ and not dealt_with";
    @results = @{$dbHandler->query($query)};
    $reporting{"Blank DOB"} = $results[0][0] || 0;

    $query = "update $schema.$table pi
    set
    dob='1/1/1990'
    where
    not dealt_with and not imported and
    btrim(dob)=\$\$\$\$";
    $log->addLine($query);
    $dbHandler->update($query);


    if($ensureAlreadyExistingPatronsAreTagged)
    {
        # Kick out lines that have matching barcodes in production but are not part of this project
        $query = "update $schema.$table pi
        set
        error_message = \$\$Duplicate barcode already in production for non-student\$\$
        from
        actor.card ac,
        actor.usr au
        where
        not pi.dealt_with and
        not pi.imported and
        au.id=ac.usr and
        ac.barcode=pi.studentid and
        au.ident_value!=\$tag\$$patronTagString\$tag\$";
        $log->addLine($query);
        $dbHandler->update($query);
    }


    ## Assign the matching home_ou
    $query = "update $schema.$table pi
    set
    home_ou = aou.id
    from
    actor.org_unit aou
    where
    not pi.dealt_with and not pi.imported and
    lower(pi.library) = lower(aou.shortname)";
    $log->addLine($query);
    $dbHandler->update($query);


    ## Gather up the rows for insert/update
    my %columnOrder = {};
    ## Add the home_ou column to the map
    my $keyCount = keys %colmap;
    print "Key Count = $keyCount\n";
    $colmap{$keyCount} = "home_ou";

    my $i = 0;
    $query = "select ";
    while ( (my $key, my $value) = each(%colmap) )
    {
        my $DBFriendly = createDBFriendlyName($value);
        $columnOrder{$DBFriendly}=$i;
        $query.= $DBFriendly.", ";
        $i++;
    }

    while ( (my $key, my $value) = each(%statcatmap) )
    {
        my $DBFriendly = createDBFriendlyName($value);
        $columnOrder{createDBFriendlyName($value)}=$i;
        $query.= $DBFriendly.", ";
        $i++;
    }

    $query.= "id";
    $columnOrder{"id"} = $i;
    # un-sql-comment the limit statement in the query for a much faster execution!
    $query.=" from $schema.$table pi where home_ou is not null and not dealt_with and error_message = \$\$\$\$
    -- limit 5";
    $log->addLine($query);

    # Loop through them and perform update/inserts and hook up the stat cats
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        # $log->addLine("moving through patron installation");
        my @row = @{$_};
        my %patron;
        while ( (my $key, my $value) = each(%colmap) ) { $patron{createDBFriendlyName($value)} = @row[$columnOrder{createDBFriendlyName($value)}]; }
        while ( (my $key, my $value) = each(%statcatmap) ) { $patron{createDBFriendlyName($value)} = @row[$columnOrder{createDBFriendlyName($value)}]; }
        $patron{"id"} = @row[$columnOrder{"id"}];
        $log->addLine(Dumper(\%patron));
        my $installSuccess = installPatron(\%patron);
        errorPatron(\%patron, $installSuccess) if ($installSuccess ne 'success');
    }



    ### reporting

    $query = "select count(*) from $schema.$table where not dealt_with";
    @results = @{$dbHandler->query($query)};
    $reporting{"Total Lines"} = $results[0][0] || 0;

    # $query = "select school,count(*) from $schema.$table where not dealt_with group by 1 order by 1";
    # @results = @{$dbHandler->query($query)};
    # $reporting{"*** School Breakdown ***"} = "\r\n";
    # foreach(@results)
    # {
        # my @row = @{$_};
        # $reporting{"*** School Breakdown ***"} .= @row[0]."   ".@row[1]."\r\n";
    # }

    $query = "select library,count(*) from $schema.$table where not dealt_with group by 1 order by 1";
    @results = @{$dbHandler->query($query)};
    $reporting{"*** Library Breakdown ***"} = "\r\n";
    foreach(@results)
    {
        my @row = @{$_};
        $reporting{"*** Library Breakdown ***"} .= @row[0]."   ".@row[1]."\r\n";
    }

    my $errored = "";
    $reporting{"Total with errors"} = 0;
    $query = "select studentid, firstname, lastname, library, school, error_message from $schema.$table where not dealt_with and error_message!=\$\$\$\$";
    @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my @row = @{$_};
        $errored.="( ".@row[0]." ".@row[1]." ".@row[2]." ".@row[3]." ".@row[4]." ) ERROR = '".@row[5]."\n";
        $reporting{"Total with errors"}++;
    }

    my @toEmail = split(/,/,$toEmail);
    my %conf = ();
    $log->addLine("sending email. '$fromEmail' -> ".Dumper(\@toEmail));
    my $email = new email($fromEmail,\@toEmail,0,0,\%conf);

    my $body = "
Dear staff,

Your file(s) have been processed. These are the files:$inputFileFriendly\r\n\r\nHere is a summary:\r\n\r\n";

    while ( (my $key, my $value) = each(%fileParsingReport) )
    {
        $body.=$key.": ".$value;
    }
    $body.="\r\n\r\n";

    my $lastReport = "";
    while ( (my $key, my $value) = each(%reporting) )
    {
        $body.=$key.": ".$value."\n" if !($key =~ m/\*/g);
        $lastReport .=$key.": ".$value."\n" if ($key =~ m/\*/g);
    }
    $body.="\r\n$lastReport";

    $body.="\r\n\r\nHere are the errored records. Stat cat errors do not prevent the patron from importing.
$errored" if $reporting{"Total with errors"} > 0;

    $body.="\r\n\r\n-MOBIUS Perl Squad-";
    my $fileCount = $#inputFiles;
    $fileCount++;
    $email->send("Evergreen Utility - Patron import results - $fileCount file(s)",$body);

    # Finally, mark all of the rows dealt_with for next execution to ignore
    $query = "update $schema.$table set dealt_with=true where not dealt_with";
    $log->addLine($query);
    $dbHandler->update($query);

    $log->addLogLine(" ---------------- Script Ending ---------------- ");

sub installPatron
{
    my $p = shift;
    my %patron = %{$p};
    my $newPatron = 0;
    my $query = '';

    my %prodMap =
    (
        'studentid'  => 'usrname',
        'lastname' => 'family_name',
        'firstname' => 'first_given_name',
        'middlename' => 'second_given_name',
        'dob' => 'dob',
        'email' => 'email',
        'phone' => 'day_phone',
        'home_ou' => 'home_ou',
        'expirationdate' => 'expire_date',
        'alias' => 'alias'
    );

    my $usr = findUsrID($patron{"studentid"});
    $newPatron = 1 if !$usr;
    my $profileID = handleProfileID($patron{"profileid"});
    my @vals = ();
    my $installQuery = "INSERT INTO actor.usr ( ";
    $installQuery = "UPDATE actor.usr SET " if !$newPatron;
    my $valuesClause = 'VALUES(';
    my $colCount = 1;
    while ( (my $key, my $value) = each(%prodMap) )
    {
        my $insVal = $patron{$key};
        $insVal = "null" if length($insVal) == 0;
        $installQuery .=  "$value = \$$colCount ," if !$newPatron;
        $installQuery .=  "$value," if $newPatron;
        $valuesClause .= "\$$colCount ,";
        push @vals, $insVal;
        $colCount++;
    }

    my $active = 'true';
    # Catch the case when the data suggests that the patron needs to be inactivated
    $active = 'false' if($patron{"profileid"} eq "0");

    $installQuery .= "ident_type = 3, ident_value = \$tag\$$patronTagString\$tag\$,
    active = $active, barred = false, deleted = false, juvenile = true,
    profile = $profileID," if !$newPatron;

    $installQuery .= "ident_type, ident_value, active, barred, deleted, juvenile, profile, passwd," if $newPatron;
    $valuesClause .= "3, \$tag\$$patronTagString\$tag\$, $active, false, false, true, $profileID, E'".$patron{"studentid"}."'," if $newPatron;
    $valuesClause = substr($valuesClause,0,-1);
    $installQuery = substr($installQuery,0,-1);
    $installQuery .= " WHERE id = $usr" if !$newPatron;
    $installQuery .= " )  $valuesClause ) " if $newPatron;
    $log->addLine($installQuery);
    $log->addLine($valuesClause);
    $log->addLine(Dumper(\@vals));

    $dbHandler->updateWithParameters($installQuery, \@vals);

    ## Connect the barcode actor.card when applicable
    if( $newPatron )
    {
        $usr  = findUsrID($patron{"studentid"});
        return "Could not insert new patron - UNKNOWN" if !$usr;
        $query = "INSERT INTO actor.card (barcode,usr) VALUES (\$1,\$2)";
        @vals = ($patron{"studentid"}, $usr);
        $log->addLine($query);
        $dbHandler->updateWithParameters($query, \@vals);
        $query = "UPDATE actor.usr set card = (select max(id) from actor.card where usr = $usr) where id = $usr";
        $log->addLine($query);
        $dbHandler->update($query);
    }

    # Add 12 hours to the expiration date to deal with timezone issues
    $query = "UPDATE actor.usr set expire_date = expire_date + '12 hours'::interval WHERE id = $usr";
    $log->addLine($query);
    $dbHandler->update($query);

    connectPatronToUsr(\%patron, $usr);

    # Ensure that the card is active
    $query = "UPDATE actor.card SET active = TRUE WHERE usr = $usr AND barcode = \$\$".$patron{"studentid"}."\$\$";
    $log->addLine($query);
    $dbHandler->update($query);

     ## Address updates/Inserts
    $query = "SELECT mailing_address from actor.usr where id= $usr";
    my @results = @{$dbHandler->query($query)};
    my $mailingID = '';
    $mailingID = $results[0][0] if $results[0][0];
    $log->addLine("Mailing ID = $mailingID");
    $mailingID = 0 if length($mailingID) == 0;

    %prodMap =
    (
        'city'  => 'city',
        'county' => 'county',
        'state' => 'state',
        'zip' => 'post_code',
        'street1' => 'street1',
        'country' => 'country'
    );

    my $installQuery = "INSERT INTO actor.usr_address( ";
    $installQuery = "UPDATE actor.usr_address SET " if $mailingID;
    my $valuesClause = 'VALUES(';
    my $colCount = 1;
    @vals = ();
    while ( (my $key, my $value) = each(%prodMap) )
    {
        my $insVal = $patron{$key};
        $insVal = 'US' if ( (length($insVal) == 0) && $key eq 'country' );
        $insVal = "no-value" if length($insVal) == 0;
        $installQuery .=  "$value = \$$colCount ," if $mailingID;
        $installQuery .=  "$value ," if !$mailingID;
        $valuesClause .= "\$$colCount,";
        push @vals, $insVal ;
        $colCount++;
    }

    $valuesClause = substr($valuesClause,0,-1);
    $installQuery = substr($installQuery,0,-1);
    $installQuery .= " WHERE id = $mailingID" if $mailingID;
    $installQuery .= ",usr )  $valuesClause, $usr ) " if !$mailingID;
    $log->addLine($installQuery);
    # $log->addLine($valuesClause);
    $log->addLine(Dumper(\@vals));
    $dbHandler->updateWithParameters($installQuery, \@vals);

    if (!$mailingID) # link the newly inserted address back to actor.usr
    {
        $query = "UPDATE actor.usr SET mailing_address = (SELECT MAX(id) FROM actor.usr_address WHERE usr = $usr) WHERE id = $usr";
        $log->addLine($query);
        $dbHandler->update($query);
    }


    while ( (my $key, my $value) = each(%statcatmap) )
    {
        # Ignore statcat placeholders
        if( !($value =~ m/statcat/g ) )
        {
            my $DBFriendly = createDBFriendlyName($value);
            my $statCatValue = $patron{$DBFriendly};
            $statCatValue =~ s/^\s+|\s+$//g;
            if( length($statCatValue) > 0 )
            {
                errorPatron(\%patron, "Could not install stat cat '$value' with '".$patron{$DBFriendly}."'") if (!setStatCat($usr, $value, $patron{$DBFriendly}));
            }
        }
    }


    setImported(\%patron);

    setPasswd(\%patron, $usr);

    return "success";

}

sub handleProfileID
{
    my $attemptID = shift;
    my $query = "select id from permission.grp_tree where id=$attemptID";
    my @results = @{$dbHandler->query($query)};
    my $testID = 0;
    $testID = $results[0][0] if $results[0][0];
    if(!$testID)
    {
        $testID = $defaultProfileID || 2;
    }

    return $testID;
}

sub setPasswd
{
    my $p = shift;
    my %patron = %{$p};
    my $usrid = shift;
    # $log->addLine("passwd patron object\n".Dumper(\%patron));
 	my $pass = substr($patron{"studentid"},-4);
    # $log->addLine("Setting passwd = $pass");
    my $query = "select * from actor.create_salt('main')";
    my @results = @{$dbHandler->query($query)};
    my @row = @{@results[0]};
    my $salt = @row[0];
    $query = "select * from actor.set_passwd($usrid,'main',
    md5(\$salt\$$salt\$salt\$||md5(\$pass\$$pass\$pass\$)),
    \$\$$salt\$\$
    )";
    my $result = $dbHandler->query($query);
    # $log->addLine("salt update = ".Dumper($result));
}

sub setStatCat
{
    my $usr = shift;
    my $statCat = shift;
    my $statCatEntry = shift;

    return 0 if( !$statCatEntry || length($statCatEntry)==0 || !$statCat || length($statCat)==0);

    my $ouID = findUsrSystemID($usr);

    return 0 if( !$ouID );

    ## Make sure that the stat cat exists
    my $found = 0;
    my $query = "select 1 from actor.stat_cat where owner=$ouID and name=\$\$$statCat\$\$";
    # $log->addLine($query);
    my @results = @{$dbHandler->query($query)};
    $found = 1 foreach(@results);
    my @vars = ($statCat);
    $query = "insert into actor.stat_cat(owner,name,allow_freetext)  values($ouID, \$1 , false)";
    $log->addLine($query) if (!$found);
    $dbHandler->updateWithParameters($query,\@vars) if (!$found);


    ## Make sure that the stat cat entry exists
    $found = 0;
    $query = "
    select 1 from actor.stat_cat_entry
    where
    stat_cat = (select id from actor.stat_cat where owner=$ouID and name= \$\$$statCat\$\$ ) and
    owner=$ouID and
    value=\$\$$statCatEntry\$\$";
    # $log->addLine($query);
    @results = @{$dbHandler->query($query)};
    $found = 1 foreach(@results);
    @vars = ($statCatEntry);
    $query = "
    insert into
    actor.stat_cat_entry(owner,value,stat_cat)
    values($ouID, \$1 , (select id from actor.stat_cat where owner=$ouID and name=\$\$$statCat\$\$))";
    $log->addLine($query) if (!$found);
    $dbHandler->updateWithParameters($query,\@vars) if (!$found);


    ## See if the patron already has this setup
    $found = 0;
    $query = "select id from actor.stat_cat_entry_usr_map where
    stat_cat = (select id from actor.stat_cat where owner=$ouID and name=\$\$$statCat\$\$) and
    target_usr = $usr";
    @results = @{$dbHandler->query($query)};
    $found = $results[0][0] foreach(@results);
    $log->addLine("stat cat map: found = $found");
    ## If not, insert the stat cat for this patron
    @vars = ($statCatEntry,$usr);

    $query = "insert into actor.stat_cat_entry_usr_map(stat_cat,stat_cat_entry,target_usr)
    values((select id from actor.stat_cat where owner=$ouID and name=\$\$$statCat\$\$), \$1 , \$2 )";

    $query = "update actor.stat_cat_entry_usr_map set stat_cat_entry = \$1 , target_usr = \$2 where id = $found" if $found;

    $log->addLine($query);
    $log->addLine(Dumper(\@vars));
    $dbHandler->updateWithParameters($query,\@vars);

    return 1;
}

sub findUsrSystemID
{
    my $usr = shift;
    my $query = "select id from actor.org_unit_ancestors( (select home_ou from actor.usr where id= $usr ) ) where parent_ou=1 and ou_type=2";
    $log->addLine($query);
    my @results = @{$dbHandler->query($query)};
    my $ret = 0;
    $ret = $results[0][0] if $results[0][0];
    # print "$usr systemID =  $ret";
    return $ret;
}

sub findUsrID
{
    my $barcode = shift;
    # Figure out if this is a new patron or not
    # $log->addLine("Looking for $barcode");
    my $query = "select usr from actor.card where barcode=\$\$$barcode\$\$";
    $log->addLine($query);
    my @results = @{$dbHandler->query($query)};
    my $usr = 0;
    $usr = $results[0][0] if $results[0][0];

    # Attempt to match on usrname instead of card
    if(!$usr)
    {
        $query = "select id from actor.usr where usrname=\$\$$barcode\$\$";
        $log->addLine($query);
        @results = @{$dbHandler->query($query)};
        $usr = $results[0][0] if $results[0][0];
    }

    return $usr;
}

sub connectPatronToUsr
{
    my $p = shift;
    my %patron = %{$p};
    my $usr = shift;
    $query = "update $schema.$table
    set
    usr_id = \$1
    where id = ".$patron{"id"};
    $log->addLine($query);
    my @vals = ($usr);
    $dbHandler->updateWithParameters($query, \@vals);
}

sub setImported
{
    my $p = shift;
    my %patron = %{$p};
    my $usr = shift;
    $query = "update $schema.$table
    set
    imported = true
    where id = ".$patron{"id"};
    $log->addLine($query);
    $dbHandler->update($query);
}

sub errorPatron
{
    my $p = shift;
    my %patron = %{$p};
    my $error = shift;
    return 0 if( !$error || length($error)==0 );

    $query = "update $schema.$table
    set
    error_message = error_message || E' ' || \$1
    where id = ".$patron{"id"};
    $log->addLine($query);
    my @vals = ($error);
    $dbHandler->updateWithParameters($query, \@vals);
}

sub getDateFromFile
{
    my $nameString = shift;
    my $ret = "now() + \$\$1 year\$\$::interval";
    my @s = split(/_/,$nameString);
    # Strict formula YYYYMMDD_FILENAME.ext
    if( ($#s > 0) && (length(@s[0]) == 8) && (@s[0] =~ m/^(\d{4})(\d{2})(\d{2})$/) )
    {
        @s[0] =~ s/(\d{4})(\d{2})(\d{2})/\1-\2-\3/;
        $ret = "\$\$".@s[0]."\$\$::date";
    }
    print "Expire date set to $ret\n";
    return $ret;
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
	my $query = "select * from INFORMATION_SCHEMA.COLUMNS where table_name = '$table' and table_schema='$schema'";
	my @results = @{$dbHandler->query($query)};
	if($#results==-1)
	{
        $query = "select schema_name from information_schema.schemata where schema_name='$schema'";
        my @results = @{$dbHandler->query($query)};
        if($#results <  0)
        {
            $query = "CREATE SCHEMA $schema";
            $dbHandler->update($query);
        }

		$query = "CREATE TABLE $schema.$table
		(
		id bigserial NOT NULL,
        ";
        while ( (my $key, my $value) = each(%columns) ) { $query.= $value." text,"; }
        while ( (my $key, my $value) = each(%statcatmap) )
        {
            my $columnName = createDBFriendlyName($value);
            $query.= $columnName." text,";
        }
        $query.="
        home_ou bigint,
        usr_id bigint,
        file_name text,
        imported boolean default false,
        error_message text DEFAULT \$\$\$\$,
        dealt_with boolean default false,
        insert_date timestamp with time zone NOT NULL DEFAULT now()
        CONSTRAINT $table"."_id_pkey PRIMARY KEY (id)
        )";

		$dbHandler->update($query);
        $log->addLine($query);
	}
    else  ## sync up columns if needed
    {
        my @two = (\%columns);
        push @two , \%statcatmap;

        foreach(@two)
        {
            my %thisone = %{$_};
            while ( (my $key, my $value) = each(%thisone) )
            {

                my $friendlyColumnName = createDBFriendlyName($value);
                my $query = "
                SELECT *
                    FROM information_schema.COLUMNS
                    WHERE
                    TABLE_SCHEMA = '$schema'
                    AND TABLE_NAME = '$table'
                    AND lower(COLUMN_NAME) = '$friendlyColumnName'
                    ";
                my @res = @{$dbHandler->query($query)};
                if($#res == -1)
                {
                    my $query = "ALTER TABLE $schema.$table ADD COLUMN $friendlyColumnName text";
                    $log->addLine($query);
                    $dbHandler->update($query);
                }
            }
        }
    }
}

sub createDBFriendlyName
{
    my $inc = shift;
    my $ret = lc $inc;
    $ret =~s/^\s*//g;
    $ret =~s/\s*$//g;
    $ret =~s/\s/_/g;
    $ret =~s/[\-\.'"\[\]\{\}\/\(\)\?\!\>\<]//g;
    return $ret;
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
