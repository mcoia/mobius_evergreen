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
    our $profileID;
    our $fromEmail;
    our $toEmail;
    our $inputDirectory;
    my $logFile;
    my $inputFileFriendly;
    my %fileParsingReport = ();
    
    
    GetOptions (
    "logfile=s" => \$logFile,
    "xmlconfig=s" => \$xmlconf,
    "schema=s" => \$schema,
    "primarykey" => \$primarykey,
    "profileid=s" => \$profileID,
    "fromemail=s" => \$fromEmail,
    "toemail=s" => \$toEmail,
    "directory=s" => \$inputDirectory
    )
    or die("Error in command line arguments\nYou can specify
    --logfile configfilename (required)
    --xmlconfig  pathto_opensrf.xmlml
    --schema (eg. m_slmpl)
    --primarykey (create id column)
    --profileid (Databaes ID for the profile \"permission\" group)
    --fromemail spoofed from email address
    --toemail comma separated email addresses for the report
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
     if(!$profileID)
    {
        print "patron profile Database ID with --profileid\n";
        exit;
    }
    
    @inputFiles = @{getFiles($inputDirectory)};
    
        
    $log = new Loghandler($logFile);
    $log->truncFile("");
    $log->addLogLine(" ---------------- Script Starting ---------------- ");		

    
	my $dt = DateTime->now(time_zone => "local"); 
	my $fdate = $dt->ymd; 
	my $ftime = $dt->hms;
	my $dateString = "$fdate $ftime";
    
    my %dbconf = %{getDBconnects($xmlconf)};
	$dbHandler = new DBhandler($dbconf{"db"},"172.31.38.234",$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"}); #$dbconf{"dbhost"}
    
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
        
        my $queryInsertsHead = $queryInserts;
        my $queryByHandHead = $queryByHand;
        
        while ( my $row = $csv->getline( $fh ) )
        {
            
            my $valid = 0;
            my @rowarray = @{$row};
            if(scalar @rowarray != $sanitycheckcolumnnums )
            {
                $log->addLine("Error parsing line $rownum\nIncorrect number of columns: ". scalar @rowarray);
                $fileParsingReport{"*** $bareFilename ***"} .= "\r\nError parsing line $rownum\nIncorrect number of columns: ". scalar @rowarray;
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
                $fileParsingReport{"*** $bareFilename ***"} .= "\r\nImporting $accumulatedTotal / $rownum";
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
        
        # # delete the file so we don't read it again
        # Disabled because we are going to let bash do this 
        # so that we don't halt execution of this script in case of errors
        # unlink $file;
    }
    
    ## Now scrub data and compare data to production and apply logic 
    
    my %reporting = ();
    
    # First, remove blank student ID's/
    my $query = "select count(*) from $schema.patron_import where btrim(studentID)=\$\$\$\$ and not dealt_with";
    @results = @{$dbHandler->query($query)};
    $reporting{"Blank Student ID"} = $results[0][0] || 0;
    
    $query = "delete from $schema.patron_import where btrim(studentID)=\$\$\$\$";
    $log->addLine($query);
    $dbHandler->update($query);
    
    # Delete file header rows    
    $query = "delete from $schema.patron_import where lower(btrim(guardian))~\$\$guard\$\$";
    $log->addLine($query);
    $dbHandler->update($query);
    
    # Kick out lines that do not match actor.org_unit
    $query = "update $schema.patron_import 
    set
    error_message = \$\$Library '\$\$||library||\$\$' does not match anything in the database\$\$ 
    where not dealt_with and not imported and lower(library) not in(select lower(shortname) from actor.org_unit)";
    $log->addLine($query);
    $dbHandler->update($query);
    
    # Default DOB to 1/1/1990
    $query = "select count(*) from $schema.patron_import where btrim(dob)=\$\$\$\$ and not dealt_with";
    @results = @{$dbHandler->query($query)};
    $reporting{"Blank DOB"} = $results[0][0] || 0;
    
    $query = "update $schema.patron_import pi 
    set
    dob='1/1/1990'
    where
    not dealt_with and not imported and
    btrim(dob)=\$\$\$\$";
    $log->addLine($query);
    $dbHandler->update($query);
    
   
    # Kick out lines that have matching barcodes in production but are not part of this project
    $query = "update $schema.patron_import pi 
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
    au.ident_value!='Student ID Project'";
    $log->addLine($query);
    $dbHandler->update($query);
    
    
    ## Assign the matching home_ou
    $query = "update $schema.patron_import pi
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
    $colmap{16} = "home_ou";
    my $i = 0;
    $query = "select ";
    while ( (my $key, my $value) = each(%colmap) )
    {
        $columnOrder{$value}=$i;
        $query.= $value.", ";
        $i++;
    }
    
    $query.= "id";
    $columnOrder{"id"} = $i;
    # un-sql-comment the limit statement in the query for a much faster execution!
    $query.=" from $schema.patron_import pi where not dealt_with and error_message = \$\$\$\$ -- limit 2";
    $log->addLine($query);
    
    # Loop through them and perform update/inserts and hook up the stat cats
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        $log->addLine("moving through patron installation");
        my @row = @{$_};
        my %patron;
        while ( (my $key, my $value) = each(%colmap) ) { $patron{$value} = @row[$columnOrder{$value}]; }
        $patron{"id"} = @row[$columnOrder{"id"}];
        $log->addLine(Dumper(\%patron));
        my $installSuccess = installPatron(\%patron);
        errorPatron(\%patron, $installSuccess) if ($installSuccess ne 'success');
    }
    
    
    
    ### reporting
    
    $query = "select count(*) from $schema.patron_import where not dealt_with";
    @results = @{$dbHandler->query($query)};
    $reporting{"Total Lines"} = $results[0][0] || 0;
    
    $query = "select school,count(*) from $schema.patron_import where not dealt_with group by 1 order by 1";
    @results = @{$dbHandler->query($query)};
    $reporting{"*** School Breakdown ***"} = "\r\n";
    foreach(@results)
    {
        my @row = @{$_};
        $reporting{"*** School Breakdown ***"} .= @row[0]."   ".@row[1]."\r\n";
    }
    
    $query = "select library,count(*) from $schema.patron_import where not dealt_with group by 1 order by 1";
    @results = @{$dbHandler->query($query)};
    $reporting{"*** Library Breakdown ***"} = "\r\n";
    foreach(@results)
    {
        my @row = @{$_};
        $reporting{"*** Library Breakdown ***"} .= @row[0]."   ".@row[1]."\r\n";
    }
    
    my $errored = "";
    $reporting{"Total with errors"} = 0;
    $query = "select studentid, firstname, lastname, library, school, error_message from $schema.patron_import where not dealt_with and error_message!=\$\$\$\$";
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
    
    $email->send("Evergreen Utility - Patron import results - ". scalar $#inputFiles." file(s)",$body);
    
    # Finally, mark all of the rows dealt_with for next execution to ignore
    $query = "update $schema.patron_import set dealt_with=true where not dealt_with";
    $log->addLine($query);
    $dbHandler->update($query);
    
    $log->addLogLine(" ---------------- Script Ending ---------------- ");

sub installPatron
{
    my $p = shift;
    my %patron = %{$p};
    my $newPatron = 0;
    
    my %prodMap = 
    (
        'studentID'  => 'usrname',
        'lastName' => 'family_name',
        'firstName' => 'first_given_name',
        'middleName' => 'second_given_name',
        'dob' => 'dob',
        'guardian' => 'ident_value2',
        'email' => 'email',
        'phone' => 'day_phone',
        'home_ou' => 'home_ou'
    );
    
    my $usr = findUsrID($patron{"studentID"});
    $newPatron = 1 if !$usr;
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
    $installQuery .= "ident_type = 3, ident_value = E'Student ID Project', 
    expire_date = now() + E'1 year'::interval,
    active = true, barred = false, deleted = false, juvenile = true,
    profile = $profileID," if !$newPatron;
    
    $installQuery .= "ident_type, ident_value, expire_date, active, barred, deleted, juvenile, profile, passwd," if $newPatron;
    $valuesClause .= "3, E'Student ID Project', now() + (btrim(E' 1 year')::interval), true, false, false, true, $profileID, E'".$patron{"studentID"}."'," if $newPatron;
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
        $usr  = findUsrID($patron{"studentID"});
        return "Could not insert new patron - UNKNOWN" if !$usr;
        $query = "INSERT INTO actor.card (barcode,usr) VALUES (\$1,\$2)";
        @vals = ($patron{"studentID"}, $usr);
        $log->addLine($query);
        $dbHandler->updateWithParameters($query, \@vals);
        $query = "UPDATE actor.usr set card = (select max(id) from actor.card where usr = $usr) where id = $usr";
        $log->addLine($query);
        $dbHandler->update($query);
    }
    
    connectPatronToUsr(\%patron, $usr);
    
    # Ensure that the card is active
    $query = "UPDATE actor.card SET active = TRUE WHERE usr = $usr AND barcode = \$\$".$patron{"studentID"}."\$\$";
    $log->addLine($query);
    $dbHandler->update($query);
    
     ## Address updates/Inserts
    $query = "SELECT mailing_address from actor.usr where id= $usr";
    my @results = @{$dbHandler->query($query)};
    $mailingID = $results[0][0] if $results[0][0];
    $log->addLine("Mailing ID = $mailingID");
    $mailingID = 0 if length($mailingID) == 0;    
   
    %prodMap =
    (   
        'city'  => 'city',
        'county' => 'county',
        'state' => 'state',
        'zip' => 'post_code',
        'street1' => 'street1'
    );
    
    my $installQuery = "INSERT INTO actor.usr_address( ";
    $installQuery = "UPDATE actor.usr_address SET " if $mailingID;
    my $valuesClause = 'VALUES(';
    my $colCount = 1;
    @vals = ();
    while ( (my $key, my $value) = each(%prodMap) )
    {
        my $insVal = $patron{$key};
        $insVal = "null" if length($insVal) == 0;
        $installQuery .=  "$value = \$$colCount ," if $mailingID;
        $installQuery .=  "$value ," if !$mailingID;
        $valuesClause .= "\$$colCount,";
        push @vals, $insVal ;
        $colCount++;
    }
   
    $valuesClause = substr($valuesClause,0,-1);
    $installQuery = substr($installQuery,0,-1);
    $installQuery .= " WHERE id = $mailingID" if $mailingID;
    $installQuery .= ",usr, country )  $valuesClause, $usr, E'US' ) " if !$mailingID;
    $log->addLine($installQuery);
    # $log->addLine($valuesClause);
    $log->addLine(Dumper(\@vals));
    
    $dbHandler->updateWithParameters($installQuery, \@vals);
    
    my $genderStatValue = "Male";
    $genderStatValue = "Female" if lc($patron{"gender"}) eq 'f';
    $genderStatValue = "Other" if ( (lc($patron{"gender"}) ne 'm') && (lc($patron{"gender"}) ne 'f') );
    
    errorPatron(\%patron, "Could not install stat cat 'Student - School' with '".$patron{"school"}."'") if (!setStatCat($usr, "Student - School", $patron{"school"}));
    errorPatron(\%patron, "Could not install stat cat 'Student - Gender' with '$genderStatValue'") if (!setStatCat($usr, "Student - Gender", $genderStatValue));
    
    
    setImported(\%patron);
    
    setPasswd(\%patron, $usr);
    
    return "success";
    
}

sub setPasswd
{
    my $p = shift;
    my %patron = %{$p};
    my $usrid = shift;
    # $log->addLine("passwd patron object\n".Dumper(\%patron));
 	my $pass = substr($patron{"studentID"},-4);
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
    
    ## Make sure that the stat cat exists
    my $found = 0;
    my $query = "select 1 from actor.stat_cat where owner=1 and name=\$\$$statCat\$\$";
    # $log->addLine($query);
    my @results = @{$dbHandler->query($query)};
    $found = 1 foreach(@results);
    my @vars = ($statCat);
    $query = "insert into actor.stat_cat(owner,name,allow_freetext)  values(1, \$1 , false)";
    $log->addLine($query) if (!$found);
    $dbHandler->updateWithParameters($query,\@vars) if (!$found);
    
    
    ## Make sure that the stat cat entry exists
    $found = 0;
    $query = "
    select 1 from actor.stat_cat_entry
    where
    stat_cat = (select id from actor.stat_cat where owner=1 and name= \$\$$statCat\$\$ ) and
    owner=1 and
    value=\$\$$statCatEntry\$\$";
    # $log->addLine($query);
    @results = @{$dbHandler->query($query)};
    $found = 1 foreach(@results);
    @vars = ($statCatEntry);
    $query = "
    insert into
    actor.stat_cat_entry(owner,value,stat_cat)
    values(1, \$1 , (select id from actor.stat_cat where owner=1 and name=\$\$$statCat\$\$))";
    $log->addLine($query) if (!$found);
    $dbHandler->updateWithParameters($query,\@vars) if (!$found);
       
    
    ## See if the patron already has this setup
    $found = 0;
    $query = "select id from actor.stat_cat_entry_usr_map where 
    stat_cat = (select id from actor.stat_cat where owner=1 and name=\$\$$statCat\$\$) and 
    target_usr = $usr";
    @results = @{$dbHandler->query($query)};
    $found = $results[0][0] foreach(@results);
    $log->addLine("stat cat map: found = $found");
    ## If not, insert the stat cat for this patron
    @vars = ($statCatEntry,$usr);
    
    $query = "insert into actor.stat_cat_entry_usr_map(stat_cat,stat_cat_entry,target_usr)
    values((select id from actor.stat_cat where owner=1 and name=\$\$$statCat\$\$), \$1 , \$2 )";
    
    $query = "update actor.stat_cat_entry_usr_map set stat_cat_entry = \$1 , target_usr = \$2 where id = $found" if $found;
    
    $log->addLine($query);
    $log->addLine(Dumper(\@vars));
    $dbHandler->updateWithParameters($query,\@vars) if (!$found);
    
    return 1;
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
    $query = "update $schema.patron_import
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
    $query = "update $schema.patron_import
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
    
    $query = "update $schema.patron_import
    set
    error_message = error_message || E' ' || \$1
    where id = ".$patron{"id"};
    $log->addLine($query);
    my @vals = ($error);
    $dbHandler->updateWithParameters($query, \@vals);
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
