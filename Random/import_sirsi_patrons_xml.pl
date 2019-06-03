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

    my $xmlconf = "/openils/conf/opensrf.xml";
    our $log;
    our $dbHandler;
    our $inputFiles;
    our $primarykey;
    our $schema;
    my $logFile;
    my $xmlfile;
    
    GetOptions (
    "logfile=s" => \$logFile,
    "xmlconfig=s" => \$xmlconf,
    "schema=s" => \$schema,
    "xmlfile=s" => \$xmlfile,
    "primarykey" => \$primarykey,
    "files=s@" => \$inputFiles
    )
    or die("Error in command line arguments\nYou can specify
    --logfile configfilename (required)
    --xmlconfig  pathto_opensrf.xml
    --xmlfile  pathtoinputxmlfile.xml
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
    
	$log->truncFile("");
	$log->addLogLine(" ---------------- Script Starting ---------------- ");
	
	my %dbconf = %{getDBconnects($xmlconf)};
	$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
    
    my $firstTimeThrough = 1;
    
    foreach(@{$inputFiles})
    {
        my $startingUser = 1;
        my $chunkSize = 50;
        my $chunk = getChunk($_,$startingUser,$chunkSize);
        while($chunk)
        {
            my $xml = new XML::Simple;
            my $data = $xml->XMLin($chunk);
            #$log->addLine( Dumper($data) );
            my @users = ();
            my %columns = ();
            while ( (my $user, my $userdata) = each(%{$data}))
            {  
                if(ref($userdata) eq 'HASH')     # in case the chunk size is 1            
                {
                    my %s;
                    my $all = xmlLoop($userdata, "", \%s);
                    # $log->addLine("Done Looping");
                    # $log->addLine(Dumper($all));
                    while ((my $internal, my $value ) = each(%{$all}))
                    {
                        $columns{$internal} = 0;
                    }
                    push(@users, $all);
                }
                elsif(ref($userdata) eq 'ARRAY')
                {
                    foreach(@{$userdata})
                    {
                        my %s;
                        my $all = xmlLoop($_, "", \%s);
                        # $log->addLine("Done Looping");
                        # $log->addLine(Dumper($all));
                        while ((my $internal, my $value ) = each(%{$all}))
                        {
                            $columns{$internal} = 0;
                        }
                        push(@users, $all);
                    }
                }
                else
                {
                    $log->addLine("Spitting out this junk data.");
                    $log->addLine(Dumper($userdata));
                }
            }
            # $log->addLine("USER DATA DUMP: ". Dumper(@users) );
            
            my %loopvars = 
            (
            "patron_file" => "\$answer = ( !(\$internal =~ m/^bill/) && !(\$internal =~ m/^charge/) && !(\$internal =~ m/^hold/) )", 
            "patron_bill_file" => "\$answer = \$internal =~ m/^bill/",
            "patron_charge_file" => "\$answer = \$internal =~ m/^charge/",
            "patron_hold_file" => "\$answer = \$internal =~ m/^hold/"
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
                
                my $makeTable = "CREATE TABLE $schema.$table (";
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
                            # my $searchphrase = $search;
                            # # get the phrase that we are searching for from the regex inside of the above hash
                            # $searchphrase =~ s/([^\^]*)\^([^\/]*).*/$2/;
                            # $log->addLine("Search phrase is: $searchphrase");
                            
                            $t =~ s/^([a-z]*)([^a-z]*)(.*)/$3/;
                            #$log->addLine("Search phrase is: $t");
                            #$log->addLine("billcols is getting $t");
                            $billcols{$t}=0;
                            $t = $_;
                            $t =~ s/^([a-z]*)([^a-z]*)(.*)/$1$2/;
                            $billsects{$t}=0;
                        }
                        else{$log->addLine("billsect is getting $t"); $billcols{$t}=0; };
                        #$log->addLine("billsects is getting $t");
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
                
                $makeTable =~ s/#/_/g;
                $makeTable = substr($makeTable,0,-1);
                $makeTable.=")";
                $log->addLogLine($makeTable);
                $dbHandler->update("DROP TABLE IF EXISTS $schema.$table") if $firstTimeThrough;
                $dbHandler->update($makeTable) if $firstTimeThrough;
                
                # Make sure that the table has all of the potientially new columns defined
                expandTable(\@cols,$schema,$table) if ($table eq "patron_file" && !$firstTimeThrough);
                expandTable(\@billfinal,$schema,$table) if ($table ne "patron_file" && !$firstTimeThrough);
                
                my $queryheader = "INSERT INTO $schema.$table (";
                if ($table ne "patron_file"){$queryheader.="$_,"  for @billfinal;}
                if ($table eq "patron_file"){$queryheader.="$_,"  for @cols;}
                $queryheader =~ s/#/_/g;
                $queryheader = substr($queryheader,0,-1);
                $queryheader .= ")\n VALUES\n";
                my $query = '';
                # insert 5k rows at a time
                my $chompsize = 5000;
                my $usercount=0;
                my $runInsert=0;

                
                $log->addLogLine(Dumper(\@billfinal));
                $log->addLogLine(Dumper(\@billsections));
                foreach(@users)
                {
                    $usercount++;
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
                                    $key = $_;
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
                                    $row.="\$datainsert\$$data\$datainsert\$,";
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
                            $runInsert = 1 if $hadData;
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
                                $query.="\$datainsert\$$data\$datainsert\$,";
                            }
                            else
                            {
                                $query.="null,";
                            }
                        }
                        $query = substr($query,0,-1);
                        $query .= "),\n";
                        $runInsert = 1
                    }
                    if($usercount > $chompsize)
                    {
                        $query = substr($query,0,-2);
                        $log->addLogLine($queryheader.$query);
                        $dbHandler->update($queryheader.$query);
                        $query = '';
                        $usercount=0;
                    }
                }
                $query = substr($query,0,-2);
                $log->addLogLine($queryheader.$query);
                
                #insert the patron data
                $dbHandler->update($queryheader.$query) if $runInsert;
                undef @cols;
                undef @billfinal;
                undef @billsections;
            }
            
            $firstTimeThrough = 0; # Turn off drop table and create table queries
            $startingUser += $chunkSize;
            $chunk = getChunk($_,$startingUser,$chunkSize);
        }
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
	# $holdcount ++ if($parent =~ m/^hold/);
	my $compiled = @_[2];
	my %compiled = %{$compiled};
    my $dt = DateTime->now(time_zone => "local"); 
    my $fdate = $dt->ymd; 
    my $ftime = $dt->hms;
    my $dateString = "$fdate $ftime";
	# $log->addLine(Dumper($compiled)) if($holdcount > 10);
	# return \%compiled if($holdcount > 10);
	#$log->addLine(Dumper($compiled));
	if(ref($xmlscrap) eq 'ARRAY')
	{
		#$log->addLine("It's an Array");
		my $i=0;
		foreach (@{$xmlscrap})
		{
			#$log->addLine("Recursing $_");
            # my $dt = DateTime->now(time_zone => "local"); 
            # my $fdate = $dt->ymd; 
            # my $ftime = $dt->hms;
            # my $dateString = "$fdate $ftime";
			$compiled = xmlLoop($_, $parent."_".$i, $compiled);
			$i++;
            # my $afterProcess = DateTime->now(time_zone => "local");
            # my $difference = $afterProcess - $dt;
            # my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
            # my $duration =  $format->format_duration($difference);
            # print "ARRAY $duration\n";
		}
	}
	elsif(ref($xmlscrap) eq 'HASH')
	{
		#$log->addLine("It's an HASH");
		while ((my $internal, my $value ) = each(%{$xmlscrap}))
		{
			# $log->addLine("Recursing $value");
            # my $dt = DateTime->now(time_zone => "local"); 
            # my $fdate = $dt->ymd; 
            # my $ftime = $dt->hms;
            # my $dateString = "$fdate $ftime";
			$compiled = xmlLoop($value, $parent."_".$internal, $compiled);
            # my $afterProcess = DateTime->now(time_zone => "local");
            # my $difference = $afterProcess - $dt;
            # my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
            # my $duration =  $format->format_duration($difference);
            # print "HASH $duration\n";
		}
	}
	else
	{
		#$log->addLine("It's data");
        # my $dt = DateTime->now(time_zone => "local"); 
        # my $fdate = $dt->ymd; 
        # my $ftime = $dt->hms;
        # my $dateString = "$fdate $ftime";
		my $i=0;
		my %compiled = %{$compiled};
		my $key = $parent;
		$key =~ s/^\s*//;
		$key =~ s/^_*//;
		$key =~ s/\s$//;
		$key =~ s/[\s,]/_/g;
		$key =~ s/\//_/g;
        $key =~ s/#/_/g;
        $key =~ s/\-/_/g;
        $key =~ s/'/_/g;
        $key =~ s/[^a-zA-Z0-9]/_/g;
		while( ($compiled{$key."_".$i} ) )
		{
			$i++;
		}
		# $log->addLine("Assigning ".$key."_".$i." = $xmlscrap");
		$compiled{$key."_".$i} = $xmlscrap;
		$compiled = \%compiled;
        # my $afterProcess = DateTime->now(time_zone => "local");
        # my $difference = $afterProcess - $dt;
        # my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
        # my $duration =  $format->format_duration($difference);
        # print "DATA $duration\n";
	}
    my $afterProcess = DateTime->now(time_zone => "local");
    my $difference = $afterProcess - $dt;
    my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
    my $duration =  $format->format_duration($difference);
    #print "OUTTER $duration\n" if $duration ne '00:00';
	return $compiled;
}

sub getChunk
{
    my $file = shift;
    my $starting = shift;
    my $chunkSize = shift;
    
    print "reading $file\n";
    print "starting $starting\n";
    print "chunkSize $chunkSize\n";
    open my $info, $file or die "Could not open $file: $!";
   
    binmode(inputfile, ":utf8");
    
    my $xmlheader = "";
    my $stopReadingHeader = 0;
    my $userCount = 0;
    my $usersAdded = 0;
    my $ret = '';
    
    while( my $line = <$info>)
    {       
        if (!$stopReadingHeader)
        {
            $xmlheader .= $line if( !($line =~ m/<user>/ ) );
            $stopReadingHeader = 1 if($line =~ m/<user>/ );
        }
        $userCount++ if( $line =~ m/<user>/ );
        if( ($stopReadingHeader) && ($usersAdded < $chunkSize ) && ($starting < ($userCount + 1) ) )
        {
            $ret .= $line;
            $usersAdded++ if( $line =~ m/<\/user>/ );
        }
        #last if $. == 2;
    }
     # print "
# userCount = $userCount
# usersAdded = $usersAdded
# ";
    close($info);
    my $finalret = $xmlheader.$ret;
    $finalret.="</report>\n" if !($finalret =~ m/<\/report>/); # only append the closing tag when it's not already there from the source file
    return 0 if $starting > $userCount;
    return $finalret;
}

sub expandTable
{
    my @cols = @{@_[0]};
    my $schema = @_[1];
    my $table = @_[2];
    
    my $query = "select column_name FROM information_schema.columns where table_schema = '$schema' and table_name='$table'";
    my @results = @{$dbHandler->query($query)};
    my @additionalColumns = ();
    foreach(@cols)
    {
        my $column = $_;
        my $found = 0;
        foreach(@results)
        {
            my @row = @{$_};
            my $comp = @row[0];
            # print "Checking '$comp' == '$column'";
            $found=1 if($comp eq $column);
        }
        push(@additionalColumns, $column) if !$found;
    }
    foreach(@additionalColumns)
    {
        my $query = "ALTER TABLE $schema.$table ADD COLUMN $_ TEXT";
        $log->addLine($query);
        $dbHandler->update($query);
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

 
 