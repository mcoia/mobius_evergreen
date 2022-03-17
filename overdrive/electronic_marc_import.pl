#!/usr/bin/perl

# ---------------------------------------------------------------
# Copyright © 2019 MOBIUS
# Blake Graham-Henderson <blake@mobiusconsortium.org>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# ---------------------------------------------------------------

# These Perl modules are required:
# install Email::MIME
# install Email::Sender::Simple
# install Digest::SHA1
# install REST::Client

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
use LWP::Simple;
use OpenILS::Application::AppUtils;
use DateTime::Format::Duration;
use Digest::SHA1;
use File::stat;
use Getopt::Long;
use REST::Client;
use LWP::UserAgent;
use Digest::SHA qw(hmac_sha256_base64);
use HTML::Entities;
use pQuery;
use POSIX;

our $configFile;
our $debug = 0;
our $reprocess = -1;
our $searchDeepMatch = 0;
our $match901c = 0;
our $reportOnly = -1;
our $continueJob = 0;
our $resha = 0;


GetOptions (
"config=s" => \$configFile,
"reprocess=s" => \$reprocess,
"search_deep" => \$searchDeepMatch,
"report_only=s" => \$reportOnly,
"continue=s" => \$continueJob,
"match_901c" => \$match901c,
"resha" => \$resha,
"debug" => \$debug,
)
or die("Error in command line arguments\nYou can specify
--config configfilename                       [Path to the config file - required]
--reprocess jobID                             [Optional: Skip the import process and re-process provided job ID]
--search_deep                                 [Optional: Cause the software to spend more time searching for BIB matches]
--match_901c                                  [Optional: Cause the software to match existing BIBS using the incoming MARC 901c]
--report_only jobID                           [Optional: Only email the report for a previous job. Provide the job ID]
--continue jobID                              [Optional: Cause the software to finish an old job that was not finsihed]
--resha flag                                  [Optional: Cause the software to loop through all of the previously imported bibs and recalculate the matching sha]
--debug flag                                  [Cause more output and logging]
\n");

 our $mobUtil = new Mobiusutil();
 our $conf = $mobUtil->readConfFile($configFile);
 our %conf;
 our $jobid = -1;
 our $log;
 our $archivefolder;
 our $importSourceName;
 our $importBIBTagName;
 our $importBIBTagNameDB;
 our $remotefolder;
 our $dbHandler;
 our $domainname = '';
 our $bibsourceid = -1;
 our $recurseFTP = 1;
 our $lastDateRunFilePath;
 our $cert;
 our $certKey;
 our @shortnames;
 our %marcEdits = ();


 if(!$configFile)
 {
    print "Please specify a config file\n";
    exit;
 }

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
        my @reqs = ("server","login","password","remotefolder","sourcename","tempspace","archivefolder","dbhost","db","dbuser","dbpass","port","participants","logfile","incomingmarcfolder","recordsource","ignorefiles","removalfiles","bibtag");

        # There are some special directives required when cloudlibrary is selected
        push(@reqs, ("lastdatefile","certpath","certkeypath")) if( lc ($conf{"recordsource"}) eq 'cloudlibrary');

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

        $lastDateRunFilePath = $conf{"lastdatefile"} if( lc ($conf{"recordsource"}) eq 'cloudlibrary');
        $cert = $conf{"certpath"} if( lc ($conf{"recordsource"}) eq 'cloudlibrary');
        $certKey = $conf{"certkeypath"} if( lc ($conf{"recordsource"}) eq 'cloudlibrary');

        $archivefolder = $conf{"archivefolder"};
        $importSourceName = $conf{"sourcename"};
        $remotefolder = $conf{"remotefolder"};
        $importBIBTagName = $conf{"bibtag"};
        $importBIBTagNameDB = $conf{"bibtag"};
        $importBIBTagNameDB =~ s/\s/\-/g;
        $domainname = $conf{"domainname"} || '';
        $recurseFTP = $conf{"recurse"} || 1;
        $recurseFTP = lc $recurseFTP;
        $recurseFTP = ($recurseFTP eq 'n' ? 0 : 1);

        if(!(-d $archivefolder))
        {
            $valid = 0;
            print "Sorry, the archive folder does not exist: $archivefolder\n";
            $errorMessage = "Sorry, the archive folder does not exist: $archivefolder";
        }
        #remove trailing slash
        $archivefolder =~ s/\/$//;

        my @editArray = ('add','replace','remove','removesubfield');
        foreach(@editArray)
        {
            my @a = ();
            $marcEdits{$_} = \@a;
            undef @a;
        }

        parseMARCEdits();

        my @files;

        if($valid)
        {
            my @marcOutputRecords;
            my @marcOutputRecordsRemove;
            my $removalsViaMARC = 1;
            @shortnames = split(/,/,$conf{"participants"});
            for my $y(0.. $#shortnames)
            {
                @shortnames[$y]=$mobUtil->trim(@shortnames[$y]);
            }
            eval{$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});};
            if ($@)
            {
                print "Could not establish a connection to the database\n";
                alertErrorEmail("Could not establish a connection to the database");
                exit 1;
            }

            setupSchema($dbHandler);

            reCalcSha() if $resha;

            my $doSomething = 0;

            if($reprocess != -1)
            {
                $bibsourceid = getbibsource();
                $jobid = $reprocess;
                $doSomething = resetJob($reprocess);
            }
            elsif($continueJob)
            {
                $bibsourceid = getbibsource();
                # Make sure the provided job exists
                my $query = "select id from e_bib_import.import_status where job = $continueJob and status=\$\$new\$\$";
                updateJob("Processing",$query);
                my @results = @{$dbHandler->query($query)};
                $jobid = $continueJob if $#results > -1;
                $doSomething = 1 if $#results > -1;
                my $t = $#results;
                $t++; # 0 based to 1 based
                print "Nothing unfinished for job $continueJob. Nothing to do.\n" if $#results < 0;
                print "Continuing job $continueJob with $t thing(s) to process\n" if $#results > -1;
                undef @results;
            }
            elsif($reportOnly == -1) ## Make sure we are not just running reports
            {
                @files = @{getmarc()};

                if($#files!=-1)
                {
                    $bibsourceid = getbibsource();
                    $jobid = createNewJob('processing');
                    if($jobid==-1)
                    {
                        $errorMessage = "Could not create a new job number in the schema - ";
                        $log->addLogLine($errorMessage);
                        deleteFiles(\@files);
                        $errorMessage."\n$_" foreach(@files);
                        alertErrorEmail($errorMessage);
                        exit;
                    }
                    $doSomething = prepFiles(\@files);
                }
            }
            $doSomething = 1 if $reportOnly != -1;
            $jobid = $reportOnly if $reportOnly != -1;

            if($doSomething) # File prep resulted in stuff we need to do or it's a re-process
            {
                if ($reportOnly == -1)
                {
                    # Send a comfort message explaining that we have received the files and it might take some time before
                    # they receive the finished message. Only when it's type folder and deep match searching is configured.
                    sendWelcomeMessage(\@files);

                    my $startTime = DateTime->now(time_zone => "local");
                    my $displayInterval = 100;
                    my $recalcTimeInterval = 500;
                    my $bibsImported = 0;
                    my $authsImported = 0;
                    ## Bib Imports
                    my $query = "SELECT id,title,z01,sha1,marc_xml,filename from e_bib_import.import_status where type=\$\$importbib\$\$ and job=$jobid and status=\$\$new\$\$ order by id";
                    updateJob("Processing",$query);
                    my @results = @{$dbHandler->query($query)};
                    my $count = 0;
                    my $totalCount = 0;
                    $bibsImported = 1 if ($#results > -1);
                    foreach(@results)
                    {
                        my @row = @{$_};
                        importMARCintoEvergreen(@row[0],@row[1],@row[2],@row[3],@row[4]);
                        $count++;
                        $totalCount++;
                        ($count, $startTime) = displayRecPerSec("Import Bibs", $count, $startTime,  $displayInterval, $recalcTimeInterval, $totalCount, $#results);
                    }
                    undef @results;

                    ## Authority Imports
                    my $query = "SELECT filename from e_bib_import.import_status where type=\$\$importauth\$\$ and job=$jobid and status=\$\$new\$\$ group by 1";
                    updateJob("Processing",$query);
                    my @results = @{$dbHandler->query($query)};
                    my $count = 0;
                    $startTime = DateTime->now(time_zone => "local");
                    foreach(@results)
                    {
                        my @row = @{$_};
                        importAuthority(@row[0]);
                        $count++;
                        $totalCount++;
                        ($count, $startTime) = displayRecPerSec("Import Authority", $count, $startTime,  $displayInterval, $recalcTimeInterval, $totalCount, $#results);
                        $authsImported = 1 if !$authsImported;
                    }
                    undef @results;

                    ## Removals
                    my $query = "SELECT id,title,z01,sha1,marc_xml,filename,type from e_bib_import.import_status where type~\$\$remov\$\$ and job=$jobid and status=\$\$new\$\$ order by type,id";
                    updateJob("Processing",$query);
                    my @results = @{$dbHandler->query($query)};
                    my $count = 0;
                    $startTime = DateTime->now(time_zone => "local");
                    foreach(@results)
                    {
                        my @row = @{$_};
                        my $removalViaMARAC = 1;
                        $removalViaMARAC = 0 if @row[6] eq 'isbn_remove';
                        print "Removal Type: @row[6]  isbnRemoval = $removalViaMARAC\n" if $debug;
                        removeBibsEvergreen(@row[0],@row[1],@row[2],@row[3],@row[4],$removalViaMARAC);
                        $count++;
                        $totalCount++;
                        ($count, $startTime) = displayRecPerSec("Bib Removal", $count, $startTime,  $displayInterval, $recalcTimeInterval, $totalCount, $#results);
                    }
                    undef @results;

                    ## Authority linker when there were bibs imported AND Auth imports
                    if($bibsImported && $authsImported && $conf{'authority_link_script_cmd'})
                    {
                        my $query = "SELECT bib from e_bib_import.import_status where type=\$\$importbib\$\$ and job=$jobid and bib is not null order by bib";
                        updateJob("Processing",$query);
                        my @results = @{$dbHandler->query($query)};
                        my $count = 0;
                        $startTime = DateTime->now(time_zone => "local");
                        foreach(@results)
                        {
                            my @row = @{$_};
                            my $cmd = $conf{'authority_link_script_cmd'} . ' ' . @row[0];
                            $log->addLogLine($cmd);
                            system($cmd);
                            $count++;
                            $totalCount++;
                            ($count, $startTime) = displayRecPerSec("Authority Linker", $count, $startTime,  $displayInterval, $recalcTimeInterval, $totalCount, $#results);
                        }
                        undef @results;
                    }

                }

                my $report = runReports();
                my $duration = calculateTimeDifference($dt);

                my $body = "Hi Team,\r\n\r\n";
                $body .= "Thanks for your file(s). It took me some time to work on it:\r\n$duration\r\n\r\n";
                $body .= "I've digested the file(s):\r\n\r\n";
                $body .= $report;
                $body .= "\r\n\r\nImport Type: ".$conf{"recordsource"};
                $body .= "\r\nConnected to: ".$conf{"server"} if($conf{"recordsource"} ne 'folder');
                $body .= "\r\nYours Truly,\r\nThe friendly MOBIUS server";

                updateJob("Processing","Email sending:\n$body");

                my @tolist = ($conf{"alwaysemail"});
                my $email = new email($conf{"fromemail"},\@tolist,$valid,1,\%conf);
                $email->send("Evergreen Electronic Import Summary - $importBIBTagName Job # $jobid",$body);

                updateJob("Completed","");
            }

            $log->addLogLine(" ---------------- Script Ending ---------------- ");
        }
        else
        {
            print "Config file does not define some of the required directives. See Log for details\n";
        }
    }
    else
    {
        print "Config file: 'logfile' directive is required\n";
    }
}

sub displayRecPerSec
{
    my $type = shift;
    my $recCount = shift;
    my $startTime = shift;
    my $displayInterval = shift;
    my $recalcTimeInterval = shift;
    my $totalCount = shift;
    my $totalRows = shift;
    $totalRows++; # 0 based to 1 based

    my @ret = ($recCount, $startTime);
    my $statement = "$type : " . calcRecPerSec($recCount, $startTime) . " Records / Second";
    print"$statement  $totalCount / $totalRows\n" if($recCount % $displayInterval == 0);

    @ret = (0, DateTime->now(time_zone => "local")) if($recCount % $recalcTimeInterval == 0);  #time to restart time

    undef $type;
    undef $recCount;
    undef $startTime;
    undef $displayInterval;
    undef $recalcTimeInterval;
    undef $totalCount;
    undef $totalRows;
    undef $statement;
    return @ret;
}

sub calcRecPerSec
{
    my $recCount = shift;
    my $startTime = shift;

    my $afterProcess = DateTime->now(time_zone => "local");
    my $difference = $afterProcess - $startTime;
    my $format = DateTime::Format::Duration->new(pattern => '%d %H %M %S');
    my $duration =  $format->format_duration($difference);
    (my $days, my $hours, my $minutes, my $seconds) = split(/\s/,$duration);
    my $totalSeconds = ($days * 24 * 60 * 60) + ($hours * 60 * 60) + ($minutes * 60) + $seconds;
    $totalSeconds = 1 if $totalSeconds < 1;
    my $ret = $recCount / $totalSeconds;
    undef $afterProcess;
    undef $recCount;
    undef $startTime;
    undef $difference;
    undef $format;
    undef $duration;
    undef $days;
    undef $hours;
    undef $minutes;
    undef $seconds;
    undef $totalSeconds;
    return $mobUtil->makeEvenWidth(substr($ret,0,7), 15);
}

sub runReports
{
    ## Reporting
    my $ret = "";
    my %totals = ();
    my %grandTotals = ();
    my @sortStatus = ();
    my $grandTotalNum = 0;

    ### Overall File Totals
    my $query = "
    select filename,status,count(*)
    from
    e_bib_import.import_status
    where
    job = $jobid
    group by 1,2
    order by 1,2";

    my @results = @{$dbHandler->query($query)};
    my $currFile = "";
    my $currFileTotal = 0;

    foreach(@results)
    {
        my @row = @{$_};
        if(@row[0] ne $currFile)
        {
            if( ($currFile ne '') && ($totals{$currFile}) )
            {
                $ret .= "\r\n\r\n--- $currFile ---\r\n";
                $ret .= "\t$currFileTotal Total record(s)\r\n";
                my @a = @{$totals{$currFile}};
                $ret .= "\t$_\r\n" foreach(@a);
                $ret .= "\r\n";
            }
            $currFileTotal = 0;
            $currFile = @row[0];
            my @a = ();
            $totals{$currFile} = \@a;
            push(@sortStatus, @row[1]) if(!($grandTotals{@row[1]}));
        }
        $currFileTotal += @row[2];
        my @a = @{$totals{$currFile}};
        push (@a, @row[2] . " " . @row[1]);
        $totals{$currFile} = \@a;

        $grandTotals{@row[1]} = 0 if(!($grandTotals{@row[1]}));
        $grandTotals{@row[1]} += @row[2];
        $grandTotalNum += @row[2];
    }
    undef @results;

    # Report the last loop
    $ret .= "\r\n\r\n--- $currFile ---\r\n";
    $ret .= "\t$currFileTotal Total record(s)\r\n";
    my @a = @{$totals{$currFile}};
    $ret .= "\t$_\r\n" foreach(@a);
    $ret .= "\r\n";
    undef @a;

    @sortStatus = sort @sortStatus;

    if($grandTotalNum > 0)
    {
        $ret .= "--- Grand Total ---\r\n";
        $ret .= "$grandTotalNum Total\r\n";
        $ret .= $grandTotals{$_} . " $_\r\n" foreach(@sortStatus);
        $ret .= "\r\n\r\n\r\n";
    }

    ### Import summary
    $query = "
    select
    z01,title,status,bib
    from
    e_bib_import.import_status
    where
    job = $jobid and
    type = \$\$importbib\$\$ and
    status not in ('inserted','matched and overlayed')";

    $ret .= reportSummaryChunk("Interesting Imports",$query);

    ### Removal summary
    $query = "
    select
    z01,title,status,bib
    from
    e_bib_import.import_status
    where
    job = $jobid and
    type ~ \$\$remov\$\$ and
    status not in ('removed bib','removed related 856','No matching bib in DB')";

    $ret .= reportSummaryChunk("Interesting Removals",$query);

    $ret .= gatherOutputReport();

    # Authority reports
    $query = "select marc_xml from e_bib_import.import_status where type=\$\$importauth\$\$ and job=$jobid";
    updateJob("Processing",$query);
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my @row = @{$_};
        my $batchID = @row[0];
        # For some reason, eg_staged_bib_overlay prefixes the tables with auths_
        $batchID = "auths_$batchID";
        my $interestingImports = "";

        # Gather up the new authority bibs with heading
        my $query = "
        select aaa.auth_id,(select left(string_agg(ash.value,', ' ),20) from authority.simple_heading ash where ash.record=aaa.auth_id) from
        auth_load.$batchID aaa
        where
        aaa.auth_id is not null and
        aaa.imported

        union all

        select aaa.new_auth_id,(select left(string_agg(ash.value,', ' ),20) from authority.simple_heading ash where ash.record=aaa.new_auth_id) from
        auth_load.$batchID aaa
        where
        aaa.new_auth_id is not null and
        aaa.imported
        ";
        $log->addLogLine($query);
        my @resultss = @{$dbHandler->query($query)};
        foreach(@resultss)
        {
            my @row = @{$_};
            my $id = @row[0];
            my $heading = @row[1];
            $heading =~ tr/\x20-\x7f//cd;
            $interestingImports .= $id . " '$heading'\r\n";
        }

        # Gather up the non imported authority bibs with heading
        my $query = "select auth_id||' '||new_auth_id||' '||cancelled_auth_id,heading from auth_load.$batchID where not imported";
        $log->addLogLine($query);
        my @resultss = @{$dbHandler->query($query)};
        foreach(@resultss)
        {
            my @row = @{$_};
            $log->addLine( join(';', @row) );
            my $id = @row[0];
            my $heading = @row[1];
            $interestingImports .= "not worked - " . $id . " - '$heading'\r\n";
        }
        $interestingImports = truncateOutput($interestingImports, 5000);
        $ret .= "#### Authority Batch $batchID ####\r\n$interestingImports" if ( length($interestingImports) > 0);
    }


    return $ret;

}

sub reportSummaryChunk
{
    my $title = shift;
    my $query = shift;
    my $ret = "";
    my $summ = "";

    my @results = @{$dbHandler->query($query)};

    if($#results > -1)
    {
        my $interestingImports = "";
        my %status = ();
        foreach(@results)
        {
            my @row = @{$_};
            my @t = ();
            $status{@row[2]} = \@t if !$status{@row[2]};
            @t = @{$status{@row[2]}};
            @row[1] =~ tr/\x20-\x7f//cd;
            my @temp = (@row[0],@row[1]);
            push @t, [@temp];
            $status{@row[2]} = \@t;
        }
        $ret .= "#### $title ####\r\n";
        while ( (my $key, my $value) = each(%status) )
        {
            my @c = @{$value};
            my $c = $#c;
            $c++;
            $ret .= "$key: $c time(s), record details:\r\n";
            my @vv = @{$value};
            foreach(@vv)
            {
                my @v = @{$_};
                $interestingImports .= $key . " - '" . @v[0] . "' '" . @v[1] . "'\r\n";
            }
            $interestingImports = truncateOutput($interestingImports, 5000);
            $ret .= "$interestingImports\r\n\r\n";
        }
    }
    return $ret;
}

sub prepFiles
{
    my @files = @{@_[0]};
    my $dbValPos = 1;
    my $ret = 0;
    my $insertTop = "INSERT INTO e_bib_import.import_status(filename,bibtag,z01,title,sha1,type,marc_xml,job)
    VALUES\n";
    my @vals = ();
    my $dbInserts = $insertTop;
    my $rowCount=0;

    for my $b(0..$#files)
    {
        my $thisfilename = lc($files[$b]);
        my $filenameForDB = $files[$b];
        updateJob("Processing","Parsing: $archivefolder/".$files[$b]);
        if(! ( ($thisfilename =~ m/csv/) || ($thisfilename =~ m/tsv/) ) )
        {
            my @fsp = split('\.',$thisfilename);
            my $fExtension = pop @fsp;
            $fExtension = lc $fExtension;
            my $file;
            $file = MARC::File::USMARC->in("$archivefolder/".$files[$b]) if $fExtension !=~ m/xml/;
            $file = MARC::File::XML->in("$archivefolder/".$files[$b]) if $fExtension =~ m/xml/;
            my $isRemoval = compareStringToArray($thisfilename,$conf{'removalfiles'});
            my $isAuthority = compareStringToArray($thisfilename,$conf{'authorityfiles'});
            while ( my $marc = $file->next() )
            {
                $dbInserts.="(";
                $marc = add9($marc) if ( !$isRemoval && !$conf{'import_as_is'} );
                my $importType = "importbib";
                $importType = "removal" if $isRemoval;
                $importType = "importauth" if $isAuthority;
                my $z01 = getsubfield($marc,'001','');
                my $t = getsubfield($marc,'245','a');
                my $sha1 = calcSHA1($marc);
                $sha1 .= ' '.calcSHA1($marc, 1); # append the baby SHA
                my $thisXML = convertMARCtoXML($marc);
                $dbInserts.="\$$dbValPos,";
                $dbValPos++;
                push(@vals,$filenameForDB);
                $dbInserts.="\$$dbValPos,";
                $dbValPos++;
                push(@vals,$importBIBTagNameDB);
                $dbInserts.="\$$dbValPos,";
                $dbValPos++;
                push(@vals,$z01);
                $dbInserts.="\$$dbValPos,";
                $dbValPos++;
                push(@vals,$t);
                $dbInserts.="\$$dbValPos,";
                $dbValPos++;
                push(@vals,$sha1);
                $dbInserts.="\$$dbValPos,";
                $dbValPos++;
                push(@vals,$importType);
                $dbInserts.="\$$dbValPos,";
                $dbValPos++;
                push(@vals,$thisXML);
                $dbInserts.="\$$dbValPos";
                $dbValPos++;
                push(@vals,$jobid);
                $dbInserts.="),\n";
                $rowCount++;
                ($dbInserts, $dbValPos, @vals) = dumpRowsIfFull($insertTop, $dbInserts, $dbValPos, \@vals);
                $ret = 1;
                last if $isAuthority; # Authority loads via external script and just needs the file name
            }
            $file->close();
            undef $file;
        }
        else
        {
            my $tfile = new Loghandler($archivefolder."/".$files[$b]);
            my @lines = @{$tfile->readFile()};
            my $commas = 0;
            my $tabs = 0;
            foreach(@lines)
            {
                my @split = split(/,/,$_);
                $commas+=$#split;
                @split = split(/\t/,$_);
                $tabs+=$#split;
            }
            my $delimiter = $commas > $tabs ? "," : "\t";
            foreach(@lines)
            {
                my $fullLine = $_;
                my @split = split(/$delimiter/,$_);
                foreach(@split)
                {
                    my $ent = $mobUtil->trim($_);
                    $ent =~ s/\D//g;
                    if( ( length($ent) == 13 ) or ( length($ent) == 10 ) )
                    {
                        $dbInserts.="(";
                        $dbInserts.="\$$dbValPos,";
                        $dbValPos++;
                        push(@vals,$filenameForDB);
                        $dbInserts.="\$$dbValPos,";
                        $dbValPos++;
                        push(@vals,$importBIBTagNameDB);
                        $dbInserts.="\$$dbValPos,";
                        $dbValPos++;
                        push(@vals,$ent);
                        $dbInserts.="\$$dbValPos,";
                        $dbValPos++;
                        push(@vals,$ent);
                        $dbInserts.="\$$dbValPos,";
                        $dbValPos++;
                        push(@vals,$ent);
                        $dbInserts.="\$$dbValPos,";
                        $dbValPos++;
                        push(@vals,"isbn_remove");
                        $dbInserts.="\$$dbValPos,";
                        $dbValPos++;
                        push(@vals,$fullLine);
                        $dbInserts.="\$$dbValPos";
                        $dbValPos++;
                        push(@vals,$jobid);
                        $dbInserts.="),\n";
                        $rowCount++;
                        ($dbInserts, $dbValPos, @vals) = dumpRowsIfFull($insertTop, $dbInserts, $dbValPos, \@vals);
                        $ret = 1;
                    }
                }
            }
        }
    }
    dumpRowsIfFull($insertTop, $dbInserts, $dbValPos, \@vals, 1) if $dbValPos > 1; # Dump what's left into the DB

    return $ret;
}

sub dumpRowsIfFull
{
    my $insertTop = @_[0];
    my $dbInserts = @_[1];
    my $count = @_[2];
    my @vals = @{@_[3]};
    my $dumpAnyway = @_[4] || 0;

    my @ret = ($dbInserts,$count,@vals);

    # Dump to database every 5000 values or when explicitly called for
    if( ($count > 5000) || $dumpAnyway )
    {
        $dbInserts = substr($dbInserts,0,-2); #chopping off [,\n]
        my $readyForStatusUpdate = $debug ? $dbInserts : '';
        if($debug) #slow slow slow - only debug mode
        {
            $readyForStatusUpdate = "\n";
            my $i = 1;
            foreach(@vals)
            {
                my $temp = $_;
                $temp = substr($temp,0,100) if(length($temp) > 100);
                $readyForStatusUpdate =~ s/\$$i([,\)])/\$data\$$temp\$data\$$1/;
                $i++;
            }
        }
        updateJob("Processing","Dumping memory to DB $count $readyForStatusUpdate");
        $log->addLine("Final insert statement:\n$dbInserts") if $debug;
        $dbHandler->updateWithParameters($dbInserts,\@vals);
        undef $dbInserts;
        my $dbInserts = $insertTop;
        undef @vals;
        my @vals = ();
        @ret = ($dbInserts,1,@vals);
    }

    return @ret;
}

sub resetJob
{
    my $resetJob = shift;
    my $ret = 0;
    my $query = "select count(*) from e_bib_import.import_status where job=$jobid";
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        $ret=@row[0];
    }
    if($ret)
    {
        $query = "update e_bib_import.import_status set status=\$1 , processed = false , bib = null , row_change_time = now() where job= \$2";
        updateJob("Processing","--Reset Job - \n$query");
        my @vals = ('new',$jobid);
        $dbHandler->updateWithParameters($query,\@vals);
    }

    return $ret;
}

sub sendWelcomeMessage
{
    my @files = @{$_[0]};
    my $files = join("\r\n",@files);
    my @tolist = ($conf{"alwaysemail"});
    my $body =
"Hello,

Just letting you know that I have begun processing the provided files:
$files";
    $body .= "\r\n
This software is configured to perform deep search matches against the database. This is slow but thorough.
Depending on the number of records, it could be days before you receive the finished message. FYI." if($searchDeepMatch);

    $body .= "
I'll send a follow-up email when I'm done.

Yours Truly,
The friendly MOBIUS server
";

    $log->addLine("Sending Welcome message:\r\n$body");

    my $email = new email($conf{"fromemail"},\@tolist,1,1,\%conf);
    $email->send("Evergreen Electronic Import Summary - $importBIBTagName Import Report Job # $jobid WINDING UP", $body);

}

sub alertErrorEmail
{
    my $error = shift;
    my @tolist = ($conf{"alwaysemail"});
    my $email = new email($conf{"fromemail"},\@tolist,1,0,\%conf);
    $email->send("Evergreen Utility - $importBIBTagName Import Report Job # $jobid - ERROR","$error\r\n\r\n-Evergreen Perl Squad-");
}

sub truncateOutput
{
    my $ret = @_[0];
    my $length = @_[1];
    if(length($ret)>$length)
    {
        $ret = substr($ret,0,$length)."\nTRUNCATED FOR LENGTH\n\n";
    }
    return $ret;
}

sub gatherOutputReport
{
    my $ret = "";
    my $newRecordCount = 0;
    my $updatedRecordCount = 0;
    my $mergedRecords = '';
    my $itemsAssignedRecords = '';

    #bib_marc_update table report new bibs
    my $query = "select count(*) from e_bib_import.bib_marc_update where job=$jobid and new_record is true";
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        $newRecordCount=@row[0];
    }
    #bib_marc_update table report non new bibs
    $query = "select count(*) from e_bib_import.bib_marc_update where job=$jobid and new_record is not true";
    @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        $updatedRecordCount=@row[0];
    }

    #bib_merge table report
    $query = "select leadbib,subbib from e_bib_import.bib_merge where job=$jobid";
    @results = @{$dbHandler->query($query)};
    my $count=0;
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        $mergedRecords.=@row[0]." < ".@row[1]."\r\n";
        $count++;
    }
    if($count>0)
    {
        $mergedRecords = truncateOutput($mergedRecords,5000);
        $mergedRecords="$count records were merged - The left number is the winner\r\n".$mergedRecords;
        $mergedRecords."\r\n\r\n\r\n";
    }

    #item_reassignment table report
    $query = "select target_bib,prev_bib from e_bib_import.item_reassignment where job=$jobid";
    @results = @{$dbHandler->query($query)};
    $count=0;
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        $itemsAssignedRecords.=@row[0]." < ".@row[1]."\r\n";
        $count++;
    }
    if($count>0)
    {
        $itemsAssignedRecords = truncateOutput($itemsAssignedRecords,5000);
        $itemsAssignedRecords="$count Records had physical items assigned - The left number is where the items were moved\r\n".$itemsAssignedRecords;
        $itemsAssignedRecords."\r\n\r\n\r\n";
    }

    $ret .= $newRecordCount." New record(s) were created.\r\n\r\n\r\n" if($newRecordCount > 0);
    $ret .= $updatedRecordCount." Record(s) were updated\r\n\r\n\r\n" if($updatedRecordCount > 0);
    $ret .= $mergedRecords if ($mergedRecords ne '');
    $ret .= $itemsAssignedRecords if ($itemsAssignedRecords ne '');

    #print $ret;
    return $ret;

}

sub deleteFiles
{
    my @files = @{@_[0]};
    foreach(@files)
    {
        my $t = new Loghandler("$archivefolder/$_");
        $log->addLogLine("Deleting $_");
        $t->deleteFile();
    }
}

sub parseFileName
{
    my $fullPath = @_[0];
    my @sp = split('/',$fullPath);
    my $path=substr($fullPath,0,( (length(@sp[$#sp]))*-1) );

    my @fsp = split('\.',@sp[$#sp]);
    my $fExtension = pop @fsp;
    my $baseFileName = join('.', @fsp);
    $baseFileName= Encode::encode("CP1252", $baseFileName);
    my @ret = ($path,$baseFileName,$fExtension);
    return \@ret;
}

sub moveFile
{
    my $file = @_[0];
    my $destination = @_[1];
    my $fhandle = new Loghandler($file);

    if( $fhandle->copyFile($destination) )
    {
        if(! (unlink($file)) )
        {
            print "Unable to delete $file";
            return 0;
        }
    }
    undef $fhandle;
    return 1;
}

sub getmarc
{
    my @ret;
    if( (lc$conf{"recordsource"} ne 'folder') && (lc$conf{"recordsource"} ne 'ftp') && (lc$conf{"recordsource"} ne 'cloudlibrary') && (lc$conf{"recordsource"} ne 'marcivehttps') )
    {
        $log->addLogLine("Unsupported external source: " . lc$conf{"recordsource"});
        exit;
    }
    @ret = @{getmarcFromFolder()}  if(lc$conf{"recordsource"} eq 'folder');
    @ret = @{getmarcFromFTP()}  if(lc$conf{"recordsource"} eq 'ftp');
    @ret = @{getMarcFromCloudlibrary()}  if(lc$conf{"recordsource"} eq 'cloudlibrary');
    @ret = @{getMarcFromMarciveHTTPS()}  if(lc$conf{"recordsource"} eq 'marcivehttps');
    return \@ret;
}

sub getmarcFromFolder
{
    my $incomingfolder = $conf{'incomingmarcfolder'};

    my @ret = ();
    my @files;
    #Get all files in the directory path
    @files = @{dirtrav(\@files,$incomingfolder)};

    foreach(@files)
    {
        my $filename = $_;
        my @filePathParse = @{parseFileName($filename)};
        $filename = @filePathParse[1].'.'.@filePathParse[2];
        my $download = decideToDownload($filename);

        if($download)
        {
            if(-e "$archivefolder/$filename")
            {
                my $size = stat("$archivefolder/$filename")->size; #[7];
                my $rsize = stat("$incomingfolder/$filename")->size;
                $log->addLogLine("$filename Local: $size Remote: $rsize");
                if($size ne $rsize)
                {
                    $log->addLine("$archivefolder/$filename differes in size remote $filename");
                    unlink("$archivefolder/$filename");
                }
                else
                {
                    $log->addLine("skipping $filename");
                    unlink(@filePathParse[0].'/'.$filename);
                    $download=0;
                }
            }
            else
            {
                $log->addLogLine("NEW $filename");
            }
            if($download)
            {
                my $fhandle = new Loghandler();
                my $worked = moveFile(@filePathParse[0].'/'.$filename,"$archivefolder/$filename");
                if($worked)
                {
                    push (@ret, $filename);
                }
            }
        }
    }
    $log->addLine(Dumper(\@ret));
    return \@ret;
}

sub getmarcFromFTP
{
    my $server = $conf{'server'};
    $server=~ s/http:\/\///gi;
    $server=~ s/ftp:\/\///gi;

    my $loops=0;
    my $login = $conf{'login'};
    my $password = $conf{'password'};
    my @ret = ();

    $log->addLogLine("**********FTP starting -> $server with $login and $password");

    my $ftp = Net::FTP->new($server, Debug => 0, Passive=> 1)
    or die $log->addLogLine("Cannot connect to ".$server);
    $ftp->login($login,$password)
    or die $log->addLogLine("Cannot login ".$ftp->message);
    $ftp->cwd($remotefolder);
    my @interestingFiles = ();
    @interestingFiles = @{ftpRecurse($ftp, \@interestingFiles)};
    $log->addLine(Dumper(\@interestingFiles)) if $debug;
    foreach(@interestingFiles)
    {
        my $filename = $_;
        my $download = decideToDownload($filename);

        if($download)
        {
            if(-e "$archivefolder/"."$filename")
            {
                my $size = stat("$archivefolder/"."$filename")->size; #[7];
                my $rsize = findFTPRemoteFileSize($filename, $ftp, $size);
                if($size ne $rsize)
                {
                    $log->addLine("Local: $size") if $debug;
                    $log->addLine("remot: $rsize") if $debug;
                    $log->addLine("$archivefolder/"."$filename differes in size");
                    unlink("$archivefolder/"."$filename");
                }
                else
                {
                    $log->addLine("skipping $filename");
                    $download=0;
                }
            }
            else
            {
                $log->addLine("NEW $filename");
            }
            if($download)
            {
                my $path = $archivefolder."/".$filename;
                $path = substr($path,0,rindex($path,'/'));
                # $log->addLine("Path = $path");
                if(!-d $path)
                {
                    $log->addLine("$path doesnt exist - creating directory");
                    make_path($path, {
                    verbose => 0,
                    mode => 0755,
                    });
                }
                $log->addLogLine("Downloading $filename to $archivefolder/$filename");
                my $worked = $ftp->get($filename,"$archivefolder/$filename");
                if($worked)
                {
                    push (@ret, "$filename");
                }
            }
        }
    }
    $log->addLogLine("Closing FTP connection");
    $ftp->quit;
    $log->addLogLine("**********FTP session closed ***************");
    $log->addLine(Dumper(\@ret));
    return \@ret;
}

sub findFTPRemoteFileSize
{
    my $filename = shift;
    my $ftp = shift;
    my $localFileSize = shift;

    my $rsize = $ftp->size($filename);

    my $testUsingDirMethod = 0;
    $testUsingDirMethod = 1 if($rsize =~ m/\D/);
    $testUsingDirMethod = 1 if( (!$testUsingDirMethod) && (length($rsize) <4) );
    if($testUsingDirMethod)
    {
        my @rsizes = $ftp->dir($filename);
        $rsize = @rsizes[0] ? @rsizes[0] : '0';
        #remove the filename from the string
        my $rfile = $filename;
        # parenthesis and slashes in the filename screw up the regex
        $rfile =~ s/\(/\\(/g;
        $rfile =~ s/\)/\\)/g;
        $rfile =~ s/\//\\\//g;
        $rsize =~ s/$rfile//g;
        $log->addLine($rsize);
        my @split = split(/\s+/, $rsize);

        # Build the year string
        my $dt = DateTime->now(time_zone => "local");
        my $fdate = $dt->ymd;
        my $year = substr($fdate,0,4);
        my @years = ();
        my $i = 0;
        push @years, $year-- while($i++ < 3);
        $year = substr($fdate,0,4);
        $i = 0;
        push @years, $year++ while($i++ < 3);
        $year = '';
        $year.="$_ " foreach(@years);

        foreach(@split)
        {
            # Looking for something that looks like a filesize in bytes. Example output from ftp server:
            # -rwxr-x---  1 northkcm System     15211006 Apr 25  2019 Audio_Recorded Books eAudio Adult Subscription_4_25_2019.mrc
            # -rwxr-x---  1 scenicre System         9731 Apr 09  2018 Zinio_scenicregionalmo_2099_Magazine_12_1_2017.mrc
            # We can expect a file that contains a single marc record to be reasonable in size ( > 1k)
            # Therefore, we need to find a string of at least 4 numeric characters. Need to watch out for "year" numerics

            next if($_ =~ m/\D/); #ignore fields containing anything other than numbers
            next if index($year,$_) > -1; #ignore fields containing a year value close to current year

            if(length($_) > 3)
            {
                $rsize = $_;
                # if we find that one of the values exactly matches local file size, then we just set it to that
                last if($localFileSize eq $_);

                # Need to allow for a small filesize margin to compensate for the ASCII byte count versus UTF-8
                # This isn't exactly perfect, but I'm going with a margin of 98 percent
                my $lowerNumber = $localFileSize;
                my $higherNumber = $_;
                $lowerNumber = $_ if $_ < $localFileSize;
                $higherNumber = $localFileSize if $_ < $localFileSize;
                my $percent = ($lowerNumber / $higherNumber) * 100;
                $log->addLine("Filesize percentage: $percent") if $debug;
                if($percent > 98)
                {
                    # Make them equal
                    $rsize = $localFileSize;
                    last;
                }
            }
        }
    }
    return $rsize;
}

sub getMarcFromCloudlibrary
{
    my $startDate = '0001-01-01';
    my $lastRemovalDate = '0001-01-01';
    if( -e $lastDateRunFilePath)
    {
        my $previousRunDateTimeFile = new Loghandler($lastDateRunFilePath);
        my @previousRunTime = @{$previousRunDateTimeFile->readFile()};

        # It's always going to be the first line in the file
        my $previousRunTime = @previousRunTime[0];
        $previousRunTime =~ s/\n$//g;
        my $lastRemovalRunTime = @previousRunTime[1];
        $lastRemovalRunTime =~ s/\n$//g;
        $log->addLine("reading last run file and got $previousRunTime and $lastRemovalRunTime");
        my ($y,$m,$d) = $previousRunTime =~ /^([0-9]{4})\-([0-9]{2})\-([0-9]{2})\z/
            or die;
        $startDate = $y.'-'.$m.'-'.$d;
        my ($y,$m,$d) = $lastRemovalRunTime =~ /^([0-9]{4})\-([0-9]{2})\-([0-9]{2})\z/
            or die;
        $lastRemovalDate = $y.'-'.$m.'-'.$d;
    }

    my $dateNow = DateTime->now(time_zone => "GMT");

    my $endDate = $dateNow->ymd();

    my @newRecords = @{_getMarcFromCloudlibrary($startDate, $endDate)};
    # Done gathering up new records.

    # Decide if it's been long enough to check for deletes
    my $dateNow = DateTime->today( time_zone => "GMT" );
    my ($y,$m,$d) = $lastRemovalDate =~ /^([0-9]{4})\-([0-9]{2})\-([0-9]{2})\z/
        or die;
    my $previousDate = new DateTime({ year => $y, month=> $m, day => $d });
    $previousDate->truncate( to => 'day' );
    my $difference = $dateNow - $previousDate;
    my $format = DateTime::Format::Duration->new(pattern => '%Y %m %e');
    my $duration =  $format->format_duration($difference);
    $log->addLine("duration raw = $duration");
    my ($y,$m,$d) = $duration =~ /^([^\s]*)\s([^\s]*)\s([^\s]*)/;


    $log->addLine("Duration from last deletes: $dateNow minus $previousDate = $y years, $m months $d days apart");
    $duration = $y*365;
    $duration+= $m*30;
    $duration+= $d;
    $log->addLine("Duration = ".$duration);
    $duration = $duration*1;
    my $removeOutput = '';
    if($duration > 30)
    {
        my $bib_sourceid = getbibsource();
        my $queryDB = "SELECT record,value,(select marc from biblio.record_entry where id=a.record) from metabib.real_full_rec a where
        record in(select id from biblio.record_entry where not deleted and
        source in($bib_sourceid) and create_date < '$startDate') and
        tag='035'";
        updateJob("Processing",$queryDB);
        my @results = @{$dbHandler->query($queryDB)};
        foreach(@results)
        {
            my @row = @{$_};
            my $bib = @row[0];
            my $itemID = @row[1];
            $itemID =~ s/^[^\s]*\s([^\s]*)/\1/g;
            $log->addLine("Item ID = $itemID");
            my $marcxml = @row[2];
            my @checkIfRemoved = @{_getMarcFromCloudlibrary($startDate, $endDate, $itemID)};
            # not found in the cloudlibrary collection, add it to the remove array
            if($#checkIfRemoved == -1)
            {
                $marcxml =~ s/(<leader>.........)./${1}a/;
                my $marcobj = MARC::Record->new_from_xml($marcxml);
                $removeOutput.= convertMARCtoXML($marcobj);;
            }
        }
    }


    my $outputFileName = $mobUtil->chooseNewFileName($archivefolder,"import$endDate","xml");
    my $outputFileRemove = $mobUtil->chooseNewFileName($archivefolder,"import$endDate"."remove","xml");

    my @ret = ();

    my $newOutput = '';
    foreach(@newRecords)
    {
        $newOutput .= convertMARCtoXML($_);
    }

    if(length($newOutput) > 0)
    {
        my $outputFile = new Loghandler($outputFileName);
        $outputFile->appendLine($newOutput);
        $outputFileName =~ s/$archivefolder//;
        push (@ret, $outputFileName);
    }

    if(length($removeOutput) > 0)
    {
        my $outputFile = new Loghandler($outputFileRemove);
        $outputFile->appendLine($removeOutput);
        $outputFileRemove =~ s/$archivefolder//;
        push (@ret, $outputFileRemove);
    }

    $log->addLine(Dumper(\@ret));
    return \@ret;
}

sub _getMarcFromCloudlibrary
{
    my $baseURL = $conf{"server"};
    my $library = $conf{"login"};
    my $apikey = $conf{"password"};
    my $startDate = @_[0];
    my $endDate = @_[1];
    my $recordID = @_[2];
    my $uri = "/cirrus/library/$library/data/marc";

    updateJob("Processing","Starting API connection");
    my $records = 1;
    my $offset = 1;
    my $resultGobLimit = 50;
    my @allRecords = ();
    my $stop = 0;
    while( ($records && !$stop) )
    {
        $records = 0;
        my $query = "?startdate=$startDate&enddate=$endDate&offset=$offset&limit=$resultGobLimit";
        $uri .= "/$recordID" if $recordID;
        $query = '' if $recordID;
        my $date = DateTime->now(time_zone => "GMT");

        my $dayofmonth = $date->day();
        $dayofmonth = '0'.$dayofmonth if length($dayofmonth) == 1;

        my $timezonestring = $date->time_zone_short_name();
        $timezonestring =~ s/UTC/GMT/g;
        my $dateString = $date->day_abbr().", ".
        $dayofmonth." ".
        $date->month_abbr()." ".
        $date->year()." ".
        $date->hms()." ".
        $timezonestring;

        my $digest = hmac_sha256_base64($dateString."\nGET\n".$uri.$query, $apikey);
        while (length($digest) % 4) {
            $digest .= '=';
        }

        my $client = REST::Client->new({
             host    => $baseURL,
             cert    => $cert,
             key     => $certKey,
         });
        updateJob("Processing","API query: GET, $uri$query");
        my $answer = $client->request('GET',$uri.$query,'',
            {
            '3mcl-Datetime'=>$dateString,
            '3mcl-Authorization'=>"3MCLAUTH library.$library:".$digest,
            '3mcl-apiversion'=>"3mcl-apiversion: 2.0"
            }
        );
        my $allXML = $answer->responseContent();
        # $log->addLine($allXML);
        my @unparsableEntities = ("ldquo;","\"","rdquo;","\"","ndash;","-","lsquo;","'","rsquo;","'","mdash;","-","supl;","");
        my $i = 0;
        while(@unparsableEntities[$i])
        {
            my $loops = 0;
            while(index($allXML,@unparsableEntities[$i]) != -1)
            {
                my $index = index($allXML,@unparsableEntities[$i]);
                my $find = @unparsableEntities[$i];
                my $findlength = length($find);
                my $first = substr($allXML,0,$index);
                $index+=$findlength;
                my $last = substr($allXML,$index);
                my $rep = @unparsableEntities[$i+1];
                #$log->addLine("Found at $index and now replacing $find");
                $allXML = $first.$rep.$last;
                # $allXML =~ s/(.*)&?$find(.*)/$1$rep$2/g;
                #$log->addLine("just replaced\n$allXML");
                $loops++;
                exit if $loops > 15;
            }
            $i+=2;
        }
        #$log->addLine("after scrubbed\n$allXML");
        $allXML = decode_entities($allXML);
        $allXML =~ s/(.*)<\?xml[^>]*>(.*)/$1$2/;
        $allXML =~ s/(<marc:collection[^>]*>)(.*)/$2/;
        $allXML =~ s/(.*)<\/marc:collection[^>]*>(.*)/$1$2/;
        my @records = split("marc:record",$allXML);
        updateJob("Processing","Received ".$#records." records");
        foreach (@records)
        {
            # Doctor the xml
            my $thisXML = $_;
            $thisXML =~ s/^>//;
            $thisXML =~ s/<\/?$//;
            $thisXML =~ s/<(\/?)marc\:/<$1/g;
            if(length($thisXML) > 0)
            {
                $thisXML =~ s/>\s+</></go;
                $thisXML =~ s/\p{Cc}//go;
                $thisXML = OpenILS::Application::AppUtils->entityize($thisXML);
                $thisXML =~ s/[\x00-\x1f]//go;
                $thisXML =~ s/^\s+//;
                $thisXML =~ s/\s+$//;
                $thisXML =~ s/<record><leader>/<leader>/;
                $thisXML =~ s/<collection/<record/;
                $thisXML =~ s/<\/record><\/collection>/<\/record>/;

                my $record = MARC::Record->new_from_xml("<record>".$thisXML."</record>");
                push (@allRecords, $record);
                $offset++;
            }
        }
        $records = 1 if($#records > -1);
        #$stop = 1 if($#allRecords > 200);
        $log->addLine("records = $records and stop = $stop");
        $log->addLine("Record Count: ".$#allRecords);
    }
    return \@allRecords;

}

sub getMarcFromMarciveHTTPS
{

    if( (length($conf{'server'}) < 4) || (length($conf{'remotefolder'}) < 4) )
    {
        $log->addLogLine("Marcive settings for 'server' and 'remotefolder' are insufficient: '".$conf{'server'}."' / '".$conf{'remotefolder'}."'");
        exit;
    }
    my $server = $conf{'server'}.'&s='.$conf{'remotefolder'};
    my @ret = ();

    $log->addLogLine("**********MARCIVE HTTPS starting -> $server");
    my @interestingFiles = ();

    my $rowNum = 0;
    $log->addLine(pQuery->get($server)->content);
    pQuery($server)->find("tr")->each(sub {
        if($rowNum > 0) ## Skipping the title row
        {
            my $i = shift;
            my $row = $_;
            my $colNum = 0;
            my %file = (filename => '', size => '', downloadlink => '');
            pQuery("td",$row)->each(sub {
                shift;
                if($colNum == 0) # filename
                {
                    pQuery("a",$_)->each(sub {
                        my $a_html = pQuery($_)->toHtml;
                        shift;
                        $file{'filename'} = pQuery($_)->text();
                        $a_html =~ s/.*?href=['"]([^'"]*)['"].*$/$1/g;
                        $file{'downloadlink'} = $a_html;
                    });
                }
                elsif($colNum == 1)
                {
                    my @t = split(/\s/,pQuery($_)->text());
                    $file{'size'} = @t[0];
                }
                $colNum++;
            });
            push (@interestingFiles, \%file);
        }
        $rowNum++;
    });

    $log->addLine(Dumper(\@interestingFiles)) if $debug;
    foreach(@interestingFiles)
    {
        my %file = %{$_};
        my $filename = $file{'filename'};
        my $download = decideToDownload($filename);

        if($download)
        {
            if(-e "$archivefolder/"."$filename")
            {
                my $size = stat("$archivefolder/"."$filename")->size; #[7];
                my $rsize = $file{'size'};
                $log->addLine("Local: $size") if $debug;
                $log->addLine("remot: $rsize") if $debug;
                if($size ne $rsize)
                {
                    $log->addLine("Local: $size") if $debug;
                    $log->addLine("remot: $rsize") if $debug;
                    $log->addLine("$archivefolder/"."$filename differes in size");
                    unlink("$archivefolder/"."$filename");
                }
                else
                {
                    $log->addLine("skipping $filename");
                    $download=0;
                }
            }
            else
            {
                $log->addLine("NEW $filename");
            }
            if($download)
            {
                my $path = $archivefolder."/".$filename;
                $path = substr($path,0,rindex($path,'/'));
                if(!-d $path)
                {
                    $log->addLine("$path doesnt exist - creating directory");
                    make_path($path, {
                    verbose => 0,
                    mode => 0755,
                    });
                }
                $path = $archivefolder."/".$filename;
                getstore($file{'downloadlink'}, $path);
                if(-e $path)
                {
                    push (@ret, "$filename");
                }
                else
                {
                    $log->addLogLine("Unable to download ".$file{'downloadlink'});
                }
            }
        }
    }
    $log->addLogLine("********** MARCIVE HTTPS DONE ***************");
    $log->addLine(Dumper(\@ret));
    return \@ret;
}

sub ftpRecurse
{
    my $ftpOb = @_[0];
    my @interestingFiles = @{@_[1]};
    # return \@interestingFiles if($#interestingFiles > 2);

    my @remotefiles = $ftpOb->ls();
    foreach(@remotefiles)
    {
        # $log->addLine("pwd = ".$ftpOb->pwd." cwd into $_");
        if($ftpOb->cwd($ftpOb->pwd."/".$_)) #it's a directory
        {
            #let's go again
            @interestingFiles = @{ftpRecurse($ftpOb,\@interestingFiles)} if $recurseFTP;
            #$log->addLine("going to parent dir from = ".$ftpOb->pwd);
            $ftpOb->cdup();
        }
        else #it's not a directory
        {
            my $pwd = $ftpOb->pwd;
            if(decideToDownload($_))
            {
                my $full = $pwd."/".$_;
                push (@interestingFiles , $full);
            }
        }
    }
    return \@interestingFiles;
}

sub decideToDownload
{
    my $filename = @_[0];
    $filename = lc($filename);
    my $download = 1;
    my @onlyProcess = ();
    if($conf{'onlyprocess'}) # This is optional but if present, very restrictive
    {
        my $go = 0;
        $go = compareStringToArray($filename,$conf{'onlyprocess'});
        $log->addLogLine("Ignoring file $filename because it didn't contain one of these: ".$conf{'onlyprocess'}) if !$go;
        return 0 if !$go;
    }

    $download = 0 if ( compareStringToArray($filename,$conf{'ignorefiles'}) );
    $log->addLogLine("Ignoring file $filename due to a match in ".$conf{'ignorefiles'}) if !$download;
    return $download;
}

sub compareStringToArray
{
    my $wholeString = @_[0];
    my $tphrases = @_[1];
    return 0 if !$tphrases;
    my @phrases = split(/\s/,$tphrases);
    my $ret = 0;
    $wholeString = lc $wholeString;
    foreach(@phrases)
    {
        my $phrase = lc $_;
        return 1 if ($wholeString =~ /$phrase/);
    }
    return $ret;
}

sub add9
{
    my $marc = @_[0];
    my @recID = $marc->field('856');
    if(@recID)
    {
        #$marc->delete_fields( @recID );
        for my $rec(0..$#recID)
        {
            #print Dumper(@recID[$rec]);
            for my $t(0.. $#shortnames)
            {
                my @sub3 = @recID[$rec]->subfield( '3' );
                my $ind2 = @recID[$rec]->indicator(2);
                my $ignore=0;
                foreach(@sub3)
                {
                    if(lc($_) eq 'excerpt')
                    {
                        $ignore=1;
                    }
                }
                if($ind2 ne '0')
                {
                    $ignore=1;
                }
                if(!$ignore)
                {
                    my @s7 = @recID[$rec]->subfield( '7' );
                    if($#s7==-1)
                    {
                        @recID[$rec]->add_subfields('7'=>$importBIBTagName);
                    }
                    my @subfields = @recID[$rec]->subfield( '9' );
                    my $shortnameexists=0;
                    for my $subs(0..$#subfields)
                    {
                    #print "Comparing ".@subfields[$subs]. " to ".@shortnames[$t]."\n";
                        if(@subfields[$subs] eq @shortnames[$t])
                        {
                            # print "Same!\n";
                            $shortnameexists=1;
                        }
                    }
                    #print "shortname exists: $shortnameexists\n";
                    if(!$shortnameexists)
                    {
                        #print "adding ".@shortnames[$t]."\n";
                        @recID[$rec]->add_subfields('9'=>@shortnames[$t]);
                    }
                }
            }
        }
    }
    return $marc;
}

## All calls to this function have been removed because the database functions seem to babysit this pretty well
sub removeOldCallNumberURI
{
    my $bibid = @_[0];

    my $uriids = '';
    my $query = "select uri from asset.uri_call_number_map WHERE call_number in
    (
        SELECT id from asset.call_number WHERE record = $bibid AND label = \$\$##URI##\$\$
    )";
    updateJob("Processing","$query");
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my @row = @{$_};
        $uriids.=@row[0].",";
    }
    $uriids = substr($uriids,0,-1);

    my $query = "
    DELETE FROM asset.uri_call_number_map WHERE call_number in
    (
        SELECT id from asset.call_number WHERE record = $bibid AND label = \$\$##URI##\$\$
    )
    ";
    updateJob("Processing","$query");
    $dbHandler->update($query);
    $query = "
    DELETE FROM asset.uri_call_number_map WHERE call_number in
    (
        SELECT id from asset.call_number WHERE  record = $bibid AND label = \$\$##URI##\$\$
    )";
    updateJob("Processing","$query");
    $dbHandler->update($query);

    if(length($uriids) > 0)
    {
        $query = "DELETE FROM asset.uri WHERE id in ($uriids)";
    updateJob("Processing","$query");
        $dbHandler->update($query);
    }
    $query = "
    DELETE FROM asset.call_number WHERE  record = $bibid AND label = \$\$##URI##\$\$
    ";
    updateJob("Processing","$query");
    $dbHandler->update($query);
    $query = "
    DELETE FROM asset.call_number WHERE  record = $bibid AND label = \$\$##URI##\$\$
    ";
    updateJob("Processing","$query");
    $dbHandler->update($query);

}

sub recordAssetCopyMove
{
    my $oldbib = @_[0];
    my $newbib = @_[1];
    my $overdriveMatchString = @_[2];
    my $statusID = @_[2];
    my $query = "select id from asset.copy where call_number in(select id from asset.call_number where record in($oldbib) and label!=\$\$##URI##\$\$) and not deleted";
    my @cids;
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my @row = @{$_};
        push(@cids,@row[0]);
    }

    foreach(@cids)
    {
        print "There were asset.copies on $oldbib even after attempting to put them on a deduped bib\n";
        $log->addLine("\t$oldbib\tContained physical Items");
         $query = "
        INSERT INTO e_bib_import.item_reassignment(copy,prev_bib,target_bib,statusid,job)
        VALUES ($_,$oldbib,$newbib,$statusID, $jobid)";
        $log->addLine("$query");
        updateJob("Processing","recordAssetCopyMove  $query");
        $dbHandler->update($query);
    }
}

sub recordBIBMARCChanges
{
    my $bibID = @_[0];
    my $oldMARC = @_[1];
    my $newMARC = @_[2];
    my $newRecord = @_[3];

     my $query = "
    INSERT INTO e_bib_import.bib_marc_update(record,prev_marc,changed_marc,job,new_record)
    VALUES (\$1,\$2,\$3,\$4,\$5)";
    my @values = ($bibID,$oldMARC,$newMARC,$jobid,$newRecord);
    $dbHandler->updateWithParameters($query,\@values);
}

sub mergeBIBs
{

    my $oldbib = @_[0];
    my $newbib = @_[1];
    my $overdriveMatchString = @_[2];
    my $statusID = @_[3];

    return 0 if ($oldbib = $newbib); #short circuit if we are merging the same bibs together

    updateJob("Processing","mergeBIBs oldbib: $oldbib newbib=$newbib overdriveMatchString=$overdriveMatchString");
    $log->addLine("mergeBIBs oldbib: $oldbib newbib=$newbib overdriveMatchString=$overdriveMatchString") if $debug;

    recordAssetCopyMove($oldbib,$newbib,$overdriveMatchString,$statusID);
    my $query = "INSERT INTO e_bib_import.bib_merge(leadbib,subbib,statusid,job) VALUES($newbib,$oldbib,$statusID,$jobid)";
    $log->addLine("MERGE:\t$newbib\t$oldbib") if $debug;

    updateJob("Processing","mergeBIBs  $query");

    $log->addLine($query);
    $dbHandler->update($query);
    #print "About to merge assets\n";
    $query = "SELECT asset.merge_record_assets($newbib, $oldbib)";

    updateJob("Processing","mergeBIBs  $query");

    $log->addLine($query);
    $dbHandler->query($query);
    #print "Merged\n";
}

sub reCalcSha
{
    $bibsourceid = getbibsource();
    my $query = "
    SELECT bre.marc,sha1_full,sha1_mid,bre.id
    FROM
    biblio.record_entry bre
    JOIN e_bib_import.bib_sha1 eibs ON (eibs.bib=bre.id and not bre.deleted)
    WHERE
    eibs.bib_source = $bibsourceid";
    $log->addLine($query);
    my @results = @{$dbHandler->query($query)};
    my $count = 0;
    print "Looping " . $#results . " records for sha recalculation\n";
    foreach(@results)
    {
        my @row = @{$_};
        my $marc = @row[0];
        $marc =~ s/(<leader>.........)./${1}a/;
        $marc = MARC::Record->new_from_xml($marc);
        my $shafull_db = @row[1];
        my $shamid_db = @row[2];
        my $id = @row[3];
        my $shafull_recalc = calcSHA1($marc);
        my $shamid_recalc = calcSHA1($marc,1);
        if( ($shafull_db ne $shafull_recalc) || ($shamid_db ne $shamid_recalc) )
        {
            $query = "UPDATE e_bib_import.bib_sha1 SET sha1_full = \$1 , sha1_mid = \$2 WHERE bib = \$3";
            my @vals = ($shafull_recalc, $shamid_recalc, $id);
            $log->addLine($query."\n". Dumper(\@vals));
            $dbHandler->updateWithParameters($query, \@vals);
            undef @vals;
        }
        undef $marc;
        undef $shafull_db;
        undef $shamid_db;
        undef $shafull_recalc;
        undef $shamid_recalc;
        $count++;
        print "$count done\n" if($count % 1000 == 0);
    }

    exit;
}

sub calcSHA1
{
    my $marc = shift;
    my $babySHA = shift;
    my $sha1 = Digest::SHA1->new;
    $sha1->add(  length(getsubfield($marc,'007',''))>6 ? substr( getsubfield($marc,'007',''),0,6) : '' );
    $sha1->add(getsubfield($marc,'245','h'));
    $sha1->add(getsubfield($marc,'001','')) if !$babySHA;
    $sha1->add(getsubfield($marc,'245','a'));
    $sha1->add(getsubfield($marc,'245','b'));
    $sha1->add(getsubfield($marc,'100','a'));
    return $sha1->hexdigest;
}

sub getsubfield
{
    my $marc = @_[0];
    my $tag = @_[1];
    my $subtag = @_[2];
    my $ret;
    #print "Extracting $tag $subtag\n";
    if($marc->field($tag))
    {
        if($tag<10)
        {
            #print "It was less than 10 so getting data\n";
            $ret = $marc->field($tag)->data();
        }
        elsif($marc->field($tag)->subfield($subtag))
        {
            $ret = $marc->field($tag)->subfield($subtag);
        }
    }
    #print "got $ret\n";
    $ret = utf8::is_utf8($ret) ? Encode::encode_utf8($ret) : $ret;
    return $ret;
}

sub removeBibsEvergreen
{
    my $statusID = @_[0];
    my $title = @_[1]; # will be filled in with the ISBN number if it's a removal via ISBN
    my $z01 = @_[2];
    my $sha1 = @_[3];
    my $marcXML = @_[4];
    my $removalsViaMARC = @_[5];

    print "removeBibsEvergreen removalsViaMARC = $removalsViaMARC\n" if $debug;
    my $query;

    my $r =0;
    my $removed = 0;
    my $loops = 0;

    updateJob("Processing","removeBibsEvergreen");

    #print "Working on removeBibsEvergreen\n";

    if( $removalsViaMARC )
    {
        my $marc = $marcXML;
        $marc =~ s/(<leader>.........)./${1}a/;
        $marc = MARC::Record->new_from_xml($marc);
        my $bibid = findRecord($marc, $sha1, $z01);
        if($bibid!=-1) #already exists
        {
            attemptRemoveBibs($bibid, $statusID);
        }
        else
        {
            $query = "update e_bib_import.import_status set status = \$1 , processed = true, row_change_time = now() where id = \$2";
            my @values = ('No matching bib in DB', $statusID);
            $dbHandler->updateWithParameters($query,\@values);
        }
        $loops++;
    }
    else
    {
        my @marcOutputRecordsRemove = ();
        $log->addLine("Removing bibs via ISBN");
        my @ids = @{findMatchingISBN($title)};
        push(@marcOutputRecordsRemove, @ids);
        foreach(@marcOutputRecordsRemove)
        {
            my $bib = $_;
            my @temp = @{$bib};
            my $marc = @temp[1];
            my $title = getsubfield($marc,'245','a');
            $log->addLine("Removing $title ID=".@temp[0]);
            my @pass = ([@{$bib}]);
            attemptRemoveBibs(\@pass, $statusID);
            $loops++;
        }
        if($#marcOutputRecordsRemove == -1)
        {
            $query = "update e_bib_import.import_status set processed = true , status = \$1 , row_change_time = now() where id = \$2";
            my @values = ("No matching bib in DB", $statusID);
            $dbHandler->updateWithParameters($query,\@values);
        }
    }
}

sub importMARCintoEvergreen
{
    my $statusID = @_[0];
    my $title = @_[1];
    my $z01 = @_[2];
    my $sha1 = @_[3];
    my $marcXML = @_[4];

    my $query;

    updateJob("Processing","importMARCintoEvergreen");
    my $marc = $marcXML;
    $marc =~ s/(<leader>.........)./${1}a/;
    $marc = MARC::Record->new_from_xml($marc);

    $query = "update e_bib_import.import_status set status = \$\$processing\$\$, row_change_time = now() where id=$statusID";
    my @vals = ();
    $dbHandler->updateWithParameters($query,\@vals);

    updateJob("Processing","updating 245h and 856z");
    $marc = readyMARCForInsertIntoDB($marc);
    my $bibid=-1;
    my $bibid = findRecord($marc, $sha1, $z01);

    if($bibid!=-1) #already exists so update the marc
    {
        chooseWinnerAndDeleteRest($bibid, $sha1, $marc, $title, $statusID);
    }
    elsif( !$conf{'do_not_import_new'} )  ##need to insert new bib instead of update
    {

        my $starttime = time;
        my $max = getEvergreenMax();
        my $thisXML = convertMARCtoXML($marc);
        my @values = ($thisXML);
        $query = "INSERT INTO BIBLIO.RECORD_ENTRY(fingerprint,last_xact_id,marc,quality,source,tcn_source,owner,share_depth) VALUES(null,'IMPORT-$starttime',\$1,null,$bibsourceid,\$\$$importBIBTagNameDB-script $sha1\$\$,null,null)";
        $log->addLine($query);
        my $res = $dbHandler->updateWithParameters($query,\@values);
        #print "$res";
        my $newmax = getEvergreenMax("$importBIBTagNameDB-script $sha1", $max); # Get a more accurate ID in case the DB is busy right now
        if( ($newmax != $max) && ($newmax > 0) )
        {
            $log->addLine("$newmax\thttp://$domainname/eg/opac/record/$newmax?locg=1;expand=marchtml#marchtml");
            $query = "update e_bib_import.import_status set status = \$1 , bib = \$2 , processed = true, row_change_time = now() where id = \$3";
            @values = ('inserted', $newmax, $statusID);
            $dbHandler->updateWithParameters($query,\@values);
            updateDBSHA1($sha1, $newmax, $bibsourceid);
        }
        else
        {
            $query = "update e_bib_import.import_status set status = \$1 , processed = true, row_change_time = now() where id = \$2";
            @values = ('failed to insert', $statusID);
            $dbHandler->updateWithParameters($query,\@values);
        }
        undef $starttime;
        undef $max;
        undef $thisXML;
        undef @values;
        undef $newmax;
    }
    else
    {
        $log->addLine("Skipping $statusID because it didn't match anything and script is configure to NOT IMPORT");
        $query = "update e_bib_import.import_status set status = \$1 , bib = \$2 , processed = true, row_change_time = now() where id = \$3";
        my @values = ('skipped', -1, $statusID);
        $dbHandler->updateWithParameters($query,\@values);
    }
    undef $statusID;
    undef $title;
    undef $z01;
    undef $sha1;
    undef $statusID;
    undef $marcXML;
    undef $query;
    undef $marc;
}

sub importAuthority
{
    my $inputFile = @_[0];

    updateJob("Processing","importAUTHORITYintoEvergreen");

    my $rowID = 0;
    my $query = "select id from e_bib_import.import_status where job = $jobid and filename = \$\$$inputFile\$\$";
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
         my @row = @{$_};
         $rowID = @row[0];
         last;
    }
    return 0 if $rowID == 0; # This should exist, if not, play it safe and exit

    $inputFile  = "$archivefolder/$inputFile";

    # Increase batch ID based upon how many came before
    my $batchID = 0;
    $query = "select count(*) from e_bib_import.import_status where job=$jobid and status=\$\$finished\$\$";
    $log->addLogLine($query);
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my @row = @{$_};
        $batchID = @row[0];
    }

    my @previousBatchNames = ();

    $query = "select marc_xml from e_bib_import.import_status where job=$jobid and type=\$\$importauth\$\$";
    $log->addLogLine($query);
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my @row = @{$_};
        push @previousBatchNames, @row[0];
    }

    my $fullBatchName = "auth$jobid" . "_$batchID";

    my $alreadyused = 1;

    while($alreadyused)
    {
        $alreadyused = 0;
        foreach(@previousBatchNames)
        {
            $alreadyused = 1 if($_ eq $fullBatchName);
            $fullBatchName.='_0' if($_ eq $fullBatchName);
        }
    }

    $query = "update e_bib_import.import_status set marc_xml = \$1 , row_change_time = now() where id = \$2";
    my @values = ($fullBatchName, $rowID);
    $dbHandler->updateWithParameters($query,\@values);

    # we are going to use the eg_staged_bib_overlay tool to import the authority records. This tool needs to be available in the directory specified in the config file
    my $bashOutputFile = $conf{"tempspace"}."/authload$jobid";
    my $execScript = $conf{"eg_staged_bib_overlay_dir"}."/eg_staged_bib_overlay";

    $query = "update e_bib_import.import_status set status = \$1 , row_change_time = now() where id = \$2";
    my @values = ('running stage_auths', $rowID);
    $dbHandler->updateWithParameters($query,\@values);

    my $cmd = "$execScript --schema auth_load --batch $fullBatchName --db ".$conf{"db"}." --dbuser ".$conf{"dbuser"}." --dbhost ".$conf{"dbhost"}." --dbpw ".$conf{"dbpass"}." --action stage_auths $inputFile > $bashOutputFile";
    $log->addLogLine($cmd);
    system($cmd);

    $query = "update e_bib_import.import_status set status = \$1 , row_change_time = now() where id = \$2";
    my @values = ('running match_auths', $rowID);
    $dbHandler->updateWithParameters($query,\@values);

    $cmd = "$execScript --schema auth_load --batch $fullBatchName --db ".$conf{"db"}." --dbuser ".$conf{"dbuser"}." --dbhost ".$conf{"dbhost"}." --dbpw ".$conf{"dbpass"}." --action match_auths >> $bashOutputFile";
    $log->addLogLine($cmd);
    system($cmd);

    $query = "update e_bib_import.import_status set status = \$1 , row_change_time = now() where id = \$2";
    my @values = ('running load_new_auths', $rowID);
    $dbHandler->updateWithParameters($query,\@values);

    $cmd = "$execScript --schema auth_load --batch $fullBatchName --db ".$conf{"db"}." --dbuser ".$conf{"dbuser"}." --dbhost ".$conf{"dbhost"}." --dbpw ".$conf{"dbpass"}." --action load_new_auths >> $bashOutputFile";
    $log->addLogLine($cmd);
    system($cmd);

    $query = "update e_bib_import.import_status set status = \$1 , row_change_time = now() where id = \$2";
    my @values = ('running overlay_auths_stage1', $rowID);
    $dbHandler->updateWithParameters($query,\@values);

    $cmd = "$execScript --schema auth_load --batch $fullBatchName --db ".$conf{"db"}." --dbuser ".$conf{"dbuser"}." --dbhost ".$conf{"dbhost"}." --dbpw ".$conf{"dbpass"}." --action overlay_auths_stage1 >> $bashOutputFile";
    $log->addLogLine($cmd);
    system($cmd);

    $query = "update e_bib_import.import_status set status = \$1 , row_change_time = now() where id = \$2";
    my @values = ('running overlay_auths_stage2', $rowID);
    $dbHandler->updateWithParameters($query,\@values);

    $cmd = "$execScript --schema auth_load --batch $fullBatchName --db ".$conf{"db"}." --dbuser ".$conf{"dbuser"}." --dbhost ".$conf{"dbhost"}." --dbpw ".$conf{"dbpass"}." --action overlay_auths_stage2 >> $bashOutputFile";
    $log->addLogLine($cmd);
    system($cmd);

    $query = "update e_bib_import.import_status set status = \$1 , row_change_time = now() where id = \$2";
    my @values = ('running overlay_auths_stage3', $rowID);
    $dbHandler->updateWithParameters($query,\@values);

    $cmd = "$execScript --schema auth_load --batch $fullBatchName --db ".$conf{"db"}." --dbuser ".$conf{"dbuser"}." --dbhost ".$conf{"dbhost"}." --dbpw ".$conf{"dbpass"}." --action overlay_auths_stage3 >> $bashOutputFile";
    $log->addLogLine($cmd);
    system($cmd);

    $query = "update e_bib_import.import_status set status = \$1 , row_change_time = now() where id = \$2";
    my @values = ('running link_auth_auth', $rowID);
    $dbHandler->updateWithParameters($query,\@values);

    $cmd = "$execScript --schema auth_load --batch $fullBatchName --db ".$conf{"db"}." --dbuser ".$conf{"dbuser"}." --dbhost ".$conf{"dbhost"}." --dbpw ".$conf{"dbpass"}." --action link_auth_auth >> $bashOutputFile";
    $log->addLogLine($cmd);
    system($cmd);

    $query = "update e_bib_import.import_status set status = \$1 , processed = true, row_change_time = now() where id = \$2";
    my @values = ('finished', $rowID);
    $dbHandler->updateWithParameters($query,\@values);
}

sub attemptRemoveBibs
{

    my @list = @{@_[0]};
    my $statusID = @_[1];

    # Reset the status column to be blank as we loop through all of the related bibs, appending results to status
    my $query = "update e_bib_import.import_status set status = \$\$\$\$ , row_change_time = now() where id = \$1";
    my @values = ($statusID);
    $dbHandler->updateWithParameters($query,\@values);
    foreach(@list)
    {
        my @attrs = @{$_};
        my $id = @attrs[0];
        my $marcobj = @attrs[1];
        my $score = @attrs[2];
        my $marcxml = @attrs[3];
        my $answer = decideToDeleteOrRemove9($marcobj);
        if( $answer eq '1' )
        {
            my $query = "SELECT ID,BARCODE FROM ASSET.COPY WHERE CALL_NUMBER IN
            (SELECT ID FROM ASSET.CALL_NUMBER WHERE RECORD=$id) AND NOT DELETED";
            $log->addLine($query);
            my @results = @{$dbHandler->query($query)};
            if($#results > -1) # There are non-deleted copies attached
            {
                $query = "update e_bib_import.import_status set status = status || \$\$[ $id - \$\$ || \$1 || \$\$]\$\$ , bib = \$2 , row_change_time = now() where id = \$3";
                my @values = ('failed to removed bib due to copies attached', $id, $statusID);
                $dbHandler->updateWithParameters($query,\@values);
            }
            if($#results == -1)
            {
                my $query = "UPDATE BIBLIO.RECORD_ENTRY SET DELETED=\$\$t\$\$ WHERE ID = \$1";
                $log->addLine($query);
                my @values = ($id);
                my $res = $dbHandler->updateWithParameters($query,\@values);
                if($res)
                {
                    $query = "update e_bib_import.import_status set status = status || \$\$[ $id - \$\$ || \$1 || \$\$]\$\$ , bib = \$2 , row_change_time = now() where id = \$3";
                    @values = ('removed bib', $id, $statusID);
                    $dbHandler->updateWithParameters($query,\@values);
                }
                else
                {
                    $query = "update e_bib_import.import_status set status = status || \$\$[ $id - \$\$ || \$1 || \$\$]\$\$ , bib = \$2 , row_change_time = now() where id = \$3";
                    @values = ('failed to removed bib', $id, $statusID);
                    $dbHandler->updateWithParameters($query,\@values);
                }
            }
        }
        else
        {
            my $finalMARCXML = convertMARCtoXML($answer);
            recordBIBMARCChanges($id, $marcxml, $finalMARCXML,0);
            my @values = ($finalMARCXML);
            my $query = "UPDATE BIBLIO.RECORD_ENTRY SET marc=\$1 WHERE ID=$id";

            updateJob("Processing","chooseWinnerAndDeleteRest   $query");

            $log->addLine($query);
            $log->addLine("$id\thttp://$domainname/eg/opac/record/$id?locg=1;expand=marchtml#marchtml\thttp://$domainname/eg/opac/record/$id?locg=1;expand=marchtml#marchtml\t0");
            my $res = $dbHandler->updateWithParameters($query,\@values);
            if($res)
            {
                $query = "update e_bib_import.import_status set status = status || \$\$[ $id - \$\$ || \$1 || \$\$]\$\$ , bib = \$2 , row_change_time = now() where id = \$3";
                @values = ('removed related 856', $id, $statusID);
                $dbHandler->updateWithParameters($query,\@values);
            }
            else
            {
                $query = "update e_bib_import.import_status set status = status || \$\$[ $id - \$\$ || \$1 || \$\$]\$\$ , bib = \$2 , row_change_time = now() where id = \$3";
                @values = ('failed to remove related 856', $id, $statusID);
                $dbHandler->updateWithParameters($query,\@values);
            }
        }
    }
    $query = "update e_bib_import.import_status set processed = true , row_change_time = now() where id = \$1";
    @values = ($statusID);
    $dbHandler->updateWithParameters($query,\@values);
}

sub decideToDeleteOrRemove9
{
    my $marc = @_[0];
    my @eight56s = $marc->field("856");
    my @eights;
    my $original856 = $#eight56s + 1;

    my %urls;
    my $nonMatch = 0;
    foreach(@eight56s)
    {
        my $thisField = $_;
        my $ind2 = $thisField->indicator(2);
        if($ind2 eq '0')
        {
            my @ninposes = ();
            my $poses=0;
            #deleting subfields requires knowledge of what position among all of the subfields they reside.
            #so we have to record at what positions each of the 9's are ahead of time.
            foreach($thisField->subfields())
            {
                my @f = @{$_};
                if(@f[0] eq '9')
                {
                    push (@ninposes, $poses);
                }
                $poses++;
            }
            my @nines = $thisField->subfield("9");
            my @delete9s = ();
            my $ninePos = 0;
            my $nonMatchThis856 = 0;
            foreach(@nines)
            {
                my $looking = $_;
                my $found = 0;
                foreach(@shortnames)
                {
                    if($looking eq $_)
                    {
                        $found=1;
                    }
                }
                if($found)
                {
                    push(@delete9s, @ninposes[$ninePos]);
                }
                else
                {
                    $nonMatch=1;
                    $nonMatchThis856 = 1;
                }
                $ninePos++;
            }
            if(!$nonMatchThis856)
            {
                #Just delete the whole field because it only contains these 9's
                $marc->delete_field($thisField);
            }
            else
            {
                #Some of the 9's do not belong to this group, so we just want to delete ours
                #and preserve the record
                $thisField->delete_subfield(code=> '9', 'pos' => \@delete9s) if ($#delete9s > -1);
            }
            undef @ninposes;
            undef @delete9s;
            undef $nonMatchThis856;
        }

    }
    if(!$nonMatch) #all of the 9s on this record belong to this group and no one else, time to delete the record
    {
        return 1;
    }
    #There were some 9s for other groups, just remove ours and preserve the record
    return $marc;

}

sub findMatchingISBN
{
    my $isbn = @_[0];
    my @ret = ();

    my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE
    tcn_source~\$\$$importBIBTagNameDB-script\$\$ AND
    source=$bibsourceid AND
    NOT DELETED AND
    ID IN(
    select source from metabib.identifier_field_entry
    WHERE
    index_vector  @@ to_tsquery(\$\$$isbn\$\$)
    )
    ";
    # $log->addLine($query);
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        my $id = @row[0];
        my $marc = @row[1];
        my $marcobj = $marc;
        $marcobj =~ s/(<leader>.........)./${1}a/;
        my $marcobj = MARC::Record->new_from_xml($marcobj);
        my $score = 0;
        my @arr = ($id,$marcobj,$score,$marc);
        push (@ret, [@arr]);
    }
    return \@ret;
}

sub chooseWinnerAndDeleteRest
{
    my @list = @{@_[0]};
    my $sha1 = @_[1];
    my $newMarc = @_[2];
    my $title = @_[3];
    my $statusID = @_[4];
    my $chosenWinner = 0;
    my $bestScore=0;
    my $finalMARC;
    my $i=0;
    my $winnerBibID;
    my $winnerOGMARCxml;
    my $matchnum = $#list+1;
    my $overdriveMatchString = $newMarc->field('001') ? $newMarc->field('001')->data() : '';
    foreach(@list)
    {
        my @attrs = @{$_};
        my $id = @attrs[0];
        my $score = @attrs[2];
        my $marcxml = @attrs[3];
        if($score>$bestScore)
        {
            $bestScore=$score;
            $chosenWinner=$i;
            $winnerBibID = $id;
            $winnerOGMARCxml = $marcxml;
        }
        $i++;
        undef @attrs;
        undef $id;
        undef $score;
        undef $marcxml;
    }

    if($conf{'import_as_is'})
    {
        $finalMARC = $newMarc;
    }
    else
    {
        $finalMARC = @{@list[$chosenWinner]}[1];
        $i=0;
        #
        # This loop is merging all of the existing bibs in the database into the winning bib ID (in the database)
        # And before it merges them, it soaks up the 856's from the about-to-be deleted bib into $finalMARC
        #
        foreach(@list)
        {
            my @attrs = @{$_};
            my $id = @attrs[0];
            my $marc = @attrs[1];
            my $marcxml = @attrs[3];
            if($i!=$chosenWinner)
            {
                $finalMARC = mergeMARC856($finalMARC, $marc);
                mergeBIBs($id, $winnerBibID, $overdriveMatchString, $statusID);
            }
            $i++;
            undef @attrs;
            undef $id;
            undef $marc;
            undef $marcxml;
        }
        # Marc manipulations need to be ran upon the target bib in the DB as well.
        $finalMARC = readyMARCForInsertIntoDB($finalMARC);

        # here we prefer the incoming file MARC as "the" marc, but we need the gathered 856's.
        # which is why it's passed as the second argument
        # At this point, the 9's have been added to the newMarc (data from e_bib_import)
        $finalMARC = mergeMARC856($newMarc, $finalMARC);
    }

    my $newmarcforrecord = convertMARCtoXML($finalMARC);
    print "Headed into recordBIBMARCChanges\n" if $debug;
    recordBIBMARCChanges($winnerBibID, $winnerOGMARCxml, $newmarcforrecord, 0);

    my $thisXML = convertMARCtoXML($finalMARC);
    my @values = ();
    my $query = "";
    if($conf{'tcn_source_authority'})
    {
        $query = "UPDATE BIBLIO.RECORD_ENTRY SET tcn_source = \$1 , source = \$2 , marc = \$3  WHERE ID = \$4";
        @values = ("$importBIBTagNameDB-script $sha1", $bibsourceid, $thisXML, $winnerBibID);
    }
    else
    {
        $query = "UPDATE BIBLIO.RECORD_ENTRY SET source = \$1 , marc = \$2  WHERE ID = \$3";
        @values = ($bibsourceid, $thisXML, $winnerBibID);
    }

    $query = "UPDATE BIBLIO.RECORD_ENTRY SET  marc = \$1  WHERE ID = \$2" if $conf{'import_as_is'}; # do not update the source if as is
    @values = ($thisXML, $winnerBibID) if $conf{'import_as_is'}; # do not update the source if as is

    print "Updating MARC XML in DB BIB $winnerBibID\n" if $debug;
    updateJob("Processing","chooseWinnerAndDeleteRest   $query");

    # $log->addLine($thisXML);
    $log->addLine("$winnerBibID\thttp://$domainname/eg/opac/record/$winnerBibID?locg=1;expand=marchtml#marchtml\thttp://$domainname/eg/opac/record/$winnerBibID?locg=1;expand=marchtml#marchtml\t$matchnum");
    my $res = $dbHandler->updateWithParameters($query,\@values);
    #print "$res\n";
    if($res)
    {
        $query = "update e_bib_import.import_status set status = \$1, processed = true , bib = \$2 , row_change_time = now() where id = \$3";
        my @vals = ('matched and overlayed',$winnerBibID,$statusID);
        $dbHandler->updateWithParameters($query,\@vals);
        if($conf{'tcn_source_authority'})
        {
            my @shas = split(/\s/,$sha1);
            for my $i (0..$#shas)
            {
                my $shacol = "sha1_full";
                $shacol = "sha1_mid" if ($i == 1);
                $query = "UPDATE e_bib_import.bib_sha1 SET $shacol = \$1 WHERE bib = \$2 AND bib_source = \$3 AND $shacol != \$4";
                @vals = (@shas[$i], $winnerBibID, $bibsourceid, @shas[$i]);
                $dbHandler->updateWithParameters($query,\@vals);
            }
            undef @shas;
        }
    }
    else
    {
        $query = "update e_bib_import.import_status set status = \$1, processed = true, row_change_time = now() where id = \$2";
        my @vals = ('failed',$statusID);
        $dbHandler->updateWithParameters($query,\@vals);
    }

    undef @list;
    undef $sha1;
    undef $newMarc;
    undef $title;
    undef $statusID;
    undef $chosenWinner;
    undef $bestScore;
    undef $finalMARC;
    undef $i;
}

sub findRecord
{
    my $marcsearch = @_[0];
    my $sha1 = @_[1];
    my $zero01 = @_[2];

    my @ret;
    my $none=1;
    my $foundIDs;
    my $count=0;
    my @shas = split(/\s/,$sha1);
    my $query = "";

    if( $match901c && $marcsearch->subfield('901',"c") )
    {
        $query = "
        select
        bre.id,
        bre.marc from
        biblio.record_entry bre
        where
        bre.id=" . $marcsearch->subfield('901',"c");
        my $fetch = getMatchingMARC($query, 'sha1');
        if(ref $fetch eq 'ARRAY')
        {
            $none = 0;
            @ret = @{dedupeMatchArray(\@ret, $fetch)};
        }
    }
    else
    {
        for my $i (0..$#shas)
        {
            my $shacol = "sha1_full";
            $shacol = "sha1_mid" if $i == 1;

            print "Searching for sha1 match @shas[$i]\n" if $debug;
            $query = "
            select
            bre.id,
            bre.marc from
            biblio.record_entry bre,
            e_bib_import.bib_sha1 ebs
            where
            bre.id=ebs.bib and
            ebs.$shacol = \$sha\$@shas[$i]\$sha\$ and
            ebs.bib_source=$bibsourceid and
            not bre.deleted and
            bre.id > -1
            ";

            $query.="
            union all

            select bre.id,bre.marc
            from
            biblio.record_entry bre left join e_bib_import.bib_sha1 ebs on(ebs.bib=bre.id)
            where
            not bre.deleted and
            bre.id > -1 and
            bre.source=$bibsourceid and
            bre.tcn_source~\$sha\$@shas[$i]\$sha\$ and
            ebs.bib is null
            " if $searchDeepMatch;

            my $fetch = getMatchingMARC($query, 'sha1');
            if(ref $fetch eq 'ARRAY')
            {
                $none = 0;
                @ret = @{dedupeMatchArray(\@ret, $fetch)};
            }
        }
    }

    foreach(@ret)
    {
        my $row = $_;
        my @row = @{$row};
        my $id = @row[0];
        $foundIDs.="$id,";
        $none=0;
        $count++;
    }


    if($zero01)
    {
        # fail safe, so that we don't match a huge number of marc records based upon a super tiny 001
        # We are requiring at least 6 non-whitespace characters to appear in the 001 for matching
        my $z01Check = $zero01;
        $z01Check =~ s/[\s\t]//g;
        if($searchDeepMatch && length($z01Check) > 5)  ## This matches other bibs based upon the vendor's 001 which is usually moved to the 035, hence MARC ~
        {
            $foundIDs = substr($foundIDs,0,-1);
            if(length($foundIDs)<1)
            {
                $foundIDs="-1";
            }
            my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE MARC ~ \$\$$zero01\$\$ and ID not in($foundIDs) and deleted is false ";
            my $fetch = getMatchingMARC($query, '001');
            if(ref $fetch eq 'ARRAY')
            {
                $none = 0;
                my $c = $#ret;
                @ret = @{dedupeMatchArray(\@ret, $fetch)};
                $c = $#ret - $c;
                $count += $c;
            }
        }
    }

    if($none)
    {
        print "Didn't find one\n" if $debug;
        return -1;
    }
    print "Count matches: $count\n" if $debug;
    updateJob("Processing","Count matches: $count");

    return \@ret;
}

sub dedupeMatchArray
{
    my $establishedArr = shift;
    my $incArray = shift;
    my %exists = ();
    if( (ref $incArray eq 'ARRAY') && (ref $establishedArr eq 'ARRAY') )
    {
        my @est = @{$establishedArr};
        my @inc = @{$incArray};
        foreach(@est)
        {
            my $row = $_;
            my @row = @{$row};
            $exists{@row[0]} = 1;
        }
        foreach(@inc)
        {
            my $row = $_;
            my @row = @{$row};
            if( !$exists{@row[0]} )
            {
                $exists{@row[0]} = 1;
                push (@est, [@row]);
            }
        }
        $establishedArr = \@est;
    }

    return $establishedArr;
}

sub getMatchingMARC
{
    my $query = shift;
    my $type = shift;
    my @ret = ();
    updateJob("Processing","$query");
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        my $id = @row[0];
        print "found matching $type: $id\n" if $debug;
        my $marc = @row[1];
        my $prevmarc = $marc;
        $prevmarc =~ s/(<leader>.........)./${1}a/;
        $prevmarc = MARC::Record->new_from_xml($prevmarc);
        my $score = scoreMARC($prevmarc,$log);
        my @matched = ($id,$prevmarc,$score,$marc);
        push (@ret, [@matched]);
    }

    return \@ret if $#ret > -1;
    return 0;
}

sub readyMARCForInsertIntoDB
{
    my $marc = @_[0];
    return $marc if $conf{'import_as_is'};
    $marc = fixLeader($marc);

    # my $before = new Loghandler("/mnt/evergreen/before.txt");
    # my $after = new Loghandler("/mnt/evergreen/after.txt");
    # $before->truncFile($marc->as_formatted);
    while ( (my $type, my $array) = each(%marcEdits) )
    {
        if( (ref $array eq 'ARRAY') && (@{$array}[0]) )
        {
            foreach(@{$array})
            {
                # print Dumper($_);
                if(ref $_ eq 'HASH')
                {   
                    my %thisOne = %{$_};
                    my @def = @{$thisOne{'def'}};
                    my $howmany = $thisOne{'howmany'} || '1';
                    $marc = doMARCEdit(\@def, $marc, $type, $howmany);
                }
            }
        }
    }
    # $after->truncFile($marc->as_formatted);
    # exit;

    # Some old hard coded stuff before the configuration file was implemented
    # my $lbyte6 = substr($marc->leader(),6,1);

    # my $two45 = $marc->field('245');
    # my @e856s = $marc->field('856');
    # my @e022s = $marc->field('020');
    # foreach(@e022s)
    # {
        # my $thisfield = $_;
        # # $log->addLine(Dumper($thisfield->subfields()));
        # $thisfield->delete_subfield(code => 'z');
        # my $hasMore = 0;
        # foreach($thisfield->subfields())
        # {
            # my @s = @{$_};
            # foreach(@s)
            # {
                # $hasMore = 1;
            # }
        # }
        # # $log->addLine("Deleting the whole field") if !$hasMore;
        # $marc->delete_field($thisfield) if !$hasMore;
    # }
    # if($two45)
    # {
        # my $value = "item";
        # # if($lbyte6 eq 'm' || $lbyte6 eq 'i')
        # # {
            # $value = "eBook";
            # if($lbyte6 eq 'i')
            # {
                # $value = "eAudioBook";
            # }
            # elsif($lbyte6 eq 'g')
            # {
                # $value = "eVideo";
            # }
            # if($two45->subfield('h'))
            # {
                # #$two45->update( 'h' => "[Overdrive downloadable $value] /" );
            # }
            # else
            # {
                # #$two45->add_subfields('h' => "[Overdrive downloadable $value] /");
            # }
        # # }
        # if(@e856s)
        # {
            # foreach(@e856s)
            # {
                # my $thisfield = $_;
                # my $ind2 = $thisfield->indicator(2);
                # if($ind2 eq '0') #only counts if the second indicator is 0 ("Resource") documented here: http://www.loc.gov/marc/bibliographic/bd856.html
                # {
                    # my @sub3 = $thisfield->subfield( '3' );
                    # my $ignore=0;
                    # foreach(@sub3)
                    # {
                        # if(lc($_) eq 'excerpt')
                        # {
                            # $ignore=1;
                        # }
                        # if(lc($_) eq 'image')
                        # {
                            # $ignore=1;
                        # }
                    # }
                    # if(!$ignore)
                    # {
                        # #$thisfield->delete_subfield(code => 'z');
                        # #$thisfield->add_subfields('z'=> "Instantly available on ebrary");
                    # }
                # }
            # }
        # }
    # }
    return $marc;
}

sub doMARCEdit
{
    my $def = shift;
    my $marc = shift;
    my $type = shift;
    my $howmany = shift;

    my @def = @{$def};

    if($type eq 'remove')
    {
        foreach(@def)
        {
            my @splits = split(/_/,$_);
            my $fieldDef = shift @splits;
            my $ind1Def = shift @splits;
            my $ind2Def = shift @splits;
            my $subfieldDef = shift @splits;
            my @f = @{findMatchingFields($marc, $fieldDef, $ind1Def, $ind2Def, $subfieldDef)};

            if($#f > -1) # without this, perl adds $#f + 1, it resolves to "1" when $#f = -1
            {
                my $pos = 0;
                $howmany = $#f + 1 if($howmany eq 'all');
                while($pos < $howmany)
                {
                    $marc->delete_field(@f[$pos]);
                    $pos++;
                }
            }
        }
    }
    elsif($type eq 'removesubfield' && $marc->field(@def[0]))
    {
        print "Deleting subfield ".@def[0]." ".@def[1]."\n" if $debug;
        my @field = $marc->field(@def[0]);
        $marc->delete_field(@field);
        my $pos = 0;
        $howmany = $#field + 1 if($howmany eq 'all');
        while($pos < $howmany)
        {
            my $thisField = @field[$pos];

            if($thisField->subfield(@def[1]))
            {
                $thisField->delete_subfield(code => @def[1]);
            }
            my @theRest = $thisField->subfields();
            $marc->insert_grouped_field($thisField) if(@theRest[0]);
            undef @theRest;
            $pos++;
        }
        while($pos < $#field + 1)
        {
            my $thisField = @field[$pos];
            $marc->insert_grouped_field($thisField);
            $pos++;
        }
    }
    elsif( ($type eq 'add') || ($type eq 'replace') )
    {
        my $numCheck = @def[0] + 0;
        my $field;
        $field = MARC::Field->new(@def[0], '') if($numCheck < 10);
        $field = MARC::Field->new(@def[0],' ',' ', 'a' => '') if($numCheck > 9);
        $field->delete_subfield('a') if($numCheck > 9);

        if($type eq 'replace' && $marc->field(@def[0]))
        {
            my @fields = $marc->field(@def[0]);
            my $pos = 0;
            $howmany = $#fields + 1 if($howmany eq 'all');
            while($pos < $howmany)
            {
                $field = @fields[$pos];
                $marc = doMARCEdit_Field($field, $marc, $numCheck, \@def, 1);
                $pos++;
            }
        }
        else
        {
            $marc = doMARCEdit_Field($field, $marc, $numCheck, \@def, 0);
        }
    }

    return $marc;
}

sub doMARCEdit_Field
{
    my $field = shift;
    my $marc = shift;
    my $numCheck = shift;
    my $def = shift;
    my $alreadyExists = shift || 0;

    my @def = @{$def};
    if($numCheck < 10)
    {
        my $data = $field->data();
        $data = $mobUtil->insertDataIntoColumn($data, @def[2], @def[1]);

        $field->update($data);
        $marc->insert_grouped_field($field) if !$alreadyExists;
    }
    else
    {
        my @ind = ('', @def[1], @def[2]);
        for my $i (1..$#ind)
        {
            if(@ind[$i] ne 'same')
            {
                $field->set_indicator($i, @ind[$i]);
            }
        }
        my @subfield = $field->subfield( @def[3] );
        if(@subfield[0])
        {
            $field->delete_subfields(@def[3]);
            #only deal with the first one
            shift @subfield;
            push (@subfield, @def[4]);
            $field->add_subfields(@def[3] => $_) foreach(@subfield);
        }
        else
        {
            $field->add_subfields(@def[3] => @def[4]);
        }
        $marc->insert_grouped_field($field) if !$alreadyExists;
    }
    return $marc;
}

sub findMatchingFields
{
    my $marc = shift;
    my $fieldDef = shift;
    my $ind1Def = shift;
    my $ind2Def = shift;
    my $subfieldDef = shift;
    my @ret = ();
    my @fields = $marc->field($fieldDef);
    if($subfieldDef)
    {
        foreach(@fields)
        {
            my %comps = ( $ind1Def => $_->indicator(1), $ind2Def => $_->indicator(2) );
            my $passedIndicatorTests = 1;
            while ( (my $key, my $value) = each(%comps) )
            {
                if($key eq 'none') # Definition means only non-defined or null value indicators are allowed
                {
                    $passedIndicatorTests = 0 if($value eq '0'); # handle the case when the indicator is '0' and we defined "none". Not the same :)
                    if($value ne '0')
                    {
                        $passedIndicatorTests = 0 if ($value && !($value =~ m/^[\\\/\s]$/) && ($value ne 'undef'));
                    }
                }
                elsif($key ne 'all')
                {
                    $passedIndicatorTests = 0 if !($key eq $value);
                }
            }
            if($passedIndicatorTests)
            {
                if($subfieldDef ne 'all')
                {
                    my $subfield = $_->subfield($subfieldDef);
                    push (@ret, $_) if $subfield;
                }
                else
                {
                    push (@ret, $_);
                }
            }
        }
    }
    else
    {
        return \@fields;
    }
    return \@ret;
}

sub mergeMARC856
{
    my $marc = @_[0];
    my $marc2 = @_[1];

    my @eight56s = $marc->field("856");
    my @eight56s_2 = $marc2->field("856");
    my @eights;
    my $original856 = $#eight56s + 1;
    @eight56s = (@eight56s,@eight56s_2);

    my %urls;
    foreach(@eight56s)
    {
        my $thisField = $_;
        my $ind2 = $thisField->indicator(2);
        # Just read the first $u and $z
        my $u = $thisField->subfield("u");
        my $z = $thisField->subfield("z");
        my $s7 = $thisField->subfield("7");

        if($u) #needs to be defined because its the key
        {
            if(!$urls{$u})
            {
                if($ind2 ne '0')
                {
                    $thisField->delete_subfields('9');
                    $thisField->delete_subfields('z');
                }
                $urls{$u} = $thisField;
            }
            else
            {
                my @nines = $thisField->subfield("9");
                my $otherField = $urls{$u};
                my @otherNines = $otherField->subfield("9");
                my $otherZ = $otherField->subfield("z");
                my $other7 = $otherField->subfield("7");
                if(!$otherZ)
                {
                    if($z)
                    {
                        $otherField->add_subfields('z'=>$z);
                    }
                }
                if(!$other7)
                {
                    if($s7)
                    {
                        $otherField->add_subfields('7'=>$s7);
                    }
                }
                if($conf{"merge_9s"})
                {
                    foreach(@nines)
                    {
                        my $looking = $_;
                        my $found = 0;
                        foreach(@otherNines)
                        {
                            if($looking eq $_)
                            {
                                $found=1;
                            }
                        }
                        if($found==0 && $ind2 eq '0')
                        {
                            $otherField->add_subfields('9' => $looking);
                        }
                    }
                }
                if($ind2 ne '0')
                {
                    $thisField->delete_subfields('9');
                    $thisField->delete_subfields('z');
                }

                $urls{$u} = $otherField;
            }
        }

    }

    my $finalCount = scalar keys %urls;
    if($original856 != $finalCount)
    {
        $log->addLine("There was $original856 and now there are $finalCount");
    }

    my $dump1=Dumper(\%urls);
    my @remove = $marc->field('856');
    #$log->addLine("Removing ".$#remove." 856 records");
    $marc->delete_fields(@remove);

    while ((my $internal, my $mvalue ) = each(%urls))
    {
        $marc->insert_grouped_field( $mvalue );
    }
    return $marc;
}

sub getEvergreenMax
{
    my $sha1 = shift;
    my $lastMax = shift;
    my $query = "SELECT MAX(id) FROM biblio.record_entry";

    $query .= " WHERE" if($sha1 || $lastMax);
    $query .= " id > $lastMax" if($lastMax);
    $query .= " AND " if($sha1 && $lastMax);
    $query .= " tcn_source = \$tcn_source\$$sha1\$tcn_source\$" if($sha1);

    updateJob("Processing", $query) if($debug);
    $log->addLine($query) if($debug);

    my @results = @{$dbHandler->query($query)};
    my $dbmax = 0;
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        $dbmax = @row[0];
    }
    return $dbmax;
}

sub convertMARCtoXML
{
    my $marc = @_[0];
    my $thisXML =  $marc->as_xml(); #decode_utf8();

    #this code is borrowed from marc2bre.pl
    $thisXML =~ s/\n//sog;
    $thisXML =~ s/^<\?xml.+\?\s*>//go;
    $thisXML =~ s/>\s+</></go;
    $thisXML =~ s/\p{Cc}//go;
    $thisXML = OpenILS::Application::AppUtils->entityize($thisXML);
    $thisXML =~ s/[\x00-\x1f]//go;
    $thisXML =~ s/^\s+//;
    $thisXML =~ s/\s+$//;
    $thisXML =~ s/<record><leader>/<leader>/;
    $thisXML =~ s/<collection/<record/;
    $thisXML =~ s/<\/record><\/collection>/<\/record>/;

    #end code
    return $thisXML;
}

sub getbibsource
{
    my $squery = "SELECT ID FROM CONFIG.BIB_SOURCE WHERE SOURCE = \$\$$importSourceName\$\$";
    my @results = @{$dbHandler->query($squery)};
    if($#results==-1 && !$conf{'import_as_is'})
    {
        print "Didnt find '$importSourceName' in bib_source, now creating it...\n";
        my $query = "INSERT INTO CONFIG.BIB_SOURCE(QUALITY,SOURCE) VALUES(90,\$\$$importSourceName\$\$)";
        my $res = $dbHandler->update($query);
        print "Update results: $res\n";
        @results = @{$dbHandler->query($squery)};
    }

    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        return @row[0];
    }
    return 1;
}

sub createNewJob
{
    my $status = @_[0];
    my $query = "INSERT INTO e_bib_import.job(status) values('$status')";
    my $results = $dbHandler->update($query);
    if($results)
    {
        $query = "SELECT max( ID ) FROM e_bib_import.job";
        my @results = @{$dbHandler->query($query)};
        foreach(@results)
        {
            my $row = $_;
            my @row = @{$row};
            $jobid = @row[0];
            return @row[0];
        }
    }
    return -1;
}

sub updateJob
{
    my $status = @_[0];
    my $action = @_[1];
    my $query = "UPDATE e_bib_import.job SET last_update_time=now(),status=\$1, CURRENT_ACTION_NUM = CURRENT_ACTION_NUM+1,current_action=\$2 where id=\$3";
    $log->addLine($action) if $debug;
    my @vals = ($status,$action,$jobid);
    my $results = $dbHandler->updateWithParameters($query,\@vals);
    return $results;
}

## Leaving this function for debugging purposes. It's not called anywhere but could be useful
sub findMatchInArchive
{
    my @matchList = @{@_[0]};
    my @files;

    #Get all files in the directory path
    @files = @{dirtrav(\@files,$archivefolder)};
    my @ret;

    for my $b(0..$#files)
    {
        my $file = MARC::File::USMARC->in($files[$b]);
        while ( my $marc = $file->next() )
        {
            my $t = $marc->leader();
            my $su=substr($marc->leader(),6,1);
            print "Leader:\n$t\n$su\n";
            if(1)#$su eq 'a')
            {
                my $all = $marc->as_formatted();
                foreach(@matchList)
                {
                    if($all =~ m/$_/g)
                    {
                        my @booya = ($files[$b]);
                        push(@ret,$files[$b]);
                        print "This one: ".$files[$b]." matched '$_'\n";
                        return \@booya;
                    }
                }
            }
        }
        $file->close();
        undef $file;
    }
}

sub dirtrav
{
    my @files = @{@_[0]};
    my $pwd = @_[1];
    opendir(DIR,"$pwd") or die "Cannot open $pwd\n";
    my @thisdir = readdir(DIR);
    closedir(DIR);
    foreach my $file (@thisdir)
    {
        if(($file ne ".") and ($file ne ".."))
        {
            if (-d "$pwd/$file")
            {
                push(@files, "$pwd/$file");
                @files = @{dirtrav(\@files,"$pwd/$file")};
            }
            elsif (-f "$pwd/$file")
            {
                push(@files, "$pwd/$file");
            }
        }
    }
    return \@files;
}

sub fixLeader
{
    my $marc = @_[0];
    #This is disabled because Shon did not want to change the icon in the catalog
    return $marc;
    my $fullLeader = $marc->leader();
    if(substr($fullLeader,6,1) eq 'a')
    {
        #print "Leader has an a:\n$fullLeader";
        $fullLeader = substr($fullLeader,0,6).'m'.substr($fullLeader,7);
        $marc->leader($fullLeader);
        my $fullLeader = $marc->leader();
        #print "Changed to:\n$fullLeader";
    }
    return $marc;
}

sub scoreMARC
{
    my $marc = shift;

    my $score = 0;
    $score+= score($marc,2,100,400,'245');
    $score+= score($marc,1,1,150,'100');
    $score+= score($marc,1,1.1,150,'110');
    $score+= score($marc,0,50,200,'6..');
    $score+= score($marc,0,50,100,'02.');

    $score+= score($marc,0,100,200,'246');
    $score+= score($marc,0,100,100,'130');
    $score+= score($marc,0,100,100,'010');
    $score+= score($marc,0,100,200,'490');
    $score+= score($marc,0,10,50,'830');

    $score+= score($marc,1,.5,50,'300');
    $score+= score($marc,0,1,100,'7..');
    $score+= score($marc,2,2,100,'50.');
    $score+= score($marc,2,2,100,'52.');

    $score+= score($marc,2,.5,200,'51.', '53.', '54.', '55.', '56.', '57.', '58.');

    return $score;
}

sub score
{
    my ($marc) = shift;
    my ($type) = shift;
    my ($weight) = shift;
    my ($cap) = shift;
    my @tags = @_;
    my $ou = Dumper(@tags);
    #$log->addLine("Tags: $ou\n\nType: $type\nWeight: $weight\nCap: $cap");
    my $score = 0;
    if($type == 0) #0 is field count
    {
        #$log->addLine("Calling count_field");
        $score = count_field($marc,\@tags);
    }
    elsif($type == 1) #1 is length of field
    {
        #$log->addLine("Calling field_length");
        $score = field_length($marc,\@tags);
    }
    elsif($type == 2) #2 is subfield count
    {
        #$log->addLine("Calling count_subfield");
        $score = count_subfield($marc,\@tags);
    }
    $score = $score * $weight;
    if($score > $cap)
    {
        $score = $cap;
    }
    $score = int($score);
    #$log->addLine("Weight and cap applied\nScore is: $score");
    return $score;
}

sub count_subfield
{
    my ($marc) = $_[0];
    my @tags = @{$_[1]};
    my $total = 0;
    #$log->addLine("Starting count_subfield");
    foreach my $tag (@tags)
    {
        my @f = $marc->field($tag);
        foreach my $field (@f)
        {
            my @subs = $field->subfields();
            my $ou = Dumper(@subs);
            #$log->addLine($ou);
            if(@subs)
            {
                $total += scalar(@subs);
            }
        }
    }
    #$log->addLine("Total Subfields: $total");
    return $total;

}

sub count_field
{
    my ($marc) = $_[0];
    my @tags = @{$_[1]};
    my $total = 0;
    foreach my $tag (@tags)
    {
        my @f = $marc->field($tag);
        $total += scalar(@f);
    }
    return $total;
}

sub field_length
{
    my ($marc) = $_[0];
    my @tags = @{$_[1]};

    my @f = $marc->field(@tags[0]);
    return 0 unless @f;
    my $len = length($f[0]->as_string);
    my $ou = Dumper(@f);
    #$log->addLine($ou);
    #$log->addLine("Field Length: $len");
    return $len;
}

sub updateDBSHA1
{
    my $sha1 = shift;
    my $bibid = shift;
    my $source = shift;
    my $query = "select count(*) from e_bib_import.bib_sha1 where bib=$bibid";
    my @results = @{$dbHandler->query($query)};
    my @values = ();
    my @shas = split(/\s/, $sha1);
    my $count=0;
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        $count = @row[0];
    }
    if($count > 0)
    {
        $query = "update e_bib_import.bib_sha1 set sha1_full = \$1 , sha1_mid = \$2 where bib = \$3";
        updateJob($query);
        @values = (@shas[0], @shas[1], $bibid);
    }
    else
    {
        $query = "insert into e_bib_import.bib_sha1(bib,bib_source,sha1_full,sha1_mid) values( \$1, \$2, \$3, \$4 )";
        updateJob($query);
        @values = ($bibid, $source, @shas[0], @shas[1]);
    }
    $dbHandler->updateWithParameters($query,\@values);

}

sub parseMARCEdits
{

    my %confGroups = ('control' => 'marc_edit_control_', 'standard' => 'marc_edit_standard_');
    my $test = 1;
    my $count = 0;

    while (( my $internal, my $mvalue ) = each(%conf))
    {
        while (( my $gtype, my $groupID ) = each(%confGroups))
        {
            if( $internal =~ m/$groupID/g)
            {
                $count++;
                $test = parseMARCEdits_sanity_check($gtype, $mvalue);
                print "\n\nThere is an error in the MARC Manipulation definitions. Please see log for details:\n\nless ". $conf{"logfile"} ."\n\n" unless $test;
                exit unless $test;
                my %tt = %{$test};
                my @ar = @{$marcEdits{$tt{'type'}}};
                push (@ar, \%tt);
                $marcEdits{$tt{'type'}} = \@ar;
                $log->addLine(Dumper(\%marcEdits));
            }
        }
    }
}

sub parseMARCEdits_sanity_check
{
    my $gtype = shift;
    my $value = shift;
    my @allowedTypes = ('add','remove','removesubfield','replace');
    my %typeExpectedArraySize = ('standard' => 5, 'control' => 3 ); # one based
    $log->addLogLine("Attempting to parse '$value'");
    my %check = ();
    my $exec = '%check = (' . $value . ');';

    local $@;
    eval($exec);

    $log->addLogLine("Failed to parse '$value'") if $@;
    return 0 if $@;

    $log->addLine(Dumper(\%check)) if $debug;

    if(!$check{"type"})
    {
    
        $log->addLogLine("Type Undefined '$value'");
        return 0;
    }

    if(!$check{"def"})
    {
    
        $log->addLogLine("def Undefined '$value'");
        return 0;
    }

    # Check type values
    my $allowedTypeExists = 0;
    foreach(@allowedTypes)
    {
        $allowedTypeExists = 1 if $_ eq $check{'type'};
    }
    $log->addLogLine("Invalid type '". $check{'type'} ."'") if !$allowedTypeExists;
    return 0 if !$allowedTypeExists;

    my @def = @{$check{'def'}};

    if($check{'type'} eq 'removesubfield')
    {
        my $totalArray = scalar @def;
        $log->addLogLine("Incorrect number of array values (expecting: 2)") if $totalArray != 2;
        return 0 if $totalArray != 2;
    }
    elsif($check{'type'} ne 'remove')
    {
        my $totalArray = scalar @def;
        $log->addLogLine("Incorrect number of array values (expecting: ".$typeExpectedArraySize{$gtype} .")") if $totalArray != $typeExpectedArraySize{$gtype};
        return 0 if $totalArray != $typeExpectedArraySize{$gtype};
    }

    my $fieldTest = testField(@def[0], $gtype, $check{'type'});
    return $fieldTest unless $fieldTest;

    if($check{'type'} eq 'remove')
    {
        foreach(@def)
        {
            $fieldTest = testField($_, $gtype, $check{'type'});
            return $fieldTest unless $fieldTest;
        }
    }

    if($check{'type'} eq 'removesubfield')
    {
        $fieldTest = testField(@def[0], $gtype, $check{'type'});
        return $fieldTest unless $fieldTest;
        $fieldTest = testSubfield(@def[1], $gtype);
        return $fieldTest unless $fieldTest;
    }

    # The rest of the tests are for non-removals
    if($check{'type'} ne 'remove' && $check{'type'} ne 'removesubfield')
    {
        # control field tests
        if($gtype eq 'control')
        {
            if(!(@def[1] =~ m/^\d+$/))
            {
                $log->addLogLine("Invalid MARC field def: '".@def[1]."' is not a valid column position");
                return 0;
            }
        }
        # Standard field tests
        else
        {
            $fieldTest = testIndicator(@def[1], $gtype);
            return $fieldTest unless $fieldTest;
            $fieldTest = testIndicator(@def[2], $gtype);
            return $fieldTest unless $fieldTest;
            $fieldTest = testSubfield(@def[3], $gtype);
            return $fieldTest unless $fieldTest;
        }
    }
    return \%check;
}

sub testSubfield
{
    my $subfield = shift;
    if( !($subfield =~ m/^[\dA-Za-z]$/) )
    {
        $log->addLogLine("Invalid MARC subfield def: ".$subfield);
        return 0;
    }
    return 1;
}

sub testIndicator
{
    my $ind = shift;
    return 1 if !$ind; #null is fine
    return 1 if $ind eq 'same'; #controlled vocab, "same" means no change

    if( !($ind =~ m/^\d$/) )
    {
        $log->addLogLine("Invalid MARC indicator def: ".$ind);
        return 0;
    }
    return 1;
}

sub testField
{
    my $field = shift;
    my $gtype = shift;
    my $rtype = shift;

    my @splits = split(/_/,$mobUtil->trim($field));
    $field = shift @splits;
    my $ind1 = shift @splits;
    my $ind2 = shift @splits;
    my $subfield = shift @splits;

    if( !($field =~ m/^\d\d\d$/) )
    {
        $log->addLogLine("Invalid MARC field def: ".$field);
        return 0;
    }

    if($rtype eq 'remove' && $ind1 && !$subfield)
    {
        $log->addLogLine("Tag removal requires both indicators and subfield to be defined like this: 'xxx_ind1_ind2_subfield'");
        return 0;
    }

    if($rtype ne 'remove' && $subfield)
    {
        $log->addLogLine("Subfield definition not allowed unless 'remove' is specified subfield def: '$subfield' defined with field '$field'");
        return 0;
    }

    my @checks = ($ind1, $ind2, $subfield);

    foreach(@checks)
    {
        if($_ && ( ($_ ne 'all') && (length($_) != 1) && ($_ ne 'none') )) # Definition either needs to be specific or "all" or "none"
        {
            $log->addLogLine("Invalid MARC subfield def: '$field' '$ind1' '$ind2' '$subfield'");
            return 0;
        }
    }

    my $numTest = $field + 0;

    if($numTest > 9 && $gtype eq 'control')
    {
        $log->addLogLine("Invalid MARC field def: '$field' is not a control field");
        return 0;
    }
    if($numTest < 10 && $gtype eq 'standard')
    {
        $log->addLogLine("Invalid MARC field def: '$field' is not a standard field");
        return 0;
    }

    if($numTest > 999)
    {
        $log->addLogLine("Invalid MARC field def: '$field' is out of range");
        return 0;
    }
    return 1;
}

sub calculateTimeDifference
{
    my $dt = shift;
    my $now = DateTime->now(time_zone => "local");
    my $difference = $now - $dt;
    my $format = DateTime::Format::Duration->new(pattern => '%M %S');
    my $duration =  $format->format_duration($difference);
    my ($min, $sec) = split(/\s/,$duration);
    my $days = 0;
    my $hours = 0;
    if($min > 60)
    {
        $hours = floor($min / 60);
        $min = $min % 60;
        if ($hours > 24)
        {
            $days = floor($hours / 24);
            $hours = $hours % 24;
        }
    }
    return "$days days, $hours hours, $min minutes and $sec seconds";
}

sub reingest
{
    my $bibid = shift;
    my $query =
    "SELECT metabib.reingest_metabib_field_entries(bib_id := \$1, skip_facet := FALSE, skip_browse := FALSE, skip_search := FALSE, skip_display := FALSE)";
    my @vals = ($bibid);
    $dbHandler->updateWithParameters($query, \@vals);

    $query = "SELECT metabib.reingest_record_attributes(rid := id, prmarc := marc)
    FROM biblio.record_entry
    WHERE id = \$1
    ";
    $dbHandler->updateWithParameters($query, \@vals);
}

sub setupSchema
{
    my $dbHandler = @_[0];
    my $query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'e_bib_import'";
    my @results = @{$dbHandler->query($query)};
    if($#results==-1)
    {
        $query = "CREATE SCHEMA e_bib_import";
        $dbHandler->update($query);

        $query = "CREATE TABLE e_bib_import.job
        (
        id bigserial NOT NULL,
        start_time timestamp with time zone NOT NULL DEFAULT now(),
        last_update_time timestamp with time zone NOT NULL DEFAULT now(),
        status text default 'processing',
        current_action text,
        current_action_num bigint default 0,
        CONSTRAINT job_pkey PRIMARY KEY (id)
          )";
        $dbHandler->update($query);

        $query = "CREATE TABLE e_bib_import.import_status(
        id bigserial NOT NULL,
        bibtag text,
        filename text,
        z01 text,
        title text,
        sha1 text,
        type text default \$\$import\$\$,
        status text default \$\$new\$\$,
        processed boolean default false,
        row_change_time timestamp default now(),
        marc_xml text,
        bib bigint,
        job  bigint NOT NULL,
        CONSTRAINT import_status_pkey PRIMARY KEY (id),
        CONSTRAINT import_status_fkey FOREIGN KEY (job)
        REFERENCES e_bib_import.job (id) MATCH SIMPLE)";
        $dbHandler->update($query);

        $query = "CREATE TABLE e_bib_import.item_reassignment(
        id serial,
        copy bigint,
        prev_bib bigint,
        target_bib bigint,
        statusid bigint,
        change_time timestamp default now(),
        job  bigint NOT NULL,
        CONSTRAINT item_reassignment_fkey FOREIGN KEY (job)
        REFERENCES e_bib_import.job (id) MATCH SIMPLE,
        CONSTRAINT item_reassignment_statusid_fkey FOREIGN KEY (statusid)
        REFERENCES e_bib_import.import_status (id) MATCH SIMPLE
        )";
        $dbHandler->update($query);

        $query = "CREATE TABLE e_bib_import.bib_marc_update(
        id bigserial NOT NULL,
        record bigint,
        prev_marc text,
        changed_marc text,
        new_record boolean NOT NULL DEFAULT false,
        change_time timestamp default now(),
        job  bigint NOT NULL,
        CONSTRAINT bib_marc_update_fkey FOREIGN KEY (job)
        REFERENCES e_bib_import.job (id) MATCH SIMPLE)";
        $dbHandler->update($query);

        $query = "CREATE TABLE e_bib_import.bib_merge(
        id bigserial NOT NULL,
        leadbib bigint,
        subbib bigint,
        statusid bigint,
        change_time timestamp default now(),
        job  bigint NOT NULL,
        CONSTRAINT bib_merge_fkey FOREIGN KEY (job)
        REFERENCES e_bib_import.job (id) MATCH SIMPLE,
        CONSTRAINT bib_merge_statusid_fkey FOREIGN KEY (statusid)
        REFERENCES e_bib_import.import_status (id) MATCH SIMPLE)";
        $dbHandler->update($query);

        $query = "CREATE TABLE e_bib_import.nine_sync(
        id bigserial NOT NULL,
        record bigint,
        nines_synced text,
        url text,
        change_time timestamp default now())";
        $dbHandler->update($query);

        $query = "CREATE TABLE e_bib_import.bib_sha1(
        bib bigint,
        bib_source bigint,
        sha1_full text,
        sha1_mid text,
        CONSTRAINT bib_sha1_bib_fkey FOREIGN KEY (bib)
        REFERENCES biblio.record_entry (id) MATCH SIMPLE,
        CONSTRAINT bib_sha1_bib_source_fkey FOREIGN KEY (bib_source)
        REFERENCES config.bib_source (id) MATCH SIMPLE)";
        $dbHandler->update($query);

        $query = "CREATE INDEX e_bib_import_bib_sha1_full_idx
        ON e_bib_import.bib_sha1
        USING btree (sha1_full)";
        $dbHandler->update($query);

        $query = "CREATE INDEX e_bib_import_bib_sha1_mid_idx
        ON e_bib_import.bib_sha1
        USING btree (sha1_mid)";
        $dbHandler->update($query);

        $query = "CREATE INDEX e_bib_import_bib_sha1_bib_idx
        ON e_bib_import.bib_sha1
        USING btree (bib)";
        $dbHandler->update($query);

        $query = "CREATE INDEX e_bib_import_bib_sha1_bib_source_idx
        ON e_bib_import.bib_sha1
        USING btree (bib_source)";
        $dbHandler->update($query);

        $query = "CREATE INDEX e_bib_import_bib_sha1_sha1_full_bib_source_idx
        ON e_bib_import.bib_sha1
        USING btree (sha1_full,bib_source)";
        $dbHandler->update($query);

        $query = "CREATE INDEX e_bib_import_bib_sha1_sha1_mid_bib_source_idx
        ON e_bib_import.bib_sha1
        USING btree (sha1_mid,bib_source)";
        $dbHandler->update($query);

        $query = "CREATE INDEX e_bib_import_import_status_job_idx
        ON e_bib_import.import_status
        USING btree (job)";
        $dbHandler->update($query);

        $query = "CREATE INDEX e_bib_import_import_status_status_idx
        ON e_bib_import.import_status
        USING btree (status)";
        $dbHandler->update($query);

        $query = "CREATE INDEX e_bib_import_import_status_type_idx
        ON e_bib_import.import_status
        USING btree (type)";
        $dbHandler->update($query);

        $query = "CREATE INDEX e_bib_import_import_status_bib_idx
        ON e_bib_import.import_status
        USING btree (bib)";
        $dbHandler->update($query);
    }
}

 exit;
