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

use lib qw(../../);
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

our $configFile;
our $debug = 0;
our $reprocess = -1;
our $searchDeepMatch = 0;
our $reportOnly = -1;
 
 
GetOptions (
"config=s" => \$configFile,
"reprocess=s" => \$reprocess,
"search_deep" => \$searchDeepMatch,
"report_only" => \$reportOnly,
"debug" => \$debug,
)
or die("Error in command line arguments\nYou can specify
--config configfilename                       [Path to the config file - required]
--reprocess jobID                             [Optional: Skip the import process and re-process provided job ID]
--search_deep                                 [Optional: Cause the software to spend more time searching for BIB matches]
--report_only jobID                           [Optional: Only email the report for a previous job. Provide the job ID]
--debug flag                                  [Cause more output - not implemented yet]
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
        #$log->truncFile("");
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

            my $doSomething = 0;
            
            if($reprocess != -1)
            {
                $bibsourceid = getbibsource();
                $jobid = $reprocess;
                $doSomething = resetJob($reprocess);
            }
            elsif($reportOnly == -1) ## Make sure we are not just running reports
            {
                @files = @{getmarcFromFolder()}  if(lc$conf{"recordsource"} eq 'folder');
                @files = @{getmarcFromFTP()}  if(lc$conf{"recordsource"} eq 'ftp');
                @files = @{getMarcFromCloudlibrary()}  if(lc$conf{"recordsource"} eq 'cloudlibrary');

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
                    sendWelcomeMessage(\@files) if ( $searchDeepMatch && (lc$conf{"recordsource"} eq 'folder') && $doSomething );

                    ## Imports
                    my $query = "SELECT id,title,z01,sha1,marc_xml,filename from e_bib_import.import_status where type=\$\$import\$\$ and job=$jobid order by id";
                    updateJob("Processing",$query);
                    my @results = @{$dbHandler->query($query)};
                    foreach(@results)
                    {
                        my @row = @{$_};
                        importMARCintoEvergreen(@row[0],@row[1],@row[2],@row[3],@row[4]);
                    }
                    
                    ## Removals
                    my $query = "SELECT id,title,z01,sha1,marc_xml,filename,type from e_bib_import.import_status where type!=\$\$import\$\$ and job=$jobid order by type,id";
                    updateJob("Processing",$query);
                    @results = @{$dbHandler->query($query)};
                    foreach(@results)
                    {
                        my @row = @{$_};
                        my $removalViaMARAC = 1;
                        $removalViaMARAC = 0 if @row[6] eq 'isbn_remove';
                        print "Removal Type: @row[6]  isbnRemoval = $removalViaMARAC\n" if $debug;
                        removeBibsEvergreen(@row[0],@row[1],@row[2],@row[3],@row[4],$removalViaMARAC);
                    }
                }

                my $report = runReports();
                
                my $afterProcess = DateTime->now(time_zone => "local");
                my $difference = $afterProcess - $dt;
                my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
                my $duration =  $format->format_duration($difference);

                my $body =
                    "Import Type: ".$conf{"recordsource"}.
                    "\r\nConnected to: ".$conf{"server"}.
                    "\r\nDuration: $duration".
                    "\r\n$report".
                    "\r\n\r\n\r\n-Evergreen Perl Squad-";

                updateJob("Processing","Email sending:\n$body");

                my @tolist = ($conf{"alwaysemail"});
                my $email = new email($conf{"fromemail"},\@tolist,$valid,1,\%conf);
                $email->send("Evergreen Utility - $importBIBTagName Import Report Job # $jobid",$body);
                
                updateJob("Completed","");
            }
            else
            {
                alertErrorEmail("There were some errors during the getmarc function, we are stopping execution.");
                exit;
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

sub runReports
{
    ## Reporting
    my $ret = "";

    ### Overall Totals
    my $query = "select count(*),filename from e_bib_import.import_status where job = $jobid group by 2";

    my @results = @{$dbHandler->query($query)};

    $ret .= "File Breakdown:\n\r";
    my $totalrows = 0;
    foreach(@results)
    {
        my @row = @{$_};
        $ret .= @row[0] . " records in ".@row[1] . "\r\n";
        $totalrows += @row[0];
    }
    $ret .= "\r\nTotal: $totalrows\r\n\r\n";
    undef @results;

    ### Import summary
    my %status = ();

    $query = "select z01,title,status,bib from e_bib_import.import_status where job = $jobid and type = \$\$import\$\$ ";

    my @results = @{$dbHandler->query($query)};

    $ret .= "Import Summary:\n\r";
    foreach(@results)
    {
        my @row = @{$_};
        my @t = ();
        $status{@row[2]} = \@t if !$status{@row[2]};
        @t = @{$status{@row[2]}};
        my @temp = (@row[0],@row[1],@row[3]);
        push @t, [@temp];
        $status{@row[2]} = \@t;
    }
    my $interestingImports = "";
    while ( (my $key, my $value) = each(%status) )
    {
        my @c = @{$value};
        my $c = $#c;
        $c++;
        $ret .= "$key: $c occurrences\r\n";
        if($key ne 'inserted' && $key ne 'matched and overlayed')
        {
            my @vv = @{$value};
            foreach(@vv)
            {
                my @v = @{$_};
                $interestingImports .= $key . " - '" . @v[0] . "' '" . @v[1] . "' BIB: " . @v[2] . "\r\n";
            }
        }
    }
    
    $ret .= "Interesting imports\r\n$interestingImports" if ( length($interestingImports) > 0);
    undef @results;
    
    
    ### Removal summary
    my %status = ();
    
    $query = "select z01,title,status from e_bib_import.import_status where job = $jobid and type ~ \$\$remov\$\$ ";

    my @results = @{$dbHandler->query($query)};
    
    $ret .= "Removal Summary:\n\r";
    foreach(@results)
    {
        my @row = @{$_};
        my @t = ();
        $status{@row[2]} = \@t if !$status{@row[2]};
        @t = @{$status{@row[2]}};
        my @temp = (@row[0],@row[1]);
        push @t, [@temp];
        $status{@row[2]} = \@t;
    }
    my $interestingImports = "";
    while ( (my $key, my $value) = each(%status) )
    {
        my @c = @{$value};
        my $c = $#c;
        $c++;
        $ret .= "$key: $c occurrences\r\n";
        if($key ne 'removed bib' && $key ne 'removed related 856' && $key ne 'No matching bib in DB')
        {
            my @vv = @{$value};
            foreach(@vv)
            {
                my @v = @{$_};
                $interestingImports .= $key . " - '" . @v[0] . "' '" . @v[1] . "'\r\n";
            }
        }
    }
    
    $ret .= "Interesting Removals\r\n$interestingImports" if ( length($interestingImports) > 0);
    undef @results;

    $ret .= gatherOutputReport();

    return $ret;

}

sub prepFiles
{
    my @files = @{@_[0]};
    my $dbValPos = 1;
    my $ret = 0;
    my $insertTop = "INSERT INTO e_bib_import.import_status(filename,z01,title,sha1,type,marc_xml,job)    
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
            while ( my $marc = $file->next() )
            {
                $dbInserts.="(";
                $marc = add9($marc) if !$isRemoval;
                my $importType = "import";
                $importType = "removal" if $isRemoval;
                my $z01 = getsubfield($marc,'001','');
                my $t = getsubfield($marc,'245','a');
                my $sha1 = calcSHA1($marc);
                my $thisXML = convertMARCtoXML($marc);
                $dbInserts.="\$$dbValPos,";
                $dbValPos++;
                push(@vals,$filenameForDB);
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
                        ($dbInserts, $dbValPos, @vals) = dumpRowsIfFull($insertTop,$dbInserts, $dbValPos, \@vals);
                        $ret = 1;
                    }
                }
            }
        }
    }
    dumpRowsIfFull($insertTop,$dbInserts, $dbValPos, \@vals, 1) if $dbValPos > 1; # Dump what's left into the DB

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
We have received these files:
$files

This software is configured to perform deep search matches against the database. This is slow but thorough.
Depending on the number of records, it could be days before you receive the finished message. FYI.
\r\n\r\n-Evergreen Perl Squad-";

    $log->addLine("Sending Welcome message:\r\n$body");

    my $email = new email($conf{"fromemail"},\@tolist,1,1,\%conf);
    $email->send("Evergreen Utility - $importBIBTagName Import Report Job # $jobid - Starting", $body);

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
    my $newRecordCount=0;
    my $updatedRecordCount=0;
    my $mergedRecords='';
    my $itemsAssignedRecords='';
    my $undedupeRecords='';
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

    my $ret=$newRecordCount." New record(s) were created.\r\n\r\n\r\n".
    $updatedRecordCount." Record(s) were updated\r\n\r\n\r\n".$mergedRecords.$itemsAssignedRecords;
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
    $log->addLine(Dumper(\@interestingFiles));
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
                # print "Local: $size\n";
                # print "remot: $rsize\n";
                if($size ne $rsize)
                {
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
                my $worked = $ftp->get($filename,"$archivefolder/$filename");
                if($worked)
                {
                    push (@ret, "$filename");
                }
            }
        }
    }
    $ftp->quit
    or die $log->addLogLine("Unable to close FTP connection");
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
        my $dt = DateTime->now(time_zone => "local");
        my $fdate = $dt->ymd;        
        my $year = substr($fdate,0,4);
        
        $year += 1900;
        foreach(@split)
        {
            # Looking for something that looks like a filesize in bytes. Example output from ftp server:
            # -rwxr-x---  1 northkcm System     15211006 Apr 25  2019 Audio_Recorded Books eAudio Adult Subscription_4_25_2019.mrc
            # -rwxr-x---  1 scenicre System         9731 Apr 09  2018 Zinio_scenicregionalmo_2099_Magazine_12_1_2017.mrc
            # We can expect a file that contains a single marc record to be reasonable in size ( > 1k)
            # Therefore, we need to find a string of at lease 4 numeric characters. Need to watch out for "year" numerics

            next if($year eq $_); #ignore year
            next if($_ =~ m/\D/); #ignore fields containing anything other than numbers

            if(length($_) > 3)
            {
                $rsize = $_;
                # if we find that one of the values exactly matches local file size, then we just set it to that
                last if($localFileSize eq $_);
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
    my @phrases = split(/\s/,$tphrases);
    my $ret = 0;
    $wholeString = lc $wholeString;
    foreach(@phrases)
    {
        my $phrase = lc $_;
        return 1 if ($wholeString =~ m/$phrase/g);
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
                            print "Same!\n";
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

    updateJob("Processing","mergeBIBs oldbib: $oldbib newbib=$newbib overdriveMatchString=$overdriveMatchString");

    recordAssetCopyMove($oldbib,$newbib,$overdriveMatchString,$statusID);
    my $query = "INSERT INTO e_bib_import.bib_merge(leadbib,subbib,statusid,job) VALUES($newbib,$oldbib,$statusID,$jobid)";
    #$log->addLine("MERGE:\t$newbib\t$oldbib");

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

sub calcSHA1
{
    my $marc = @_[0];
    my $sha1 = Digest::SHA1->new;
    $sha1->add(  length(getsubfield($marc,'007',''))>6 ? substr( getsubfield($marc,'007',''),0,6) : '' );
    $sha1->add(getsubfield($marc,'245','h'));
    $sha1->add(getsubfield($marc,'001',''));
    $sha1->add(getsubfield($marc,'245','a'));
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
    $marc = readyMARCForInsertIntoME($marc);
    my $bibid=-1;
    my $bibid = findRecord($marc, $sha1, $z01);
    
    if($bibid!=-1) #already exists so update the marc
    {
        chooseWinnerAndDeleteRest($bibid, $sha1, $marc, $title, $statusID);
    }
    else  ##need to insert new bib instead of update
    {
        my $starttime = time;
        my $max = getEvergreenMax();
        my $thisXML = convertMARCtoXML($marc);
        my @values = ($thisXML);
        $query = "INSERT INTO BIBLIO.RECORD_ENTRY(fingerprint,last_xact_id,marc,quality,source,tcn_source,owner,share_depth) VALUES(null,'IMPORT-$starttime',\$1,null,$bibsourceid,\$\$$importBIBTagNameDB-script $sha1\$\$,null,null)";
        $log->addLine($query);
        my $res = $dbHandler->updateWithParameters($query,\@values);
        #print "$res";
        my $newmax = getEvergreenMax();
        if($newmax != $max)
        {
            $log->addLine("$newmax\thttp://$domainname/eg/opac/record/$newmax?locg=157;expand=marchtml#marchtml");
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
    }
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
            $log->addLine("$id\thttp://$domainname/eg/opac/record/$id?locg=4;expand=marchtml#marchtml\thttp://$domainname/eg/opac/record/$id?locg=157;expand=marchtml#marchtml\t0");
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
            my @ninposes;
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
                $thisField->delete_subfield(code=> '9', 'pos' => \@delete9s);
            }
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
    my $overdriveMatchString = $newMarc->field('001')->data();
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
    }
    $finalMARC = @{@list[$chosenWinner]}[1];
    $i=0;
    foreach(@list)
    {
        my @attrs = @{$_};
        my $id = @attrs[0];
        my $marc = @attrs[1];
        my $marcxml = @attrs[3];
        if($i!=$chosenWinner)
        {
            $finalMARC = mergeMARC856($finalMARC, $marc);
            $finalMARC = fixLeader($finalMARC);
            mergeBIBs($id, $winnerBibID, $overdriveMatchString, $statusID);
        }
        $i++;
    }
    # melt the incoming e_bib_import 856's retaining the rest of the marc from the DB
    # At this point, the 9's have been added to the newMarc (data from e_bib_import)
    $finalMARC = mergeMARC856($finalMARC, $newMarc);
    $finalMARC = fixLeader($finalMARC);
    my $newmarcforrecord = convertMARCtoXML($finalMARC);
    print "Headed into recordBIBMARCChanges\n" if $debug;
    recordBIBMARCChanges($winnerBibID, $winnerOGMARCxml, $newmarcforrecord,0);
    
    my $thisXML = convertMARCtoXML($finalMARC);
    my @values = ("$importBIBTagNameDB-script $sha1", $bibsourceid, $thisXML, $winnerBibID);
    my $query = "UPDATE BIBLIO.RECORD_ENTRY SET tcn_source = \$1 , source = \$2 , marc = \$3  WHERE ID = \$4";
    
    print "Updating MARC XML in DB BIB $winnerBibID\n" if $debug;
    updateJob("Processing","chooseWinnerAndDeleteRest   $query");
    
    # $log->addLine($thisXML);
    $log->addLine("$winnerBibID\thttp://$domainname/eg/opac/record/$winnerBibID?locg=4;expand=marchtml#marchtml\thttp://$domainname/eg/opac/record/$winnerBibID?locg=157;expand=marchtml#marchtml\t$matchnum");
    my $res = $dbHandler->updateWithParameters($query,\@values);
    #print "$res\n";
    if($res)
    {
        $query = "update e_bib_import.import_status set status = \$1, processed = true , bib = \$2 , row_change_time = now() where id = \$3";
        my @vals = ('matched and overlayed',$winnerBibID,$statusID);
        $dbHandler->updateWithParameters($query,\@vals);
    }
    else
    {
        $query = "update e_bib_import.import_status set status = \$1, processed = true, row_change_time = now() where id = \$2";
        my @vals = ('failed',$statusID);
        $dbHandler->updateWithParameters($query,\@vals);
    }

}

sub findRecord
{
    my $marcsearch = @_[0];
    my $sha1 = @_[1];
    my $zero01 = @_[2];
    
    print "Searching for sha1 match $sha1\n" if $debug;
    my $query = "
    select 
    bre.id,
    bre.marc from 
    biblio.record_entry bre,
    e_bib_import.bib_sha1 ebs
    where 
    bre.id=ebs.bib and
    ebs.sha1 = \$sha\$$sha1\$sha\$ and
    ebs.bib_source=$bibsourceid and 
    not bre.deleted
    ";
    
    $query.="
    union all
    
    select bre.id,bre.marc
    from
    biblio.record_entry bre left join e_bib_import.bib_sha1 ebs on(ebs.bib=bre.id)
    where
    not bre.deleted and
    bre.source=$bibsourceid and
    bre.tcn_source~\$sha\$$sha1\$sha\$ and
    ebs.bib is null
    " if $searchDeepMatch;
    
    updateJob("Processing","$query") if $debug;
    
    my @results = @{$dbHandler->query($query)};
    my @ret;
    my $none=1;
    my $foundIDs;
    my $count=0;
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        my $id = @row[0];
        my $marc = @row[1];
        my $prevmarc = $marc;
        $prevmarc =~ s/(<leader>.........)./${1}a/;
        $prevmarc = MARC::Record->new_from_xml($prevmarc);
        print "found matching sha1: $id\n" if $debug;
        my $score = scoreMARC($prevmarc);
        my @matchedsha = ($id,$prevmarc,$score,$marc);
        $foundIDs.="$id,";
        push (@ret, [@matchedsha]);
        $none=0;
        $count++;
    }
    
    if($searchDeepMatch)  ## This matches other bibs based upon the vendor's 001 which is usually moved to the 035, hence MARC ~ 
    {
        $foundIDs = substr($foundIDs,0,-1);
        if(length($foundIDs)<1)
        {
            $foundIDs="-1";
        }
        my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE MARC ~ \$\$$zero01\$\$ and ID not in($foundIDs) and deleted is false ";
        updateJob("Processing","$query");
        my @results = @{$dbHandler->query($query)};
        foreach(@results)
        {
            my $row = $_;
            my @row = @{$row};
            my $id = @row[0];
            print "found matching 001: $id\n";
            my $marc = @row[1];
            my $prevmarc = $marc;
            $prevmarc =~ s/(<leader>.........)./${1}a/;    
            $prevmarc = MARC::Record->new_from_xml($prevmarc);
            my $score = scoreMARC($prevmarc,$log);
            my @matched001 = ($id,$prevmarc,$score,$marc);
            push (@ret, [@matched001]);
            $none=0;
            $count++;
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

sub readyMARCForInsertIntoME
{
    my $marc = @_[0];
    $marc = fixLeader($marc);
    my $lbyte6 = substr($marc->leader(),6,1);

    my $two45 = $marc->field('245');
    my @e856s = $marc->field('856');
    my @e022s = $marc->field('020');
    foreach(@e022s)
    {
        my $thisfield = $_;
        # $log->addLine(Dumper($thisfield->subfields()));
        $thisfield->delete_subfield(code => 'z');
        my $hasMore = 0;
        foreach($thisfield->subfields())
        {
            my @s = @{$_};
            foreach(@s)
            {
                $hasMore = 1;
            }
        }
        # $log->addLine("Deleting the whole field") if !$hasMore;
        $marc->delete_field($thisfield) if !$hasMore;
    }
    if($two45)
    {
        my $value = "item";
        # if($lbyte6 eq 'm' || $lbyte6 eq 'i')
        # {
            $value = "eBook";
            if($lbyte6 eq 'i')
            {
                $value = "eAudioBook";
            }
            elsif($lbyte6 eq 'g')
            {
                $value = "eVideo";
            }
            if($two45->subfield('h'))
            {
                #$two45->update( 'h' => "[Overdrive downloadable $value] /" );
            }
            else
            {
                #$two45->add_subfields('h' => "[Overdrive downloadable $value] /");
            }
        # }
        if(@e856s)
        {
            foreach(@e856s)
            {
                my $thisfield = $_;
                my $ind2 = $thisfield->indicator(2);
                if($ind2 eq '0') #only counts if the second indicator is 0 ("Resource") documented here: http://www.loc.gov/marc/bibliographic/bd856.html
                {    
                    my @sub3 = $thisfield->subfield( '3' );
                    my $ignore=0;
                    foreach(@sub3)
                    {
                        if(lc($_) eq 'excerpt')
                        {
                            $ignore=1;
                        }
                        if(lc($_) eq 'image')
                        {
                            $ignore=1;
                        }
                    }
                    if(!$ignore)
                    {
                        #$thisfield->delete_subfield(code => 'z');
                        #$thisfield->add_subfields('z'=> "Instantly available on ebrary");
                    }
                }
            }
        }
    }
    return $marc;
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
    my $query = "SELECT MAX(ID) FROM BIBLIO.RECORD_ENTRY";
    #return 1000;
    my @results = @{$dbHandler->query($query)};
    my $dbmax=0;
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        $dbmax = @row[0];
    }
    #print "DB Max: $dbmax\n";
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
    if($#results==-1)
    {
        print "Didnt find $importSourceName in bib_source, now creating it...\n";
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
    $log->addLine($action);
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
    my $count=0;
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        $count=@row[0];
    }
    if($count>0)
    {
        $query = "update e_bib_import.bib_sha1 set sha1=\$1 where bib=\$2";
        updateJob($query);
        @values = ($sha1, $bibid);
    }
    else
    {
        $query = "insert into e_bib_import.bib_sha1(bib,bib_source,sha1) values(\$1,\$2,\$3 )";
        updateJob($query);
        @values = ($bibid,$source,$sha1);
    }
    $dbHandler->updateWithParameters($query,\@values);

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
        sha1 text,
        CONSTRAINT bib_sha1_bib_fkey FOREIGN KEY (bib)
        REFERENCES biblio.record_entry (id) MATCH SIMPLE,
        CONSTRAINT bib_sha1_bib_source_fkey FOREIGN KEY (bib_source)
        REFERENCES config.bib_source (id) MATCH SIMPLE)";
        $dbHandler->update($query);

        $query = "CREATE INDEX e_bib_import_bib_sha1_idx
        ON e_bib_import.bib_sha1
        USING btree (sha1)";
        $dbHandler->update($query);
        
        $query = "CREATE INDEX e_bib_import_bib_bib_idx
        ON e_bib_import.bib_sha1
        USING btree (bib)";
        $dbHandler->update($query);
        
        $query = "CREATE INDEX e_bib_import_bib_bib_source_idx
        ON e_bib_import.bib_sha1
        USING btree (bib_source)";
        $dbHandler->update($query);

    }
}

 exit;
