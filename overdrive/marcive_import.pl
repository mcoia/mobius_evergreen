#!/usr/bin/perl

# These Perl modules are required:
# install Email::MIME
# install Email::Sender::Simple
# install Digest::SHA1


=for comment
/**


*/

=cut


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
use MARC::Charset 'marc8_to_utf8';
use MARC::Batch;

 my $configFile = @ARGV[0];
 if(!$configFile)
 {
    print "Please specify a config file\n";
    exit;
 }

 our $mobUtil = new Mobiusutil();
 our $conf = $mobUtil->readConfFile($configFile);
 our %conf;

 our $jobid=-1;
 our $log;
 our $archivefolder;
 our $importSourceName;
 our $importSourceNameDB;
 our $dbHandler;

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
        # $log->truncFile("");
        $log->addLogLine(" ---------------- Script Starting ---------------- ");
        my @reqs = ("server","login","password","sourcename","tempspace","archivefolder","dbhost","db","dbuser","dbpass","port","participants","logfile","eg_staged_bib_overlay_dir");
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
        $archivefolder = $conf{"archivefolder"};
        $importSourceName = $conf{"sourcename"};
        $importSourceNameDB = $importSourceName;
        $importSourceNameDB =~ s/\s/\-/g;

        if(!(-d $archivefolder))
        {
            $valid = 0;
            print "Sorry, the archive folder does not exist: $archivefolder\n";
            $errorMessage = "Sorry, the archive folder does not exist: $archivefolder";
        }
        #remove trailing slash
        $archivefolder =~ s/\/$//;
        my $finalImport = 0;
        my @info;
        my @infoAuthority;
        my $count=0;
        my $countAuthority=0;
        my @files;

        if($valid)
        {
            my @marcOutputRecords;
            my @authorityOutputRecords;

            $dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});
            setupSchema($dbHandler);

            # @files = ("/2017/06-Jun/test2.mrc");
            # @files = @{dirtrav(\@files,"/mnt/evergreen/utilityscripts/electronic_imports/molib2go_import/archive/2017/FY2017\\ Weeded\\ Titles")};
            # @files = @{dirtrav(\@files,"/mnt/evergreen/tmp/test/marc_records/marcive/archive/output/ftp/YSVMnWrp/test")};
            @files = @{getmarc($conf{"server"},$conf{"login"},$conf{"password"},$archivefolder,$log)};
            $log->addLine(Dumper(\@files));
            if(@files[$#files]!=-1)
            {
            #print Dumper(@files);
                my $cnt = 0;
                for my $b(0..$#files)
                {
                    my $thisfilename = lc($files[$b]);
                    $log->addLogLine("Parsing: $archivefolder".$files[$b]);
                    my $file = MARC::File::USMARC->in("$archivefolder".$files[$b]);


                    if(! ($thisfilename =~ m/all/))
                    {
                        while ( my $marc = $file->next() )
                        {
                            push(@marcOutputRecords,$marc);
                        }
                        $cnt++;
                    }
                    else
                    {
                        while ( my $marc = $file->next() )
                        {
                            push(@authorityOutputRecords,$marc);
                        }
                    }

                    $cnt++;
                    $file->close();
                    undef $file;
                }

                my $outputFile = $mobUtil->chooseNewFileName($conf{"tempspace"},"temp","mrc");
                my $marcout = new Loghandler($outputFile);
                $marcout->deleteFile();
                my $outputFileAuthority = $mobUtil->chooseNewFileName($conf{"tempspace"},"tempauthority","mrc");
                my $marcoutAuthority = new Loghandler($outputFileAuthority);
                $marcoutAuthority->deleteFile();


                my $output;

                foreach(@marcOutputRecords)
                {
                    my $marc = $_;
                    $output.= $marc->as_usmarc();
                    $count++;
                }
                $log->addLogLine("Outputting $count record(s) into $outputFile");
                $marcout->appendLineRaw($output);

                $output='';
                foreach(@authorityOutputRecords)
                {
                    my $marc = $_;
                    $output.=$marc->as_usmarc();
                    $countAuthority++;
                }
                $log->addLogLine("Outputting $countAuthority record(s) into $outputFileAuthority");
                $marcoutAuthority->appendLineRaw($output);
                undef $output;

                eval{$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});};
                if ($@)
                {
                    $log->addLogLine("Could not establish a connection to the database");
                    $log->addLogLine("Deleting $outputFile");
                    $marcout->deleteFile();
                    deleteFiles(\@files);
                    $valid = 0;
                    $errorMessage = "Could not establish a connection to the database";
                }
                if($valid)
                {
                    if(($count+$countAuthority) > 0)
                    {
                        $jobid = createNewJob('processing');
                        if($jobid!=-1)
                        {
                            @info = @{importMARCintoEvergreen($outputFile,$log,$dbHandler,$mobUtil)};
                            $finalImport = 1;
                            $log->addLine(Dumper(\@info));
                            @infoAuthority = @{importAuthority($outputFileAuthority,$log,$dbHandler,$mobUtil)};
                        }
                        else
                        {
                            $errorMessage = "Could not create a new job number in the schema - delete the downloaded files and restart.";
                            $log->addLogLine("Could not create a new job number in the schema - delete the downloaded files and restart.");
                            foreach(@files)
                            {
                                $log->addLogLine($_);
                            }
                        }
                    }
                }
                $marcout->deleteFile();
                updateJob("Completed","");
            }
            else
            {
                $log->addLogLine("There were some errors during the getmarc function, we are stopping execution. Any partially downloaded files are deleted.");
                $errorMessage = "There were some errors during the getmarc function, we are stopping execution. Any partially downloaded files are deleted.";
            }
        }
        if($finalImport)
        {
            my @notworked = @{@info[1]};
            my @updated = @{@info[2]};

            $log->addLogLine("Finished importing, moving to reporting");

            my $notWorkedCount = $#notworked+1;
            my $updatedCount = $#updated+1;

            my $fileCount = $#files+1;
            my $afterProcess = DateTime->now(time_zone => "local");
            my $difference = $afterProcess - $dt;
            my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
            my $duration =  $format->format_duration($difference);
            my $fileList;
            my $authorityUpdate;
            my $authorityNew;
            my $authorityFail;
            my $successUpdateTitleList;
            my $failedTitleList;
            foreach(@files)
            {
                my $temp = $_;
                $temp = substr($temp,rindex($temp, '/')+1);
                $fileList.="$temp ";
            }

            my $csvlines;

            foreach(@notworked)
            {
                my $title = $_;
                $failedTitleList.=$title."!!!!";
                my $csvline = "\"$dateString\",\"$errorMessage\",\"Failed Insert\",\"\",\"$title\",\"$duration\",\"$count Record(s)\",\"$fileCount File(s)\",\"$notWorkedCount failed\",\"$updatedCount Updated\",\"$fileList\"";
                $csvline=~s/\n//g;
                $csvline=~s/\r//g;
                $csvline=~s/\r\n//g;
                $csvlines.="$csvline\n";
            }
            foreach(@updated)
            {
                my @both = @{$_};
                my $bibid = @both[0];
                my $title = @both[1];
                $successUpdateTitleList.=$bibid." ".$title."!!!!";
                my $csvline = "\"$dateString\",\"$errorMessage\",\"Success Update\",\"$bibid\",\"$title\",\"$duration\",\"$count Record(s)\",\"$fileCount File(s)\",\"$notWorkedCount failed\",\"$updatedCount Updated\",\"$fileList\"";
                $csvline=~s/\n//g;
                $csvline=~s/\r//g;
                $csvline=~s/\r\n//g;
                $csvlines.="$csvline\n";
            }

            my $totalSuccess=1;
            $totalSuccess=0 if($notWorkedCount>0);

            @updated = @{@infoAuthority[0]};
            my @worked = @{@infoAuthority[1]};
            @notworked = @{@infoAuthority[2]};

            $notWorkedCount = $#notworked+1;
            $updatedCount = $#updated+1;
            my $newCount = $#worked+1;

            $log->addLogLine("Authority notworked");
            foreach(@notworked)
            {
                my @both = @{$_};
                my $bibid = @both[0];
                my $title = @both[1];
                $authorityFail.=$bibid." ".$title."!!!!";
                my $csvline = "\"$dateString\",\"$errorMessage\",\"Failed Insert\",\"\",\"$title\",\"$duration\",\"$count Record(s)\",\"$fileCount File(s)\",\"$notWorkedCount Authority failed\",\"$updatedCount Authority Updated $newCount Authority Created\",\"$fileList\"";
                $csvline=~s/\n//g;
                $csvline=~s/\r//g;
                $csvline=~s/\r\n//g;
                $csvlines.="$csvline\n";
            }
            $log->addLogLine("Authority updated");
            foreach(@updated)
            {
                my @both = @{$_};
                my $bibid = @both[0];
                my $title = @both[1];
                $authorityUpdate.=$bibid." ".$title."!!!!";
                my $csvline = "\"$dateString\",\"$errorMessage\",\"Success Update\",\"$bibid\",\"$title\",\"$duration\",\"$count Record(s)\",\"$fileCount File(s)\",\"$notWorkedCount failed\",\"$updatedCount Updated\",\"$fileList\"";
                $csvline=~s/\n//g;
                $csvline=~s/\r//g;
                $csvline=~s/\r\n//g;
                $csvlines.="$csvline\n";
            }
            $log->addLogLine("Authority worked");
            foreach(@worked)
            {
                my @both = @{$_};
                my $bibid = @both[0];
                my $title = @both[1];
                $authorityNew.=$bibid." ".$title."!!!!";
                my $csvline = "\"$dateString\",\"$errorMessage\",\"Success Update\",\"$bibid\",\"$title\",\"$duration\",\"$count Record(s)\",\"$fileCount File(s)\",\"$notWorkedCount failed\",\"$updatedCount Updated\",\"$fileList\"";
                $csvline=~s/\n//g;
                $csvline=~s/\r//g;
                $csvline=~s/\r\n//g;
                $csvlines.="$csvline\n";
            }


            if($conf{"csvoutput"})
            {
                my $csv = new Loghandler($conf{"csvoutput"});
                $csv->addLine($csvlines);
                undef $csv;
            }
            if(length($errorMessage)>0)
            {
                my @tolist = ($conf{"alwaysemail"});
                my $email = new email($conf{"fromemail"},\@tolist,1,0,\%conf);
                $email->send("Evergreen Utility - $importSourceName Import Report Job # $jobid - ERROR","$errorMessage\r\n\r\n-Evergreen Perl Squad-");

            }
            else
            {
                $successUpdateTitleList = truncateOutput($successUpdateTitleList,5000);
                $failedTitleList = truncateOutput($failedTitleList,5000);

                $authorityNew = truncateOutput($authorityNew,5000);
                $authorityUpdate = truncateOutput($authorityUpdate,5000);
                $authorityFail = truncateOutput($authorityFail,5000);

                $totalSuccess=0 if($notWorkedCount>0);

                $log->addLogLine("Emailing....");

                my @tolist = ($conf{"alwaysemail"});
                my $email = new email($conf{"fromemail"},\@tolist,$valid,$totalSuccess,\%conf);
                my $reports = gatherOutputReport($log,$dbHandler);
                my $body = "Connected to: !!!! ".$conf{"server"}."!!!!Gathered:!!!!$count adds and $countAuthority authority records from $fileCount file(s)!!!! Duration: $duration
    !!!!!!!!Files:!!!!$fileList
$reports!!!!!!!!
**************** Bib Record Section ******************!!!!
Successful Updates:!!!!
$successUpdateTitleList!!!!
Unsuccessful:!!!!$failedTitleList!!!!!!!!
**************** Authority Section ******************!!!!
New Authority Imports:!!!!
$authorityNew!!!!!!!!
Successful Updates:!!!!
$authorityUpdate!!!!
Unsuccessful:!!!!
$authorityFail!!!!!!!!
Authority failures can be due to \"cancellation authorities\"!!!!!!!!
-Evergreen Perl Squad-";
                $body = OpenILS::Application::AppUtils->entityize($body);
                $body =~ s/[\x00-\x1f]//go;
                $body =~ s/!!!!/\r\n/go;
                $email->send("Evergreen Utility - $importSourceName Import Report Job # $jobid",$body);

            }
        }
        $log->addLogLine(" ---------------- Script Ending ---------------- ");
    }
    else
    {
        print "Config file does not define 'logfile'\n";
    }
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
    my $log = @_[0];
    my $dbHandler = @_[1];
    my $newRecordCount=0;
    my $updatedRecordCount=0;
    my $mergedRecords='';
    my $itemsAssignedRecords='';
    my $undedupeRecords='';
    #bib_marc_update table report new bibs
    my $query = "select count(*) from molib2go.bib_marc_update where job=$jobid and new_record is true";
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        $newRecordCount=@row[0];
    }
    #bib_marc_update table report non new bibs
    $query = "select count(*) from molib2go.bib_marc_update where job=$jobid and new_record is not true";
    @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        $updatedRecordCount=@row[0];
    }

    #bib_merge table report
    $query = "select leadbib,subbib from molib2go.bib_merge where job=$jobid";
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
    $query = "select target_bib,prev_bib from molib2go.item_reassignment where job=$jobid";
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

    #undedupe table report
    $query = "select undeletedbib,oldleadbib,(select label from asset.call_number where id=a.moved_call_number) from molib2go.undedupe a where job=$jobid";
    @results = @{$dbHandler->query($query)};
    $count=0;
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        $undedupeRecords.=@row[0]." < ".@row[1]." - '".@row[2]."'\r\n";
        $count++;
    }
    if($count>0)
    {
        $undedupeRecords = truncateOutput($undedupeRecords,5000);
        $undedupeRecords="$count records had physical items and were moved onto a previously deduped bib - The left number is the undeleted deduped bib\r\n
        The right is the molib2go bib that had it's items moved onto the undeduped bib.\r\n
        We have included the call number as well\r\n".$undedupeRecords;
        $undedupeRecords."\r\n\r\n\r\n";
    }
    my $ret=$newRecordCount." New record(s) were created.\r\n\r\n\r\n".
    $updatedRecordCount." Record(s) were updated\r\n\r\n\r\n".$mergedRecords.$itemsAssignedRecords.$undedupeRecords;
    #print $ret;
    return $ret;

}

sub deleteFiles
{
    my $log = @_[0];
    my @files = @{@_[1]};
    foreach(@files)
    {
        my $t = new Loghandler($archivefolder.$_);
        $log->addLogLine("Deleting $_");
        $t->deleteFile();
    }
}

sub getmarc
{
    my $server = @_[0];
    $server=~ s/http:\/\///gi;
    $server=~ s/ftp:\/\///gi;

    my $loops=0;
    my $login = @_[1];
    my $password = @_[2];
    my $archivefolder = @_[3];
    my @ret = ();

    $log->addLogLine("**********FTP starting -> $server with $login and $password");

    my $ftp = Net::FTP->new($server, Debug => 0, Passive=> 1)
    or die $log->addLogLine("Cannot connect to ".$server);
    $ftp->login($login,$password)
    or die $log->addLogLine("Cannot login ".$ftp->message);
    my @interestingFiles = ();
    # push @interestingFiles , "/2017/02-Feb/mo2go20170214_01 (9 ebook records).dat";
    # push @interestingFiles , "/2017/01-Jan/mo2go20170103.dat";
    # push @interestingFiles , "/2017/FY2017 Weeded Titles/Weeded Titles FY2017.csv";
    $ftp->cwd("/output/ftp/YSVMnWrp");
    @interestingFiles = @{ftpRecurse($ftp, \@interestingFiles)};
    $log->addLine(Dumper(\@interestingFiles));
    foreach(@interestingFiles)
    {
        my $download = 1;
        my $filename = $_;

        # gotta escape the space character when working on the bash prompt
        my $localfilename = $filename;
        $localfilename =~ s/\s/\\ /g;
        # gotta remove the ampersand character when working on the bash prompt
        $localfilename =~s/&/_/g;
        $log->addLine("Checking $archivefolder".$filename);
        if(-e "$archivefolder"."$filename")
        {
            my $size = stat("$archivefolder"."$filename")->size; #[7];
            my @rsizes = $ftp->dir($filename);
            my $rsize = @rsizes[0] ? @rsizes[0] : '0';
            #remove the filename from the string
            my $rfile = $filename;
            # parenthesis and slashes in the filename screw up the regex
            $rfile =~ s/\(/\\(/g;
            $rfile =~ s/\)/\\)/g;
            $rfile =~ s/\//\\\//g;
            $rsize =~ s/$rfile//g;
            $log->addLine($rsize);
            my @split = split(/\s+/, $rsize);
            @split = reverse @split;
            pop @split while $#split > 4;
            $rsize = pop @split;
            $log->addLine("Local: $size");
            $log->addLine("Remot: $rsize");
            if($size ne $rsize)
            {
                $log->addLine("$archivefolder"."$filename differes in size remote $filename");
                unlink("$archivefolder"."$filename");
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
            my $path = $archivefolder.$filename;
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
            # $log->addLine("Downloading to $archivefolder$filename");
            my $worked = $ftp->get($filename,$archivefolder.$filename);
            if($worked)
            {
                push (@ret, "$filename");
            }
        }
    }

    $ftp->quit
    or die $log->addLogLine("Unable to close FTP connection");
    $log->addLogLine("**********FTP session closed ***************");
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
            @interestingFiles = @{ftpRecurse($ftpOb,\@interestingFiles)};
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
    if( ( (lc($filename) =~ m/\.dat/g) || ( lc($filename) =~ m/\.csv/g) || ( lc($filename) =~ m/stat/g) || ( lc($filename) =~ m/related/g) ) )
    {
        return 0;
    }
    return $download;
}

sub removeOldCallNumberURI
{
    my $bibid = @_[0];
    my $dbHandler = @_[1];

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


sub importMARCintoEvergreen
{
    my @ret;
    my @worked;
    my @notworked;
    my @updated;
    my $inputFile = @_[0];
    my $log = @_[1];
    my $dbHandler = @_[2];
    my $mobUtil = @_[3];
    my $file = MARC::File::USMARC->in( $inputFile );
    my $r =0;
    my $overlay = 0;
    my $query;
    #print "Working on importMARCintoEvergreen\n";
    updateJob("Processing","importMARCintoEvergreen");

    while ( my $marc = $file->next() )
    {

        if(1)#$r>8686)#$overlay<16)
        {
            #my $tcn = getTCN($log,$dbHandler);  #removing this because it has an auto created value in the DB
            my $title = getsubfield($marc,'245','a');
            my $t901a = getsubfield($marc,'901','a');
            #print "Importing $title\n";
            my $bibid=-1;
            my $bibid = findRecord($marc, $dbHandler, $log);

            if($bibid!=-1) #already exists so update the marc
            {
                my @comeback = @{chooseWinnerAndDeleteRest($bibid, $dbHandler, $marc, \@notworked, \@updated, $log)};
                @updated = @{@comeback[0]};
                @notworked = @{@comeback[1]};
                $overlay+=$#updated+1;
            }
            else
            {
                push(@notworked, $t901a." not mached, probably deleted");
            }
        }
        $r++;
    }
    $file->close();
    undef $file;
    push(@ret, (\@worked, \@notworked, \@updated));
    #print Dumper(@ret);
    return \@ret;
}

sub importAuthority
{
    my @ret;
    my @worked;
    my @notworked;
    my @updated;
    our %conf;
    my $inputFile = @_[0];
    my $log = @_[1];
    my $dbHandler = @_[2];
    my $mobUtil = @_[3];

    updateJob("Processing","importAUTHORITYintoEvergreen");

    # we are going to use the eg_staged_bib_overlay tool to import the authority records. This tool needs to be available in the directory specified in the config file
    my $bashOutputFile = $conf{"tempspace"}."/authload$jobid";
    my $execScript = $conf{"eg_staged_bib_overlay_dir"}."/eg_staged_bib_overlay";

    my $cmd = "$execScript --schema auth_load --batch auth$jobid --db ".$conf{"db"}." --dbuser ".$conf{"dbuser"}." --dbhost ".$conf{"dbhost"}." --dbpw ".$conf{"dbpass"}." --action stage_auths $inputFile > $bashOutputFile";
    $log->addLogLine($cmd);
    system($cmd);
    $cmd = "$execScript --schema auth_load --batch auth$jobid --db ".$conf{"db"}." --dbuser ".$conf{"dbuser"}." --dbhost ".$conf{"dbhost"}." --dbpw ".$conf{"dbpass"}." --action match_auths >> $bashOutputFile";
    $log->addLogLine($cmd);
    system($cmd);
    $cmd = "$execScript --schema auth_load --batch auth$jobid --db ".$conf{"db"}." --dbuser ".$conf{"dbuser"}." --dbhost ".$conf{"dbhost"}." --dbpw ".$conf{"dbpass"}." --action load_new_auths >> $bashOutputFile";
    $log->addLogLine($cmd);
    system($cmd);
    $cmd = "$execScript --schema auth_load --batch auth$jobid --db ".$conf{"db"}." --dbuser ".$conf{"dbuser"}." --dbhost ".$conf{"dbhost"}." --dbpw ".$conf{"dbpass"}." --action overlay_auths_stage1 >> $bashOutputFile";
    $log->addLogLine($cmd);
    system($cmd);
    $cmd = "$execScript --schema auth_load --batch auth$jobid --db ".$conf{"db"}." --dbuser ".$conf{"dbuser"}." --dbhost ".$conf{"dbhost"}." --dbpw ".$conf{"dbpass"}." --action overlay_auths_stage2 >> $bashOutputFile";
    $log->addLogLine($cmd);
    system($cmd);
    $cmd = "$execScript --schema auth_load --batch auth$jobid --db ".$conf{"db"}." --dbuser ".$conf{"dbuser"}." --dbhost ".$conf{"dbhost"}." --dbpw ".$conf{"dbpass"}." --action overlay_auths_stage3 >> $bashOutputFile";
    $log->addLogLine($cmd);
    system($cmd);

    # Gather up the updated authority bibs with heading
    my $query = "
        select aaa.auth_id,(select left(string_agg(ash.value,', ' ),20) from authority.simple_heading ash where ash.record=aaa.auth_id) from
    auth_load.auths_auth$jobid aaa
    where
    aaa.auth_id is not null and
    aaa.imported";
    $log->addLogLine($query);
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        my $id = @row[0];
        my $heading = @row[1];
        my @t = ($id,$heading);
        push @updated, [@t];
    }

    # Gather up the new authority bibs with heading
    my $query = "
        select aaa.new_auth_id,(select left(string_agg(ash.value,', ' ),20) from authority.simple_heading ash where ash.record=aaa.new_auth_id) from
        auth_load.auths_auth$jobid aaa
        where
        aaa.new_auth_id is not null and
        aaa.imported";
    $log->addLogLine($query);
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        my $id = @row[0];
        my $heading = @row[1];
        my @t = ($id,$heading);
        push @worked, [@t];
    }


    # Gather up the non imported authority bibs with heading
    my $query = "select auth_id||' '||new_auth_id||' '||cancelled_auth_id,heading from auth_load.auths_auth$jobid where not imported";
    $log->addLogLine($query);
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        $log->addLine($_);
        my $row = $_;
        my @row = @{$row};
        my $id = @row[0];
        my $heading = @row[1];
        my @t = ($id,$heading);
        push @notworked, [@t];
    }

    push @ret, [@updated];
    push @ret, [@worked];
    push @ret, [@notworked];

    return \@ret;
}

sub chooseWinnerAndDeleteRest
{
    my @list = @{@_[0]};
    my $dbHandler = @_[1];
    my $finalMARC = @_[2];
    my @notworked = @{@_[3]};
    my @updated = @{@_[4]};
    my $log = @_[5];
    my $title;
    my $winnerBibID;
    my $ogmarcxml;
    $title = $finalMARC->field('245') if $finalMARC->field('245');
    $title = $title->subfield('a') if($title && $title->subfield('a'));
    $finalMARC = convertMARCtoXML($finalMARC);
    foreach(@list)
    {
        my @attrs = @{$_};
        my $id = @attrs[0];
        $ogmarcxml = @attrs[3];
        $winnerBibID = $id;
    }

    my @values = ($finalMARC);
    my $query = "UPDATE BIBLIO.RECORD_ENTRY SET marc=\$1 , editor=1 WHERE ID=$winnerBibID";
updateJob("Processing","chooseWinnerAndDeleteRest   $query");
    $log->addLine($query);
    $log->addLine("$winnerBibID\thttp://missourievergreen.org/eg/opac/record/$winnerBibID?locg=4;expand=marchtml#marchtml\thttp://mig.missourievergreen.org/eg/opac/record/$winnerBibID?locg=157;expand=marchtml#marchtml");
    my $res = $dbHandler->updateWithParameters($query,\@values);
    #print "$res\n";
    if($res)
    {
        my @temp = ($winnerBibID, $title);
        push @updated, [@temp];
    }
    else
    {
        push (@notworked, $winnerBibID);
    }
    my @ret;
    push @ret, [@updated];
    push @ret, [@notworked];

    return \@ret;

}

sub findRecord
{
    my $marcsearch = @_[0];
    if ( $marcsearch->field('901') )
    {
        my $bibid = $marcsearch->field('901')->subfield('c');
        my $dbHandler = @_[1];
        my $log = @_[2];
        my $query = "SELECT bre.ID,bre.MARC FROM BIBLIO.RECORD_ENTRY bre WHERE bre.id = $bibid and not deleted";
    updateJob("Processing","$query");
        my @results = @{$dbHandler->query($query)};
        my @ret;
        my $none=1;
        my $count=0;
        foreach(@results)
        {
            my $row = $_;
            my @row = @{$row};
            my $id = @row[0];
            my $marc = @row[1];
            print "found matching: $id\n";
            my $prevmarc = $marc;
            $prevmarc =~ s/(<leader>.........)./${1}a/;
            $prevmarc = MARC::Record->new_from_xml($prevmarc);
            my $score = scoreMARC($prevmarc,$log);
            my @matchedsha = ($id,$prevmarc,$score,$marc);
            push (@ret, [@matchedsha]);
            $none=0;
            $count++;
        }
        if($none)
        {
            return -1;
        }
        print "Count matches: $count\n";
    updateJob("Processing","Count matches: $count");
        return \@ret;
    }
    return -1;
}

sub mergeMARC856
{
    my $marc = @_[0];
    my $marc2 = @_[1];
    my $log = @_[2];
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
    my $dbHandler = @_[0];

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

sub getTCN
{
    my $log = @_[0];
    my $dbHandler = @_[1];
    my $dbmax=getEvergreenMax($dbHandler);
    $dbmax++;
    my $result = 1;
    my $seed=0;
    my $ap="";
    my $trys = 0;
    while($result==1)
    {
        my $query = "SELECT COUNT(*) FROM BIBLIO.RECORD_ENTRY WHERE TCN_VALUE = 'od$dbmax$ap'";
        my @results = @{$dbHandler->query($query)};
        foreach(@results)
        {
            my $row = $_;
            my @row = @{$row};
            if(@row[0]==0)
            {
                $result=0;
            }
        }
        $ap = "_".$seed;
        $seed++;
        $trys++;
    }
    if($trys>1)
    {
        $log->addLogLine("Needed to change tcn $trys times to find: 'od$dbmax$ap'");
    }
    return "od$dbmax$ap";
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

sub createNewJob
{
    my $status = @_[0];
    my $query = "INSERT INTO molib2go.job(status) values('$status')";
    my $results = $dbHandler->update($query);
    if($results)
    {
        $query = "SELECT max( ID ) FROM molib2go.job";
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
    my $query = "UPDATE molib2go.job SET last_update_time=now(),status='$status', CURRENT_ACTION_NUM = CURRENT_ACTION_NUM+1,current_action='$action' where id=$jobid";
    $log->addLine($action);
    my $results = $dbHandler->update($query);
    return $results;
}

sub findPBrecordInME
{
    my $dbHandler = @_[0];
    #my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE MARC ~ '\"9\">PB' limit 14";
    my $query = "select id,marc from biblio.record_entry where marc ~* 'overdrive' AND marc ~* 'ebook' AND ID IN(SELECT RECORD FROM ASSET.CALL_NUMBER WHERE LABEL!=\$\$##URI##\$\$ and deleted is false)";
    my @results = @{$dbHandler->query($query)};
    my @each;
    my @ret;
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        my $id = @row[0];
        my $marc = @row[1];
        @each = ($id,$marc);
        push(@ret,[@each]);
    }
    return \@ret;
}

sub findMatchInArchive
{
    my @matchList = @{@_[0]};
    my $archiveFolder = @_[1];
    my @files;
    #Get all files in the directory path
    @files = @{dirtrav(\@files,$archiveFolder)};
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
    my $log = shift;

    my $score = 0;
    $score+= score($marc,2,100,400,$log,'245');
    $score+= score($marc,1,1,150,$log,'100');
    $score+= score($marc,1,1.1,150,$log,'110');
    $score+= score($marc,0,50,200,$log,'6..');
    $score+= score($marc,0,50,100,$log,'02.');

    $score+= score($marc,0,100,200,$log,'246');
    $score+= score($marc,0,100,100,$log,'130');
    $score+= score($marc,0,100,100,$log,'010');
    $score+= score($marc,0,100,200,$log,'490');
    $score+= score($marc,0,10,50,$log,'830');

    $score+= score($marc,1,.5,50,$log,'300');
    $score+= score($marc,0,1,100,$log,'7..');
    $score+= score($marc,2,2,100,$log,'50.');
    $score+= score($marc,2,2,100,$log,'52.');

    $score+= score($marc,2,.5,200,$log,'51.', '53.', '54.', '55.', '56.', '57.', '58.');

    return $score;
}

sub score
{
    my ($marc) = shift;
    my ($type) = shift;
    my ($weight) = shift;
    my ($cap) = shift;
    my ($log) = shift;
    my @tags = @_;
    my $ou = Dumper(@tags);
    #$log->addLine("Tags: $ou\n\nType: $type\nWeight: $weight\nCap: $cap");
    my $score = 0;
    if($type == 0) #0 is field count
    {
        #$log->addLine("Calling count_field");
        $score = count_field($marc,$log,\@tags);
    }
    elsif($type == 1) #1 is length of field
    {
        #$log->addLine("Calling field_length");
        $score = field_length($marc,$log,\@tags);
    }
    elsif($type == 2) #2 is subfield count
    {
        #$log->addLine("Calling count_subfield");
        $score = count_subfield($marc,$log,\@tags);
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
    my $log = $_[1];
    my @tags = @{$_[2]};
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
    my $log = $_[1];
    my @tags = @{$_[2]};
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
    my $log = $_[1];
    my @tags = @{$_[2]};

    my @f = $marc->field(@tags[0]);
    return 0 unless @f;
    my $len = length($f[0]->as_string);
    my $ou = Dumper(@f);
    #$log->addLine($ou);
    #$log->addLine("Field Length: $len");
    return $len;
}


sub setupSchema
{
    my $dbHandler = @_[0];
    my $query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'molib2go'";
    my @results = @{$dbHandler->query($query)};
    if($#results==-1)
    {
        $query = "CREATE SCHEMA molib2go";
        $dbHandler->update($query);
        $query = "CREATE TABLE molib2go.job
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
        $query = "CREATE TABLE molib2go.item_reassignment(
        id serial,
        copy bigint,
        prev_bib bigint,
        target_bib bigint,
        change_time timestamp default now(),
        job  bigint NOT NULL,
        CONSTRAINT item_reassignment_fkey FOREIGN KEY (job)
        REFERENCES molib2go.job (id) MATCH SIMPLE
        )";
        $dbHandler->update($query);
        $query = "CREATE TABLE molib2go.bib_marc_update(
        id serial,
        record bigint,
        prev_marc text,
        changed_marc text,
        new_record boolean NOT NULL DEFAULT false,
        change_time timestamp default now(),
        job  bigint NOT NULL,
        CONSTRAINT bib_marc_update_fkey FOREIGN KEY (job)
        REFERENCES molib2go.job (id) MATCH SIMPLE)";
        $dbHandler->update($query);
        $query = "CREATE TABLE molib2go.bib_merge(
        id serial,
        leadbib bigint,
        subbib bigint,
        change_time timestamp default now(),
        job  bigint NOT NULL,
        CONSTRAINT bib_merge_fkey FOREIGN KEY (job)
        REFERENCES molib2go.job (id) MATCH SIMPLE)";
        $dbHandler->update($query);
        $query = "CREATE TABLE molib2go.undedupe(
        id serial,
        oldleadbib bigint,
        undeletedbib bigint,
        undeletedbib_electronic_score bigint,
        undeletedbib_marc_score bigint,
        moved_call_number bigint,
        change_time timestamp default now(),
        job  bigint NOT NULL,
        CONSTRAINT undedupe_fkey FOREIGN KEY (job)
        REFERENCES molib2go.job (id) MATCH SIMPLE)";
        $dbHandler->update($query);
        $query = "CREATE TABLE molib2go.nine_sync(
        id serial,
        record bigint,
        nines_synced text,
        url text,
        change_time timestamp default now())";
        $dbHandler->update($query);
    }
}

 exit;


