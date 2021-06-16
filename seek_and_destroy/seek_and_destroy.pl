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
use email;
use DateTime;
use utf8;
use Encode;
use DateTime;
use LWP::Simple;
use OpenILS::Application::AppUtils;
use DateTime::Format::Duration;
use Digest::SHA1;
use XML::Simple;
use Unicode::Normalize;
use Getopt::Long;



my $configFile = @ARGV[0];
my $xmlconf = "/openils/conf/opensrf.xml";
our $dryrun=0;
our $debug=0;
our $reportonly=0;
our $runCleamMetarecords=0;
our $runElectronic=0;
our $runAudioBook=0;
our $runVideo=0;
our $runLargePrint=0;
our $runMusic=0;
our $runMoveElectronicBooks=0;
our $runMoveElectronicAudioBooks=0;
our $runMoveAudioBooks=0;
our $runDedupe=0;
our $runFindElectronic856TOC=0;
our $resyncTattle=0;
our $doNotRunFullReEval=0;

our %tattleSystemDone = ();
our $resetScores=0;

my $help = "
You can specify
[Global settings]
--config configfilename                       [Path to the config file - required]             
--xmlconfig pathto_opensrf.xml                [Defaults to /openils/conf/opensrf.xml]
--debug flag                                  [Cause more output]
--dryrun flag                                 [Cause no production table updates]
--runCleamMetarecords flag                    [Run the Metarecord clean routine]

[What to execute - used mainly]
--runElectronic flag                          [Find bibs that are possibly Electronic (wave cleanup)]
--runAudioBook flag                           [Find bibs that are possibly AudioBook (wave cleanup)]
--runVideo flag                               [Find bibs that are possibly DVD/BluRay/VHS (wave cleanup)]
--runLargePrint flag                          [Find bibs that are possibly Large Print (wave cleanup)]
--runMusic flag                               [Find bibs that are possibly Music (wave cleanup)]

[What to execute - rusty old code, hardly used]
--runMoveElectronicBooks flag                 [Find items attached to electronic bibs and attempt to move items to non-electronic version]
--runMoveElectronicAudioBooks flag            [Find items attached to electronic AudioBook bibs and attempt to move items to non-electronic version]
--runMoveAudioBooks flag                      [Find items attached to AudioBook bibs and attempt to move items to non-AudioBook version]
--runFindElectronic856TOC flag                [Find Electronic 856 links that have indicators that cause URL's to be clickable but they are the Table of Contents]

[This is used AFTER all of the formats have been ran]
--runDedupe flag                              [Find Duplicate bibs and merge those that are close enough]
--resetScores flag                            [Empty out the schema table of any bib scores - recommended when running dedupe]

[A nice flag to skip everything and just run the report]
--reportonly flag                             [Skip everything and only run a report - Reports are always run at the end]
--resyncTattle flag                           [Cause the software to re-import the Tattle queries from query file]

**** IMPORTANT SETTING **** 
--doNotRunFullReEval flag                     [This will cause the software to execute the bib conversion based on previously collected scores]

NOTE: Used in conjunction with [What to execute - used mainly]
      If you set this setting, the software will convert bibs to the selected format based on a previous run.
      See schema seekdestroy.bib_scores and subroutine [sub findInvalidMARC] in this script
\n";

GetOptions (
"config=s" => \$configFile,
"xmlconfig=s" => \$xmlconf,
"dryrun" => \$dryrun,
"runCleamMetarecords" => \$runCleamMetarecords,
"runElectronic" => \$runElectronic,
"runAudioBook" => \$runAudioBook,
"runVideo" => \$runVideo,
"runLargePrint" => \$runLargePrint,
"runMusic" => \$runMusic,
"runMoveElectronicBooks" => \$runMoveElectronicBooks,
"runMoveElectronicAudioBooks" => \$runMoveElectronicAudioBooks,
"runMoveAudioBooks" => \$runMoveAudioBooks,
"runDedupe" => \$runDedupe,
"runFindElectronic856TOC" => \$runFindElectronic856TOC,
"resetScores" => \$resetScores,
"doNotRunFullReEval" => \$doNotRunFullReEval,
"reportonly" => \$reportonly,
"resyncTattle" => \$resyncTattle,
"debug" => \$debug
)
or die("Error in command line arguments $help");

if(! -e $xmlconf)
{
	print "I could not find the xml config file: $xmlconf $help";
	exit 0;
}
 if(!$configFile)
 {
	print "Please specify a config file\n $help";
	exit;
 }

	our $mobUtil = new Mobiusutil();  
	my $conf = $mobUtil->readConfFile($configFile);
	our $jobid=-1;
	our $log;
	our $dbHandler;
	our $audio_book_score_when_audiobooks_dont_belong;
	our $electronic_score_when_bib_is_considered_electronic;
	our @electronicSearchPhrases;
	our @audioBookSearchPhrases;
	our @microficheSearchPhrases;
	our @microfilmSearchPhrases;
	our @videoSearchPhrases;
	our @largePrintBookSearchPhrases;
	our @musicSearchPhrases;
	our @musicSearchPhrasesAddition;
	our @playawaySearchPhrases;
	our @seekdestroyReportFiles =();
	our %queries;
	our %conf;
	our $baseTemp;
	our $domainname;
  
 if($conf)
 {
	%conf = %{$conf};
	if($conf{"queryfile"})
	{
		my $queries = $mobUtil->readQueryFile($conf{"queryfile"});
		if($queries)
		{
			%queries = %{$queries};
            undef $queries;
		}
		else
		{
			print "Please provide a queryfile stanza in the config file\n";
			exit;
		}
	}
	else
	{
		print "Please provide a queryfile stanza in the config file\n";
		exit;	
	}
	$audio_book_score_when_audiobooks_dont_belong = $conf{"audio_book_score_when_audiobooks_dont_belong"};
	$electronic_score_when_bib_is_considered_electronic = $conf{"electronic_score_when_bib_is_considered_electronic"};
	#print "electronic_score_when_bib_is_considered_electronic = $electronic_score_when_bib_is_considered_electronic\n";
	#print "audio_book_score_when_audiobooks_dont_belong = $audio_book_score_when_audiobooks_dont_belong\n";
	@electronicSearchPhrases = $conf{"electronicsearchphrases"} ? @{$mobUtil->makeArrayFromComma($conf{"electronicsearchphrases"})} : ();
	@audioBookSearchPhrases = $conf{"audiobooksearchphrases"} ? @{$mobUtil->makeArrayFromComma($conf{"audiobooksearchphrases"})} : ();
	@microficheSearchPhrases = $conf{"microfichesearchphrases"} ? @{$mobUtil->makeArrayFromComma($conf{"microfichesearchphrases"})} : ();
	@microfilmSearchPhrases = $conf{"microfilmsearchphrases"} ? @{$mobUtil->makeArrayFromComma($conf{"microfilmsearchphrases"})} : ();
	@videoSearchPhrases = $conf{"videosearchphrases"} ? @{$mobUtil->makeArrayFromComma($conf{"videosearchphrases"})} : ();
	@largePrintBookSearchPhrases = $conf{"largeprintbooksearchphrases"} ? @{$mobUtil->makeArrayFromComma($conf{"largeprintbooksearchphrases"})} : ();
	@musicSearchPhrases = $conf{"musicsearchphrases"} ? @{$mobUtil->makeArrayFromComma($conf{"musicsearchphrases"})} : ();
	@musicSearchPhrasesAddition = $conf{"musicsearchphrasesaddition"} ? @{$mobUtil->makeArrayFromComma($conf{"musicsearchphrasesaddition"})} : ();
	@playawaySearchPhrases = $conf{"playawaysearchphrases"} ? @{$mobUtil->makeArrayFromComma($conf{"playawaysearchphrases"})} : ();
	
	if ($conf{"logfile"})
	{
		my $dt = DateTime->now(time_zone => "local"); 
		my $fdate = $dt->ymd;
		my $ftime = $dt->hms;
		my $dateString = "$fdate $ftime";
		$log = new Loghandler($conf->{"logfile"});
		$log->truncFile("");
		$log->addLogLine(" ---------------- Script Starting ---------------- ");
		print "Executing job  tail the log for information (".$conf{"logfile"}.")\nDryrun = $dryrun\n";		
		my @reqs = ("logfile","tempdir","domainname","playawaysearchphrases","musicsearchphrases","musicsearchphrasesaddition","largeprintbooksearchphrases","videosearchphrases","microfilmsearchphrases","microfichesearchphrases","audiobooksearchphrases","electronicsearchphrases"); 
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
			my %dbconf = %{getDBconnects($xmlconf)};
			$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
			setupSchema();
			$baseTemp = $conf{"tempdir"};
			$domainname = lc($conf{"domainname"});
			$baseTemp =~ s/\/$//;
			$baseTemp.='/';
			$domainname =~ s/\/$//;
			$domainname =~ s/^http:\/\///;
			$domainname.='/';
			$domainname = 'http://'.$domainname;
			if(!$reportonly)
			{
				$jobid = createNewJob('processing');
				if($jobid!=-1)
				{	
					print "You can see what operation the software is executing with this query:\nselect * from  seekdestroy.job where id=$jobid\n";
					
					
                    $dbHandler->update("truncate SEEKDESTROY.BIB_MATCH") if $resetScores;
                    $dbHandler->update("truncate SEEKDESTROY.BIB_SCORE") if $resetScores;

					cleanMetaRecords() if $runCleamMetarecords;
                    findInvalidElectronicMARC() if $runElectronic;
                    findInvalidAudioBookMARC() if $runAudioBook;
                    findInvalidDVDMARC() if $runVideo;
                    findInvalidLargePrintMARC() if $runLargePrint;
                    findInvalidMusicMARC() if $runMusic;
	
					findPhysicalItemsOnElectronicBooks() if $runMoveElectronicBooks;
                    findPhysicalItemsOnElectronicAudioBooks() if $runMoveElectronicAudioBooks;
                    findItemsCircedAsAudioBooksButAttachedNonAudioBib() if $runMoveAudioBooks;
                    findItemsNotCircedAsAudioBooksButAttachedAudioBib() if $runMoveAudioBooks;

                    findPossibleDups() if $runDedupe;
                    findInvalid856TOCURL() if $runFindElectronic856TOC;

                    
                    #######################################################
                    #
                    # Custom Hack code
                    #
                    #######################################################

                    # tag902s();
					# my $problemPhrase = "MARC with audiobook phrases but incomplete marc";
					# my $subQueryConvert = $queries{"non_audiobook_bib_convert_to_audiobook"};
					# $subQueryConvert =~ s/\$problemphrase/$problemPhrase/g;
					# updateScoreWithQuery("select id,marc from biblio.record_entry where id in($subQueryConvert)"); 
					# my $problemPhrase = "MARC with music phrases but incomplete marc";
					# my $subQueryConvert = $queries{"non_music_bib_convert_to_music"};
					# $subQueryConvert =~ s/\$problemphrase/$problemPhrase/g;
					# updateScoreWithQuery("select id,marc from biblio.record_entry where id in($subQueryConvert)");

					# my $results = $dbHandler->query("select marc from biblio.record_entry where id=1362462")->[0];
                    # determineWhichVideoFormat(1362462,$results->[0]);
                    # updateScoreWithQuery("select id,marc from biblio.record_entry where id in(244015)"); 
                    # exit;
                    # updateScoreWithQuery("select bibid,(select marc from biblio.record_entry where id=bibid) from 
                    # (
                    # select distinct bib1 as \"bibid\" from SEEKDESTROY.BIB_MATCH
                    # union
                    # select distinct bib2 as \"bibid\" from SEEKDESTROY.BIB_MATCH
                    # ) as a");
					# updateScoreWithQuery("select id,marc from biblio.record_entry where id in(select record from SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=\$\$MARC with audiobook phrases but incomplete marc\$\$)
					# and lower(marc) ~ \$\$abridge\$\$");

                    # updateScoreWithQuery("select id,marc from biblio.record_entry where id in
                    # (select record from 
                    # SEEKDESTROY.bib_score where score_time < now() - '5 days'::interval) and not deleted
                    # ");
                    # exit;

                    # updateScoreWithQuery("select distinct id,marc from biblio.record_entry where id in
                    # (select record from 
                    # SEEKDESTROY.bib_score where winning_score~'video_score' and winning_score_score=0)");
                    # updateScoreWithQuery("select id,marc from biblio.record_entry where id=243577");

                    #updateScoreWithQuery("select id,marc from biblio.record_entry where id in(select oldleadbib from seekdestroy.undedupe)");

					#findItemsCircedAsAudioBooksButAttachedNonAudioBib(1242779);
					#findItemsNotCircedAsAudioBooksButAttachedAudioBib(0);
                    # updateScoreCache();

                    # 007 byte 4: v=DVD b=VHS s=Blueray
                    # substr(007,4,1)
                    # Blue-ray:
                    # vd uscza-
                    # DVD:
                    # vd mvaizu
                    # VHS:
                    # vf-cbahou

                    # Playaway query chunk:
                    # (
                            # (
                            # split_part(marc,$$tag="007">$$,3) ~ 'sz' 
                            # and 
                            # split_part(marc,$$tag="007">$$,2) ~ 'cz' 
                            # )
                        # or
                            # (
                            # split_part(marc,$$tag="007">$$,2) ~ 'sz' 
                            # and 
                            # split_part(marc,$$tag="007">$$,3) ~ 'cz' 
                            # )
                        # )
                        
                        # Find Biblio.record_entry without opac icons:
                    # select id from biblio.record_entry where not deleted and 
                    # id not in(select id from metabib.record_attr_flat where attr='icon_format')
                    # 32115 rows

					
				}
			}
			updateJob("Executing reports and email","");
		
			
			my @tolist = ($conf{"alwaysemail"});
			if(length($errorMessage)==0) #none of the code currently sets an errorMessage but maybe someday
			{
				my $email = new email($conf{"fromemail"},\@tolist,$valid,1,\%conf);
				my @reports = @{reportResults()};
				my @attachments = (@{@reports[1]}, @seekdestroyReportFiles);
				my $reports = @reports[0];
				my $afterProcess = DateTime->now(time_zone => "local");
				my $difference = $afterProcess - $dt;
				my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
				my $duration =  $format->format_duration($difference);
				my $displayjobid = $jobid;
				$displayjobid="Report Only" if($reportonly);
				$email->sendWithAttachments("Evergreen Utility - Catalog Audit Job # $displayjobid","$reports\r\n\r\nDuration: $duration\r\n\r\n-Evergreen Perl Squad-",\@attachments);
				foreach(@attachments)
				{
					unlink $_ or warn "Could not remove $_\n";
				}
				
			}
			elsif(length($errorMessage)>0)
			{
				my @tolist = ($conf{"alwaysemail"});
				my $email = new email($conf{"fromemail"},\@tolist,1,0,\%conf);
				#$email->send("Evergreen Utility - Catalog Audit Job # $jobid - ERROR","$errorMessage\r\n\r\n-Evergreen Perl Squad-");
			}
			updateJob("Completed","");
		}
		$log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile'\n";		
	}
}

sub reportResults
{
	my @reportBlurbs = ();
	my @attachments = ();

    my $jobSummaryReports = jobSummaryReports();
    if($jobSummaryReports)
    {
        my %t = %{$jobSummaryReports};
        push(@reportBlurbs, @{ $t{"reportBlurbs"} }) if $t{"reportBlurbs"};
        push(@attachments, @{ $t{"attachments"} }) if $t{"attachments"};
    }
    my $tattleTaleReports = tattleTaleReports();
    if($tattleTaleReports)
    {
        my %t = %{$tattleTaleReports};
        push(@reportBlurbs, @{ $t{"reportBlurbs"} }) if $t{"reportBlurbs"};
        push(@attachments, @{ $t{"attachments"} }) if $t{"attachments"};
    }


	my $ret=""; # $newRecordCount."\r\n\r\n".$updatedRecordCount."\r\n\r\n".$mergedRecords.$itemsAssignedRecords.$copyMoveRecords.$undedupeRecords.$AudiobooksPossbileEAudiobooks.$itemsAttachedToDeletedBibs.$itemsAttachedToElectronicBibs.$AudiobookItemsOnNonAudiobookBibs.$videoItemsOnNonvideoBibs.$largePrintItemsOnNonLargePrintBibs.$nonLargePrintItemsOnLargePrintBibs;
	#print $ret;
	my @returns = ($ret,\@attachments);
	return \@returns;
}

sub tattleTaleReports
{
    my @reportBlurbs = ();
	my @attachments = ();
    my %reportDivisions = ();
    my %ret = ();

    ## Make sure that the output folder is defined and contains at least one slash in the file path. It's an absolute path.
    ## We don't want to delete / folder tree!
    if($conf{"reportoutputroot"} && $conf{"reportoutputroot"} !=~ m/^\/$/g && $conf{"reportoutputroot"} =~ m/\//g)
    {
        # Clear any old reports
        $conf{"reportoutputroot"} .= '/'; # Make sure it ends with a slash. Double slashes at the end are fine for linux
        deleteOldTattleReports() if $conf{"reportoutputroot"};
        make_path($conf{"reportoutputroot"}, {mode => 7644, }) if !(-d $conf{"reportoutputroot"});

        my $query = "select id,name,query from seekdestroy.tattle_report order by 1 -- limit 1";
        my @reports = @{$dbHandler->query($query)};	
        foreach(@reports)
        {
            my @thisReport = @{$_};
            my $id = @thisReport[0];
            my $name = @thisReport[1];
            $query = @thisReport[2];
            $query =~ s/!!!reportid!!!/$id/g;
            updateJob("Processing","reportResults $query");
            my @results = @{$dbHandler->query($query)};
            if($#results>-1)
            {
                my @header = @{$dbHandler->getColumnNames()};
                my $systemColID = 0;
                my $copyidColID = 0;
                for my $i (0..$#header)
                {
                    $systemColID = $i if @header[$i] =~ m/systemid/gi;
                    $copyidColID = $i if @header[$i] =~ m/copyid/gi;
                }
                my $summary = summaryReportResults(\@results,$systemColID,"System",$name);
                
                my @outputs = ([@header],@results);
                if($conf{"reportoutputroot"})  ## Signifies that this configuration is setup to write to HTML for staff review/evals
                {
                    writeTattleHTML(\@outputs, $systemColID, $copyidColID, $name, $id);
                }
                else
                {
                    my $filename = makeFriendlyFileName($name);
                    $filename .= ".csv";
                    createCSVFileFrom2DArray(\@outputs,$baseTemp.$filename);
                    push(@attachments,$baseTemp.$filename); 
                }
            }
            undef @results;
        }

        createTattleReportIndex() if($conf{"reportoutputroot"});
    }
    $ret{"attachments"} = \@attachments if @attachments[0];
    $ret{"reportBlurbs"} = \@reportBlurbs if @reportBlurbs[0];

    return \%ret;
}

sub writeTattleHTML
{
    my @results = @{@_[0]};
    my $systemColNum = @_[1];
    my $copyidColID = @_[2];
    my $reportTitle = @_[3];
    my $reportID = @_[4];
    my $header = shift @results;
    my @headers = @{$header};
    my $htmlHeader = "<thead>\n<tr>";
    my @printCols = ();
    my %colStyle = ();
    for my $i (0..$#headers)
    {
        if(@headers[$i] =~ /bib/gi)
        {
            push(@printCols, $i);
            $colStyle{$i} = "<td><a target = '_blank' href='/eg/staff/cat/catalog/record/!!!value!!!'>!!!value!!!</a></td>\n";
            $htmlHeader .= "<th>" . @headers[$i] . "</th>\n";
        }
        elsif(@headers[$i] =~ /barcode/gi)
        {
            push(@printCols, $i);
            $colStyle{$i} = "<td class = 'itembarcode' barcode = '!!!value!!!'><a target = '_blank' href='/eg/staff/cat/item/!!!copyid!!!'>!!!value!!!</a></td>\n";
            $htmlHeader .= "<th>" . @headers[$i] . "</th>\n";
        }
        elsif(@headers[$i] =~ /copyid/gi)
        {
            push(@printCols, $i);
            $colStyle{$i} = "
            <td>
            <span class='ignorespansucess hide'>Ignored</span>
            <span class='ignorespanfail hide'>Failed to ignore</span>
            <span class='loader hide'></span>
            <button class='ignorebutton' copyid='!!!value!!!' >Ignore</button>
            </td>\n";
            $htmlHeader .= "<th>Ignore</th>\n";
        }
        elsif( (@headers[$i] =~ /call/gi) || (@headers[$i] =~ /icon/gi) || (@headers[$i] =~ /branch/gi) || (@headers[$i] =~ /issue/gi) || (@headers[$i] =~ /title/gi) )
        {
            push(@printCols, $i);
            $colStyle{$i} = "<td>!!!value!!!</td>";
            $htmlHeader .= "<th>" . @headers[$i] . "</th>\n";
        }
    }
    $htmlHeader .= "</tr></thead>\n";

    my %reports = %{reportSplitIntoGroups(\@results, $systemColNum)};
    $log->addLine(Dumper(\%reports));
    while ((my $system, my $rowss) = each(%reports))
    {
        my $shortOUName = getShortOU($system);
        next if (!$shortOUName || $shortOUName eq '');
        my $folderPath = $conf{"reportoutputroot"} . $shortOUName;
        my $index = seedTattleSystemIndex($folderPath);
        my $htmlOutput = "
        <div class='report-title'>$reportTitle</div>
        <div class='datatable-outter'>
        <table reportid='$reportID' class='cell-border compact stripe hover order-column row-border'>\n$htmlHeader<tbody>";
        my @rows = @{$rowss};
        foreach(@rows)
        {
            my @thisRow = @{$_};
            my $thisCopyID = @thisRow[$copyidColID];
            $htmlOutput .= "<tr>";
            foreach(@printCols)
            {
                my $blurb = $colStyle{$_};
                my $replace = @thisRow[$_];
                $replace = OpenILS::Application::AppUtils->entityize($replace) if $replace;
                $blurb =~ s/!!!value!!!/$replace/gi;
                $blurb =~ s/!!!copyid!!!/$thisCopyID/gi;
                $htmlOutput .= $blurb;
            }
            $htmlOutput .= "</tr>";
        }
        $htmlOutput .= "</tbody></table></div>";
        $index->addLine($htmlOutput);
        if (!$tattleSystemDone{$system})
        {
            $tattleSystemDone{$system} = 1;
            $index->addLine(getTattleJSMagic());
        }
        $htmlOutput = "";
        undef $index;
    }
}

sub getTattleJSMagic
{
    my $ret = '
    <script>
    
    $(document).ready(function()
    {
        $("table").each(function()
        {
            $(this).DataTable();
        });
        $(".ignorebutton").each(function()
        {
            var reportID = $(this).parents("table");
            if(reportID[0])
            {
                reportID = $(reportID[0]).attr("reportid");
                var copyID = $(this).attr("copyid");
                if(reportID && copyID)
                {
                    $(this).click(function() 
                    {
                        handleIgnore(reportID, copyID);
                    });
                }
            }
        });
    });
    
    function handleIgnore(reportID, copyID)
    {
        var elements = getIgnoreButtonElements(reportID, copyID);
        elements["a"].hide();
        if( !elements["error"].hasClass("hide") )
        {
            elements["error"].addClass("hide");
        }
        elements["loader"].removeClass("hide");
        var url = "/eg/opac/tattler?copyid=" + copyID + "&reportid=" + reportID;
        console.log("attempting: " + url);
        $.get(url, function(data)
        {
            elements["loader"].addClass("hide");
            if(data && data == "1")
            {
                elements["success"].removeClass("hide");
            }
            else if (data && data == "2")
            {
                elements["success"].html("Already ignored. Next server execution will remove this row");
                elements["success"].removeClass("hide");
            }
            else
            {
                elements["error"].removeClass("hide");
                elements["a"].html("Try again");
                elements["a"].show();
            }
        });
    }
    
   function getIgnoreButtonElements(reportID, copyID)
    {
        var ret = [];
        $("table").each(function()
        {
            var thisTable = $(this);
            var repID = thisTable.attr("reportid");
            if(repID == reportID)
            {
                $(this).find("button").each(function()
                {
                    var thisa = $(this);
                    var cID = thisa.attr("copyid");
                    if(cID == copyID)
                    {
                        ret["a"] = thisa;
                        thisa.siblings().each(function()
                        {
                            if( $(this).hasClass("ignorespansucess") )
                            {
                                ret["success"] = $(this);
                            }
                            else if( $(this).hasClass("ignorespanfail") )
                            {
                                ret["error"] = $(this);
                            }
                            else if( $(this).hasClass("loader") )
                            {
                                ret["loader"] = $(this);
                            }
                        });
                    }
                });
            }
        });

        return ret;
        
    }
    </script>
    
    ';
    
    return $ret;
}

sub deleteOldTattleReports
{
    remove_tree( $conf{"reportoutputroot"}, {keep_root => 1, error => \my $err} );
    if ($err && @$err)
    {
        print "There was a problem cleaning out " .$conf{"reportoutputroot"}. "\n";
        $log->addLogLine("There was a problem cleaning out " .$conf{"reportoutputroot"});
        for my $diag (@$err)
        {
            my ($file, $message) = %$diag;
            if ($file eq '')
            {
                print "general error: $message\n";
            }
            else
            {
                print "problem unlinking $file: $message\n";
            }
        }
    }
}

sub createTattleReportIndex
{
    my $path = $conf{"reportoutputroot"};
    my $rootIndex = $path."/index.html";
    my $htmlOutput = "<html><body><ul>\n";
    $rootIndex = new Loghandler($rootIndex);
    $rootIndex->truncFile("");
	opendir(DIR, $path) or die $!;
	while (my $file = readdir(DIR)) 
	{
        if( !($file =~ /\./ ) )
        {
            if ( (-d "$path/$file") && (-f "$path/$file/index.html") )
            {
                $htmlOutput .= "<li><a href='$file/index.html'>$file</a></li>\n";
            }
        }
	}
	$htmlOutput .= "</ul></body></html>";
    $rootIndex->addLine($htmlOutput);
}

sub getShortOU
{
    my $name = shift;
    my $col = "id";
    $col = "name" if $name =~ /\D/;
    my $ret = 0;
    my $query = "select shortname from actor.org_unit where $col = \$namename\$$name\$namename\$";
    my @results = @{$dbHandler->query($query)};
    foreach(@results)
    {
        my @row = @{$_};
        $ret = @row[0];
    }
    return $ret;
}

sub makeFriendlyFileName
{
    my $outFileName = shift;
    $outFileName =~ s/^\s+//;
    $outFileName =~ s/^\t+//;
    $outFileName =~ s/\s+$//;
    $outFileName =~ s/\t+$//;
    $outFileName =~ s/^_+//;
    $outFileName =~ s/_+$//;
    $outFileName =~ s/\s/_/g;
    $outFileName =~ s/\///g;
    $outFileName =~ s/://g;
    return $outFileName;
}

sub jobSummaryReports
{
    my @reportBlurbs = ();
    my @attachments = ();
    my %ret = ();

    return 0 if $jobid == -1;
    
	#bib_marc_update table report non new bibs
	my $query = "select extra,count(*) from seekdestroy.bib_marc_update where job=$jobid and new_record is not true group by extra";
	updateJob("Processing","reportResults $query");
	my @results = @{$dbHandler->query($query)};
    my $updatedRecordCount = "";
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		$updatedRecordCount.=@row[1]." records were updated for this reason: ".@row[0]."\r\n";
	}
    push(@reportBlurbs, $updatedRecordCount) if (length($updatedRecordCount) > 0);

	#bib_marc_update table report new bibs
	my $query = "select extra,count(*) from seekdestroy.bib_marc_update where job=$jobid and new_record is true group by extra";
	updateJob("Processing","reportResults $query");
	my @results = @{$dbHandler->query($query)};
    my $newRecordCount = "";
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};		
		$newRecordCount.=@row[1]." records were created for this reason: ".@row[0]."\r\n";
	}
	push(@reportBlurbs, $newRecordCount) if (length($newRecordCount) > 0);

	#bib_merge table report
	my $query = "select leadbib as \"Winning Bib\",subbib as \"Deleted or Merged Bib\" from seekdestroy.bib_merge where job=$jobid";
	updateJob("Processing","reportResults $query");
	@results = @{$dbHandler->query($query)};
    my $mergedRecords = "";
	if($#results>-1)
	{	
		my $count = $#results+1;
		$mergedRecords="$count records were merged\r\n";
		my @header =  @{$dbHandler->getColumnNames()};
		my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."Merged_bibs.csv");
		push(@attachments,$baseTemp."Merged_bibs.csv");
	}
	push(@reportBlurbs, $mergedRecords) if (length($mergedRecords) > 0);

	#call_number_move table report
	$query = "select
    tobib as \"Destination Bib\",
    frombib as \"Source Bib\",
    (select label from asset.call_number where id=a.call_number) as \"Call Number\",
	(select name from actor.org_unit where id=(select owning_lib from asset.call_number where id=a.call_number)) as \"Owning Library\"
	from seekdestroy.call_number_move a
    where job=$jobid
	and frombib not in(select oldleadbib from seekdestroy.undedupe where job=$jobid) and tobib is not null";
	updateJob("Processing","reportResults $query");	
	@results = @{$dbHandler->query($query)};
    my $itemsAssignedRecords = "";
	if($#results>-1)
	{
		$itemsAssignedRecords = summaryReportResults(\@results,3,"Owning Library","AUTOMATED FIX: Moved Call Numbers");
        my @header = @{$dbHandler->getColumnNames()};
        my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."Moved_call_numbers.csv");
		push(@attachments,$baseTemp."Moved_call_numbers.csv");
	}
    push(@reportBlurbs, $itemsAssignedRecords) if (length($itemsAssignedRecords) > 0);

	#call_number_move FAILED table report
	$query = "select
    frombib as \"Source Bib\",
	(select name from actor.org_unit where id=(select owning_lib from asset.call_number where id=a.call_number)) as \"Call Number Owning Library\",
	(select label from asset.call_number where id=a.call_number) as \"Call Number\"
    from seekdestroy.call_number_move a
    where job=$jobid
	and frombib not in(select oldleadbib from seekdestroy.undedupe where job=$jobid) and tobib is null
	order by (select name from actor.org_unit where id=(select owning_lib from asset.call_number where id=a.call_number))";
	updateJob("Processing","reportResults $query");
	@results = @{$dbHandler->query($query)};
    my $itemsFailedAssignedRecords = "";
	if($#results>-1)
	{	
		$itemsFailedAssignedRecords = summaryReportResults(\@results,1,"Owning Library","FAILED: Call Numbers FAILED to be moved");
		my @header = @{$dbHandler->getColumnNames()};
		my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."Failed_call_number_moves.csv");
		push(@attachments,$baseTemp."Failed_call_number_moves.csv");
	}
    push(@reportBlurbs, $itemsFailedAssignedRecords) if (length($itemsFailedAssignedRecords) > 0);

	#copy_move table report
	$query = "select
    (select barcode from asset.copy where id=a.copy) as \"Barcode\",
	(select record from asset.call_number where id=a.fromcall) as \"Source Bib\",
	(select record from asset.call_number where id=a.tocall) as \"Destination Bib\",
	(select label from asset.call_number where id=a.tocall) as \"Call Number\",
	(select name from actor.org_unit where id=(select circ_lib from asset.copy where id=a.copy)) as \"Circulating Library\"
	from seekdestroy.copy_move a where job=$jobid";
	updateJob("Processing","reportResults $query");
	@results = @{$dbHandler->query($query)};
    my $copyMoveRecords = "";
	if($#results>-1)
	{	
		$copyMoveRecords = summaryReportResults(\@results,4,"Circulating Library","AUTOMATED FIX: Copies moved");
		my @header = @{$dbHandler->getColumnNames()};
		my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."Copy_moves.csv");
		push(@attachments,$baseTemp."Copy_moves.csv");
	}
	push(@reportBlurbs, $copyMoveRecords) if (length($copyMoveRecords) > 0);

	#undedupe table report
	$query = "select 
	undeletedbib as \"Undeleted Bib\",
	oldleadbib as \"Old Leading Bib\",
	(select label from asset.call_number where id=a.moved_call_number) as \"Call Number\",
	(select name from actor.org_unit where id=(select owning_lib from asset.call_number where id=a.moved_call_number)) as \"Owning Library\"
	from seekdestroy.undedupe a where job=$jobid";
	updateJob("Processing","reportResults $query");
	@results = @{$dbHandler->query($query)};	
    my $undedupeRecords = "";
	if($#results>-1)
	{	
		$undedupeRecords = summaryReportResults(\@results,3,"Owning Library","AUTOMATED FIX: Un-deduplicated Records");
		my @header = @{$dbHandler->getColumnNames()};
		my @outputs = ([@header],@results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."Undeduplicated_Bibs.csv");
		push(@attachments,$baseTemp."Undeduplicated_Bibs.csv");
	}
	push(@reportBlurbs, $undedupeRecords) if (length($undedupeRecords) > 0);

    #Possible Electronic
	$query =  $queries{"possible_electronic"};
	$query = "select	
	tag902 as \"903\",
	(select deleted from biblio.record_entry where id= outsidesbs.record) as \"Deleted\",
    record as \"BIB ID\",
    \$\$$domainname"."eg/opac/record/\$\$||record||\$\$?expand=marchtml\$\$ as \"OPAC Link\",
    winning_score as \"Winning Score\",
    opac_icon as \"OPAC ICON\",
    winning_score_score as \"Winning Score\",
    winning_score_distance as \"Winning Score Distance\",
    second_place_score as \"Second Place Score\",
    circ_mods as \"Circ Modifiers\",
    call_labels as \"Call Numbers\",
    copy_locations as \"Locations\",
    score as \"Record Quality\",
    record_type as \"Record Type\",
    audioformat as \"Audio Format\",
    videoformat as \"Video Format\",
    electronic as \"Electronic\",
    audiobook_score as \"Audiobook Score\",
    music_score as \"Music Score\",
    playaway_score as \"Playaway Score\",
    largeprint_score as \"Largeprint Score\",
    video_score as \"Video Score\",
    microfilm_score as \"Microfilm Score\",
    microfiche_score as \"Microfiche Score\"
    from
    seekdestroy.bib_score outsidesbs where record in( $query )
    order by (select deleted from biblio.record_entry where id= outsidesbs.record),winning_score,winning_score_distance,electronic,second_place_score,circ_mods,call_labels,copy_locations
  ";
	updateJob("Processing","reportResults $query");
	@results = @{$dbHandler->query($query)};
    my $AudiobooksPossbileEAudiobooks = "";
	if($#results>-1)
	{	
		my $count = $#results+1;
		$AudiobooksPossbileEAudiobooks = "$count Possible Electronic (see attached bibs_possible_electronic.csv)";
		my @header = @{$dbHandler->getColumnNames()};
		my @outputs = ([@header], @results);
		createCSVFileFrom2DArray(\@outputs,$baseTemp."bibs_possible_electronic.csv");
		push(@attachments,$baseTemp."bibs_possible_electronic.csv");
	}
    push(@reportBlurbs, $AudiobooksPossbileEAudiobooks) if (length($AudiobooksPossbileEAudiobooks) > 0);

    $ret{"attachments"} = \@attachments if @attachments[0];
    $ret{"reportBlurbs"} = \@reportBlurbs if @reportBlurbs[0];

    return \%ret;
}

sub reportSplitIntoGroups
{
    my @results = @{@_[0]};
	my $namecolumnpos = @_[1];
	my %ret = ();
	foreach(@results)
	{
		my @row = @{$_};
        my @ar = ();
		if($ret{@row[$namecolumnpos]})
		{
            @ar = @{$ret{@row[$namecolumnpos]}};
		}
		push(@ar,[@row]);
        $ret{@row[$namecolumnpos]} = \@ar;
	}
    return \%ret;
}

sub summaryReportResults
{
	my @results = @{@_[0]};
	my $namecolumnpos = @_[1];
	my $nameColumnName = @_[2];
	my $title = @_[3];
	my %ret = ();
	my @sorted = ();
	my $total = 0;
	my $summary='';
	foreach(@results)
	{
		my @row = @{$_};
		if($ret{@row[$namecolumnpos]})
		{
			$ret{@row[$namecolumnpos]}++;
		}
		else
		{
			$ret{@row[$namecolumnpos]}=1;
			push(@sorted, @row[$namecolumnpos]);
		}
		$total++;
	}
	my $i=1;
	while($i<$#sorted+1)
	{
		if($ret{@sorted[$i]} > $ret{@sorted[$i-1]})
		{
			my $temp = @sorted[$i];
			@sorted[$i]=@sorted[$i-1];
			@sorted[$i-1] = $temp;
			$i-=2 unless $i<2;
			$i-- unless $i<1;
		}
		$i++;
	}
	my $header = "Count";
	$header = $mobUtil->insertDataIntoColumn($header,$nameColumnName,11)."\r\n";
	foreach(@sorted)
	{
		my $line = $ret{$_};
		$line = $mobUtil->insertDataIntoColumn($line," ".$_,11);
		$summary.="$line\r\n";
	}
	my $line = $total;
	$line = $mobUtil->insertDataIntoColumn($line," Total",11);
	$summary.="$line\r\n";
	
	my $titleStars = "*";
	while(length($titleStars)<length($title)){$titleStars.="*";}
	$title="$titleStars\r\n$title\r\n$titleStars\r\n";
	$summary = $title.$header.$summary;
	return $summary;
}

sub createCSVFileFrom2DArray
{
	my @results = @{@_[0]};
	my $fileName = @_[1];
	my $fileWriter = new Loghandler($fileName);
	$fileWriter->deleteFile();
	my $output = "";
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $csvLine = $mobUtil->makeCommaFromArray(\@row,);
		$output.=$csvLine."\n";
	}
	$fileWriter->addLine($output);
	return $output;
}

sub truncateOutput
{
	my $ret = @_[0];
	my $length = @_[1];
	if(length($ret)>$length)
	{
		$ret = substr($ret,0,$length)."\r\nTRUNCATED FOR LENGTH\n\n";
	}
	return $ret;
}

sub tag902s
{
	my $query = "
		select record,extra,(select marc from biblio.record_entry where id=a.record) from SEEKDESTROY.BIB_MARC_UPDATE a";
 
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		my $reason = @row[1];
		my $marc = @row[2];
		my $note = '';
		if($reason eq "Correcting for DVD in the leader/007 rem 008_23")
		{
			$note='D V D';
		}
		elsif($reason eq "Correcting for Audiobook in the leader/007 rem 008_23")
		{
			$note='A u d i o b o o k';
		}
		elsif($reason eq "Correcting for Electronic in the 008/006")
		{
			$note='E l e c t r o n i c';
		}
		else
		{
			print "Skipping $bibid\n";
			next;
		}
		my $xmlresult = $marc;
		$xmlresult =~ s/(<leader>.........)./${1}a/;
		#$log->addLine($xmlresult);
		my $check = length($xmlresult);
		#$log->addLine($check);
		$xmlresult = fingerprintScriptMARC($xmlresult,$note);
		$xmlresult =~s/<record>//;
		$xmlresult =~s/<\/record>//;
		$xmlresult =~s/<\/collection>/<\/record>/;
		$xmlresult =~s/<collection/<record  /;
		$xmlresult =~s/XMLSchema-instance"/XMLSchema-instance\"  /;
		$xmlresult =~s/schema\/MARC21slim.xsd"/schema\/MARC21slim.xsd\"  /;
		
		#$log->addLine($xmlresult);
		#$log->addLine(length($xmlresult));
		if(length($xmlresult)!=$check)
		{		
			updateMARC($xmlresult,$bibid,'false',"Tagging 903 for $note");
		}
		else
		{
			print "Skipping $bibid - Already had the 903 for $note\n";
		}
	}
}

sub findInvalid856TOCURL
{
	my $query = "
select id,marc from biblio.record_entry where not deleted and 
lower(marc) ~ \$\$<datafield tag=\"856\" ind1=\"4\" ind2=\"0\"><subfield code=\"3\">table of contents.+?</datafield>\$\$
or
lower(marc) ~ \$\$<datafield tag=\"856\" ind1=\"4\" ind2=\"0\"><subfield code=\"3\">publisher description.+?</datafield>\$\$
	";
 
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $id = @row[0];
		my $marc = @row[1];
		if(!isScored( $id))
		{
			my @scorethis = ($id,$marc);
			my @st = ([@scorethis]);			
			updateScoreCache(\@st);
		}
		$query="INSERT INTO SEEKDESTROY.PROBLEM_BIBS(RECORD,PROBLEM,JOB) VALUES (\$1,\$2,\$3)";
		my @values = ($id,"MARC with table of contents E-Links",$jobid);
		$dbHandler->updateWithParameters($query,\@values);
	}
}

sub setMARCForm
{
	my $marc = @_[0];
	my $char = @_[1];
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my $marcr = populate_marc($marcob);	
	my %marcr = %{normalize_marc($marcr)};    
	my $replacement;
	my $altered=0;
	if($marcr{tag008})
	{
		my $z08 = $marcob->field('008');
		$marcob->delete_field($z08);
		#print "$marcr{tag008}\n";
		$replacement=$mobUtil->insertDataIntoColumn($marcr{tag008},$char,24);
		#print "$replacement\n";
		$z08->update($replacement);
		$marcob->insert_fields_ordered($z08);
		$altered=1;
	}
	elsif($marcr{tag006})
	{
		my $z06 = $marcob->field('006');
		$marcob->delete_fields($z06);
		#print "$marcr{tag006}\n";
		$replacement=$mobUtil->insertDataIntoColumn($marcr{tag006},$char,7);
		#print "$replacement\n";
		$z06->update($replacement);
		$marcob->insert_fields_ordered($z06);
		$altered=1;
	}
	if(!$altered && $char ne ' ')
	{
		$replacement=$mobUtil->insertDataIntoColumn("",$char,24);
		$replacement=$mobUtil->insertDataIntoColumn($replacement,' ',39);
		my $z08 = MARC::Field->new( '008', $replacement );
		#print "inserted new 008\n".$z08->data()."\n";
		$marcob->insert_fields_ordered($z08);
	}
	
	my $xmlresult = convertMARCtoXML($marcob);
	return $xmlresult;
}

sub updateMARCSetElectronic
{	
	my $bibid = @_[0];
	my $marc = @_[1];
	$marc = setMARCForm($marc,'s');
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my $marcr = populate_marc($marcob);	
	my %marcr = %{normalize_marc($marcr)};
	# we have to remove the 007s because they conflict for playaway.
	foreach(@{$marcr{tag007}})
	{		
		if(substr($_->data(),0,1) eq 's')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
		}
		elsif(substr($_->data(),0,1) eq 'v')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
		}
	}
	my $xmlresult = convertMARCtoXML($marcob);
	$xmlresult = fingerprintScriptMARC($xmlresult,'E l e c t r o n i c');
	updateMARC($xmlresult,$bibid,'false','Correcting for Electronic in the 008/006');
}

sub determineWhichAudioBookFormat
{
	my $bibid = @_[0];
	my $marc = @_[1];
	my $query = "select circ_mods,copy_locations,call_labels from seekdestroy.bib_score where record=$bibid";
	my @results = @{$dbHandler->query($query)};
	my $cass=0;
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my $flatmarc = $marcob->as_formatted;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $score=@row[0].@row[1].@row[2].$flatmarc;
		my @sp = split('cassette',lc($score));
		$cass = $#sp;
	}
	# $log->addLine("cassette score $bibid , $cass");
	if ($cass > 0)
	{
		# $log->addLine("Going to audiocassette");
		updateMARCSetCassetteAudiobook($bibid,$marc);
	}
	else
	{
		# $log->addLine("Going to audiocd");
		updateMARCSetCDAudioBook($bibid,$marc);
	}
}

sub updateMARCSetCassetteAudiobook
{	
	
	my $bibid = @_[0];
	my $marc = @_[1];
	$marc = setMARCForm($marc,' ');
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my $marcr = populate_marc($marcob);	
	my %marcr = %{normalize_marc($marcr)};    
	my $replacement;
	my $altered=0;
	foreach(@{$marcr{tag007}})
	{		
		if(substr($_->data(),0,1) eq 's')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
			#print $z07->data()."\n";
			$replacement=$mobUtil->insertDataIntoColumn($z07->data(),'s',1);
			$replacement=$mobUtil->insertDataIntoColumn($replacement,'l',4);
			#print "$replacement\n";			
			$z07->update($replacement);
			$marcob->insert_fields_ordered($z07);
			$altered=1;
		}
		elsif(substr($_->data(),0,1) eq 'v')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
			#print "removed video 007\n";
		}
	}
	if(!$altered)
	{
		my $z07 = MARC::Field->new( '007', 'sd lsngnnmmned' );
		#print "inserted new 007\n".$z07->data()."\n";
		$marcob->insert_fields_ordered($z07);
	}
	my $xmlresult = convertMARCtoXML($marcob);
	$xmlresult = fingerprintScriptMARC($xmlresult,'C A S A u d i o b o o k');
	$xmlresult = updateMARCSetSpecifiedLeaderByte($bibid,$xmlresult,7,'i');
	updateMARC($xmlresult,$bibid,'false','Correcting for Cassette Audiobook in the leader/007 rem 008_23');
}

sub updateMARCSetCDAudioBook
{	
	my $bibid = @_[0];
	my $marc = @_[1];
	$marc = setMARCForm($marc,' ');
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my $marcr = populate_marc($marcob);	
	my %marcr = %{normalize_marc($marcr)};    
	my $replacement;
	my $altered=0;
	foreach(@{$marcr{tag007}})
	{		
		if(substr($_->data(),0,1) eq 's')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
			#print $z07->data()."\n";
			$replacement=$mobUtil->insertDataIntoColumn($z07->data(),'s',1);
			$replacement=$mobUtil->insertDataIntoColumn($replacement,'f',4);
			#print "$replacement\n";			
			$z07->update($replacement);
			$marcob->insert_fields_ordered($z07);
			$altered=1;
		}
		elsif(substr($_->data(),0,1) eq 'v')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
			#print "removed video 007\n";
		}
	}
	if(!$altered)
	{
		my $z07 = MARC::Field->new( '007', 'sd fsngnnmmned' );
		#print "inserted new 007\n".$z07->data()."\n";
		$marcob->insert_fields_ordered($z07);
	}
	my $xmlresult = convertMARCtoXML($marcob);
	$xmlresult = fingerprintScriptMARC($xmlresult,'C D A u d i o b o o k');
	$xmlresult = updateMARCSetSpecifiedLeaderByte($bibid,$xmlresult,7,'i');
	updateMARC($xmlresult,$bibid,'false','Correcting for CD Audiobook in the leader/007 rem 008_23');
}

sub determineWhichVideoFormat
{
	my $bibid = @_[0];
	my $marc = @_[1];
	my $query = "select circ_mods,copy_locations,call_labels from seekdestroy.bib_score where record=$bibid";
	my @results = @{$dbHandler->query($query)};
	my $dvd=0;
	my $vhs=0;
	my $blu=0;
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my $flatmarc = $marcob->as_formatted;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $score=@row[0].@row[1].@row[2].$flatmarc;
		my @sp = split('dvd',lc($score));
		$dvd=$#sp;
		@sp = split('bluray',lc($score));
		$blu = $#sp;
		@sp = split('blu-ray',lc($score));
		$blu += $#sp;
		@sp = split('vhs',lc($score));
		$vhs = $#sp;
	}
	$log->addLine("video score $bibid , $dvd, $blu, $vhs");
	if( ($dvd >= $blu) && ($dvd >= $vhs) )
	{
		$log->addLine("Choosing DVD for $bibid");
		updateMARCSetVideoFormat($bibid,$marc,'dvd');
	}
	elsif( ($blu >= $dvd) && ($blu >= $vhs) )
	{
		$log->addLine("Choosing BLURAY for $bibid");
		updateMARCSetVideoFormat($bibid,$marc,'blu-ray');
	}
	else
	{
		$log->addLine("Choosing VHS for $bibid");
		updateMARCSetVideoFormat($bibid,$marc,'vhs');
	}
}

sub updateMARCSetVideoFormat
{	
	my $bibid = @_[0];
	my $marc = @_[1];
    my $type = @_[2];

    my %data = 
    (
        'vhs' =>
        {
            'letter' => 'b',
            'tag' => 'V H S',
            '007' => 'vf cbahou',
            'log' => 'VHS'
        },
        'blu-ray' =>
        {
            'letter' => 's',
            'tag' => 'B L U R A Y',
            '007' => 'vd csaizq',
            'log' => 'Bluray'
        },
        'default' =>
        {
            'letter' => 'v',
            'tag' => 'D V D',
            '007' => 'vd cvaizq',
            'log' => 'DVD'
        }
    );
    $type = 'default' if !$data{$type}; # Default is DVD if type isn't in the array
	$marc = setMARCForm($marc,' ');
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my $marcr = populate_marc($marcob);	
	my %marcr = %{normalize_marc($marcr)};    
	my $replacement;
	my $altered=0;
	foreach(@{$marcr{tag007}})
	{		
		if(substr($_->data(),0,1) eq 'v')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
			#print $z07->data()."\n";
			$replacement=$mobUtil->insertDataIntoColumn($z07->data(),'v',1);
			$replacement=$mobUtil->insertDataIntoColumn($replacement,$data{$type}{'letter'},5);
			#print "$replacement\n";			
			$z07->update($replacement);
			$marcob->insert_fields_ordered($z07);
			$altered=1;
		}
		elsif(substr($_->data(),0,1) eq 's')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
			#print "removed video 007\n";
		}
	}
	if(!$altered)
	{
		my $z07 = MARC::Field->new( '007', $data{$type}{'007'} );
		#print "inserted new 007\n".$z07->data()."\n";
		$marcob->insert_fields_ordered($z07);
	}
	my $xmlresult = convertMARCtoXML($marcob);
	$xmlresult = fingerprintScriptMARC($xmlresult,$data{$type}{'tag'});
	$xmlresult = updateMARCSetSpecifiedLeaderByte($bibid,$xmlresult,7,'g');
	updateMARC($xmlresult,$bibid,'false','Correcting for '.$data{$type}{'log'}.' in the leader/007 rem 008_23');
}

sub determineWhichMusicFormat
{
	my $bibid = @_[0];
	my $marc = @_[1];

    # Stay Orderly for logs
    my @formatOrder = ('cdmusic','phonomusic','casmusic','music');
    my %formatDetermineMatrix = 
    (
        'cdmusic' =>
        {
            'score' => 0,
            'phrases' => ['cd'],
            'autowin' => 0,
            'default' => 0
        },
        'phonomusic' =>
        {
            'score' => 0,
            'phrases' => ['33 1/3 rpm','phono'],
            'autowin' => 1,  ## If these phrases are found in the marc - then it can be no other format
            'default' => 0
        },
        'casmusic' =>
        {
            'score' => 0,
            'phrases' => ['cass','4 3/4 in'],
            'autowin' => 0,
            'default' => 0
        },
        'music' =>  ## This is default - if all scores are a tie or are 0
        {
            'score' => 0,
            'phrases' => [],
            'autowin' => 0,
            'default' => 1
        }
    );
    my $query = "select circ_mods,copy_locations,call_labels from seekdestroy.bib_score where record=$bibid";
	my @results = @{$dbHandler->query($query)};
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
    my $autowin = 0;
	my $flatmarc = $marcob->as_formatted;
	foreach(@results)
	{
        my $row = $_;
        my @row = @{$row};
        my $score=@row[0].@row[1].@row[2].$flatmarc;

        while ((my $format, my $array) = each(%formatDetermineMatrix))
        {
            my %array = %{$array};
            foreach(@{$array{'phrases'}})
            {
                my $phrase = $_;
                my @sp = split(lc($phrase), lc($score));
                $formatDetermineMatrix{$format}{'score'} += $#sp;
                $autowin = $format if($formatDetermineMatrix{$format}{'autowin'} && $#sp > 0);
            }
        }
	}
    my $logentry = "music score bib $bibid : ";
    $logentry .= $_ . " " . $formatDetermineMatrix{$_}{'score'}.", " foreach(@formatOrder);
    $log->addLine($logentry);
    my $winningScore = 0;
    my $winningFormat = '';
    my $default = 0;
    while ((my $format, my %array) = each(%formatDetermineMatrix))
    {
        if($formatDetermineMatrix{$format}{'score'} > $winningScore)
        {
            $winningScore = $formatDetermineMatrix{$format}{'score'};
            $winningFormat = $format;
            $default = 0; # just in case the last loop set this and now we have a new winner
        }
        elsif($formatDetermineMatrix{$format}{'score'} == $winningScore && $winningScore != 0)
        {
            # Condition: tie - defaulting to 'music'
            # autowin beats this though
            $default = 'music';
        }
    }
    $winningFormat = $default if($default);
	$winningFormat = $autowin if($autowin);
    $log->addLine("Choosing $winningFormat for $bibid");
	updateMARCSetMusicFormat($bibid,$marc,$winningFormat);
}

sub updateMARCSetMusicFormat
{	
	my $bibid = @_[0];
	my $marc = @_[1];
    my $type = @_[2];

    my %data = 
    (
        'cdmusic' =>
        {
            'letter' => 'f',
            'tag' => 'C D M U S I C',
            '007' => 'sd fungnn|||eu',
            'log' => 'CDMusic'
        },
        'phonomusic' =>
        {
            'letter' => 'b',
            'tag' => 'P H O N O M U S I C',
            '007' => 'sd bumennmpluu',
            'log' => 'Phonomusic'
        },
        'casmusic' =>
        {
            'letter' => 'l',
            'tag' => 'C A S S E T T E M U S I C',
            '007' => 'ss lunjlcmpnce',
            'log' => 'CassetteMusic'
        },
        'default' =>
        {
            'letter' => '',
            'tag' => 'M U S I C',
            '007' => '',
            'log' => 'Generic Music'
        }
    );
    $type = 'default' if !$data{$type}; # Default is DVD if type isn't in the array
	$marc = setMARCForm($marc,' ');
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my $marcr = populate_marc($marcob);	
	my %marcr = %{normalize_marc($marcr)};    
	my $replacement;
	my $altered=0;
	foreach(@{$marcr{tag007}})
	{		
		if(substr($_->data(),0,1) eq 's')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
            if(length($data{$type}{'letter'}) ==1)
            {
                #print $z07->data()."\n";
                $replacement=$mobUtil->insertDataIntoColumn($z07->data(),'s',1);
                $replacement=$mobUtil->insertDataIntoColumn($replacement,$data{$type}{'letter'},4);
                #print "$replacement\n";			
                $z07->update($replacement);
                $marcob->insert_fields_ordered($z07);
            }
			$altered=1;
		}
		elsif(substr($_->data(),0,1) eq 'v')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
			#print "removed video 007\n";
		}
	}
	if(!$altered && length($data{$type}{'007'}) > 0)
	{
		my $z07 = MARC::Field->new( '007', $data{$type}{'007'} );
		#print "inserted new 007\n".$z07->data()."\n";
		$marcob->insert_fields_ordered($z07);
	}
	my $xmlresult = convertMARCtoXML($marcob);
	$xmlresult = fingerprintScriptMARC($xmlresult,$data{$type}{'tag'});
	$xmlresult = updateMARCSetSpecifiedLeaderByte($bibid,$xmlresult,7,'j');
	updateMARC($xmlresult,$bibid,'false','Correcting for '.$data{$type}{'log'}.' in the leader/007 rem 008_23');
}

sub updateMARCSetLargePrint
{	
	my $bibid = @_[0];	
	my $marc = @_[1];	
	$marc = setMARCForm($marc,'d');
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my $marcr = populate_marc($marcob);	
	my %marcr = %{normalize_marc($marcr)};    
	foreach(@{$marcr{tag007}})
	{		
		if(substr($_->data(),0,1) eq 'v')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
		}
		elsif(substr($_->data(),0,1) eq 's')
		{
			my $z07 = $_;
			$marcob->delete_field($z07);
		}
	}
	my $xmlresult = convertMARCtoXML($marcob);
	$xmlresult = fingerprintScriptMARC($xmlresult,'L a r g e P r i n t');
	$xmlresult = updateMARCSetSpecifiedLeaderByte($bibid,$xmlresult,7,'a');	
	$xmlresult = updateMARCSetSpecifiedLeaderByte($bibid,$xmlresult,8,'m');
	updateMARC($xmlresult,$bibid,'false','Correcting for Large Print in the leader/007 rem 008_23');
}

sub fingerprintScriptMARC
{
	my $marc = @_[0];
	my $note = @_[1];
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);
	my @n902 = $marcob->field('903');
	my $altered = 0;
	my $dt = DateTime->now(time_zone => "local"); 
	my $fdate = $dt->mdy; 
	foreach(@n902)
	{
		my $field = $_;
		my $suba = $field->subfield('a');
		my $subd = $field->subfield('d');
		if($suba && $suba eq 'mobius-catalog-fix' && $subd && $subd eq "$note")
		{
			#print "Found a matching 903 for $note - updating that one\n";
			$altered = 1;
			my $new902 = MARC::Field->new( '903',' ',' ','a'=>'mobius-catalog-fix','b'=>"$fdate",'c'=>'formatted','d'=>"$note" );
			$marcob->delete_field($field);
			$marcob->append_fields($new902);
		}
	}
	if(!$altered)
	{
		my $new902 = MARC::Field->new( '903',' ',' ','a'=>'mobius-catalog-fix','b'=>"$fdate",'c'=>'formatted','d'=>"$note" );
		$marcob->append_fields($new902);
	}
	my $xmlresult = convertMARCtoXML($marcob);
	return $xmlresult
}

sub updateMARCSetSpecifiedLeaderByte  
{	
	my $bibid = @_[0];
	my $marc = @_[1];
	my $leaderByte = @_[2];		#1 based
	my $value = @_[3];
	my $marcob = $marc;
	$marcob =~ s/(<leader>.........)./${1}a/;
	$marcob = MARC::Record->new_from_xml($marcob);	
	my $leader = $marcob->leader();
	#print $leader."\n";
	$leader=$mobUtil->insertDataIntoColumn($leader,$value,$leaderByte);
	#print $leader."\n";
	$marcob->leader($leader);
	#print $marcob->leader()."\n";
	my $xmlresult = convertMARCtoXML($marcob);
	return $xmlresult;
}

sub updateMARC
{
	my $newmarc = @_[0];
	my $bibid = @_[1];
	my $newrecord = @_[2];
	my $extra = @_[3];
	my $query = "INSERT INTO SEEKDESTROY.BIB_MARC_UPDATE (RECORD,PREV_MARC,CHANGED_MARC,NEW_RECORD,EXTRA,JOB)
	VALUES(\$1,(SELECT MARC FROM BIBLIO.RECORD_ENTRY WHERE ID=\$2),\$3,\$4,\$5,\$6)";		
	my @values = ($bibid,$bibid,$newmarc,$newrecord,$extra,$jobid);
	$dbHandler->updateWithParameters($query,\@values);
	$query = "UPDATE BIBLIO.RECORD_ENTRY SET MARC=\$1 WHERE ID=\$2";
	updateJob("Processing","updateMARC $extra  $query");
	@values = ($newmarc,$bibid);
	$dbHandler->updateWithParameters($query,\@values);
}

sub findInvalidElectronicMARC
{
	$log->addLogLine("Starting findInvalidElectronicMARC.....");
	my $typeName = "electronic";
	my $problemPhrase = "MARC with E-Links but 008 tag is missing o,q,s";
	my $phraseQuery = $queries{"electronic_search_phrase"};
	my @additionalSearchQueries = ($queries{"electronic_additional_search"});
	my $subQueryConvert = $queries{"non_electronic_bib_convert_to_electronic"};
	my $subQueryNotConvert =  $queries{"non_electronic_bib_not_convert_to_electronic"};
	my $convertFunction = "updateMARCSetElectronic(\$id,\$marc);";	
	findInvalidMARC(
	$typeName,
	$problemPhrase,
	$phraseQuery,
	\@additionalSearchQueries,
	$subQueryConvert,
	$subQueryNotConvert,
	$convertFunction,
	\@electronicSearchPhrases
	);
}

sub findInvalidAudioBookMARC
{	
	$log->addLogLine("Starting findInvalidAudioBookMARC.....");
	my $typeName = "audiobook";
	my $problemPhrase = "MARC with audiobook phrases but incomplete marc";
	my $phraseQuery = $queries{"audiobook_search_phrase"};
	my @additionalSearchQueries = ($queries{"audiobook_additional_search"});
	my $subQueryConvert = $queries{"non_audiobook_bib_convert_to_audiobook"};
	my $subQueryNotConvert =  $queries{"non_audiobook_bib_not_convert_to_audiobook"};
	my $convertFunction = "determineWhichAudioBookFormat(\$id,\$marc);";		
	findInvalidMARC(
	$typeName,
	$problemPhrase,
	$phraseQuery,
	\@additionalSearchQueries,
	$subQueryConvert,
	$subQueryNotConvert,
	$convertFunction,
	\@audioBookSearchPhrases
	);
}

sub findInvalidDVDMARC
{
	$log->addLogLine("Starting findInvalidDVDMARC.....");
	my $typeName = "video";
	my $problemPhrase = "MARC with video phrases but incomplete marc";
	my $phraseQuery = $queries{"dvd_search_phrase"};
	my @additionalSearchQueries = ($queries{"dvd_additional_search"});
	my $subQueryConvert = $queries{"non_dvd_bib_convert_to_dvd"};
	my $subQueryNotConvert =  $queries{"non_dvd_bib_not_convert_to_dvd"};
	my $convertFunction = "determineWhichVideoFormat(\$id,\$marc);";
	findInvalidMARC(
	$typeName,
	$problemPhrase,
	$phraseQuery,
	\@additionalSearchQueries,
	$subQueryConvert,
	$subQueryNotConvert,
	$convertFunction,
	\@videoSearchPhrases
	);
}

sub findInvalidLargePrintMARC
{	
	$log->addLogLine("Starting findInvalidLargePrintMARC.....");
	my $typeName = "large_print";
	my $problemPhrase = "MARC with large_print phrases but incomplete marc";
	my $phraseQuery = $queries{"largeprint_search_phrase"};
	my @additionalSearchQueries = ();
	my $subQueryConvert = $queries{"non_large_print_bib_convert_to_large_print"};
	my $subQueryNotConvert =  $queries{"non_large_print_bib_not_convert_to_large_print"};
	my $convertFunction = "updateMARCSetLargePrint(\$id,\$marc);";	
	findInvalidMARC(
	$typeName,
	$problemPhrase,
	$phraseQuery,
	\@additionalSearchQueries,
	$subQueryConvert,
	$subQueryNotConvert,
	$convertFunction,
	\@largePrintBookSearchPhrases
	);
}

sub findInvalidMusicMARC
{	
	$log->addLogLine("Starting findInvalidMusicMARC.....");
	my $typeName = "music";
	my $problemPhrase = "MARC with music phrases but incomplete marc";
	my $phraseQuery = $queries{"music_search_phrase"};
	my @additionalSearchQueries = ($queries{"music_additional_search"});
	my $subQueryConvert = $queries{"non_music_bib_convert_to_music"};
	my $subQueryNotConvert =  $queries{"non_music_bib_not_convert_to_music"};
	my $convertFunction = "determineWhichMusicFormat(\$id,\$marc);";
	#combine both lists for gathering up bib canidates
	my @music = (@musicSearchPhrases, @musicSearchPhrasesAddition);
	findInvalidMARC(
	$typeName,
	$problemPhrase,
	$phraseQuery,
	\@additionalSearchQueries,
	$subQueryConvert,
	$subQueryNotConvert,
	$convertFunction,
	\@music
	);
}

sub findInvalidMARC
{	
	my $typeName = @_[0];
	my $problemPhrase = @_[1];
	my $phraseQuery = @_[2];
	my @additionalSearchQueries = @{@_[3]};
	my $subQueryConvert = @_[4];
	my $subQueryNotConvert = @_[5];
	my $convertFunction = @_[6];
	my @marcSearchPhrases = @{@_[7]};
	
	
	my $query = "DELETE FROM SEEKDESTROY.PROBLEM_BIBS WHERE PROBLEM=\$\$$problemPhrase\$\$";
    if(!$doNotRunFullReEval)  # This can take DAYS to finish on large datasets - hence the switch.
    {
        updateJob("Processing","findInvalidMARC  $query");
        $dbHandler->update($query);
        foreach(@marcSearchPhrases)
        {
            my $phrase = lc$_;
            my $query = $phraseQuery;
            $query =~ s/\$phrase/$phrase/g;
            $query =~ s/\$problemphrase/$problemPhrase/g;
            updateJob("Processing","findInvalidMARC  $query");
            updateProblemBibs($query,$problemPhrase,$typeName);
        }
        foreach(@additionalSearchQueries)
        {
            my $query = $_;				
            $query =~ s/\$problemphrase/$problemPhrase/g;
            updateJob("Processing","findInvalidMARC  $query");
            updateProblemBibs($query,$problemPhrase,$typeName);
        }
	}
	# Now that we have digested the possibilities - 
	# Lets weed them out into bibs that we want to convert	
	my $output='';
	my $toCSV = "";
	my $query = "select 
	tag902,
	(select deleted from biblio.record_entry where id= outsidesbs.record),record,
 \$\$$domainname"."eg/opac/record/\$\$||record||\$\$?expand=marchtml\$\$,
 winning_score,
  opac_icon \"opac icon\",
 winning_score_score,winning_score_distance,second_place_score,
 circ_mods,call_labels,copy_locations,
 score,record_type,audioformat,videoformat,electronic,audiobook_score,music_score,playaway_score,largeprint_score,video_score,microfilm_score,microfiche_score,
 (select marc from biblio.record_entry where id=outsidesbs.record)
  from seekdestroy.bib_score outsidesbs where record in( $subQueryConvert )
  order by (select deleted from biblio.record_entry where id= outsidesbs.record),winning_score,winning_score_distance,electronic,second_place_score,circ_mods,call_labels,copy_locations
";
	$query =~ s/\$problemphrase/$problemPhrase/g;
	updateJob("Processing","findInvalidMARC  $query");
	my @results = @{$dbHandler->query($query)};
	my @convertList=@results;	
	foreach(@results)
	{
		my @row = @{$_};
		my $id = @row[2];
		my $marc = @row[24];
		my $t902 = @row[0];
		my @line=@{$_};
		@line[24]='';
		$output.=$mobUtil->makeCommaFromArray(\@line,';')."\n" if ( !(lc($t902) =~ m/mz7a/) && !(lc($t902) =~ m/gd5/) );
		$toCSV.=$mobUtil->makeCommaFromArray(\@line,',')."\n"  if ( !(lc($t902) =~ m/mz7a/) && !(lc($t902) =~ m/gd5/) );
		if(!$dryrun)
		{
			eval($convertFunction) if ( !(lc($t902) =~ m/mz7a/) && !(lc($t902) =~ m/gd5/) );
		}
	}
	
	my $header = "\"902\",\"Deleted\",\"BIB ID\",\"OPAC Link\",\"Winning_score\",\"OPAC ICON\",\"Winning Score\",\"Winning Score Distance\",\"Second Place Score\",\"Circ Modifiers\",\"Call Numbers\",\"Locations\",\"Record Quality\",\"record_type\",\"audioformat\",\"videoformat\",\"electronic\",\"audiobook_score\",\"music_score\",\"playaway_score\",\"largeprint_score\",\"video_score\",\"microfilm_score\",\"microfiche_score\"";
	if(length($toCSV)>0)
	{
		my $csv = new Loghandler($baseTemp."Converted_".$typeName."_bibs.csv");
		$csv->truncFile("");
		$csv->addLine($header."\n".$toCSV);
		push(@seekdestroyReportFiles,$baseTemp."Converted_".$typeName."_bibs.csv");
	}
	$log->addLine("Will Convert these to $typeName: $#convertList\n\n\n");
	$log->addLine($output);
	@convertList=();
	
	my $query = "select	
	(select lower(split_part(split_part(split_part(marc,\$\$<datafield tag=\"903\"\$\$,2),\$\$<subfield code=\"a\">\$\$,2),\$\$<\$\$,1)) from biblio.record_entry where id= outsidesbs.record),
	(select deleted from biblio.record_entry where id= outsidesbs.record),record,
 \$\$$domainname"."eg/opac/record/\$\$||record||\$\$?expand=marchtml\$\$,
 winning_score,
  opac_icon \"opac icon\",
 winning_score_score,winning_score_distance,second_place_score,
 circ_mods,call_labels,copy_locations,
 score,record_type,audioformat,videoformat,electronic,audiobook_score,music_score,playaway_score,largeprint_score,video_score,microfilm_score,microfiche_score
  from seekdestroy.bib_score outsidesbs where record in( $subQueryNotConvert )
  order by (select deleted from biblio.record_entry where id= outsidesbs.record),winning_score,winning_score_distance,electronic,second_place_score,circ_mods,call_labels,copy_locations
";
	$query =~ s/\$problemphrase/$problemPhrase/g;
	updateJob("Processing","findInvalidMARC  $query");
	my @results = @{$dbHandler->query($query)};
	my @convertList=@results;	
	$log->addLine("Will NOT Convert these (Need Humans): $#convertList\n\n\n");
	$output='';
	$toCSV='';
	foreach(@convertList)
	{
		my @line=@{$_};
		$output.=$mobUtil->makeCommaFromArray(\@line,';')."\n";
		$toCSV.=$mobUtil->makeCommaFromArray(\@line,',')."\n";
	}	
	my $header = "\"903\",\"Deleted\",\"BIB ID\",\"OPAC Link\",\"Winning_score\",\"OPAC ICON\",\"Winning Score\",\"Winning Score Distance\",\"Second Place Score\",\"Circ Modifiers\",\"Call Numbers\",\"Locations\",\"Record Quality\",\"record_type\",\"audioformat\",\"videoformat\",\"electronic\",\"audiobook_score\",\"music_score\",\"playaway_score\",\"largeprint_score\",\"video_score\",\"microfilm_score\",\"microfiche_score\"";
	if(length($toCSV)>0)
	{
		my $csv = new Loghandler($baseTemp."Need_Humans_".$typeName."_bibs.csv");
		$csv->truncFile($header."\n".$toCSV);
		push(@seekdestroyReportFiles,$baseTemp."Need_Humans_".$typeName."_bibs.csv");
	}
	$log->addLine($output);
@convertList=();
	
}

sub updateProblemBibs
{
	my $query = @_[0];
	my $problemphrase = @_[1];
	my $typeName = @_[2];
	my @results = @{$dbHandler->query($query)};		
	$log->addLine(($#results+1)." possible invalid $typeName MARC\n\n\n");
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $id = @row[0];
		my $marc = @row[1];

		my @scorethis = ($id,$marc);
		my @st = ([@scorethis]);
		updateScoreCache(\@st);

		$query="INSERT INTO SEEKDESTROY.PROBLEM_BIBS(RECORD,PROBLEM,JOB) VALUES (\$1,\$2,\$3)";
		my @values = ($id,$problemphrase,$jobid);
		$dbHandler->updateWithParameters($query,\@values);
	}
}

sub isScored
{	
	my $bibid = @_[0];
	my $query = "SELECT ID FROM SEEKDESTROY.BIB_SCORE WHERE RECORD = $bibid";
	my @results = @{$dbHandler->query($query)};
	if($#results>-1)
	{
		return 1;
	}
	return 0;
}

sub updateScoreCache
{
	my @newIDs;
	my @newAndUpdates;
	my @updateIDs;
	if(@_[0])
	{	
		@newIDs=@{@_[0]};
	}
	else
	{
		@newAndUpdates = @{identifyBibsToScore($dbHandler)};
		@newIDs = @{@newAndUpdates[0]};
	}
	##print Dumper(@newIDs);
	#$log->addLine("Found ".($#newIDs+1)." new Bibs to be scored");	
	if(@newAndUpdates[1])
	{
		@updateIDs = @{@newAndUpdates[1]};
		#$log->addLine("Found ".($#updateIDs+1)." new Bibs to update score");	
	}
	foreach(@newIDs)
	{
		my @thisone = @{$_};
		my $bibid = @thisone[0];
		#$log->addLine("Adding Score ".$bibid);
		my $marc = @thisone[1];
		#print "bibid = $bibid";
		#print "marc = $marc";
		my $query = "DELETE FROM SEEKDESTROY.BIB_SCORE WHERE RECORD = $bibid";
		$dbHandler->update($query);
		my $marcob = $marc;
		$marcob =~ s/(<leader>.........)./${1}a/;
		$marcob = MARC::Record->new_from_xml($marcob);
		my $score = scoreMARC($marcob);
		my %allscores = %{getAllScores($marcob)};
		my %fingerprints = %{getFingerprints($marcob)};
		#$log->addLine(Dumper(%fingerprints));
		my $query = "INSERT INTO SEEKDESTROY.BIB_SCORE
		(RECORD,
		SCORE,
		ELECTRONIC,
		audiobook_score,
		largeprint_score,
		video_score,
		microfilm_score,
		microfiche_score,
		music_score,
		playaway_score,
		winning_score,
		winning_score_score,
		winning_score_distance,
		second_place_score,
		item_form,
		date1,
		record_type,
		bib_lvl,
		title,
		author,
		sd_fingerprint,
		audioformat,
		videoformat,
		eg_fingerprint,
		sd_alt_fingerprint,
		tag902) 
		VALUES($bibid,$score,
		$allscores{'electricScore'},
		$allscores{'audioBookScore'},
		$allscores{'largeprint_score'},
		$allscores{'video_score'},
		$allscores{'microfilm_score'},
		$allscores{'microfiche_score'},
		$allscores{'music_score'},
		$allscores{'playaway_score'},
		E'$allscores{'winning_score'}',
		$allscores{'winning_score_score'},
		$allscores{'winning_score_distance'},
		E'$allscores{'second_place_score'}',
		\$1,\$2,\$3,\$4,\$5,\$6,\$7,\$8,\$9,(SELECT FINGERPRINT FROM BIBLIO.RECORD_ENTRY WHERE ID=$bibid),\$10,\$11
		)";		
		my @values = (
		$fingerprints{item_form},
		$fingerprints{date1},
		$fingerprints{record_type},
		$fingerprints{bib_lvl},
		$fingerprints{title},
		$fingerprints{author},
		$fingerprints{baseline},
		$fingerprints{audioformat},
		$fingerprints{videoformat},
		$fingerprints{alternate},
		$fingerprints{tag902}
		);
		# $log->addLine($query);
		# $log->addLine(Dumper(\@values));
		$dbHandler->updateWithParameters($query,\@values);
		updateBibCircsScore($bibid);
		updateBibCallLabelsScore($bibid);
		updateBibCopyLocationsScore($bibid);
	}
	foreach(@updateIDs)
	{
		my @thisone = @{$_};
		my $bibid = @thisone[0];
		$log->addLine("Updating Score ".@thisone[0]);
		my $marc = @thisone[1];
		my $bibscoreid = @thisone[2];
		my $oldscore = @thisone[3];
		my $marcob = $marc;
		$marcob =~ s/(<leader>.........)./${1}a/;
		$marcob = MARC::Record->new_from_xml($marcob);		
		my $score = scoreMARC($marcob);		
		my %allscores = %{getAllScores($marcob)};
		my %fingerprints = %{getFingerprints($marcob)};		
		my $improved = $score - $oldscore;
		my $query = "UPDATE SEEKDESTROY.BIB_SCORE SET IMPROVED_SCORE_AMOUNT = $improved, SCORE = $score, SCORE_TIME=NOW(), 
		ELECTRONIC=$allscores{'electricScore'},
		audiobook_score=$allscores{'audioBookScore'},
		largeprint_score=$allscores{'largeprint_score'},
		video_score=$allscores{'video_score'},
		microfilm_score=$allscores{'microfilm_score'},
		microfiche_score=$allscores{'microfiche_score'},
		music_score=$allscores{'music_score'},
		playaway_score=$allscores{'playaway_score'},
		winning_score=E'$allscores{'winning_score'}',
		winning_score_score=$allscores{'winning_score_score'},
		winning_score_distance=$allscores{'winning_score_distance'},
		second_place_score=E'$allscores{'second_place_score'}',
		item_form = \$1,
		date1 = \$2,
		record_type = \$3,
		bib_lvl = \$4,
		title = \$5,
		author = \$6,
		sd_fingerprint = \$7,
		audioformat = \$8,
		videoformat = \$9,
		eg_fingerprint = (SELECT FINGERPRINT FROM BIBLIO.RECORD_ENTRY WHERE ID=$bibid),
		sd_alt_fingerprint = \$10,
		tag902 = \$11
		WHERE ID=$bibscoreid";
		my @values = (
		$fingerprints{item_form},
		$fingerprints{date1},
		$fingerprints{record_type},
		$fingerprints{bib_lvl},
		$fingerprints{title},
		$fingerprints{author},
		$fingerprints{baseline},
		$fingerprints{audioformat},
		$fingerprints{videoformat},
		$fingerprints{alternate},
		$fingerprints{tag902}
		);
		$dbHandler->updateWithParameters($query,\@values);
		updateBibCircsScore($bibid);
		updateBibCallLabelsScore($bibid);
		updateBibCopyLocationsScore($bibid);
	}
}

sub updateBibCircsScore
{	
	my $bibid = @_[0];	
	my $query = "DELETE FROM seekdestroy.bib_item_circ_mods WHERE RECORD=$bibid";
	$dbHandler->update($query);
	
	$query = "
	select ac.circ_modifier,acn.record from asset.copy ac,asset.call_number acn,biblio.record_entry bre where
	acn.id=ac.call_number and
	bre.id=acn.record and
	acn.record = $bibid and
	not acn.deleted and
	not bre.deleted and
	not ac.deleted
	group by ac.circ_modifier,acn.record
	order by record";
	my $allcircs='';
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $circmod = @row[0];
		my $record = @row[1];
		my $q="INSERT INTO seekdestroy.bib_item_circ_mods(record,circ_modifier,job)
		values
		(\$1,\$2,\$3)";
		my @values = ($record,$circmod,$jobid);
		$allcircs.=$circmod.',';
		$dbHandler->updateWithParameters($q,\@values);
	}
	$allcircs=substr($allcircs,0,-1);
	my $opacicons='';
	# get opac icon string
	$query = "select string_agg(value,\$\$,\$\$) from metabib.record_attr_flat where attr=\$\$icon_format\$\$ and id=$bibid";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		$opacicons = @row[0];
	}
	$query = "UPDATE SEEKDESTROY.BIB_SCORE SET OPAC_ICON=\$1,CIRC_MODS=\$2 WHERE RECORD=$bibid";
	my @values = ($opacicons,$allcircs);
	#$log->addLine($query);
	#$log->addLine("$opacicons $allcircs");	
	$dbHandler->updateWithParameters($query,\@values);
	
}

sub updateBibCallLabelsScore
{	
	my $bibid = @_[0];	
	my $query = "DELETE FROM seekdestroy.bib_item_call_labels WHERE RECORD=$bibid";
	$dbHandler->update($query);
	
	$query = "
	select 
	(select label from asset.call_number_prefix where id=acn.prefix)||acn.label||(select label from asset.call_number_suffix where id=acn.suffix),acn.record
	from asset.copy ac,asset.call_number acn,biblio.record_entry bre where
	acn.id=ac.call_number and
	bre.id=acn.record and
	acn.record = $bibid and
	not acn.deleted and
	not bre.deleted and
	not ac.deleted
	group by (select label from asset.call_number_prefix where id=acn.prefix)||acn.label||(select label from asset.call_number_suffix where id=acn.suffix),acn.record
	order by record";
	my $allcalls='';
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $calllabel = @row[0];
		my $record = @row[1];
		my $q="INSERT INTO seekdestroy.bib_item_call_labels(record,call_label,different_call_labels,job)
		values
		(\$1,\$2,\$3,\$4)";
		my @values = ($record,$calllabel,$#results+1,$jobid);
		$allcalls.=$calllabel.',';
		$dbHandler->updateWithParameters($q,\@values);
	}
	$allcalls=substr($allcalls,0,-1);
	
	$query = "UPDATE SEEKDESTROY.BIB_SCORE SET CALL_LABELS=\$1 WHERE RECORD=$bibid";
	my @values = ($allcalls);
	$dbHandler->updateWithParameters($query,\@values);
}

sub updateBibCopyLocationsScore
{	
	my $bibid = @_[0];	
	my $query = "DELETE FROM seekdestroy.bib_item_locations WHERE RECORD=$bibid";
	$dbHandler->update($query);
	
	$query = "
	select 
	acl.name,acn.record
	from asset.copy ac,asset.call_number acn,biblio.record_entry bre,asset.copy_location acl where
	acl.id=ac.location and
	acn.id=ac.call_number and
	bre.id=acn.record and
	acn.record = $bibid and
	not acn.deleted and
	not bre.deleted and
	not ac.deleted
	group by acl.name,acn.record
	order by record";
	my $alllocs='';
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $location = @row[0];
		my $record = @row[1];
		my $q="INSERT INTO seekdestroy.bib_item_locations(record,location,different_locations,job)
		values
		(\$1,\$2,\$3,\$4)";
		my @values = ($record,$location,$#results+1,$jobid);
		$alllocs.=$location.',';
		$dbHandler->updateWithParameters($q,\@values);
	}
	$alllocs=substr($alllocs,0,-1);
	
	$query = "UPDATE SEEKDESTROY.BIB_SCORE SET COPY_LOCATIONS=\$1 WHERE RECORD=$bibid";
	my @values = ($alllocs);
	$dbHandler->updateWithParameters($query,\@values);
}

sub findPhysicalItemsOnElectronicBooksUnDedupe
{
	# Find Electronic bibs with physical items and in the dedupe project
    # This function is site specific. No calls to it
	
	my $query = "
	select id,marc from biblio.record_entry where not deleted and lower(marc) ~ \$\$<datafield tag=\"856\" ind1=\"4\" ind2=\"0\">\$\$
	and id in
	(
	select record from asset.call_number where not deleted and id in(select call_number from asset.copy where not deleted)
	)
	and id in
	(select lead_bibid from m_dedupe.merge_map)
	and 
	(
		marc ~ \$\$tag=\"008\">.......................[oqs]\$\$
		or
		marc ~ \$\$tag=\"006\">......[oqs]\$\$
	)
	and
	(
		marc ~ \$\$<leader>......[at]\$\$
	)
	and
	(
		marc ~ \$\$<leader>.......[acdm]\$\$
	)
	";
	updateJob("Processing","findPhysicalItemsOnElectronicBooksUnDedupe  $query");
	my @results = @{$dbHandler->query($query)};
	$log->addLine(($#results+1)." Bibs with physical Items attached from the dedupe");
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		my $marc = @row[1];
		my @scorethis = ($bibid,$marc);
		my @st = ([@scorethis]);		
		updateScoreCache(\@st);
		recordAssetCopyMove($bibid);		
	}
}

sub getBibScores
{
	my $bib = @_[0];
	my $scoreType = @_[1];
	my $query = "SELECT $scoreType FROM SEEKDESTROY.BIB_SCORE WHERE RECORD=$bib";
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		return @row[0];
	}
	return -1;
}

sub addBibMatch
{
	my %queries = %{@_[0]};	
	my $matchedSomething=0;
	my $searchQuery = $queries{'searchQuery'};
	my $problem = $queries{'problem'};
	my @matchQueries = @{$queries{'matchQueries'}};
	my @takeActionWithTheseMatchingMethods = @{$queries{'takeActionWithTheseMatchingMethods'}};	
	updateJob("Processing","addBibMatch  $searchQuery");
	my @results = @{$dbHandler->query($searchQuery)};
	#$log->addLine(($#results+1)." Search Query results");
	foreach(@results)
	{
		my $matchedSomethingThisRound=0;
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		my $bibAudioScore = getBibScores($bibid,'audiobook_score');
		my $marc = @row[1];
		my $extra = @row[2] ? @row[2] : '';
		my @scorethis = ($bibid,$marc);
		my @st = ([@scorethis]);		
		updateScoreCache(\@st);
		my $query="INSERT INTO SEEKDESTROY.PROBLEM_BIBS(RECORD,PROBLEM,EXTRA,JOB) VALUES (\$1,\$2,\$3,\$4)";
		updateJob("Processing","addBibMatch  $query");
		my @values = ($bibid,$problem,$extra,$jobid);
		$dbHandler->updateWithParameters($query,\@values);
		## Now find likely candidates elsewhere in the ME DB	
		addRelatedBibScores($bibid);
		## Now run match queries starting with tight and moving down to loose
		my $i=0;
		while(!$matchedSomethingThisRound && @matchQueries[$i])
		{
			my $matchQ = @matchQueries[$i];
			$matchQ =~ s/\$bibid/$bibid/gi;
			my $matchReason = @matchQueries[$i+1];
			$i+=2;
			#$log->addLine($matchQ);
			updateJob("Processing","addBibMatch  $matchQ");
			my @results2 = @{$dbHandler->query($matchQ)};
			my $foundResults=0;
			foreach(@results2)
			{
				my @ro = @{$_};
				my $mbibid=@ro[0];
				my $holds = findHoldsOnBib($mbibid,$dbHandler);
				$query = "INSERT INTO SEEKDESTROY.BIB_MATCH(BIB1,BIB2,MATCH_REASON,HAS_HOLDS,JOB)
				VALUES(\$1,\$2,\$3,\$4,\$5)";
				updateJob("Processing","addBibMatch  $query");
				$log->addLine("Possible $bibid match: $mbibid $matchReason");
				my @values = ($bibid,$mbibid,$matchReason,$holds,$jobid);
				$dbHandler->updateWithParameters($query,\@values);
				$matchedSomething = 1;
				$matchedSomethingThisRound = 1;				
				$foundResults = 1;
			}
			if($foundResults)
			{
				my $tookAction=0;
				foreach(@takeActionWithTheseMatchingMethods)
				{
					if($_ eq $matchReason)
					{
						if($queries{'action'} eq 'moveallcopies')
						{
							$tookAction = moveCopiesOntoHighestScoringBibCandidate($bibid,$matchReason);
						}
						elsif($queries{'action'} eq 'movesomecopies')
						{
							if($queries{'ifaudioscorebelow'})
							{
								if( $bibAudioScore < $queries{'ifaudioscorebelow'} )
								{
									$tookAction = moveCopiesOntoHighestScoringBibCandidate($bibid,$matchReason,$extra);
								}
							}
							elsif($queries{'ifaudioscoreabove'})
							{
								if( $bibAudioScore > $queries{'ifaudioscoreabove'} )
								{
									$tookAction = moveCopiesOntoHighestScoringBibCandidate($bibid,$matchReason,$extra);
								}
							}

						}
						elsif($queries{'action'} eq 'mergebibs')
						{
							
						}
					}
				}
			}
		}		
	}
	return $matchedSomething;
}

sub addRelatedBibScores
{
	my $rootbib = @_[0];
	# Score bibs that have the same evergreen fingerprint
	my $query =
	"SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE FINGERPRINT = (SELECT EG_FINGERPRINT FROM SEEKDESTROY.BIB_SCORE WHERE RECORD=$rootbib)";
	updateJob("Processing","addRelatedBibScores  $query");
	#$log->addLine($query);
	updateScoreWithQuery($query);
	
	# Pickup a few more bibs that contain the same title anywhere in the MARC
	# This is very slow and it doesn't help get real matches
	# This is disabled
	
	if(0)
	{
		$query="
		SELECT LOWER(TITLE) FROM SEEKDESTROY.BIB_SCORE WHERE RECORD=$rootbib";
		updateJob("Processing","addRelatedBibScores  $query");
		my @results = @{$dbHandler->query($query)};		
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			my $title = @row[0];
			if(length($title)>5)
			{		
				$query =
				"SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE LOWER(MARC) ~ (SELECT LOWER(TITLE) FROM SEEKDESTROY.BIB_SCORE WHERE RECORD=$rootbib)";
				updateJob("Processing","addRelatedBibScores  $query");
				$log->addLine($query);
				updateScoreWithQuery($query);
			}
		}
	}
	
}


sub attemptMovePhysicalItemsOnAnElectronicBook
{
	my $oldbib = @_[0];
	my $query;
	my %queries=();
	$queries{'action'} = 'moveallcopies';
	$queries{'problem'} = "Physical items attched to Electronic Bibs";
	my @okmatchingreasons=("Physical Items to Electronic Bib exact","Physical Items to Electronic Bib exact minus date1");
	$queries{'takeActionWithTheseMatchingMethods'}=\@okmatchingreasons;
	$queries{'searchQuery'} = "
	select id,marc from biblio.record_entry where not deleted and lower(marc) ~ \$\$<datafield tag=\"856\" ind1=\"4\" ind2=\"0\">\$\$
	and id in
	(
	select record from asset.call_number where not deleted and id in(select call_number from asset.copy where not deleted)
	)	
	and 
	(
		marc ~ \$\$tag=\"008\">.......................[oqs]\$\$
		or
		marc ~ \$\$tag=\"006\">......[oqs]\$\$
	)
	and
	(
		marc ~ \$\$<leader>......[at]\$\$
	)
	and
	(
		marc ~ \$\$<leader>.......[acdm]\$\$
	)
	";	
	my @results;
	if($oldbib)
	{
		$queries{'searchQuery'} = "select id,marc from biblio.record_entry where id=$oldbib";
	}
	my @matchQueries = 
	(
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE 
		DATE1 = (SELECT DATE1 FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND RECORD_TYPE = (SELECT RECORD_TYPE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND BIB_LVL = (SELECT BIB_LVL FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Bib exact",
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE 			
		RECORD_TYPE = (SELECT RECORD_TYPE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND BIB_LVL = (SELECT BIB_LVL FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Bib exact minus date1",
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)			
		AND RECORD_TYPE = (SELECT RECORD_TYPE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Bib loose: Author, Title, Record Type",
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Bib loose: Author, Title"		
	);
	
	$queries{'matchQueries'} = \@matchQueries;
	my $success = addBibMatch(\%queries);
	return $success;
}

sub moveCopiesOntoHighestScoringBibCandidate
{
	my $oldbib = @_[0];	
	my $matchReason = @_[1];
	my @copies;
	my $moveOnlyCopies=0;
	if(@_[2])
	{
		$moveOnlyCopies=1;
		@copies = @{$mobUtil->makeArrayFromComma(@_[2])};		
	}
	my $query = "select sbm.bib2,sbs.score from SEEKDESTROY.BIB_MATCH sbm,seekdestroy.bib_score sbs where 
	sbm.bib1=$oldbib and
	sbm.match_reason=\$\$$matchReason\$\$ and
	sbs.record=sbm.bib2
	order by sbs.score";
	$log->addLine("Looking through matches");
	updateJob("Processing","moveCopiesOntoHighestScoringBibCandidate  $query");
	my @results = @{$dbHandler->query($query)};	
	$log->addLine(($#results+1)." potential bibs for destination");
	my $hscore=0;
	my $winner=0;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		my $score = @row[1];
		$log->addLine("Adding Score Possible: $score - $bibid");
		if($score>$hscore)
		{
			$winner=$bibid;
			$hscore=$score;
		}
	}
	$log->addLine("Winning Score: $hscore - $winner");
	if($winner!=0)
	{
		undeleteBIB($winner);
		#print "moveCopiesOntoHighestScoringBibCandidate from: $oldbib\n";
		if(!$moveOnlyCopies)
		{
			moveAllCallNumbers($oldbib,$winner,$matchReason);
			moveHolds($oldbib,$winner);
		}
		else
		{
			moveCopies(\@copies,$winner,$matchReason);			
		}
		return $winner;
	}
	else
	{
		$query="INSERT INTO SEEKDESTROY.CALL_NUMBER_MOVE(FROMBIB,EXTRA,SUCCESS,JOB)
		VALUES(\$1,\$2,\$3,\$4,\$5)";	
		my @values = ($oldbib,"FAILED - $matchReason",'false',$jobid);
		$log->addLine($query);				
		$log->addLine("$oldbib,\"FAILED - $matchReason\",'false',$jobid");
		updateJob("Processing","moveCopiesOntoHighestScoringBibCandidate  $query");
		$dbHandler->updateWithParameters($query,\@values);
	}
	return 0;
}

sub moveCopies
{
	my @copies = @{@_[0]};
	my $destBib = @_[1];
	my $reason = @_[2];
	foreach(@copies)
	{		
		my $copyBarcode = $_;
		print "Working on copy $copyBarcode\n";
		my $query = "SELECT OWNING_LIB,EDITOR,CREATOR,LABEL,ID,RECORD FROM ASSET.CALL_NUMBER WHERE ID = 
		(SELECT CALL_NUMBER FROM ASSET.COPY WHERE BARCODE=\$\$$copyBarcode\$\$)";
		my @results = @{$dbHandler->query($query)};					
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			my $owning_lib = @row[0];
			my $editor = @row[1];
			my $creator = @row[2];
			my $label = @row[3];
			my $oldcall = @row[4];
			my $oldbib = @row[5];
			my $destCallNumber = createCallNumberOnBib($destBib,$label,$owning_lib,$creator,$editor);
			if($destCallNumber!=-1)
			{
				print "received $destCallNumber and moving into recordCopyMove($oldcall,$destCallNumber,$reason)\n";
				recordCopyMove($oldcall,$destCallNumber,$reason);
				$query = "UPDATE ASSET.COPY SET CALL_NUMBER=$destCallNumber WHERE BARCODE=\$\$$copyBarcode\$\$";
				updateJob("Processing","moveCopies  $query");
				$log->addLine($query);
				$log->addLine("Moving $copyBarcode from $oldcall $oldbib to $destCallNumber $destBib" );
				if(!$dryrun)
				{
					$dbHandler->update($query);
				}
			}
			else
			{
				$log->addLine("ERROR! DID NOT GET A CALL NUMBER FROM createCallNumberOnBib($destBib,$label,$owning_lib,$creator,$editor)");
			}
		}
	}
}

sub findPhysicalItemsOnElectronicBooks
{
	my $success = 0;
	# Find Electronic bibs with physical items
	my $subq = $queries{"electronic_book_with_physical_items_attached"};
	my $query = "select id,marc from biblio.record_entry where id in($subq)"; 
	updateJob("Processing","findPhysicalItemsOnElectronic  $query");
	my @results = @{$dbHandler->query($query)};	
	$log->addLine(($#results+1)." Bibs with physical Items attached");
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		$success = attemptMovePhysicalItemsOnAnElectronicBook($bibid);		
	}
	
	return $success;
	
}

sub findPhysicalItemsOnElectronicAudioBooksUnDedupe
{
	# Find Electronic bibs with physical items but and in the dedupe project
    # This function is site specific. No calls to it
    
	my $query = "
	select id,marc from biblio.record_entry where not deleted and lower(marc) ~ \$\$<datafield tag=\"856\" ind1=\"4\" ind2=\"0\">\$\$
	and id in
	(
	select record from asset.call_number where not deleted and id in(select call_number from asset.copy where not deleted)
	)
	and id in
	(select lead_bibid from m_dedupe.merge_map)
	and 
	(
		marc ~ \$\$tag=\"008\">.......................[oqs]\$\$
		or
		marc ~ \$\$tag=\"006\">......[oqs]\$\$
	)
	and
	(
		marc ~ \$\$<leader>......i\$\$
	)	
	";
	updateJob("Processing","findPhysicalItemsOnElectronicAudioBooksUnDedupe  $query");
	my @results = @{$dbHandler->query($query)};
	$log->addLine(($#results+1)." Bibs with physical Items attached from the dedupe");
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		my $marc = @row[1];
		my @scorethis = ($bibid,$marc);
		my @st = ([@scorethis]);		
		updateScoreCache(\@st);
		recordAssetCopyMove($bibid);
	}

}

sub findPhysicalItemsOnElectronicAudioBooks
{
	my $success = 0;
	# Find Electronic Audio bibs with physical items
	my $subq = $queries{"electronic_audiobook_with_physical_items_attached"};
	my $query = "select id,marc from biblio.record_entry where id in($subq)"; 	
	updateJob("Processing","findPhysicalItemsOnElectronicAudioBooks  $query");
	my @results = @{$dbHandler->query($query)};	
	$log->addLine(($#results+1)." Audio Bibs with physical Items attached");
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		$success = attemptMovePhysicalItemsOnAnElectronicAudioBook($bibid);		
	}
	
	return $success;

}


sub attemptMovePhysicalItemsOnAnElectronicAudioBook
{
	my $oldbib = @_[0];
	my $query;
	my %queries=();
	$queries{'action'} = 'moveallcopies';
	$queries{'problem'} = "Physical items attched to Electronic Audio Bibs";
	my @okmatchingreasons=("Physical Items to Electronic Audio Bib exact","Physical Items to Electronic Audio Bib exact minus date1");
	$queries{'takeActionWithTheseMatchingMethods'}=\@okmatchingreasons;
	$queries{'searchQuery'} = "
	select id,marc from biblio.record_entry where not deleted and lower(marc) ~ \$\$<datafield tag=\"856\" ind1=\"4\" ind2=\"0\">\$\$
	and id in
	(
	select record from asset.call_number where not deleted and id in(select call_number from asset.copy where not deleted)
	)	
	and 
	(
		marc ~ \$\$tag=\"008\">.......................[oqs]\$\$
		or
		marc ~ \$\$tag=\"006\">......[oqs]\$\$
	)
	and
	(
		marc ~ \$\$<leader>......i\$\$
	)
	";
	my @results;
	if($oldbib)
	{
		$queries{'searchQuery'} = "select id,marc from biblio.record_entry where id=$oldbib";
	}
	my @matchQueries = 
	(
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE 
		DATE1 = (SELECT DATE1 FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND RECORD_TYPE = (SELECT RECORD_TYPE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND BIB_LVL = (SELECT BIB_LVL FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Audio Bib exact",		
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE 			
		RECORD_TYPE = (SELECT RECORD_TYPE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND BIB_LVL = (SELECT BIB_LVL FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Audio Bib exact minus date1",
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)			
		AND RECORD_TYPE = (SELECT RECORD_TYPE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Audio Bib loose: Author, Title, Record Type",
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND (ELECTRONIC < (SELECT ELECTRONIC FROM seekdestroy.bib_score WHERE RECORD = \$bibid) OR ELECTRONIC < 1)
		AND ITEM_FORM !~ \$\$[oqs]\$\$
		AND RECORD != \$bibid","Physical Items to Electronic Audio Bib loose: Author, Title"		
	);
	
	$queries{'matchQueries'} = \@matchQueries;
	my $success = addBibMatch(\%queries);
	return $success;
}

sub findItemsCircedAsAudioBooksButAttachedNonAudioBib
{
	my $oldbib = @_[0];
	my $query;
	my %sendqueries=();
	$sendqueries{'action'} = 'movesomecopies';
	$sendqueries{'ifaudioscorebelow'} = $audio_book_score_when_audiobooks_dont_belong;
	$sendqueries{'problem'} = "Non-audiobook Bib with items that circulate as 'AudioBooks'";
	my @okmatchingreasons=("AudioBooks attached to non AudioBook Bib exact","AudioBooks attached to non AudioBook Bib exact minus date1");
	$sendqueries{'takeActionWithTheseMatchingMethods'}=\@okmatchingreasons;
	# Find Bibs that are not Audiobooks and have physical items that are circed as audiobooks
	$sendqueries{'searchQuery'} = $queries{"findItemsCircedAsAudioBooksButAttachedNonAudioBib"};
	if($oldbib)
	{
		$sendqueries{'searchQuery'} = "select id,marc from biblio.record_entry where id=$oldbib";
	}
	my @matchQueries = 
	(
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE 
		DATE1 = (SELECT DATE1 FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND RECORD_TYPE = \$\$i\$\$
		AND BIB_LVL = (SELECT BIB_LVL FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)		
		AND RECORD != \$bibid","AudioBooks attached to non AudioBook Bib exact",		
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE 		
		RECORD_TYPE = \$\$i\$\$
		AND BIB_LVL = (SELECT BIB_LVL FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)		
		AND RECORD != \$bibid","AudioBooks attached to non AudioBook Bib exact minus date1",		
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)			
		AND RECORD_TYPE = \$\$i\$\$
		AND RECORD != \$bibid","AudioBooks attached to non AudioBook Bib loose"
				
	);
	
	$sendqueries{'matchQueries'} = \@matchQueries;
	my $success = addBibMatch(\%sendqueries);
	return $success;
}


sub findItemsNotCircedAsAudioBooksButAttachedAudioBib
{
	my $oldbib = @_[0];
	my $query;
	my %queries=();
	$queries{'action'} = 'movesomecopies';
	$queries{'ifaudioscoreabove'} = $audio_book_score_when_audiobooks_dont_belong;
	$queries{'problem'} = "Audiobook Bib with items that do not circulate as 'AudioBooks'";
	my @okmatchingreasons=("Non-AudioBooks attached to AudioBook Bib exact","Non-AudioBooks attached to AudioBook Bib exact minus date1");
	$queries{'takeActionWithTheseMatchingMethods'}=(); #\@okmatchingreasons;
	# Find Bibs that are Audiobooks and have physical items that are not circed as audiobooks
	$queries{'searchQuery'} = "
	select bre.id,bre.marc,string_agg(ac.barcode,\$\$,\$\$) from biblio.record_entry bre, asset.copy ac, asset.call_number acn where 
bre.marc ~ \$\$<leader>......i\$\$
and
bre.id=acn.record and
acn.id=ac.call_number and
not acn.deleted and
not ac.deleted and
ac.circ_modifier not in ( \$\$AudioBooks\$\$,\$\$CD\$\$ )
group by bre.id,bre.marc
limit 1000
	";
	if($oldbib)
	{
		$queries{'searchQuery'} = "select id,marc from biblio.record_entry where id=$oldbib";
	}
	my @matchQueries = 
	(
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE 
		DATE1 = (SELECT DATE1 FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND RECORD_TYPE != \$\$i\$\$
		AND BIB_LVL = (SELECT BIB_LVL FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)		
		AND RECORD != \$bibid","Non-AudioBooks attached to AudioBook Bib exact",		
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE 		
		RECORD_TYPE != \$\$i\$\$
		AND BIB_LVL = (SELECT BIB_LVL FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid) 
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)		
		AND RECORD != \$bibid","Non-AudioBooks attached to AudioBook Bib exact minus date1",		
		
		"SELECT RECORD FROM  seekdestroy.bib_score
		WHERE TITLE = (SELECT TITLE FROM seekdestroy.bib_score WHERE RECORD = \$bibid)
		AND AUTHOR = (SELECT AUTHOR FROM seekdestroy.bib_score WHERE RECORD = \$bibid)			
		AND RECORD_TYPE = \$\$i\$\$
		AND RECORD != \$bibid","Non-AudioBooks attached to AudioBook Bib Bib loose"
				
	);
	
	$queries{'matchQueries'} = \@matchQueries;
	my $success = addBibMatch(\%queries);
	return $success;
}

sub updateScoreWithQuery
{
	my $query = @_[0];
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		my $marc = @row[1];
		my @scorethis = ($bibid,$marc);
		#$log->addLine("Scoring: $bibid");
		my @st = ([@scorethis]);
		updateScoreCache(\@st);
	}
}

sub findPossibleDups
{

	my @formatAssignmentsBefore = ();

	if(!$dryrun)
	{
		# Record the state of the hold copy map
		my $query = "insert into seekdestroy.before_dedupe_hold_map_count (hold,count,job)
		(select hold,count(*),$jobid from action.hold_copy_map group by hold)";
		updateJob("Processing","findPossibleDups  $query");
		$dbHandler->update($query);
		
		# Record the unfilled holds
		my $query = "insert into seekdestroy.before_dedupe_hold_current_copy_null (hold,job)
		(select id,$jobid from action.hold_request where
		current_copy is null and
		cancel_time is null and
		not frozen and
		expire_time is null and
		capture_time is null)";
		updateJob("Processing","findPossibleDups  $query");
		$dbHandler->update($query);
		
		# Record the icon format summary
        $query = '
        SELECT
        (CASE WHEN mraf.value IS NULL THEN \'blank\' ELSE mraf.value END),count(*) "count"
        FROM
        biblio.record_entry bre
        LEFT JOIN metabib.record_attr_flat mraf ON (mraf.attr=$$icon_format$$ AND mraf.id=bre.id)
        WHERE
        NOT bre.deleted
        GROUP BY mraf.value
        ORDER BY "count"
        ';
		updateJob("Processing","findPossibleDups  $query");
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			my $out = join(';',@row);
			$log->addLine($out);
		}
		@formatAssignmentsBefore = @results;
	}
#
# Gather up some potential candidates based on EG Fingerprints
#
	my $query="
        SELECT
        id,fingerprint,marc
        FROM
        biblio.record_entry WHERE fingerprint IN
        (
            SELECT fingerprint FROM
            (
                SELECT fingerprint,count(*) \"count\"
                FROM
                biblio.record_entry bre
                LEFT JOIN seekdestroy.bib_score sbss ON (sbss.record=bre.id)
                WHERE
                NOT bre.deleted AND
                sbss.record IS NULL
                GROUP BY fingerprint
                HAVING count(*) > 1
            ) AS a
        )
        AND NOT deleted
        AND fingerprint != \$\$\$\$
        GROUP BY 1,2,3
        --limit 100;
		";
    updateJob("Processing","findPossibleDups  $query");
    my @results = @{$dbHandler->query($query)};
    my @st=();
    my %alreadycached=();
    my $deleteoldscorecache="";
    updateJob("Processing","findPossibleDups  looping results");
    foreach(@results)
    {
        my $row = $_;
        my @row = @{$row};
        my $id = @row[0];
        my $fingerprint = @row[1];
        my $marc = @row[2];
        if(!$alreadycached{$id})
        {
            $alreadycached{$id}=1;
            my @scorethis = ($id,$marc);
            push(@st,[@scorethis]);
            $deleteoldscorecache.="$id,";
        }
    }
    undef %alreadycached;

    $deleteoldscorecache=substr($deleteoldscorecache,0,-1);
    my $q = "delete from SEEKDESTROY.BIB_MATCH where (BIB1 IN( $deleteoldscorecache) OR BIB2 IN( $deleteoldscorecache)) and job=$jobid";
    updateJob("Processing","findPossibleDups deleting old cache bib_match   $q");
    $dbHandler->update($q);
    updateJob("Processing","findPossibleDups updating scorecache selectively");
    updateScoreCache(\@st);
	
	
	my $query="
    select
    record,
    sd_alt_fingerprint,
    score
    from
    seekdestroy.bib_score sbs2
    join biblio.record_entry bre2 on (bre2.id=sbs2.record and not bre2.deleted)
    where
    sd_alt_fingerprint||opac_icon in
    (
        select idp from
        (
            select sd_alt_fingerprint||opac_icon \"idp\",count(*)
            from
            seekdestroy.bib_score sbs
            join biblio.record_entry bre on (bre.id=sbs.record and not bre.deleted)
            where
            length(btrim(regexp_replace(regexp_replace(sbs.sd_fingerprint,\$\$\\t\$\$,\$\$\$\$,\$\$g\$\$),\$\$\\s\$\$,\$\$\$\$,\$\$g\$\$)))>5
            group by sd_alt_fingerprint||opac_icon having count(*) > 1
        ) as a
    )
    order by sd_alt_fingerprint,score desc, record
		";

    updateJob("Processing","findPossibleDups  $query");
	my @results = @{$dbHandler->query($query)};
	my $current_fp ='';
	my $master_record=-2;
	my %mergeMap = ();
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $record=@row[0];
		my $fingerprint = @row[1];
		my $score= @row[2];
		
		if($current_fp ne $fingerprint)
		{
			my $outs ="$current_fp != $fingerprint Changing master record from $master_record";
			$current_fp=$fingerprint;
			$master_record = $record;
			$outs.=" to $master_record";
			$log->addLine($outs);
		}
		else
		{
			my $hold = 0;#findHoldsOnBib($record, $dbHandler);
			my $q = "INSERT INTO SEEKDESTROY.BIB_MATCH(BIB1,BIB2,MATCH_REASON,HAS_HOLDS,JOB)
			VALUES(\$1,\$2,\$3,\$4,\$5)";
			my @values = ($master_record,$record,"Duplicate SD Fingerprint",$hold,$jobid);
			$dbHandler->updateWithParameters($q,\@values);
			print "Attempting to push into $master_record\n";
			my @temp = ($record);
			if(!$mergeMap{$master_record})
			{
				$mergeMap{$master_record} = \@temp;
			}
			else
			{
				my @al = @{$mergeMap{$master_record}};
				push(@al,$record);
				$mergeMap{$master_record} = \@al;
			}
		}
	}
    my $query = 'select bib1,bib2, 
    $$'.$domainname.'eg/opac/record/$$||bib1||$$?expand=marchtml$$ "leadlink",
    $$'.$domainname.'eg/opac/record/$$||bib2||$$?expand=marchtml$$ "sublink",
    has_holds,
    (select string_agg(value,$$,$$) from metabib.record_attr_flat where attr=$$icon_format$$ and id=sbm.bib1) "leadicon",
    (select string_agg(value,$$,$$) from metabib.record_attr_flat where attr=$$icon_format$$ and id=sbm.bib2) "subicon",
    string_agg(distinct aou_bib1.shortname,\',\'),
    string_agg(distinct aou_bib2.shortname,\',\'),
    string_agg(distinct mtfe.value,\' !! \') "title"
    from 
    seekdestroy.bib_match sbm
    left join asset.call_number acn_bib1 on (acn_bib1.record=sbm.bib1 and not acn_bib1.deleted)
    left join asset.call_number acn_bib2 on (acn_bib2.record=sbm.bib2 and not acn_bib2.deleted)
    left join actor.org_unit aou_bib1 on (aou_bib1.id=acn_bib1.owning_lib)
    left join actor.org_unit aou_bib2 on (aou_bib2.id=acn_bib2.owning_lib)
    join biblio.record_entry bre1 on (sbm.bib1=bre1.id and not bre1.deleted)
    join biblio.record_entry bre2 on (sbm.bib2=bre2.id and not bre2.deleted)
    left join metabib.title_field_entry mtfe on (mtfe.source=sbm.bib1)
    where
    job='.$jobid.'
    group by 
    1,2,3,4,5,6,7
    order by 1,2';

	my @results = @{$dbHandler->query($query)};
    my $autoList;
    my $needsHumans;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $out = join(';',@row);
        my $validator = additionalDedupeValidator(@row[0], @row[1], @row[5], @row[6]);
        if($validator eq '1')
        {
            $autoList .= $out . "\n";
            if(!$dryrun)
            {
                # Now let's merge!
                mergeBibsWithMetarecordHoldsInMind(@row[0],@row[1],"Merge Matching");
            }
        }
        else
        {
            $needsHumans .= "$validator;" . $out . "\n";
        }
        undef $out;
	}

    $log->addLogLine("These were automatically merged:");
    $log->addLine($autoList);
    $log->addLogLine("These were leftover for humans:");
    $log->addLine($needsHumans);
    undef $autoList;
    undef $needsHumans;

	if(!$dryrun)
	{
		# Compare and contrast the before and after		
		$query = "select ahcm.hold, ahcm.count, sbdhmc.count,sbdhmc.count - ahcm.count
		from (select hold,count(*) from action.hold_copy_map group by hold)as ahcm,
		seekdestroy.before_dedupe_hold_map_count sbdhmc
		where
		sbdhmc.job=$jobid and
		sbdhmc.hold=ahcm.hold and
		sbdhmc.count - ahcm.count !=0";
		updateJob("Processing","findPossibleDups  $query");
		my @results = @{$dbHandler->query($query)};
		if($#results > -1)
		{
			$log->addLine("These holds had their copy pool change");
		}
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			my $out = join(';',@row);
			$log->addLine($out);
		}
		
		
		my $query = "select ahr.id, ahr.current_copy
					from 
					action.hold_request ahr,
					seekdestroy.before_dedupe_hold_current_copy_null sbdhccn
					where
					sbdhccn.hold=ahr.id and
					current_copy is not null and 
					cancel_time is null and 
					not frozen and
					expire_time is null and
					capture_time is null ";
		updateJob("Processing","findPossibleDups  $query");
		my @results = @{$dbHandler->query($query)};
		if($#results > -1)
		{
			$log->addLine("These holds were previously unfilled");
		}
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			my $out = join(';',@row);
			$log->addLine($out);
		}
		
		# Record the icon format summary
		$query = '
        SELECT
        (CASE WHEN mraf.value IS NULL THEN \'blank\' ELSE mraf.value END),count(*) "count"
        FROM
        biblio.record_entry bre
        LEFT JOIN metabib.record_attr_flat mraf ON (mraf.attr=$$icon_format$$ AND mraf.id=bre.id)
        WHERE
        NOT bre.deleted
        GROUP BY mraf.value
        ORDER BY "count"
        ';
		updateJob("Processing","findPossibleDups  $query");
		my @results = @{$dbHandler->query($query)};
		$log->addLine("The following are the changes to the format totals after the dedupe.\nFormat;Before;After");
		foreach(@results)
		{
            my $row = $_;
            my @row = @{$row};
            foreach(@formatAssignmentsBefore)
			{
				my @prev = @{$_};
				if(@prev[0] eq @row[0])
				{
					$log->addLine( @prev[0].";".@prev[1].";".@row[1]);
				}
			}
		}
	}
}

sub additionalDedupeValidator
{
    my $leadbib = shift;
    my $subbib = shift;
    my $leadbib_format = shift;
    my $subbib_format = shift;
    my @notAllowedMergedFormats =
    (
        'dvd',
        'vhs',
        'blu-ray',
        'serial',
        'microform'
    );

    return "different formats" if $leadbib_format ne $subbib_format;
    foreach(@notAllowedMergedFormats)
    {
        return "Format not allowed to merge: $_" if $leadbib_format eq $_;
    }

    my $query = "
    select
    lead_list.list,sub_list.list
    from
    (
        select string_agg(distinct circ_lib::text, \$\$,\$\$ order by circ_lib::text) as \"list\"
        from
        asset.call_number acn
        join asset.copy ac on (ac.call_number=acn.id and not acn.deleted and not ac.deleted and acn.record=$leadbib)
    ) as lead_list,
    (
        select string_agg(distinct circ_lib::text, \$\$,\$\$ order by circ_lib::text) as \"list\"
        from
        asset.call_number acn
        join asset.copy ac on (ac.call_number=acn.id and not acn.deleted and not ac.deleted and acn.record=$subbib)
    ) as sub_list
    ";
    my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
        my $row = $_;
        my @row = @{$row};
        my $leadlist = @row[0];
        my $sublist = @row[1];
        my $bigger = length($leadlist) > length($sublist) ? $leadlist : $sublist;
        my $smaller = length($leadlist) > length($sublist) ? $sublist : $leadlist;
        #Wrap it in commas so we can delimit exactly
        $bigger = ',' . $bigger . ',';
        my @vals = split(/,/,$smaller);
        foreach(@vals)
        {
            return "branch with copies on both" if $bigger =~ m/,$_,/;
        }
    }

    return '1';

}

sub mergeBibsWithMetarecordHoldsInMind
{
	my $leadbib = @_[0];
	my $subbib = @_[1];
	my $reason = @_[2];
	my $submetarecord = -1;
	my $leadmetarecord = -1;
	my $leadmetarecordafterupdate = -1;
	my @metarecordsinsubgroup = ();
	my @metarecordsinleadgroup = ();
	my $leadmarc = 0;
	my $submarc = 0;
	
	# Get the metarecord ID for the leadbib
	my $query = "select metarecord,source from metabib.metarecord_source_map where source in($subbib,$leadbib)";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $source = @row[1];
		if($source == $leadbib)
		{
			$leadmetarecord = @row[0];
		}
		else
		{
			$submetarecord = @row[0];
		}
	}
	
	# Gather up all the other bibs in the sub metarecord group
	my $query = "select source from metabib.metarecord_source_map where metarecord = $submetarecord and source != $subbib";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		if(@row[0] != $subbib)
		{
			push(@metarecordsinsubgroup, @row[0]);
		}
	}
	
	# Gather up all the other bibs in the lead metarecord group
	my $query = "select source from metabib.metarecord_source_map where metarecord = $leadmetarecord and source != $leadbib";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		if(@row[0] != $subbib)
		{
			push(@metarecordsinleadgroup, @row[0]);
		}
	}
	
	# READ MARC FOR THE TWO BIBS
	my $query = "select id,marc from biblio.record_entry where id in($subbib,$leadbib)";
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		if(@row[0] == $leadbib)
		{
			$leadmarc = @row[1];
			$leadmarc =~ s/(<leader>.........)./${1}a/;
			$leadmarc = MARC::Record->new_from_xml($leadmarc);
		}
		if(@row[0] == $subbib)
		{
			$submarc = @row[1];
			$submarc =~ s/(<leader>.........)./${1}a/;
			$submarc = MARC::Record->new_from_xml($submarc);
		}
	}

	#Merge the 856s
	$leadmarc = mergeMARC856($leadmarc,$submarc);
    #Merge the 035's if preferred
	$leadmarc = mergeMARC035($leadmarc,$submarc) if( lc($conf{"dedupe_preserve_oclc_from_sub"}) eq 'yes');

	$leadmarc = convertMARCtoXML($leadmarc);


	moveAllCallNumbers($subbib,$leadbib,$reason);
	$query = "update biblio.record_entry set marc=\$1 where id=\$2";
	my @values = ($leadmarc,$leadbib);
	updateJob("Processing",$query);
	$dbHandler->updateWithParameters($query,\@values);
	
	# Check to see if the lead record was assigned a new metarecord group
	
	my $query = "select metarecord from metabib.metarecord_source_map where source in($leadbib) and metarecord != $leadmetarecord";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};		
		$leadmetarecordafterupdate = @row[0];
		$log->addLine("Warning, updating $leadbib caused it to land in another metarecord : $leadmetarecordafterupdate");
	}
	
	$query = "select asset.merge_record_assets($leadbib, $subbib)";
	updateJob("Processing",$query);
	$dbHandler->update($query);
	
	$query = "delete from metabib.metarecord_source_map where source = $subbib";
	updateJob("Processing",$query);
	$dbHandler->update($query);
	
	#setup the final destination variable
	my $finalmetarecord = $leadmetarecord;
	if(($leadmetarecordafterupdate != -1) && ($leadmetarecordafterupdate != $leadmetarecord) )
	{
		$finalmetarecord = $leadmetarecordafterupdate;
	}
	
	my @metarecordHoldMerges = ();
	if($#metarecordsinsubgroup == -1) # There were no other members in the group, so, delete the metarecord
	{
		my @temp = ($submetarecord, $leadmetarecord, $subbib, "Merge lead $leadmetarecord sub $submetarecord");
		push (@metarecordHoldMerges, \@temp);
	}
	if( ($leadmetarecordafterupdate != -1) && ($leadmetarecordafterupdate != $leadmetarecord) && ($#metarecordsinleadgroup == -1) )
	{ 
	# The lead bib moved to a new metarecord group AND it's old group has no other members
		my @temp = ($leadmetarecord, $leadmetarecordafterupdate, $subbib, "Lead moved after update merge lead $leadmetarecordafterupdate sub $leadmetarecord");
		push (@metarecordHoldMerges, \@temp);
	}
	
	foreach(@metarecordHoldMerges)
	{
		my @temp = @{$_};
		my $sub = @temp[0];
		my $lead = @temp[1];
		my $bibid = @temp[2];
		my $reasoni = @temp[3];
		$query = "delete from metabib.metarecord_source_map where source = $sub";
		updateJob("Processing",$query);
		$dbHandler->update($query);
		$query = "delete from metabib.metarecord where id = $sub";
		updateJob("Processing",$query);
		$dbHandler->update($query);
		$query = "select id from action.hold_request where target = $sub and hold_type=\$\$M\$\$";
		my @results = @{$dbHandler->query($query)};
		updateJob("Processing",$query);
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			recordMetaRecordChange($sub,$lead,"null",$bibid,@row[0],$reasoni);
			$query = "update action.hold_request set target = $lead where id=".@row[0];
			updateJob("Processing",$query);
			$dbHandler->update($query);
		}
	}
	
	# Fix master_record pointer
	my $query = "
	select id,fingerprint,master_record,
(select min(source) from metabib.metarecord_source_map where metarecord=a.id)
 from metabib.metarecord a where 
 master_record not in
 (select source from metabib.metarecord_source_map where metarecord=a.id  )
 and a.id in($leadmetarecord,$submetarecord)
";
	updateJob("Processing",$query);
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $metarecord = @row[0];
		my $fingerprint = @row[1];
		my $mmmasterrecord = @row[2];
		my $destmasterrecord =  @row[3];
		recordMetaRecordChange($metarecord,"null",$fingerprint,$mmmasterrecord,"null","After Merge correcting master_record to $destmasterrecord");
		$query = "update metabib.metarecord set master_record = $destmasterrecord where id = $metarecord";
		updateJob("Processing",$query);
		$dbHandler->update($query);
	}
}

sub cleanMetaRecords
{
	# Delete any references to deleted bibs in the metarecord source map (related holds will be fixed later)
	my $query = "select metarecord,source from metabib.metarecord_source_map where source in(select id from biblio.record_entry where deleted)";
	updateJob("Processing",$query);	
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $metarecord = @row[0];
		my $source = @row[1];
		recordMetaRecordChange($metarecord,"null","null",$source,"null","remove deleted bib from source map");
	}
	# Delete them
	my $query = "delete from metabib.metarecord_source_map where source in(select id from biblio.record_entry where deleted)";
	updateJob("Processing",$query);
	$dbHandler->update($query);
	
	# Update hold targets where they are pointing to an orphaned metarecord
	# AKA - nothing in metabib.metarecord_source_map
	
	my $query = "
	select mmsm.metarecord \"dest\",a.master_record,a.id \"source\",mm.fingerprint,ahr.id from 
metabib.metarecord_source_map mmsm,
(
select mm.master_record,mm.id
from
metabib.metarecord mm
left join metabib.metarecord_source_map mmsm on (mmsm.metarecord=mm.id)
where
mmsm.id is null) as a,
metabib.metarecord mm,
action.hold_request ahr
where
ahr.target=a.id and
ahr.hold_type=\$\$M\$\$ and
mm.id=mmsm.metarecord and
a.master_record=mmsm.source
";

updateJob("Processing",$query);	
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $dest = @row[0];
		my $source = @row[2];
		my $fingerprint = @row[3];
		my $holdid = @row[4];
		recordMetaRecordChange($source,$dest,$fingerprint,"null",$holdid,"Hold pointed to orphaned metarecord");
		$query = "update action.hold_request set target=$dest where id=$holdid and target=$source";
updateJob("Processing",$query);	
		$dbHandler->update($query);
	}
	
	# Record all of the ophans
	my $query = "select mm.id,mm.fingerprint,mm.master_record
    from
    metabib.metarecord mm
    left join metabib.metarecord_source_map mmsm on (mmsm.metarecord=mm.id)
    where
    mmsm.id is null";
	updateJob("Processing",$query);	
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $metarecord = @row[0];
		my $fingerprint = @row[1];
		my $bibid = @row[2];
		recordMetaRecordChange($metarecord,"null",$fingerprint,$bibid,"null","orphaned metarecord removed");
	}
	
	# Now remove the orphans
	my $query = "delete from metabib.metarecord
    where
    id in(
    select mm.id
    from
    metabib.metarecord mm
    left join metabib.metarecord_source_map mmsm on (mmsm.metarecord=mm.id)
    where
    mmsm.id is null
    )";
    updateJob("Processing",$query);
	$dbHandler->update($query);
	
	# create metarecords for bibs that do not have one
	my $query = "
	select bre.id from
    biblio.record_entry bre
    left join metabib.metarecord mm on (mm.fingerprint = bre.fingerprint)
    where
    not bre.deleted and
    mm.id is null
	";
	updateJob("Processing",$query);
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $bibid = @row[0];
		$query = "update biblio.record_entry set id=id where id=$bibid";
		updateJob("Processing",$query);	
		$dbHandler->update($query);
	}


	# Merge metarecords with matching fingerprints and move affected metarecord holds
	my $query = "
    select mmsm.metarecord,mmsm.source,mm.fingerprint,mmsm.id
    from
    metabib.metarecord_source_map mmsm
    join metabib.metarecord mm on (mmsm.metarecord=mm.id)
    join (
    select id from metabib.metarecord where fingerprint in(
            select fingerprint from (
                select fingerprint,count(*) from metabib.metarecord group by fingerprint having count(*) > 1
            ) as a
        )
        ) as bb on (bb.id=mm.id)
    order by mm.fingerprint
    ";
	updateJob("Processing",$query);	
	my @results = @{$dbHandler->query($query)};	
	my $currentfingerprint='';
	my $currentmetarecord='';
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $metarecord = @row[0];
		my $bibid = @row[1];
		my $fingerprint = @row[2];
		my $mmsmid = @row[3];
		$log->addLine("currentmetarecord = $currentmetarecord\nmetarecord = $metarecord\ncurrentfingerprint = $currentfingerprint\nfingerprint = $fingerprint");
		# The fingerprint didnt change but the metarecord did, so we need to move this bib to the previous metarecord source map
		if( ($currentfingerprint eq $fingerprint) && ($metarecord != $currentmetarecord) )
		{
			$query = "select id from action.hold_request where target = $metarecord and hold_type=\$\$M\$\$";
			updateJob("Processing",$query);	
			my @results2 = @{$dbHandler->query($query)};	
			foreach(@results2)
			{
				my $row = $_;
				my @row = @{$row};
				my $holdid = @row[0];				
				recordMetaRecordChange($metarecord,$currentmetarecord,$fingerprint,$bibid,$holdid,"Merging duplicated metarecord fingerprints");
				$query = "update action.hold_request set target = $currentmetarecord where id = $holdid and target = $metarecord";
				updateJob("Processing",$query);
				$dbHandler->update($query);
			}
			
			recordMetaRecordChange($metarecord,$currentmetarecord,$fingerprint,$bibid,"null","Merging duplicated metarecord fingerprints $mmsmid");
			$query = "update metabib.metarecord_source_map set metarecord = $currentmetarecord where id = $mmsmid and metarecord = $metarecord";
			updateJob("Processing",$query);
			$dbHandler->update($query);
			# lets see if there are anymore mapping rows for that metarecord, if not, time to delete the metabib.metarecord row
			# we dont want to leave another orphan metabib.metarecord
			$query = "select id from metabib.metarecord_source_map where metarecord = $metarecord";
			updateJob("Processing",$query);
			my @results2 = @{$dbHandler->query($query)};
			if($#results2 < 0)
			{
				$query = "delete from metabib.metarecord where id = $metarecord";
				updateJob("Processing",$query);
				$dbHandler->update($query);
			}
		}
		else
		{
			$currentmetarecord = $metarecord;
			$currentfingerprint = $fingerprint;
		}
	}
	
	
	
	# Fix bibs that are on a mismatched fingerprint metarecord
	my $query = "
select mmsm.id,bre.id,mm.id,mm.fingerprint,bre.fingerprint,counts.count,
(select id from metabib.metarecord where fingerprint=bre.fingerprint and id not in(select metarecord from metabib.metarecord_source_map where source=bre.id)) as \"dest\",
(select id from metabib.metarecord where fingerprint=bre.fingerprint) as \"exists\"
 from 
metabib.metarecord_source_map mmsm,biblio.record_entry bre, metabib.metarecord mm,
(
select source,count(*) as count from metabib.metarecord_source_map group by source 
) as counts
where 
bre.id = mmsm.source and
mm.id=mmsm.metarecord and
mm.fingerprint != bre.fingerprint and
counts.source=bre.id

order by counts.count,mmsm.source
";
	updateJob("Processing",$query);
	my @results = @{$dbHandler->query($query)};	
	my $currentbib = 0;
	my $currentmetarecord = 0;
	my @bibsthathavenowheretogo = ();
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};		
		my $mmsmid = @row[0];
		my $bibid = @row[1];		
		my $metarecord = @row[2];
		my $mmfingerprint = @row[3];
		my $bibfingerprint = @row[4];
		my $count = @row[5];  #how many times this bib is listed in the source map
		my $destmetarecord = @row[6];
		my $exists = @row[7];
		
		if(length($exists) == 0) #huston, we have a problem: this bib doesn't have a metarecord for it to go
		{
			# Deal with it later
			print "Bib with no destination: $bibid\n";
			push (@bibsthathavenowheretogo, $bibid);
			next;
		}
		if($currentbib != $bibid)
		{
			if(length($destmetarecord) > 0)
			{
				recordMetaRecordChange($metarecord,$destmetarecord,$mmfingerprint,$bibid,"null","bib fingerprint mismatch to mm");
				$query = "update metabib.metarecord_source_map set metarecord = $destmetarecord where metarecord = $metarecord and id = $mmsmid";
				updateJob("Processing",$query);
				$dbHandler->update($query);
			}
			else  # The bib has a row in mmsm already attached to the proper metarecord
			{
				recordMetaRecordChange($metarecord,$destmetarecord,$mmfingerprint,$bibid,"null","bib fingerprint mismatch to mm");
				$query = "delete from metabib.metarecord_source_map where metarecord = $metarecord and id = $mmsmid";
				updateJob("Processing",$query);
				$dbHandler->update($query);
			}
		}
		# we already did this last loop - so just delete this reference in mmsm
		else
		{
			recordMetaRecordChange($metarecord,$destmetarecord,$mmfingerprint,$bibid,"null","bib fingerprint mismatch to mm");
			$query = "delete from metabib.metarecord_source_map where metarecord = $metarecord and id = $mmsmid";
			updateJob("Processing",$query);
			$dbHandler->update($query);
		}
		
		# Now check to see if there are any more bibs in the exited metarecord. If not, we need to adjust the related M holds
		$query = "select id from metabib.metarecord_source_map where metarecord = $metarecord";
		updateJob("Processing",$query);	
		my @results2 = @{$dbHandler->query($query)};	
		if($#results2 == -1)
		{
			# Now we have to handle M hold on the metarecord that we just deleted
			$query = "select id from action.hold_request where target=$metarecord and hold_type=\$\$M\$\$";
			updateJob("Processing",$query);	
			my @results2 = @{$dbHandler->query($query)};	
			foreach(@results2)
			{
				my $row = $_;
				my @row = @{$row};
				my $holdid = @row[0];
				recordMetaRecordChange($metarecord,$destmetarecord,$mmfingerprint,$bibid,$holdid,"bib fingerprint mismatch to mm");
			}
			$query = "delete from metabib.metarecord where id = $metarecord";
			updateJob("Processing",$query);
			$dbHandler->update($query);
			
			$query = "update action.hold_request set target = $exists where target=$metarecord and hold_type=\$\$M\$\$";
			updateJob("Processing",$query);
			$dbHandler->update($query);
		}
		
		$currentbib = $bibid;
		
	}
	if($#bibsthathavenowheretogo > -1)
	{
		$log->addLine("****************PROBLEMS WITH Fix bibs that are on a mismatched fingerprint metarecord");
		$log->addLine("These bibs didn't have a destination metarecord");
		$log->addLine(Dumper(\@bibsthathavenowheretogo));
	}
	
	
	# Fix the master_record on mm where it points to a master_record that is not in the pool
	my $query = "
	select id,fingerprint,master_record,
(select min(source) from metabib.metarecord_source_map where metarecord=a.id)
 from metabib.metarecord a where 
 master_record not in
 (select source from metabib.metarecord_source_map where metarecord=a.id  )
";
	updateJob("Processing",$query);
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $metarecord = @row[0];
		my $fingerprint = @row[1];
		my $mmmasterrecord = @row[2];
		my $destmasterrecord =  @row[3];
		recordMetaRecordChange($metarecord,"null",$fingerprint,$mmmasterrecord,"null","correcting master_record to $destmasterrecord");
		$query = "update metabib.metarecord set master_record = $destmasterrecord where id = $metarecord";
		updateJob("Processing",$query);
		$dbHandler->update($query);
	}
}

sub recordMetaRecordChange
{
	my $affectedmetarecord = @_[0]|| 'null';
	my $alternatmetarecord = @_[1] || 'null';
	my $fingerprint = @_[2];
	my $bibid = @_[3] || 'null';
	my $holdid = @_[4] || 'null';
	my $extra = @_[5];
	my $query = "INSERT INTO seekdestroy.metarecord_change(
		metarecord,
		fingerprint,
		newmetarecord,
		bibid,
		holdid,
		extra,
		job)
		values ($affectedmetarecord,\$fingerprint\$$fingerprint\$fingerprint\$,$alternatmetarecord,$bibid,$holdid,\$extra\$$extra\$extra\$,$jobid)
		";
	my @values = ();	
	#updateJob("Processing","recordMetaRecordChange  $query");
	$dbHandler->updateWithParameters($query,\@values);
}

sub findHoldsOnBib
{
	my $bibid=@_[0];	
	my $hold = 0;
	my $query = "select id from action.hold_request ahr where 
	(
		ahr.id in(select hold from action.hold_copy_map where target_copy in(select id from asset.copy where call_number in(select id from asset.call_number where record=$bibid))) and
		ahr.capture_time is null and
		ahr.fulfillment_time is null and
		ahr.cancel_time is null
	)
	or
	(
		ahr.capture_time is null and
		ahr.cancel_time is null and
		ahr.fulfillment_time is null and
		ahr.hold_type=\$\$T\$\$ and
		ahr.target=$bibid
	)
	";
	updateJob("Processing","findHolds $query");
	my @results = @{$dbHandler->query($query)};
	if($#results != -1)
	{
		$hold=1;
	}
	#print "returning $hold\n";
	return $hold
}

sub recordAssetCopyMove
{
	my $oldbib = @_[0];		
	my $query = "select distinct call_number from asset.copy where call_number in(select id from asset.call_number where record in($oldbib) and label!=\$\$##URI##\$\$)";
	updateJob("Processing","recordAssetCopyMove  $query");
	my @cids;
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my @row = @{$_};
		push(@cids,@row[0]);
	}
	
	if($#cids>-1)
	{
		attemptMovePhysicalItemsOnAnElectronicBook($oldbib);
	}
	@cids = ();
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my @row = @{$_};
		my $callnum= @row[0];
		print "There were asset.copies on $oldbib even after attempting to put them on a deduped bib\n";
		$log->addLine("\t$oldbib\tContained physical Items");
		$query="INSERT INTO SEEKDESTROY.CALL_NUMBER_MOVE(CALL_NUMBER,FROMBIB,EXTRA,SUCCESS,JOB)
		VALUES(\$1,\$2,\$3,\$4,\$5)";
		my @values = ($callnum,$oldbib,"FAILED",'false',$jobid);
		$log->addLine($query);
		updateJob("Processing","recordAssetCopyMove  $query");
		$dbHandler->updateWithParameters($query,\@values);
	}
}

sub moveAssetCopyToPreviouslyDedupedBib
{
    # This function is site specific. No calls to it
	my $currentBibID = @_[0];
	my %possibles;	
	my $query = "select mmm.sub_bibid,bre.marc from m_dedupe.merge_map mmm, biblio.record_entry bre 
	where lead_bibid=$currentBibID and bre.id=mmm.sub_bibid
	and
	bre.marc !~ \$\$tag=\"008\">.......................[oqs]\$\$
	and
	bre.marc !~ \$\$tag=\"006\">......[oqs]\$\$
	";
updateJob("Processing","moveAssetCopyToPreviouslyDedupedBib  $query");
	#print $query."\n";
	my @results = @{$dbHandler->query($query)};
	my $winner=0;
	my $currentWinnerElectricScore=10000;
	my $currentWinnerMARCScore=0;
	foreach(@results)
	{
		my @row = @{$_};
		my $prevmarc = @row[1];
		$prevmarc =~ s/(<leader>.........)./${1}a/;
		$prevmarc = MARC::Record->new_from_xml($prevmarc);
		my @temp=($prevmarc,determineElectricScore($prevmarc),scoreMARC($prevmarc));
		#need to initialize the winner values
		$winner=@row[0];
		$currentWinnerElectricScore = @temp[1];
		$currentWinnerMARCScore = @temp[2];
		$possibles{@row[0]}=\@temp;
	}
	
	#choose the best deleted bib - we want the lowest electronic bib score in this case because we want to attach the 
	#items to the *most physical bib
	while ((my $bib, my $attr) = each(%possibles))
	{
		my @atts = @{$attr};
		if(@atts[1]<$currentWinnerElectricScore)
		{
			$winner=$bib;
			$currentWinnerElectricScore=@atts[1];
			$currentWinnerMARCScore=@atts[2];
		}
		elsif(@atts[1]==$currentWinnerElectricScore && @atts[2]>$currentWinnerMARCScore)
		{
			$winner=$bib;
			$currentWinnerElectricScore=@atts[1];
			$currentWinnerMARCScore=@atts[2];
		}
	}
	if($winner!=0)
	{
		undeleteBIB($winner);
		#find all of the eligible call_numbers
		$query = "SELECT ID FROM ASSET.CALL_NUMBER WHERE RECORD=$currentBibID AND LABEL!= \$\$##URI##\$\$";
updateJob("Processing","moveAssetCopyToPreviouslyDedupedBib  $query");
		my @results = @{$dbHandler->query($query)};
		foreach(@results)
		{	
			my @row = @{$_};
			my $acnid = @row[0];			
			my $callNID = moveCallNumber($acnid,$currentBibID,$winner,"Dedupe pool");
			$query = 
			"INSERT INTO seekdestroy.undedupe(oldleadbib,undeletedbib,undeletedbib_electronic_score,undeletedbib_marc_score,moved_call_number,job)
			VALUES($currentBibID,$winner,$currentWinnerElectricScore,$currentWinnerMARCScore,$callNID,$jobid)";
updateJob("Processing","moveAssetCopyToPreviouslyDedupedBib  $query");							
			$log->addLine($query);
			$dbHandler->update($query);
		}
		moveHolds($currentBibID,$winner);
	}
}

sub undeleteBIB
{
	my $bib = @_[0];	
	my $query = "select deleted from biblio.record_entry where id=$bib";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{	
		my $row = $_;
		my @row = @{$row};			
		#make sure that it is in fact deleted
		if(@row[0] eq 't' || @row[0] == 1)
		{
			my $tcn_value = $bib;
			my $count=1;			
			#make sure that when we undelete it, it will not collide its tcn_value 
			while($count>0)
			{
				$query = "select count(*) from biblio.record_entry where tcn_value = \$\$$tcn_value\$\$ and id != $bib";
				$log->addLine($query);
updateJob("Processing","undeleteBIB  $query");
				my @results = @{$dbHandler->query($query)};
				foreach(@results)
				{	
					my $row = $_;
					my @row = @{$row};
					$count=@row[0];
				}
				$tcn_value.="_";
			}
			#take the last tail off
			$tcn_value=substr($tcn_value,0,-1);
			#finally, undelete the bib making it available for the asset.call_number
			$query = "update biblio.record_entry set deleted=\$\$f\$\$,tcn_source=\$\$un-deduped\$\$,tcn_value = \$\$$tcn_value\$\$  where id=$bib";
			if(!$dryrun)
			{
				$dbHandler->update($query);
			}
		}
	}
}

sub moveAllCallNumbers
{
	my $oldbib = @_[0];
	my $destbib = @_[1];
	my $matchReason = @_[2];
	
	my $query = "select id from asset.call_number where record=$oldbib and label!=\$\$##URI##\$\$";
	$log->addLine($query);
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my @row = @{$_};
		my $calln = @row[0];
		#print "moveAllCallNumbers from: $oldbib\n";
		moveCallNumber($calln,$oldbib,$destbib,$matchReason);
	}
	
}

sub recordCopyMove
{
	my $callnumberid = @_[0];
	my $destcall = @_[1];
	my $matchReason = @_[2];
	my $query = "SELECT ID FROM ASSET.COPY WHERE CALL_NUMBER=$callnumberid";
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my @row = @{$_};
		my $copy = @row[0];
		$query="INSERT INTO SEEKDESTROY.COPY_MOVE(COPY,FROMCALL,TOCALL,EXTRA,JOB) VALUES(\$1,\$2,\$3,\$4,\$5)";
		my @values = ($copy,$callnumberid,$destcall,$matchReason,$jobid);
		#$log->addLine($query);
		$dbHandler->updateWithParameters($query,\@values);
	}
}

sub recordCallNumberMove
{
	my $callnumber = @_[0];
	my $record = @_[1];
	my $destrecord = @_[2];
	my $matchReason = @_[3];	
	
	#print "recordCallNumberMove from: $record\n";
	if($mobUtil->trim(length($destrecord))<1)
	{
		$log->addLine("tobib is null - \$callnumber=$callnumber, FROMBIB=$record, \$matchReason=$matchReason");
	}
	my $query="INSERT INTO SEEKDESTROY.CALL_NUMBER_MOVE(CALL_NUMBER,FROMBIB,TOBIB,EXTRA,JOB) VALUES(\$1,\$2,\$3,\$4,\$5)";	
	my @values = ($callnumber,$record,$destrecord,$matchReason,$jobid);
	$log->addLine($query);
	$dbHandler->updateWithParameters($query,\@values);
}
	
sub moveCallNumber
{
	my $callnumberid = @_[0];
	my $frombib = @_[1];
	#print "moveCallNumber from: $frombib\n";
	my $destbib = @_[2];
	my $matchReason = @_[3];

	my $finalCallNumber = $callnumberid;
	my $query = "SELECT ID,LABEL,RECORD FROM ASSET.CALL_NUMBER WHERE RECORD = $destbib
	AND LABEL=(SELECT LABEL FROM ASSET.CALL_NUMBER WHERE ID = $callnumberid ) 
	AND OWNING_LIB=(SELECT OWNING_LIB FROM ASSET.CALL_NUMBER WHERE ID = $callnumberid ) AND NOT DELETED";
	
	my $moveCopies=0;
	my @results = @{$dbHandler->query($query)};
	#print "about to loop the callnumber results\n";
	foreach(@results)
	{
		#print "it had a duplciate call number\n";
		## Call number already exists on that record for that 
		## owning library and label. So let's just move the 
		## copies to it instead of moving the call number			
		$moveCopies=1;		
		my @row = @{$_};
		my $destcall = @row[0];
		$log->addLine("Call number $callnumberid had a match on the destination bib $destbib and we will be moving the copies to the call number instead of moving the call number");
		recordCopyMove($callnumberid,$destcall,$matchReason);	
		$query = "UPDATE ASSET.COPY SET CALL_NUMBER=$destcall WHERE CALL_NUMBER=$callnumberid";
		updateJob("Processing","moveCallNumber  $query");
		$log->addLine("Moving copies from $callnumberid call number to $destcall");
		if(!$dryrun)
		{
			$dbHandler->update($query);
		}
		$finalCallNumber=$destcall;
	}
	
	if(!$moveCopies)
	{	
	#print "it didnt have a duplciate call number... going into recordCallNumberMove\n";
		recordCallNumberMove($callnumberid,$frombib,$destbib,$matchReason);		
		#print "done with recordCallNumberMove\n";
		$query="UPDATE ASSET.CALL_NUMBER SET RECORD=$destbib WHERE ID=$callnumberid";
		$log->addLine($query);
		updateJob("Processing","moveCallNumber  $query");
		$log->addLine("Moving call number $callnumberid from record $frombib to $destbib");
		if(!$dryrun)
		{
			$dbHandler->update($query);
		}
	}
	return $finalCallNumber;

}

sub createCallNumberOnBib
{
	my $bibid = @_[0];
	my $call_label = @_[1];
	my $owning_lib = @_[2];
	my $creator = @_[3];
	my $editor = @_[4];
	my $query = "SELECT ID FROM ASSET.CALL_NUMBER WHERE LABEL=\$\$$call_label\$\$ AND RECORD=$bibid AND OWNING_LIB=$owning_lib";
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};	
		print "got a call number that was on the record already\n";
		return @row[0];
	}
	$query = "INSERT INTO ASSET.CALL_NUMBER (CREATOR,EDITOR,OWNING_LIB,LABEL,LABEL_CLASS,RECORD) 
	VALUES (\$1,\$2,\$3,\$4,\$5,\$6)";
	$log->addLine($query);
	$log->addLine("$creator,$editor,$owning_lib,$call_label,1,$bibid");
	my @values = ($creator,$editor,$owning_lib,$call_label,1,$bibid);
	if(!$dryrun)
	{	
		$dbHandler->updateWithParameters($query,\@values);
	}
	print "Creating new call number: $creator,$editor,$owning_lib,$call_label,1,$bibid \n";
	$query = "SELECT ID FROM ASSET.CALL_NUMBER WHERE LABEL=\$\$$call_label\$\$ AND RECORD=$bibid AND OWNING_LIB=$owning_lib";
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		if($mobUtil->trim(length($bibid))<1)
		{
			$log->addLine("bibid is null - \$callnumber=".@row[0]." createCallNumberOnBib OWNING_LIB=$owning_lib LABEL=$call_label");
		}
		$query="INSERT INTO SEEKDESTROY.CALL_NUMBER_MOVE(CALL_NUMBER,TOBIB,JOB) VALUES(\$1,\$2,\$3)";
		@values = (@row[0],$bibid,$jobid);
		$log->addLine($query);
		$dbHandler->updateWithParameters($query,\@values);
		return @row[0];
	}
	return -1;	
}

sub moveHolds
{
	my $oldBib = @_[0];
	my $newBib = @_[1];
	my $query = "UPDATE ACTION.HOLD_REQUEST SET TARGET=$newBib WHERE TARGET=$oldBib AND HOLD_TYPE=\$\$T\$\$ AND fulfillment_time IS NULL AND capture_time IS NULL AND cancel_time IS NULL"; 
	$log->addLine($query);
	updateJob("Processing","moveHolds  $query");
	#print $query."\n";
	if(!$dryrun)
	{
		$dbHandler->update($query);
	}
}

sub getAllScores
{
	my $marc = @_[0];
	my %allscores = ();
	$allscores{'electricScore'}=determineElectricScore($marc);
	$allscores{'audioBookScore'}=determineAudioBookScore($marc);
	$allscores{'largeprint_score'}=determineLargePrintScore($marc);
	$allscores{'video_score'}=determineVideoScore($marc);
	$allscores{'microfilm_score'}=determineScoreWithPhrases($marc,\@microfilmSearchPhrases);
	$allscores{'microfiche_score'}=determineScoreWithPhrases($marc,\@microficheSearchPhrases);
	$allscores{'music_score'}=determineMusicScore($marc);
	$allscores{'playaway_score'}=determineScoreWithPhrases($marc,\@playawaySearchPhrases);
	my $highname='';
	my $highscore=0;
	my $highscoredistance=0;
	my $secondplacename='';
	while ((my $scorename, my $score ) = each(%allscores))
	{
		my $tempdistance=$highscore-$score;
		if($score>$highscore)
		{
			$secondplacename=$highname;
			$highname=$scorename;
			$highscoredistance=($score-$highscore);
			$highscore=$score;
		}
		elsif($score==$highscore)
		{
			$highname.=' tied '.$scorename;
			$highscoredistance=0;
			$secondplacename='';
		}
		elsif($tempdistance<$highscoredistance)
		{
			$highscoredistance=$tempdistance;
			$secondplacename=$scorename;
		}
	}
	# There is no second place when the high score is the same as the distance
	# Meaning it's next contender scored a fat 0
	if($highscoredistance==$highscore)
	{
		$secondplacename='';
	}
	$allscores{'winning_score'}=$highname;
	$allscores{'winning_score_score'}=$highscore;
	$allscores{'winning_score_distance'}=$highscoredistance;
	$allscores{'second_place_score'}=$secondplacename;
	
	return \%allscores;
}

sub determineElectricScore
{
	my $marc = @_[0];
	my @e56s = $marc->field('856');
	my @two45 = $marc->field('245');
	if(!@e56s)
	{
		return 0;
	}
	$marc->delete_fields(@two45);
	my $textmarc = $marc->as_formatted();
	$marc->insert_fields_ordered(@two45);
	my $score=0;
	my $found=0;	
	foreach(@e56s)
	{
		my $field = $_;
		my $ind2 = $field->indicator(2);		
		if($ind2 eq '0') #only counts if the second indicator is 0 ("Resource") documented here: http://www.loc.gov/marc/bibliographic/bd856.html
		{	
			my @subs = $field->subfield('u');
			foreach(@subs)
			{
				#print "checking $_ for http\n";
				if(m/http/g)
				{
					$found=1;
				}
			}
		}
	}
	foreach(@two45)
	{
		my $field = $_;		
		my @subs = $field->subfield('h');
		foreach(@subs)
		{
			my $subf = lc($_);
			foreach(@electronicSearchPhrases)
			{
				#if the phrases are found in the 245h, they are worth 5 each
				my $phrase=lc($_);
				if($subf =~ m/$phrase/g)
				{
					$score+=5;
					#$log->addLine("$phrase + 5 points 245h");
				}
			}
		}
	}
	if($found)
	{
		$score++;
	}
	foreach(@electronicSearchPhrases)
	{
		my $phrase = lc$_;
		my @c = split(lc$phrase,lc$textmarc);
		if($#c>1) # Found at least 2 matches on that phrase
		{
			$score++;
		}
	}
	#print "Electric score: $score\n";
	return $score;
}


sub determineMusicScore
{
	my $marc = @_[0];	
	my @two45 = $marc->field('245');
	#$log->addLine(getsubfield($marc,'245','a'));
	$marc->delete_fields(@two45);
	my $textmarc = lc($marc->as_formatted());
	$marc->insert_fields_ordered(@two45);
	my $score=0;
	my $listone;
	my $listtwo;
	
	foreach(@two45)
	{
		my $field = $_;		
		my @subs = $field->subfield('h');
		foreach(@subs)
		{
			my $subf = lc($_);
			foreach(@musicSearchPhrases)
			{
				#if the phrases are found in the 245h, they are worth 5 each
				my $phrase=lc($_);
				if($subf =~ m/$phrase/g)
				{
					$score+=5;
					# $log->addLine("$phrase + 5 points 245h");
					$listone=1;
				}
			}
			foreach(@musicSearchPhrasesAddition)
			{
				#if the phrases are found in the 245h, they are worth 5 each
				my $phrase=lc($_);
				if($subf =~ m/$phrase/g)
				{
					$score+=5;
					# $log->addLine("$phrase + 5 points 245h");
					$listtwo=1;
				}
			}
			if($subf =~ m/a novel/g)
			{
				#This is a novel!
				return 0;
			}
		}
		my @subs = $field->subfield('b');
		foreach(@subs)
		{
			my $subf = lc($_);
			if($subf =~ m/a novel/g)
			{
				#This is a novel!
				return 0;
			}
		}
	}

	foreach(@musicSearchPhrases)
	{
		my $phrase = lc$_;
		
		if($textmarc =~ m/$phrase/g) # Found at least 1 match on that phrase
		{
			$score++;
			# $log->addLine("$phrase + 1 points elsewhere");
			$listone=1;
		}
	}
	
	foreach(@musicSearchPhrasesAddition)
	{
		my $phrase = lc$_;
		
		if($textmarc =~ m/$phrase/g) # Found at least 1 match on that phrase
		{
			$score++;
			# $log->addLine("$phrase + 1 points elsewhere");
			$listtwo=1;
		}
	}
	
	#must contain a phrase from both lists
	if(!($listone && $listtwo))
	{
		# $log->addLine("Phrases were not found in both lists");
		return 0;
	}
	
	# The 505 contains track listing
	my @five05 = $marc->field('505');
	my $tcount;
	foreach(@five05)
	{
		my $field = $_;
		my @subfieldts = $field->subfield('t');
		foreach(@subfieldts)
		{
			$tcount++;
		}
		
	}
	#only tick once for track listings, because it may not be tracks
	#It could be poetry
	#It could be chapters on a dvd
	$score++ unless $tcount==0;
	
	my @nonmusicphrases = ('non music', 'non-music', 'abridge', 'talking books', 'recorded books');
	# Make the score 0 if non musical shows up
	my @tags = $marc->fields();
	my $found=0;
	foreach(@tags)
	{
		my $field = $_;
		my @subfields = $field->subfields();
		foreach(@subfields)
		{
			my @subfield=@{$_};
			my $test = lc(@subfield[1]);
			#$log->addLine("0 = ".@subfield[0]."  1 = ".@subfield[1]);
			foreach(@nonmusicphrases)
			{
				my $phrase = lc$_;
				#$log->addLine("$test\nfor\n$phrase");				
				if($test =~ m/$phrase/g) # Found at least 1 match on that phrase
				{
					$score=0;
					$found=1;
					#$log->addLine("$phrase 0 points!");
				}
				last if $found;
			}
			last if $found;
		}
		last if $found;
	}
	
	
	return $score;
}


sub determineAudioBookScore
{
	my $marc = @_[0];
	my @two45 = $marc->field('245');
	my @isbn = $marc->field('020');
	$marc->delete_fields(@isbn);
	$marc->delete_fields(@two45);	
	my $textmarc = lc($marc->as_formatted());
	$marc->insert_fields_ordered(@two45);
	$marc->insert_fields_ordered(@isbn);
	my $score=0;
	
	foreach(@two45)
	{
		my $field = $_;		
		my @subs = $field->subfield('h');
		foreach(@subs)
		{
			my $subf = lc($_);
			foreach(@audioBookSearchPhrases)
			{
				#if the phrases are found in the 245h, they are worth 5 each
				my $phrase=lc($_);
				if($subf =~ m/$phrase/g)
				{
					$score+=5;
					#$log->addLine("$phrase + 5 points 245h");
				}
			}
		}
	}
	foreach(@audioBookSearchPhrases)
	{
		my $phrase = lc$_;		
		if($textmarc =~ m/$phrase/g) # Found at least 1 match on that phrase
		{
			$score++;
			#$log->addLine("$phrase + 1 points elsewhere");
		}
	}
	
	return $score;
}

# Dead function - decided to score the same as the rest
sub determinePlayawayScore
{
	my $marc = @_[0];		
	my $score=0;
	my @isbn = $marc->field('020');
	$marc->delete_fields(@isbn);
	my $textmarc = lc($marc->as_formatted());
	$marc->insert_fields_ordered(@isbn);
	my @zero07 = $marc->field('007');
	my %zero07looking = ('cz'=>0,'sz'=>0);
	
	foreach(@playawaySearchPhrases)
	{
		my $phrase = lc$_;		
		if($textmarc =~ m/$phrase/g) # Found at least 1 match on that phrase
		{
			$score++;
			#$log->addLine("$phrase + 1 points elsewhere");
		}
	}
	
	if($#zero07>0)
	{
		foreach(@zero07)
		{
			my $field=$_;
			while ((my $looking, my $val ) = each(%zero07looking))
			{		
				if($field->data() =~ m/^$looking/)
				{
					$zero07looking{$looking}=1;
				}
			}
		}
		if($zero07looking{'cz'} && $zero07looking{'sz'})
		{
			my $my_008 = $marc->field('008')->data();
			my $my_006 = $marc->field('006')->data() unless not defined $marc->field('006');
			my $type = substr($marc->leader, 6, 1);				
			my $form=0;
			if($my_008)
			{
				$form = substr($my_008,23,1) if ($my_008 && (length $my_008 > 23 ));			
			}
			if (!$form)
			{
				$form = substr($my_006,6,1) if ($my_006 && (length $my_006 > 6 ));
			}			
			if($type eq 'i' && $form eq 'q')
			{
				$score=100;
			}
		}
	}
	
	return $score;
}

sub determineLargePrintScore
{
	my $marc = @_[0];
	my @searchPhrases = @largePrintBookSearchPhrases;
	my @two45 = $marc->field('245');
	#$log->addLine(getsubfield($marc,'245','a'));
	$marc->delete_fields(@two45);
	my $textmarc = lc($marc->as_formatted());
	$marc->insert_fields_ordered(@two45);
	my $score=0;
	#$log->addLine(lc$textmarc);
	foreach(@two45)
	{
		my $field = $_;		
		my @subs = $field->subfield('h');
		foreach(@subs)
		{
			my $subf = lc($_);
			foreach(@searchPhrases)
			{
				#if the phrases are found in the 245h, they are worth 5 each
				my $phrase=lc($_);
				#ignore the centerpoint/center point search phrase, those need to only match in the 260,262
				if($phrase =~ m/center/g)
				{}
				elsif($subf =~ m/$phrase/g)
				{
					$score+=5;
					#$log->addLine("$phrase + 5 points 245h");
				}
			}
		}
		my @subs = $field->subfield('a');
		foreach(@subs)
		{
			my $subf = lc($_);
			foreach(@searchPhrases)
			{
				#if the phrases are found in the 245a, they are worth 5 each
				my $phrase=lc($_);
				#ignore the centerpoint/center point search phrase, those need to only match in the 260,264
				if($phrase =~ m/center/g)
				{}
				elsif($subf =~ m/$phrase/g)
				{
					$score+=5;
					#$log->addLine("$phrase + 5 points 245a");
				}
			}
		}
	}
	foreach(@searchPhrases)
	{
		my $phrase = lc$_;
		#ignore the centerpoint/center point search phrase, those need to only match in the 260,264
		if($phrase =~ m/center/g)
		{}
		elsif($textmarc =~ m/$phrase/g) # Found at least 1 match on that phrase
		{
			$score++;
			#$log->addLine("$phrase + 1 points elsewhere");
		}
	}
	my @two60 = $marc->field('260');
	my @two64 = $marc->field('264');
	my @both = (@two60,@two64);
	foreach(@two60)
	{
		my $field = $_;		
		my @subs = $field->subfields();
		my @matches=(0,0);
		foreach(@subs)
		{
			my @s = @{$_};
			foreach(@s)
			{				
				my $subf = lc($_);
				#$log->addLine("Checking $subf");
				if($subf =~ m/centerpoint/g)
				{
					if(@matches[0]==0)
					{
						$score++;
						@matches[0]=1;
						#$log->addLine("centerpoint + 1 points");
					}
				}
				if($subf =~ m/center point/g)
				{
					if(@matches[1]==0)
					{
						$score++;
						@matches[1]=1;
						#$log->addLine("center point + 1 points");
					}
				}
			}
		}
	}
	return $score;
}

sub determineVideoScore
{
	my $marc = @_[0];	
	my @searchPhrases = @videoSearchPhrases;
	my @two45 = $marc->field('245');
	#$log->addLine(getsubfield($marc,'245','a'));
	$marc->delete_fields(@two45);
	my $textmarc = lc($marc->as_formatted());
	$marc->insert_fields_ordered(@two45);
	my $score=0;
	if($textmarc =~ m/music from the motion picture/g)
	{
		return 0;
	}
	if($textmarc =~ m/music from the movie/g)
	{
		return 0;
	}
	if($textmarc =~ m/motion picture music/g)
	{
		return 0;
	}
	if($textmarc =~ m/playaway/g  ||  $textmarc =~ m/findaway/g)
	{
		return 0;
	}
	#$log->addLine(lc$textmarc);
	foreach(@two45)
	{
		my $field = $_;		
		my @subs = $field->subfield('h');
		foreach(@subs)
		{
			my $subf = lc($_);
			foreach(@searchPhrases)
			{
				#if the phrases are found in the 245h, they are worth 5 each
				my $phrase=lc($_);
				if($subf =~ m/$phrase/g)
				{
					$score+=5;
					#$log->addLine("$phrase + 5 points 245h");
				}
			}
		}
		my @subs = $field->subfield('a');
		foreach(@subs)
		{
			my $subf = lc($_);
			foreach(@searchPhrases)
			{
				#if the phrases are found in the 245a, they are worth 5 each
				my $phrase=lc($_);
				if($subf =~ m/$phrase/g)
				{
					$score+=5;
					#$log->addLine("$phrase + 5 points 245a");
				}
			}
		}
	}
	foreach(@searchPhrases)
	{
		my $phrase = lc$_;
		#$log->addLine("$phrase");		
		if($textmarc =~ m/$phrase/g) # Found at least 1 match on that phrase
		{
			$score++;
			#$log->addLine("$phrase + 1 points elsewhere");
		}
	}
	# The 505 contains track listing
	my @five05 = $marc->field('505');
	my $tcount;
	foreach(@five05)
	{
		my $field = $_;
		my @subfieldts = $field->subfield('t');
		foreach(@subfieldts)
		{
			$score++;
		}
		
	}
	return $score;
}

sub determineScoreWithPhrases
{
	my $marc = @_[0];
	my @searchPhrases = @{@_[1]};
	my @two45 = $marc->field('245');
	#$log->addLine(getsubfield($marc,'245','a'));
	$marc->delete_fields(@two45);
	my $textmarc = lc($marc->as_formatted());
	$marc->insert_fields_ordered(@two45);
	my $score=0;
	#$log->addLine(lc$textmarc);
	foreach(@two45)
	{
		my $field = $_;		
		my @subs = $field->subfield('h');
		foreach(@subs)
		{
			my $subf = lc($_);
			foreach(@searchPhrases)
			{
				#if the phrases are found in the 245h, they are worth 5 each
				my $phrase=lc($_);
				if($subf =~ m/$phrase/g)
				{
					$score+=5;
					#$log->addLine("$phrase + 5 points 245h");
				}
			}
		}
		my @subs = $field->subfield('a');
		foreach(@subs)
		{
			my $subf = lc($_);
			foreach(@searchPhrases)
			{
				#if the phrases are found in the 245a, they are worth 5 each
				my $phrase=lc($_);
				if($subf =~ m/$phrase/g)
				{
					$score+=5;
					#$log->addLine("$phrase + 5 points 245a");
				}
			}
		}
	}
	foreach(@searchPhrases)
	{
		my $phrase = lc$_;
		#$log->addLine("$phrase");		
		if($textmarc =~ m/$phrase/g) # Found at least 1 match on that phrase
		{
			$score++;
			#$log->addLine("$phrase + 1 points elsewhere");
		}
	}
	return $score;
}

sub identifyBibsToScore
{
	my @ret;
#This query finds bibs that have not received a score at all
	my $query = "SELECT ID,MARC FROM BIBLIO.RECORD_ENTRY WHERE ID NOT IN(SELECT RECORD FROM SEEKDESTROY.BIB_SCORE) AND DELETED IS FALSE 
    -- LIMIT 100
    ";
	my @results = @{$dbHandler->query($query)};
	my @news;
	my @updates;
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $id = @row[0];
		my $marc = @row[1];
		my @temp = ($id,$marc);
		push (@news, [@temp]);
	}
#This query finds bibs that have received but the marc has changed since the last score
	$query = "SELECT SBS.RECORD,BRE.MARC,SBS.ID,SCORE FROM SEEKDESTROY.BIB_SCORE SBS,BIBLIO.RECORD_ENTRY BRE WHERE SBS.score_time < BRE.EDIT_DATE AND SBS.RECORD=BRE.ID";
	@results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $rec = @row[0];
		my $marc = @row[1];
		my $id = @row[2];
		my $score = @row[3];
		my @temp = ($rec,$marc,$id,$score);
		push (@updates, [@temp]);
	}
	push(@ret,[@news]);
	push(@ret,[@updates]);
	return \@ret;
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

sub mergeMARC035
{
	my $leadMarc = @_[0];
	my $subMarc = @_[1];
    my %lead035 = %{getOCLCFrom035($leadMarc)};
    my %sub035 = %{getOCLCFrom035($subMarc)};
    my %append = ();
    while ((my $subfield, my $value ) = each(%sub035))
    {
        foreach(@{$value})
        {
            my $subVal = $_;
            my $found = 0;
            foreach(@{$lead035{$subfield}})
            {
                $found = 1 if($_ eq $subVal)
            }
            if(!$found)
            {
                if(!$append{$subfield})
                {
                    my @a = ();
                    $append{$subfield} = \@a;
                }
                push(@{$append{$subfield}}, $subVal);
            }
            undef $found;
        }
    }
    while ((my $subfield, my $value ) = each(%append))
    {
        foreach(@{$value})
        {
            my $field = MARC::Field->new( '035', ' ', ' ', $subfield => $_ );
            $leadMarc->insert_grouped_field($field);
        }
    }
    return $leadMarc;
}

sub getOCLCFrom035
{
    my $marc = shift;
    my @l035s = $marc->field("035");
    my @subfield_list = ('a','z');
    my %ret = ();
    foreach(@l035s)
    {
        my $thisField = $_;
        foreach(@subfield_list)
        {
            my $thisSubfield = $_;
            my %temp = ();
            my @subs = $thisField->subfield($thisSubfield);
            foreach(@subs)
            {
                if($_ =~ m/\(?OCo{0,1}LC\)?.?\d+/)
                {
                    $temp{$_} = 1;
                }
            }
            while ((my $internal, my $mvalue ) = each(%temp))
            {
                if(!$ret{$thisSubfield})
                {
                    my @a = ();
                    $ret{$thisSubfield} = \@a;
                }
                push(@{$ret{$thisSubfield}}, $internal);
            }
            undef %temp;
            undef @subs;
        }
    }
    return \%ret;
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
	my $query = "INSERT INTO seekdestroy.job(status) values('$status')";
	my $results = $dbHandler->update($query);
	if($results)
	{
		$query = "SELECT max( ID ) FROM seekdestroy.job";
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

sub getFingerprints
{
	my $marcRecord = @_[0];
	my $marc = populate_marc($marcRecord);	
	my %marc = %{normalize_marc($marc)};    
	my %fingerprints;
	
    $fingerprints{baseline} = join("\t", 
	  $marc{item_form}, $marc{date1}, $marc{record_type},
	  $marc{bib_lvl}, $marc{title}, $marc{subtitle}.$marc{subtitlep}, $marc{author} ? $marc{author} : '',
	  $marc{audioformat}, $marc{videoformat}
	  );
	 $fingerprints{alternate} = join("\t", 
	  $marc{item_form}, $marc{date1}, $marc{record_type},
	  $marc{bib_lvl}, $marc{title}, $marc{subtitle}.$marc{subtitlep}, $marc{author} ? $marc{author} : '',
	  $marc{audioformat}, $marc{videoformat}, $marc{pubyear}, $marc{normalizedisbns}
	  );
	$fingerprints{item_form} = $marc{item_form};
	$fingerprints{date1} = $marc{date1};
	$fingerprints{record_type} = $marc{record_type};
	$fingerprints{bib_lvl} = $marc{bib_lvl};
	$fingerprints{title} = $marc{title};
	$fingerprints{author} = $marc{author};
	$fingerprints{audioformat} = $marc{audioformat};
	$fingerprints{videoformat} = $marc{videoformat};
	$fingerprints{tag902} = $marc{tag902};
	#print Dumper(%fingerprints);
	return \%fingerprints;
}

#This is borrowed from fingerprinter and altered a bit
sub populate_marc {
    my $record = @_[0];
    my %marc = (); $marc{isbns} = [];

    # record_type, bib_lvl
    $marc{record_type} = substr($record->leader, 6, 1);
    $marc{bib_lvl}     = substr($record->leader, 7, 1);

    # date1, date2
    my $my_008 = $record->field('008');
	my @my_007 = $record->field('007');
	my @my_902 = $record->field('903');
	my $my_006 = $record->field('006');
    $marc{tag008} = $my_008->as_string() if ($my_008);
    if (defined $marc{tag008}) {
        unless (length $marc{tag008} == 40) {
            $marc{tag008} = $marc{tag008} . ('|' x (40 - length($marc{tag008})));
#            print XF ">> Short 008 padded to ",length($marc{tag008})," at rec $count\n";
        }
        $marc{date1} = substr($marc{tag008},7,4) if ($marc{tag008});
        $marc{date2} = substr($marc{tag008},11,4) if ($marc{tag008}); # UNUSED
    }
	my $my_260 = $record->field('260');
	$marc{pubyear} = '';
	if ($my_260 and $my_260->subfield('c')) {
		my $date1 = $my_260->subfield('c');
		$date1 =~ s/\D//g;
		if (defined $date1 and $date1 =~ /\d{4}/) {
			unless ($marc{date1} and $marc{date1} =~ /\d{4}/) {
				$marc{date1} = $date1;
				$marc{fudgedate} = 1;
 #               print XF ">> using 260c as date1 at rec $count\n";
			}
			$marc{pubyear} = $date1;
		}
	}

	$marc{tag006} = $my_006->as_string() if ($my_006);
	$marc{tag007} = \@my_007 if (@my_007);
	$marc{audioformat}='';
	$marc{videoformat}='';
	foreach(@my_007)
	{
		if(substr($_->data(),0,1) eq 's' && $marc{audioformat} eq '')
		{
			$marc{audioformat} = substr($_->data(),3,1) unless (length $_->data() < 4);
		}
		elsif(substr($_->data(),0,1) eq 'v' && $marc{videoformat} eq '')
		{
			$marc{videoformat} = substr($_->data(),4,1) unless (length $_->data() < 5);
		}
	}
	$marc{tag902} = '';
	foreach(@my_902)
	{
		my @subfields = $_->subfield('a');
		foreach(@subfields)
		{
			$marc{tag902} .= $_.' ';
		}
	}
	$marc{tag902} = substr($marc{tag902},0,-1);
	#print "$marc{audioformat}\n";
	#print "$marc{videoformat}\n";
	
    # item_form
    if ( $marc{record_type} =~ /[gkroef]/ ) { # MAP, VIS
        $marc{item_form} = substr($marc{tag008},29,1) if ($marc{tag008} && (length $marc{tag008} > 29 ));
    } else {
        $marc{item_form} = substr($marc{tag008},23,1) if ($marc{tag008} && (length $marc{tag008} > 23 ));
    }	
	#fall through to 006 if 008 doesn't have info for item form
	if ($marc{item_form} eq '|')
	{
		$marc{item_form} = substr($marc{tag006},6,1) if ($marc{tag006} && (length $marc{tag006} > 6 ));
	}	

    # isbns
    # my @isbns = $record->field('020') if $record->field('020');
    # push @isbns, $record->field('024') if $record->field('024');
    # for my $f ( @isbns ) {
        # push @{ $marc{isbns} }, $1 if ( defined $f->subfield('a') and
                                        # $f->subfield('a')=~/(\S+)/ );
    # }

    # author
    for my $rec_field (100, 110, 111) {
        if ($record->field($rec_field)) {
            $marc{author} = $record->field($rec_field)->subfield('a');
            last;
        }
    }

    # oclc
    $marc{oclc} = [];
    push @{ $marc{oclc} }, $record->field('001')->as_string()
      if ($record->field('001') and $record->field('003') and
          $record->field('003')->as_string() =~ /OCo{0,1}LC/);
    for ($record->field('035')) {
        my $oclc = $_->subfield('a');
        push @{ $marc{oclc} }, $oclc
          if (defined $oclc and $oclc =~ /\(OCoLC\)/ and $oclc =~/([0-9]+)/);
    }

    if ($record->field('999')) {
        my $koha_bib_id = $record->field('999')->subfield('c');
        $marc{koha_bib_id} = $koha_bib_id if defined $koha_bib_id and $koha_bib_id =~ /^\d+$/;
    }

    # "Accompanying material" and check for "copy" (300)
    if ($record->field('300')) {
        $marc{accomp} = $record->field('300')->subfield('e');
        $marc{tag300a} = $record->field('300')->subfield('a');
    }

    # issn, lccn, title, desc, pages, pub, pubyear, edition
    $marc{lccn} = $record->field('010')->subfield('a') if $record->field('010');
    $marc{issn} = $record->field('022')->subfield('a') if $record->field('022');
    $marc{desc} = $record->field('300')->subfield('a') if $record->field('300');
    $marc{pages} = $1 if (defined $marc{desc} and $marc{desc} =~ /(\d+)/);
	
    $marc{title} = $record->field('245')->subfield('a')
      if $record->field('245');
	  
	$marc{subtitle} = $record->field('245')->subfield('b')
      if $record->field('245');
	  
	$marc{subtitlep} = $record->field('245')->subfield('p')
      if $record->field('245');
	  
    $marc{edition} = $record->field('250')->subfield('a')
      if $record->field('250');
    if ($record->field('260')) {
        $marc{publisher} = $record->field('260')->subfield('b');
        # $marc{pubyear} = $record->field('260')->subfield('c');
        # $marc{pubyear} =
          # (defined $marc{pubyear} and $marc{pubyear} =~ /(\d{4})/) ? $1 : '';
    }
	
	$marc{normalizedisbns}='';
	my @isbns = $record->subfield('020','a');
	#$log->addLine(Dumper(\@isbns));
	my %finalISBNs = ();
	my $l = 0;
	foreach(@isbns)
	{
		my $thisone = $_;
		$thisone =~ s/\D*([\d]+).*/$1/;
		#remove the last digit
		$thisone = substr($thisone,0,-1);
		if(length($thisone)>10 && substr($thisone,0,3) eq '978')
		{
			#remove the beginning 978
			$thisone = substr($thisone,3);
		}
		@isbns[$l] = $thisone;
		$l++;
	}
	@isbns = sort(@isbns);
	foreach(@isbns)
	{
		if(! $finalISBNs{$_})
		{
			$finalISBNs{$_}=1;
		}
	}
	while ((my $internal, my $mvalue ) = each(%finalISBNs))
	{
		$marc{normalizedisbns}.=$internal.' ';
	}
	$marc{normalizedisbns} = substr($marc{normalizedisbns},0,-1);
	#$log->addLine(Dumper($marc{normalizedisbns}));
	#$log->addLine(Dumper(\%marc));
    return \%marc;
}

sub normalize_marc {
    my ($marc) = @_;

    $marc->{record_type }= 'a' if ($marc->{record_type} eq ' ');
    if ($marc->{title}) {
        $marc->{title} = NFD($marc->{title});
        $marc->{title} =~ s/[\x{80}-\x{ffff}]//go;
        $marc->{title} = lc($marc->{title});
        $marc->{title} =~ s/\W+$//go;
		$marc->{title} =~ s/\s//go;
		$marc->{title} =~ s/\t//go;
    }
	if ($marc->{subtitle}) {
        $marc->{subtitle} = NFD($marc->{subtitle});
        $marc->{subtitle} =~ s/[\x{80}-\x{ffff}]//go;
        $marc->{subtitle} = lc($marc->{subtitle});
        $marc->{subtitle} =~ s/\W+$//go;
		$marc->{subtitle} =~ s/\s//go;
		$marc->{subtitle} =~ s/\t//go;
    }
	if ($marc->{subtitlep}) {
        $marc->{subtitlep} = NFD($marc->{subtitlep});
        $marc->{subtitlep} =~ s/[\x{80}-\x{ffff}]//go;
        $marc->{subtitlep} = lc($marc->{subtitlep});
        $marc->{subtitlep} =~ s/\W+$//go;
		$marc->{subtitlep} =~ s/\s//go;
		$marc->{subtitlep} =~ s/\t//go;
    }
    if ($marc->{author}) {
        $marc->{author} = NFD($marc->{author});
        $marc->{author} =~ s/[\x{80}-\x{ffff}]//go;
        $marc->{author} = lc($marc->{author});
        $marc->{author} =~ s/\W+$//go;
        if ($marc->{author} =~ /^(\w+)/) {
            $marc->{author} = $1;
        }
    }
    if ($marc->{publisher}) {
        $marc->{publisher} = NFD($marc->{publisher});
        $marc->{publisher} =~ s/[\x{80}-\x{ffff}]//go;
        $marc->{publisher} = lc($marc->{publisher});
        $marc->{publisher} =~ s/\W+$//go;
        if ($marc->{publisher} =~ /^(\w+)/) {
            $marc->{publisher} = $1;
        }
    }
    return $marc;
}

sub marc_isvalid {
    my ($marc) = @_;
    return 1 if ($marc->{item_form} and ($marc->{date1} =~ /\d{4}/) and
                 $marc->{record_type} and $marc->{bib_lvl} and $marc->{title});
    return 0;
}

sub setupSchema
{
	my $query = "DROP SCHEMA seekdestroy CASCADE";
	#$dbHandler->update($query);
	my $query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'seekdestroy'";
	my @results = @{$dbHandler->query($query)};
	if($#results==-1)
	{
		$query = "CREATE SCHEMA seekdestroy";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.job
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
		$query = "CREATE TABLE seekdestroy.bib_score(
		id serial,
		record bigint,
		score bigint,
		improved_score_amount bigint default 0,
		score_time timestamp default now(), 		
		electronic bigint,
		audiobook_score bigint,
		largeprint_score bigint,
		video_score bigint,		
		microfilm_score bigint,
		microfiche_score bigint,
		music_score bigint,
		playaway_score bigint,
		winning_score text,
		winning_score_score bigint,
		winning_score_distance bigint,
		second_place_score text,
		item_form text,
		date1 text,
		record_type text,
		bib_lvl text,
		title text,
		author text,
		sd_fingerprint text,
		audioformat text,
		videoformat text,
		circ_mods text DEFAULT ''::text,
		call_labels text DEFAULT ''::text,
		copy_locations text DEFAULT ''::text,
		opac_icon text DEFAULT ''::text,
		eg_fingerprint text,
		sd_alt_fingerprint text,
		tag902 text
		)";		
		$dbHandler->update($query);		
		$query = "CREATE TABLE seekdestroy.bib_merge(
		id serial,
		leadbib bigint,
		subbib bigint,
		change_time timestamp default now(),
		job  bigint NOT NULL,
		CONSTRAINT bib_merge_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.undedupe(
		id serial,
		oldleadbib bigint,
		undeletedbib bigint,
		undeletedbib_electronic_score bigint,
		undeletedbib_marc_score bigint,
		moved_call_number bigint,
		change_time timestamp default now(),
		job  bigint NOT NULL,
		CONSTRAINT undedupe_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.bib_match(
		id serial,
		bib1 bigint,
		bib2 bigint,
		match_reason text,
		merged boolean default false,
		has_holds boolean default false,
		job  bigint NOT NULL,
		CONSTRAINT bib_match_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.bib_item_circ_mods(
		id serial,
		record bigint,
		circ_modifier text,
		job  bigint NOT NULL,
		CONSTRAINT bib_item_circ_mods_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
        $dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.bib_item_call_labels(
		id serial,
		record bigint,
		call_label text,
		different_call_labels bigint,
		job  bigint NOT NULL,
		CONSTRAINT bib_item_call_labels_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
        $dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.bib_item_locations(
		id serial,
		record bigint,
		location text,
		different_locations bigint,
		job  bigint NOT NULL,
		CONSTRAINT bib_item_locations_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.problem_bibs(
		id serial,
		record bigint,
		problem text,
		extra text,
		job  bigint NOT NULL,
		CONSTRAINT problem_bibs_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.call_number_move(
		id serial,
		call_number bigint,
		frombib bigint,
		tobib bigint,
		extra text,
		success boolean default true,
		change_time timestamp default now(),
		job  bigint NOT NULL,
		CONSTRAINT call_number_move_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.copy_move(
		id serial,
		copy bigint,
		fromcall bigint,
		tocall bigint,
		extra text,
		change_time timestamp default now(),
		job  bigint NOT NULL,
		CONSTRAINT copy_move_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.bib_marc_update(
		id serial,
		record bigint,
		prev_marc text,
		changed_marc text,
		new_record boolean NOT NULL DEFAULT false,
		change_time timestamp default now(),
		extra text,
		job  bigint NOT NULL,
		CONSTRAINT bib_marc_update_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
        $dbHandler->update($query);
		$query = "CREATE TABLE seekdestroy.metarecord_change(
		id serial,
		metarecord bigint,
		fingerprint text,
		newmetarecord bigint,
		bibid bigint,
		holdid bigint,
		change_time timestamp default now(),
		extra text,
		job  bigint NOT NULL,
		CONSTRAINT metarecord_change_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		
		$query = "CREATE TABLE seekdestroy.before_dedupe_hold_map_count(
		id serial,
		hold bigint,
		count bigint,
		job bigint NOT NULL,
		CONSTRAINT before_dedupe_hold_map_count_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);

		$query = "CREATE TABLE seekdestroy.before_dedupe_hold_current_copy_null(
		id serial,
		hold bigint,
		job bigint NOT NULL,
		CONSTRAINT before_dedupe_hold_current_copy_null_fkey FOREIGN KEY (job)
		REFERENCES seekdestroy.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);

		$query = "CREATE TABLE seekdestroy.tattle_report(
		id serial,
		name TEXT NOT NULL,
        query TEXT NOT NULL,
        CONSTRAINT seekdestroy_report_pkey PRIMARY KEY (id)
        )
        ";        
		$dbHandler->update($query);

		$query = "CREATE TABLE seekdestroy.ignore_list(
        id serial,
		report bigint NOT NULL,
        target_copy bigint NOT NULL,
        CONSTRAINT seekdestroy_ignore_list_report_fkey FOREIGN KEY (report)
        REFERENCES seekdestroy.tattle_report (id) MATCH SIMPLE)
        ";
		$dbHandler->update($query);

        $query = "CREATE INDEX seekdestroy_bib_score_record_idx
        ON seekdestroy.bib_score
        USING btree (record)";
        $dbHandler->update($query);

        $query = "CREATE INDEX seekdestroy_bib_item_circ_mods_idx
        ON seekdestroy.bib_item_circ_mods
        USING btree (record)";
        $dbHandler->update($query);

        $query = "CREATE INDEX seekdestroy_bib_item_call_labels_idx
        ON seekdestroy.bib_item_call_labels
        USING btree (record)";
        $dbHandler->update($query);

        $query = "CREATE INDEX seekdestroy_bib_item_locations_idx
        ON seekdestroy.bib_item_locations
        USING btree (record)";
        $dbHandler->update($query);

        $query = "CREATE INDEX seekdestroy_bib_match_bib1_idx
        ON seekdestroy.bib_match
        USING btree (bib1)";
        $dbHandler->update($query);

        $query = "CREATE INDEX seekdestroy_bib_match_bib2_idx
        ON seekdestroy.bib_match
        USING btree (bib2)";
        $dbHandler->update($query);

        ## Seed the report query DB table from static query file. Humans can tweak the table later
        resyncTattleReportQueriesFromFile();
	}
    elsif($resyncTattle)
    {
        resyncTattleReportQueriesFromFile();
    }
}

sub resyncTattleReportQueriesFromFile
{
    my %seedReportQueries = (
    'questionable_large_print' => 'Large Print item to bib mis-match',
    'questionable_video_bib_to_item' => 'DVD/Blu-ray/VHS item to bib mis-match',
    'questionable_music_bib_to_item' => 'Music item to bib mis-match',
    'questionable_audiobook_bib_to_item' => 'Audiobook item to bib mis-match',
    'items_attached_to_deleted_bibs' => 'Items attched to deleted bibs',
    'electronic_book_with_physical_items_attached_for_report' => 'Items attched to Electronic bibs'
    );
    my $needInsert = 0;
    my $updateQ = "UPDATE seekdestroy.tattle_report SET query = ";
    my $insertQ = "INSERT INTO seekdestroy.tattle_report(name,query)
        VALUES
        ";
    while ((my $queryname, my $humanname) = each(%seedReportQueries))
    {
        my $query = "SELECT id FROM seekdestroy.tattle_report WHERE name = \$namedquery\$".$humanname."\$namedquery\$";
        my @results = @{$dbHandler->query($query)};
        if($#results > -1)
        {
            my @row = @{@results[0]};
            my $id = @row[0];
            my $thisUpdate = $updateQ ."\$queryqueryquery\$" . $queries{$queryname} . "\$queryqueryquery\$ where id = $id";
            updateJob("Processing",$thisUpdate);
            $dbHandler->update($thisUpdate);
            undef $thisUpdate;
            undef @row;
            undef $id;
        }
        else
        {
            $insertQ .= "(\$humanname\$$humanname\$humanname\$,\$queryqueryquery\$".$queries{$queryname}."\$queryqueryquery\$),\n";
            $needInsert = 1;
        }
        undef @results;
    }
    # Remove trailing comma
    if($needInsert)
    {
        $insertQ = substr($insertQ,0,-2);
        updateJob("Processing",$insertQ);
        $dbHandler->update($insertQ);
    }
}

sub seedTattleSystemIndex
{
    my $folderPath = shift;
    make_path($folderPath, {mode => 7644, }) if !(-d $folderPath);
    $folderPath .= "/index.html";
    my $ret = new Loghandler($folderPath);
    if (!(-e $folderPath))
    {
        if(-e $conf{"reportHTMLSeed"})
        {
            my $indexFile = new Loghandler($conf{"reportHTMLSeed"} );
            $indexFile->copyFile($folderPath);
        }
        else
        {
            $ret->truncFile('<html>
<head>
<meta content="text/html;charset=utf-8" http-equiv="Content-Type">
<meta content="utf-8" http-equiv="encoding">
<title>Library catalog format cleanup</title>
<script
			  src="https://code.jquery.com/jquery-3.5.1.min.js"
			  integrity="sha256-9/aliU8dGd2tb6OSsuzixeV4y/faTqgFtohetphbbj0="
			  crossorigin="anonymous"></script>
<script
			  src="https://code.jquery.com/ui/1.12.1/jquery-ui.min.js"
			  integrity="sha256-VazP97ZCwtekAsvgPBSUwPFKdrwD3unUfSGVYrahUqU="
			  crossorigin="anonymous"></script>

<link rel="stylesheet" type="text/css" href="../../jquery-ui.theme.min.css">
<link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/v/ju/dt-1.10.23/datatables.min.css"/>
 
<script type="text/javascript" src="https://cdn.datatables.net/v/ju/dt-1.10.23/datatables.min.js"></script>

            <style>
            .hide {
                display: none !important;
            }
            .ignorespansucess {
                color: green;
            }
            .ignorespanfail {
                color: red;
            }
            .dataTables_wrapper > div.fg-toolbar {
                padding-bottom: 49px !important;
            }
            .datatable-outter {
                width: 90%;
                margin: auto;
            }
            .report-title {
                width: 50%;
                margin: auto;
                padding: 1em;
                font-size: 30pt;
                text-align: center;
            }
             .loader {
              border: 2px solid #f3f3f3; /* Light grey */
              border-top: 2px solid #3498db; /* Blue */
              border-radius: 50%;
              width: 20px;
              height: 20px;
              animation: spin 2s linear infinite;
              display: block;
            }
            @keyframes spin {
              0% { transform: rotate(0deg); }
              100% { transform: rotate(360deg); }
            }
            a {
                text-decoration: none;
                color: #046DC1;
            }
            </style>
            </head>
            <body>');
        }
    }
    return $ret;
}

sub updateJob
{
	my $status = @_[0];
	my $action = @_[1];
	$log->addLine($action);
	my $query = "UPDATE seekdestroy.job SET last_update_time=now(),status='$status', CURRENT_ACTION_NUM = CURRENT_ACTION_NUM+1,current_action='$action' where id=$jobid";
	my $results = $dbHandler->update($query);
	return $results;
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

 
 