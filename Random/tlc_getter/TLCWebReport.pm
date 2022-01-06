#!/usr/bin/perl

package TLCWebReport;

use pQuery;
use Try::Tiny;
use Data::Dumper;

use Mobiusutil;


use parent TLCWebController;

our $attemptMax = 5;
our %selectAnswers;

our @usedBranches = ();
our $branchable = 0;
our %attempts = ();
our %filesOnDisk = ();
our $mobUtil = new Mobiusutil();

sub scrape
{
    my ($self) = shift;
    @usedBranches = ();
    $branchable = 0;
    my $exec = '%selectAnswers = (' . $self->{selectAnswers}. ');';
    eval($exec);
    my $reportsPage = $self->SUPER::getToReportSelectionPage();
    my $error = 0;
    if($reportsPage)
    {
        goReport($self, 1);
        while($branchable && getNextBranch($self,1))
        {
            print $mobUtil->boxText("Looping branches: ".getNextBranch($self,1),"#","|",3);
            sleep 1;
            if($self->getToReportSelectionPage())
            {
                goReport($self, 0);
            }
            else
            {
               print "I'm in the scrape outter loop, and I couldn't get to the main report page\n";
               $branchable = 0;
               $error = 1;
            }
        }
    }
    else
    {
        $error = 1;
    }
    if($error)
    {
        $self->takeScreenShot('failed_to_get_to_report_page');
        $self->giveUp("Died on '".$self->{name}."' on branch '".getNextBranch($self,1)."'");
    }
    # while(1)
    # {
        # readSaveFolder($self,1);
        # $self->takeScreenShot('clicked_download');
        # my $newFile = 0;
        # while(!$newFile)
        # {
            # $newFile = seeIfNewFile($self);
            # print Dumper(\%filesOnDisk);
            # sleep 1;
        # }
        # processDownloadedFile($self, $newFile, $firstTime);
    # }
    # my @files = @{readSaveFolder($self)};
    # foreach(@files)
    # {
        # processDownloadedFile( $self, $self->{saveFolder} . "/" . $_, 1) if $_ =~ /xls/;
    # }
}

sub goReport
{
    my ($self) = shift;
    my $firstTime = shift;
    resetSelectAccounting($self);
    runReport($self);
    readSaveFolder($self,1);
    clickDownloadReportCSV($self);
    my $newFile = 0;
    while(!$newFile)
    {
        $newFile = seeIfNewFile($self);
        # print Dumper(\%filesOnDisk);
        sleep 1;
    }
    processDownloadedFile($self, $newFile, $firstTime);
}

sub runReport
{
    my ($self) = shift;
    $self->takeScreenShot('filling_selects');
    fillAllOptions($self);
    $self->takeScreenShot('filled_everything');
    clickFinish($self);
    my $running = isReportRunning($self);
    my $isDone = seeIfReportIsDone($self);
    if(!$running && !$isDone)
    {
        $self->takeScreenShot('report_failed_to_start');
        $self->giveUp("Failed to get the report started\nSee screenshot for details");
    }
    my $waiting = 0;
    while(!$isDone)
    {
        $running = isReportRunning($self);
        $isDone = seeIfReportIsDone($self);
        # print "Running: $running  , isDone: $isDone\n";
        print "Waiting for '". $self->{name}."' to finish running\n" if $waiting == 0;
        if($waiting % 10 == 0)
        {
            print "\n";
            $self->takeScreenShot('report_running');
        }
        else
        {
            print ".";
            STDOUT->flush();
        }
        $waiting++;
        sleep 1;
    }
    $self->takeScreenShot('report_done');
}

sub fillAllOptions
{
    my ($self) = shift;
    my $keepGoing = 1;
    my $totalPopulateButtons = 0;
    my $totalSingles = 0;
    my $totalSingleChanged = 0;
    my @singleResults;  #holds single dropdown menu results
    my @multiResults;   #holds multi select box results
    my $doneMultis = 0;
    my $multisAttempts = 0;
    my $loops = 0;
    while($keepGoing)
    {
        my $somethingChanged = 0;
        @singleResults = @{fillSelects($self)};
        $totalSingles = @singleResults[0] if(!$totalSingles);
        $totalSingleChanged += @singleResults[1];
        if(!$loops)  ## First time through
        {
            @multiResults = @{selectAlls($self)};
            $totalPopulateButtons = clickPopulateButtons($self);
            $doneMultis = 1 if(!@multiResults[0]);
        }
        else #Not the first time
        {
            if($totalSingles > $totalSingleChanged)
            {
                # print "Filling Singles\n";
                @singleResults = @{fillSelects($self)};
                $totalSingles = @singleResults[0] if(!$totalSingles);
                $totalSingleChanged += @singleResults[1];
                $somethingChanged = 1 if @singleResults[1];
            }
            if( @multiResults[0] && @multiResults[1] )
            {
                @multiResults = @{selectAlls($self)};
                $somethingChanged = 1 if !@multiResults[1];
                $multisAttempts++;
                $multisAttempts = 0 if $somethingChanged;
                if($multisAttempts > $attemptMax)
                {
                    if(containsUnfillableMulti($self) ) #handles the case when there is a multi select box that is populated by manually enterying ID numbers
                    {
                        $doneMultis = 1;
                    }
                    else
                    {
                        print $mobUtil->boxText("We've not been able to fill out the multi select box for $attemptMax times, moving to next branch","#","|",2);
                        $self->takeScreenShot('incomplete');
                        resetSelectAccounting($self);
                        $multisAttempts = 0;
                        $doneMultis = 0;
                        $totalSingleChanged = 0;
                        $totalPopulateButtons = 0;
                        $totalSingles = 0;
                        $loops = -1;
                        sleep 5;
                    }
                }
            }
            elsif(@multiResults[0] && @multiResults[1])
            {
                $doneMultis = 0;
            }
            elsif(!$doneMultis)
            {
                $doneMultis = 1;
            }
            clickPopulateButtons($self) if($somethingChanged && $totalPopulateButtons);
        }
        $loops++;
        print "
        Total Populate Buttons:       $totalPopulateButtons
        Total Dropdowns:              $totalSingles
        Total Dropdowns Changed:      $totalSingleChanged
        Total Multi Selects:          ".@multiResults[0]."
        Total Multi Selects no opts:  ".@multiResults[1]."
        Multi Attempts:               $multisAttempts
        Multi Attempts Max:           $attemptMax
        \n";
        sleep 1;

        if( ($totalSingles > $totalSingleChanged) || !$doneMultis)
        {
            print "Still clicking report options\n";
            $self->takeScreenShot('filling_selects');
        }
        else
        {
            print $mobUtil->boxText("Report options are now satisfied! Moving to run report","#","|",2);
            $keepGoing = 0;
            fillDates($self);
            $self->takeScreenShot('filled_dates');
        }
    }
}

sub fillDates
{
    my ($self) = shift;
    my $script = 
    "
    var first = 1;
    var doms = document.querySelectorAll('input');
    for(var i=0;i<doms.length;i++)
    {   
        var thisID = doms[i].id;
        var alabel = doms[i].getAttribute('aria-label');
        if(alabel && alabel.match(/year/gi) && thisID.match(/year/gi))
        {
            if(first)
            {
                doms[i].value='1000-01-01';
                first = 0;
                var evt = new CustomEvent('change');
                doms[i].dispatchEvent(evt);
                evt = new CustomEvent('click');
                doms[i].dispatchEvent(evt);
            }
            else
            {
                doms[i].value='4000-01-01';
                var evt = new CustomEvent('change');
                doms[i].dispatchEvent(evt);
                evt = new CustomEvent('click');
                doms[i].dispatchEvent(evt);
            }
        }
    }
    
    first = 1;
    doms = document.querySelectorAll('input');
    for(var i=0;i<doms.length;i++)
    {   
        var thisID = doms[i].id;
        if(doms[i].type.match(/hidden/gi) && thisID.match(/date/gi))
        {
            if(first)
            {
                doms[i].value='1000-01-01';
                first = 0;
                 
            }
            else
            {
                doms[i].value='4000-01-01';
            }
        }
    }
    ";
    $self->doJS($script, 1);
}

sub clickFinish
{
    my ($self) = shift;
    
    my $script = "
    var doms = document.querySelectorAll('select');
    for(var i=0;i<doms.length;i++)
    {   
        var thisID = doms[i].id;
        var multi = doms[i].getAttribute('aria-multiselectable');
        if(multi && multi == 'false')
        {
             var evt = new CustomEvent('change');
             doms[i].dispatchEvent(evt);
             evt = new CustomEvent('click');
             doms[i].dispatchEvent(evt);
        }
    }
    ";
    $self->doJS($script, 1);
    sleep 1;
    $script = 
    "
    var doms = document.querySelectorAll('button');
    for(var i=0;i<doms.length;i++)
    {   
        var thisID = doms[i].id;
        var thisaction = doms[i].getAttribute('onClick');
        if(thisaction && thisID.match(/finish/gi) && thisaction.match(/finish/gi))
        {   
            doms[i].click();
        }
    }
    ";
    $self->doJS($script, 1);
    sleep 1;
}

sub isReportRunning
{
    my ($self) = shift;
    my $script = 
    "
    var doms = document.querySelectorAll('span');
    for(var i=0;i<doms.length;i++)
    {   
        var ttext = doms[i].innerHTML;
        if(ttext && ( ttext.match(/Your report is running/gi)  ||  ttext.match(/working/gi)   ) )
        {
            return 1;
        }
    }
    return 0;
    ";
    my $ret = $self->doJS($script, 1);
    return $ret;
}

sub seeIfReportIsDone
{
    my ($self) = shift;
    my $script = 
    "
    var doms = document.querySelectorAll('button');
    for(var i=0;i<doms.length;i++)
    {   
        var srcattr = doms[i].getAttribute('onclick');
        if(srcattr && srcattr.match(/finish/gi))
        {
            return 0;
        }
    }
    return 1;
    ";
    my $ret = $self->doJS($script, 1);
    return $ret;
}

sub selectAlls
{
    my ($self) = shift;
    my $script = 
    "
    var changed = 0;
    var howMany = 0;
    var noOptions = 0;
    var doms = document.querySelectorAll('select');
    for(var i=0;i<doms.length;i++)
    {   
        var thisID = doms[i].id;
        var multi = doms[i].getAttribute('aria-multiselectable');
        if(multi == 'true')
        {
            var thisChanged = 0;
            var totalAlreadySelected = 0;
            howMany++;
            var loops = 0;
            Array.from(doms[i].options).forEach(function(option_element)
            {
                var is_option_selected = option_element.selected;
                if(!is_option_selected)
                {
                    option_element.selected = true;
                    thisChanged = 1;
                }
                else
                {
                    totalAlreadySelected++;
                }
                loops++;
            });
            if(loops == 0)
            {
                noOptions++;
            }
            if(thisChanged)
            {
                 var evt = new CustomEvent('change');
                 doms[i].dispatchEvent(evt);
            }
        }
    }
    return ''+howMany+','+noOptions;
    ";
    my $selects =  $self->doJS($script, 1);
    my @s = split(/,/, $selects);
    foreach my $i (0 .. $#s)
    {
        @s[$i] += 0;
    }
    return \@s;

}

sub fillSelects
{
    my ($self) = shift;
    my %sels = %{getSingleSelectIDs($self)};
    my $total = 0;
    my $changed = 0;
    # print Dumper(\%sels);
    # print Dumper($selectAnswers{"finished"});
    # sleep 1;
    while ((my $domid, my $val) = each(%sels))
    {
        $total++;
        my $alreadyDone = 0;
        foreach(@{$selectAnswers{"finished"}})
        {
            $alreadyDone = 1 if($_ eq $val);
        }
        if(!$alreadyDone)
        {
            print "Filling '$val'\n";
            my $worked = fillThisSelect($self, $domid, $val);
            $changed++ if($worked);
            sleep 2;
        }
    }
    my @ret = ($total,$changed);
    return \@ret;
}

sub getSingleSelectIDs
{
    my ($self) = shift;
    my $script = 
    "
    var allIDs = '';
    var doms = document.querySelectorAll('select');
    for(var i=0;i<doms.length;i++)
    {   
        var thisID = doms[i].id;
        var multi = doms[i].getAttribute('aria-multiselectable');
        if(multi == 'false')
        {
            var loops = 0;
            Array.from(doms[i].options).forEach(function(option_element)
            {
                if(loops == 0)
                {
                    var option_text = option_element.text;
                    allIDs += thisID + ',' + option_text + ',';
                    var option_value = option_element.value;
                }
                loops++;
            });
        }
    }
    return allIDs.substring(0,allIDs.length - 1);
    ";
    my $selects =  $self->doJS($script, 1);
    my @s = split(/,/, $selects);
    my %sels = ();
    my $thisOne = 0;
    foreach(@s)
    {
        if($thisOne)
        {
            $sels{$thisOne} = $_;
            $thisOne = 0;
        }
        else
        {
            $thisOne = $_;
        }
    }
    return \%sels;
}

sub containsUnfillableMulti
{
    my ($self) = shift;
    my $script = 
    "
    var allIDs = '';
    var doms = document.querySelectorAll('span');
    for(var i=0;i<doms.length;i++)
    {   
        var thisHTML = doms[i].innerHTML;
        if(thisHTML && ( thisHTML.match(/Enter Borrower ID\\(s\\)/gi) ) )
        {
            return 1;
        }
    }
    return 0;
    ";
    my $ret =  $self->doJS($script, 1);
    return $ret;
}

sub fillThisSelect
{
    my ($self) = shift;
    my $domid = shift;
    my $domName = shift;
    my $worked = 0;
    if($selectAnswers{$domName})
    {
        if(substr($selectAnswers{$domName},0,1) ne ':')
        {
            $selectAnswers{$domName} =~ s/\s/\\s/g;
            print "Selecting '$selectAnswers{$domName}' from '$domid'\n";
            $worked = selectsChooseSpecificOption($self, $domid, $selectAnswers{$domName});
            if(!$worked)
            {
                # print "Couldn't select option: '".$selectAnswers{$domName}."' in dropdown '".$domName."'\n";
                $attempts{$domName}++;
            }
        }
        else
        {
            if($selectAnswers{$domName} =~ m/anything/gi)
            {
                $worked = selectsChooseAnyOption($self, $domid);
                if(!$worked)
                {
                    # print "Couldn't select option: '".$selectAnswers{$domName}."' in dropdown '".$domName."'\n";
                    $attempts{$domName}++;
                }
            }
            elsif($selectAnswers{$domName} =~ m/branches/gi)
            {
                $branchable = 1;
                print "Getting Next Branch\n";
                my $branch = getNextBranch($self, 0);
                $worked = selectsChooseSpecificOption($self, $domid, $branch);
                if(!$worked)
                {
                    # print "Couldn't select option: '$branch' in dropdown '".$domName."'\n";
                    $attempts{$domName}++;
                }
            }
        }
        if($worked)
        {
            my @fin = @{$selectAnswers{"finished"}};
            push(@fin, $domName);
            $selectAnswers{"finished"} = \@fin;
        }
        else
        {
            if($attempts{$domName} > $attemptMax)
            {
                $self->giveUp("Exceeded $attemptMax attempts on '$domName'");
            }
        }
    }
    else
    {
        $self->takeScreenShot('failed_selects');
        $self->giveUp("We've encountered a dropdown list that is not defined:\n'$domName', screenshot: failed_selects");
    }
    return $worked;
}

sub selectsChooseSpecificOption
{
    my ($self) = shift;
    my $selectID = shift;
    my $option = shift;
    $option =~ s/ /\\s/g;
    my $script = 
    "
    var doms = document.getElementById('".$selectID."');
    var index = 0;
    var found = -1;
    if(doms)
    {
        Array.from(doms.options).forEach(function(option_element)
        {
                var option_text = option_element.text;
                if(option_text.match(/".$option."/gi))
                {
                    doms.selectedIndex = index;
                    found = index;
                    var evt = new CustomEvent('change');
                    doms.dispatchEvent(evt);
                }
                index++;
        });
    }
    return found;
    ";
    my $found = $self->doJS($script, 1);
    $found += 0;
    return 1 if $found > -1;
    return 0;
}


sub selectsChooseAnyOption
{
    my ($self) = shift;
    my $selectID = shift;
    my $script = 
    "
    var doms = document.getElementById('".$selectID."');
    var index = 0;
    var found = -1;
    if(doms)
    {
        Array.from(doms.options).forEach(function(option_element)
        {
            if( (index > 0 ) && (found == -1) )
            {
                var option_text = option_element.text;
                if(!option_text.match(/\\-\\-\\-\\-\\-\\-/g))
                {
                    doms.selectedIndex = index;
                    found = index;
                }
            }
            index++;
        });
    }
    return found;
    ";
    my $found = $self->doJS($script, 1);
    $found += 0;
    return 1 if $found > -1;
    return 0;
}

sub clickPopulateButtons
{
    my ($self) = shift;
    # print "Clicking Populate Buttons\n";
    my $script = 
    "
    var doms = document.getElementsByTagName('button');
    var found = 0;
    for(var i=0;i<doms.length;i++)
    {
        var thisaction = doms[i].getAttribute('onClick');
        if(thisaction && thisaction.match(/reprompt/gi))
        {
            doms[i].click();
            found++;
        }
    }
    return found;
    ";
    my $found = $self->doJS($script, 1);
    sleep 2 if($found);
    return $found;
}

sub clickDownloadReportCSV
{
    my ($self) = shift;
    my $script = 
    "
    var tab = document.getElementById('_NS_runIn');
    tab.dispatchEvent(new MouseEvent('mouseup', { 'bubbles': true }));

    tab = document.getElementById('_NS_viewInExcel');
    tab.dispatchEvent(new MouseEvent('mouseover', { 'bubbles': true }));

    tab = document.getElementById('_NS_viewInCSV');
    tab.dispatchEvent(new MouseEvent('mouseup', { 'bubbles': true }));
    ";
    $self->doJS($script, 1);
    print "Clicked Download\n";
    my $handles = $self->{driver}->get_window_handles;
    while(!$handles->[1])
    {
        print "Waiting for popup\n";
        $handles = $self->{driver}->get_window_handles;
        print $self->{driver}->get_current_window_handle();
        sleep 1;
    }
    $handles = $self->{driver}->get_window_handles;
    my $waiting = 1;
    print $mobUtil->boxText("Waiting for download to generate","#","|",2);
    while($handles->[1])
    {
        if($waiting % 10 == 0)
        {
            print "\n";
            $self->{driver}->switch_to_window($handles->[1]);
            $self->takeScreenShot('new_window');
            $self->{driver}->switch_to_window($handles->[0]);
            # print Dumper($handles);
        }
        else
        {
            print ".";
            STDOUT->flush();
        }
        sleep 1;
        $handles = $self->{driver}->get_window_handles;
        $waiting++;
    }
    print "\n";
    print $mobUtil->boxText("Download should start","#","|",2);
}

sub processDownloadedFile
{
    my ($self) = shift;
    my $file = shift;
    my $trunc = shift;
    my $saveFolder = $self->{saveFolder};
    if(!$self->{finalFileName})
    {
        my $outFileName = $self->{outFileName} || $self->{name};
        $outFileName =~ s/^\s+//;
        $outFileName =~ s/^\t+//;
        $outFileName =~ s/\s+$//;
        $outFileName =~ s/\t+$//;
        $outFileName =~ s/^_+//;
        $outFileName =~ s/_+$//;
        $outFileName =~ s/\s/_/g;
        $outFileName = "$saveFolder/" . $outFileName . ".migdat" if (!($outFileName =~ m/$saveFolder/));
        $self->{finalFileName} = $outFileName;
        $self->SUPER::setResultFile($outFileName);
    }
    print "Writing to: " .$self->{finalFileName}."\n";
    my $outputFile = new Loghandler($self->{finalFileName});
    $outputFile->deleteFile() if $trunc;
    my $outputText = "";
    print $mobUtil->boxText("Loading '$file'","#","|",1);

    open(my $fh,"<:encoding(UTF-16)",$file) || die "error $!\n";
    while(<$fh>)
    {
        my $line = $_;
        $line =~ s/\n//g;
        $line =~ s/\r//g;
        $outputText .= "$line\n";
    }
    $outputText = substr($outputText,0,-1); # remove the last return
    print $mobUtil->boxText("Cleaning line Spans '$file'","#","|",1);
    $outputText = cleanLineSpans($self, $outputText);
    # print "back from cleaning\n";
    if(!$outputText)
    {
        $self->giveUp("There was a problem parsing and cleaning line spans '".$file."'");
    }
    print $mobUtil->boxText("Removing columns from '$file'","#","|",1);
    $outputText = removeColumns($self, $outputText);
    if(!$outputText)
    {
        $self->giveUp("There was a problem parsing and removing columns from '".$file."'");
    }

    my $lineCount = 0;
    my @lines = split(/\n/, $outputText);
    $outputText = "";
    foreach(@lines)
    {
        $lineCount++;
        if($trunc && $lineCount == 1) # Only write the header on the first file
        {
            $outputText .= "$_\n";
        }
        elsif($lineCount > 1)
        {
            $outputText .= "$_\n";
        }
    }
    $outputText = substr($outputText,0,-1); # remove the last return
    print "Writing $lineCount lines\n";
    $outputFile->addLine($outputText);
}

sub cleanLineSpans
{
    my ($self) = shift;
    my $fileContents = shift;

    my $finalout = '';
    my $errorout = '';
    my $delimiter = figureDelimiter($self,$fileContents);
    my @lines = split(/\n/, $fileContents);
    my $header = @lines[0];
    $header = $mobUtil->trim($header);
    my @headers = split(/$delimiter/,$header);
    my $headerCount = $#headers;
    # print "Header count = $headerCount , delimiter: '$delimiter'\n";
    return $fileContents if($headerCount == 0); # if there is only one column, this code doesn't work
    my $i = 1;
    my $previousLine = "";
    while($i <= $#lines)
    {
        my @thisLine = @{readFullLine($self, \@lines, $i, $delimiter, $headerCount)};
        # print "LINE:\n";
        # print Dumper(\@thisLine);
        $i = @thisLine[1];
        if(@thisLine[0])
        {
            $finalout .= @thisLine[0] . "\n";
        }
        else
        {
            print "Error on line $i\n";
            $self->{log}->addLine($finalout);
            return 0;
        }
        $i++;
        # exit if $i > 6;
    }
    $finalout = "$header\n" . substr($finalout,0,-1);
    # $self->{log}->addLine($finalout);
    # exit;
    return $finalout;
}

sub isLastElementComplete
{
    my ($self) = shift;
    my $line = shift;
    my $delimiter = shift;
    my $ret = "";
    my @info = split(/$delimiter/,$line);
    my $lastElement = pop @info;
    $lastElement =~ s/""//g; # Double quotes are escapes - remove them for this string terminator exercise
    my @lastElementChars = split(//,$lastElement);
    # print "Last element: $lastElement\n";
    # print "Last line first/last Char: '".@lastElementChars[0]."' / '".@lastElementChars[$#lastElementChars]."'\n";
    return 0 if(@lastElementChars[0] && @lastElementChars[0] eq '"' && $#lastElementChars == 0 ); # Only one character long and it's a " mark
    return 0 if(@lastElementChars[0] && @lastElementChars[0] eq '"' && @lastElementChars[$#lastElementChars] ne '"');
    return 1;
}

sub readFullLine
{
    my ($self) = shift;
    my $tlines = shift;
    my $i = shift;
    my $delimiter = shift;
    my $headerCount = shift;
    my @ret;
    my @lines = @{$tlines};
    # print Dumper($tlines);
    my $line = readLineCorrectly($self, @lines[$i], $delimiter);
    my @datas = split(/$delimiter/,$line);
    # print "First read through:\n" . Dumper($line);
    # print "Headercount = $headerCount\nDatacolumns: ".$#datas."\n";
    $i++;
    while( ($headerCount > $#datas) && ($i < $#lines + 1) )
    {
        print "looping through more lines to get more columns\n";
        my $lookingForTerminator = !isLastElementComplete($self, @datas[$#datas], $delimiter);
        print "We are looking for terminator\n" if($lookingForTerminator);
        
        $line = readLineCorrectly($self, @lines[$i], $delimiter, $lookingForTerminator);
        my @tdatas = split(/$delimiter/,$line);
        @datas[$#datas] .= shift @tdatas if($lookingForTerminator);
        foreach(@tdatas)
        {
            push (@datas, $mobUtil->trim($_));
        }
        # print "Final line so far:\n";
        # print Dumper(\@datas);
        # print "Headercount = $headerCount\nDatacolumns: ".$#datas."\n";
        # my $temp = $i + 1;
        # print "Next line: '" . @lines[$temp] . "'\n";
        if( ($i == $#lines) && ($headerCount > $#datas) ) ## Second to last line of the file, TLC likes to trim the last null columns on the last line
        {
            # print "We are on the second to last line of the file\n";
            if(length($mobUtil->trim(@lines[$i])) == 0 ) # pad the last line with blank delimiters until we've reached a complete line
            {
                 while($headerCount > $#datas)
                 {
                    push (@datas,'');
                 }
            }
        }
        $i++;
        undef $lookingForTerminator;
    }
    $i--;
    if ( ($headerCount + 1 == $#datas) && (length($mobUtil->trim(@datas[$#datas])) == 0) ) #handles the case when TLC puts blank columns on the end
    {
        # print "Popping last element from the line\n";
        pop @datas;
    }
    if($headerCount == $#datas)
    {
        my $retString = "";
        foreach(@datas)
        {
            $retString .= $_ . $delimiter;
        }
        $retString = substr($retString, 0, -1);
        # Make sure the last element of the last line is finished
        while(!isLastElementComplete($self,$retString,$delimiter))
        {
            if($#lines > $i)
            {
                $i++;
                my $frag = readLineCorrectly($self, @lines[$i], $delimiter);
                my @tdatas = split(/$delimiter/,$frag);
                foreach(@tdatas)
                {
                    $retString .= " " . $mobUtil->trim($_);
                }
            }
            else #we've encountered the bottom of the file and we still have an unterminiated string. Just end it here and be done
            {
                $retString .= '"';
            }
        }
        @ret = ($retString, $i);
    }
    else # This ended up reading some un-even number of columns
    {
        @ret = (0, $i);
    }
    return \@ret;
}

sub readLineCorrectly
{
    my ($self) = shift;
    my $line = shift;
    my $delimiter = shift;
    my $lookingForTerminator = shift || 0;
    my $ret = "";
    # can't use split because it ignores zero length fields, so we do it by hand
    my @info = @{getDelimitedLine($self,$line,$delimiter,$lookingForTerminator)};

    # print "Starting with looking = $lookingForTerminator\n";
    # print Dumper(\@info);
    foreach(@info)
    {
        # $ret .= ' ' if($lookingForTerminator);
        my $elem = $_;
        $lookingForTerminator = !isLastElementComplete($self, ($lookingForTerminator ? '#' : '' ) . $elem, $delimiter);
        $ret .= $elem;
        if(!$lookingForTerminator)
        {
            # print "Added delimiter after '$elem'\n";
            $ret .= $delimiter;
        }
    }
    $ret = substr($ret,0,-1) if(substr($ret,0,-1) eq $delimiter);
    # my @prin = split(/$delimiter/,$ret);
    # print "looking = $lookingForTerminator\nreturning\n'$ret'\n";
    # print Dumper(\@prin);
    return $ret;
}

sub getDelimitedLine
{
    my ($self) = shift;
    my $line = shift;
    my $delimiter = shift;
    my $middleOfField = shift || 0;
    # my @ret = split(/$delimiter/,$line,-1);  # This doesn't work because these files (sometimes) will put another delimiter at the end of each line
    # print Dumper(\@ret);
    # exit;
    # return \@ret;
    my @info = ();
    my @each = split(//,$line);
    my $ret = "";
    
    foreach(@each)
    {
        if( ($_ eq $delimiter) && !$middleOfField)
        {
            $ret = " " if(length($ret) == 0); #pad space for empty columns so we can use split function later
            # print "Pushing '$ret'\n";
            push(@info, $ret);
            $ret = "";
        }
        else
        {
            if( $_ eq '"' ) # this field is qoute wrapped
            {
                $middleOfField = !$middleOfField;
            }
            if( ($_ eq $delimiter) && $middleOfField)
            {
                $ret .= ''; #don't introduce the delimiter in the middle of the data. Removing those for our purposes.
            }
            else
            {
                $ret .= $_;
            }
        }
    }
    $ret = " " if(length($ret) == 0); #pad space for empty columns so we can use split function later
    push(@info, $ret);
    return \@info;
}

sub removeColumns
{
    my ($self) = shift;
    my $fileContents = shift;
    my $ret = "";
    my $delimiter = figureDelimiter($self,$fileContents);
    my @lines = split(/\n/, $fileContents);
    
    my @colsRemove = ();
    my @header = ();
    my $tabs = 0;
    my $loops = 0;
    my $lineNum = 0;
    foreach(@lines)
    {
        my @theseVals = split(/$delimiter/,$_);
        foreach my $i (0 .. $#theseVals)
        {
            if($lineNum == 0)
            {
                %colsRemove = %{getColRemovalNums($self, \@theseVals)};
            }
            $ret .= @theseVals[$i].$delimiter if(!$colsRemove{$i});
            # print "not adding '".@theseVals[$i]."' because it's in column $i\n" if($colsRemove{$i});
            # exit if(!$colsRemove{$i} && $lineNum > 0);
        }
        $ret = substr($ret,0,-1) . "\n";
        $lineNum++;
    }
    $ret = substr($ret,0,-1);
    return $ret;
}

sub getColRemovalNums
{
    my ($self) = shift;
    my $cols = shift;
    my @cols = @{$cols};
    my %ret = ();
    my @colNamesRemove = split(/,/,$self->{colRemoves});
    foreach my $i (0 .. $#colNamesRemove)
    {
        @colNamesRemove[$i] = $mobUtil->trim(@colNamesRemove[$i]);
    }
    foreach my $i (0 .. $#cols)
    {
        foreach(@colNamesRemove)
        {   
            my $comp = @cols[$i];
            $ret{$i} = 1 if(@cols[$i] =~ m/$_/gi);
        }
    }
    return \%ret;
}

sub seeIfNewFile
{
    my ($self) = shift;
    my @files = @{readSaveFolder($self)};
    foreach(@files)
    {
        if(!$filesOnDisk{$_})
        {
            # print "Detected new file: $_\n";
            checkFileReady($self, $self->{saveFolder} ."/".$_);
            return $self->{saveFolder} . "/" . $_;
        }
    }
    return 0;
}

sub figureDelimiter
{
    my ($self) = shift;
    my $fileContents = shift;
    my @lines = split(/\n/, $fileContents);
    my $commas = 0;
    my $tabs = 0;
    my $loops = 0;
    foreach(@lines)
    {
        my @split = split(/,/,$_);
        $commas+=$#split;
        @split = split(/\t/,$_);
        $tabs+=$#split;
        last if ($loops > 100);
        $loops++;
    }
    my $delimiter = $commas > $tabs ? "," : "\t";
    return $delimiter;
}

sub readSaveFolder
{
    my ($self) = shift;
    my $init = shift || 0;

    %filesOnDisk = () if $init;
    my $pwd = $self->{saveFolder};
    # print "Opening '".$self->{saveFolder}."'\n";
    opendir(DIR,$pwd) or die "Cannot open $pwd\n";
    my @thisdir = readdir(DIR);
    closedir(DIR);
    foreach my $file (@thisdir) 
    {
        # print "Checking: $file\n";
        if( ($file ne ".") && ($file ne "..") && !($file =~ /\.part/g))  # ignore firefox "part files"
        {
            # print "Checking: $file\n";
            if (-f "$pwd/$file")
            {
                @stat = stat "$pwd/$file";
                my $size = $stat[7];
                if($size ne '0')
                {
                    push(@files, "$file");
                    if($init)
                    {
                        $filesOnDisk{$file} = 1;
                    }
                }
            }
        }
    }
    return \@files;
}

sub resetSelectAccounting
{
    my ($self) = shift;
    $selectAnswers{"finished"} = ();
    while ((my $element, my $val) = each(%selectAnswers))
    {
        $attempts{$element} = 0;
    }
}

sub getNextBranch
{
    my ($self) = shift;
    my $justChecking = shift;
    
    my $lastBranch = "";
    my @branches = @{$self->{branches}};
    my @leftover = ();
    foreach(@branches)
    {
        my $thisBranch = $_;
        my $skip = 0;
        foreach(@usedBranches)
        {
            $skip = 1 if($thisBranch eq $_)
        }
        if(!$skip)
        {
            push (@usedBranches, $thisBranch) if !$justChecking;
            return $thisBranch;
        }
    }
    return 0;
}

sub checkFileReady
{
    my ($self) = shift;
    my $file = shift;
    my @stat = stat $file;
    my $baseline = $stat[7];
    $baseline+=0;
    my $now = -1;
    while($now != $baseline)
    {
        @stat = stat $file;
        $now = $stat[7];
        sleep 1;
        @stat = stat $file;
        $baseline = $stat[7];
        $baseline += 0;
        $now += 0;
    }
}


1;