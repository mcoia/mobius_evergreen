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
        print "Died on '".$self->{name}."' on branch '".getNextBranch($self,1)."'\n";
        $self->takeScreenShot('failed_to_get_to_report_page');
        $self->giveUp();
    }
    # while(1)
    # {
        # readSaveFolder($self,1);
        # print "Files before download:\n";
        # print Dumper(\%filesOnDisk);
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
}

sub goReport
{
    my ($self) = shift;
    my $firstTime = shift;
    resetSelectAccounting($self);
    runReport($self);
    readSaveFolder($self,1);
    print "Files before download:\n";
    print Dumper(\%filesOnDisk);
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
    print "Filled all options\n";
    $self->takeScreenShot('filled_everything');
    clickFinish($self);
    my $running = isReportRunning($self);
    my $isDone = seeIfReportIsDone($self);
    if(!$running && !$isDone)
    {
        print "Failed to get the report started\nSee screenshot for details";
        $self->takeScreenShot('report_failed_to_start');
        exit;
    }
    my $waiting = 0;
    while($running || !$isDone)
    {
        $running = isReportRunning($self);
        $isDone = seeIfReportIsDone($self);
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
                print "Filling Singles\n";
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
        ";
        sleep 1;

        if( ($totalSingles > $totalSingleChanged) || !$doneMultis)
        {
            print "\nStill clicking stuff\n";
            $self->takeScreenShot('filling_selects');
        }
        else
        {
            print "\nAll Square! Moving to run report $totalSingles  $totalSingleChanged $doneMultis\n";
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
    $self->doJS($script);
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
    $self->doJS($script);
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
    var doms = document.querySelectorAll('img');
    for(var i=0;i<doms.length;i++)
    {   
        var srcattr = doms[i].getAttribute('src');
        if(srcattr && srcattr.match(/action_view_html/gi))
        {
            return 1;
        }
    }
    return 0;
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
    print "Filling multis\n";
    my $selects =  $self->doJS($script);
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
    print Dumper(\%sels);
    print Dumper($selectAnswers{"finished"});
    sleep 1;
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
                print "Couldn't select option: '".$selectAnswers{$domName}."' in dropdown '".$domName."'\nPlease define it in config\n";
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
                    print "Couldn't select option: '".$selectAnswers{$domName}."' in dropdown '".$domName."'\nPlease define it in config\n";
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
                    print "Couldn't select option: '$branch' in dropdown '".$domName."'\nPlease define it in config\n";
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
                print "Exceeded $attemptMax attempts on '$domName' \nGiving up\n";
                $self->giveUp();
            }
        }
    }
    else
    {
        print "We've encountered a dropdown list that is not defined:\n'$domName'\nReport: '" . $self->{name} . "'\nPlease define it in config\n";
        $self->takeScreenShot('failed_selects');
        exit;
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
    my $found = $self->doJS($script);
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
    my $found = $self->doJS($script);
    $found += 0;
    return 1 if $found > -1;
    return 0;
}

sub clickPopulateButtons
{
    my ($self) = shift;
    print "Clicking Populate Buttons\n";
    my $script = 
    "
    var doms = document.getElementsByTagName('button');
    var found = 0;
    for(var i=0;i<doms.length;i++)
    {
        var thisaction = doms[i].getAttribute('onClick');
        if(thisaction.match(/reprompt/gi))
        {
            doms[i].click();
            found++;
        }
    }
    return found;
    ";
    my $found = $self->doJS($script);
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
    $self->doJS($script);
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
            print Dumper($handles);
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
    my $outFileName = $self->{outFileName} || $self->{name};
    $outFileName =~ s/^\s+//;
    $outFileName =~ s/^\t+//;
    $outFileName =~ s/\s+$//;
    $outFileName =~ s/\t+$//;
    $outFileName =~ s/^_+//;
    $outFileName =~ s/_+$//;
    $outFileName =~ s/\s/_/g;
    $outFileName = "$saveFolder/" . $outFileName . ".migdat" if (!($outFileName =~ m/$saveFolder/));
    $self->{outFileName} = $outFileName;
    print "Writing to: $outFileName\n";
    my $outputFile = new Loghandler($outFileName);
    $outputFile->deleteFile() if $trunc;

    my $lineCount = 0;
    my $outputText = "";
    print "reading $file\n";
    open(my $fh,"<:encoding(UTF-16)",$file) || die "error $!\n";
    while(<$fh>)
    {
        $lineCount++;
        my $line = $_;
        $line =~ s/\n//g;
        $line =~ s/\r//g;
        if($trunc && $lineCount == 1) # Only write the header on the first file
        {
            $outputText .= "$line\n";
        }
        elsif($lineCount > 1)
        {
            $outputText .= "$line\n";
        }
    }
    $outputText = substr($outputText,0,-1); # remove the last return
    print "Writing $lineCount lines\n";
    $outputFile->addLine($outputText);
}

sub seeIfNewFile
{
    my ($self) = shift;
    my @files = @{readSaveFolder($self)};
    foreach(@files)
    {
        if(!$filesOnDisk{$_})
        {
            print "Detected new file: $_\n";
            checkFileReady($self, $self->{saveFolder} ."/".$_);
            return $self->{saveFolder} . "/" . $_;
        }
    }
    return 0;
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
                push(@files, "$file");
                if($init)
                {
                    $filesOnDisk{$file} = 1;
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