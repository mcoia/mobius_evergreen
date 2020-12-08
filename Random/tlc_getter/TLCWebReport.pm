#!/usr/bin/perl

package TLCWebReport;

use pQuery;
use Try::Tiny;
use Data::Dumper;


use parent TLCWebController;

our $attemptMax = 5;
our %selectAnswers;

our @usedBranches = ();
our $branchable = 0;
our %attempts = ();
our %filesOnDisk = ();

sub scrape
{
    my ($self) = shift;
    
    my $exec = '%selectAnswers = (' . $self->{selectAnswers}. ');';
    eval($exec);
    $selectAnswers{"finished"} = ();
    while ((my $element, my $val) = each(%selectAnswers))
    {
        $attempts{$element} = 0;
    }

    my $reportsPage = $self->SUPER::getToReportSelectionPage();
    if($reportsPage)
    {
        $self->takeScreenShot('filling_selects');
        fillAllOptions($self);
        print "done\n";
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
            print "Waiting for '". $self->{name}."' to finish running";
            if($waiting % 10 == 0)
            {
                print ",taking screenshot\n";
                $self->takeScreenShot('report_running');
            }
            else
            {
                print "\n";
            }
            $waiting++;
            sleep 1;
        }
        $self->takeScreenShot('report_done');
        readSaveFolder($self,1);
        clickDownloadReportCSV($self);
        print "Clicked Download\n";
        my $newFile = 0;
        while(!$newFile)
        {
            $newFile = seeIfNewFile($self);
            print Dumper(\%filesOnDisk);
            sleep 1;
        }
        $self->takeScreenShot('clicked_download');
    }
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
    my $loops = 0;
    while($keepGoing)
    {
        my $somethingChanged = 0;
        @singleResults = @{fillSelects($self)};
        $totalSingles = @singleResults[0] if(!$totalSingles);
        $totalSingleChanged += @singleResults[1];
        if(!$loops)
        {
            @multiResults = @{selectAlls($self)};
            $totalPopulateButtons = clickPopulateButtons($self);
            $doneMultis = 1 if(!@multiResults[0]);
        }
        else #Not the first time
        {
            if($totalSingles > $totalSingleChanged)
            {
                @singleResults = @{fillSelects($self)};
                $totalSingles = @singleResults[0] if(!$totalSingles);
                $totalSingleChanged += @singleResults[1];
                $somethingChanged = 1 if @singleResults[1];
            }
            if(@multiResults[0] && !@multiResults[2] && (@multiResults[1] < @multiResults[0]) )
            {
                @multiResults = @{selectAlls($self)};
                $somethingChanged = 1 if @multiResults[1];
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
        Total Multi Selects Changed:  ".@multiResults[1]."
        Total Multi Selects no opts:  ".@multiResults[2]."
        ";
        sleep 1;
        if( ($totalSingles > $totalSingleChanged) || !$doneMultis)
        {
            print "\nStill clicking stuff\n";
            $self->takeScreenShot('filling_selects');
        }
        else
        {
            print "\nAll Square! Moving to run report \n";
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
    $self->{driver}->execute_script($script);
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
    $self->{driver}->execute_script($script);
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
    $self->{driver}->execute_script($script);
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
    my $ret = $self->{driver}->execute_script($script);
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
    my $ret = $self->{driver}->execute_script($script);
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
            howMany++;
            var loops = 0;
            Array.from(doms[i].options).forEach(function(option_element)
            {
                var is_option_selected = option_element.selected;
                
                if(!is_option_selected)
                {
                    option_element.selected = true;
                    if(!thisChanged)
                    {
                        changed++;
                    }
                    thisChanged = 1;
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
    return ''+howMany+','+changed+','+noOptions;
    ";
    my $selects =  $self->{driver}->execute_script($script);
    my @s = split(/,/, $selects);
    return \@s;

}

sub fillSelects
{
    my ($self) = shift;
    my %sels = %{getSingleSelectIDs($self)};
    my $total = 0;
    my $changed = 0;
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
            print "Filling $domid    '$val'\n";
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
    my $selects =  $self->{driver}->execute_script($script);
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
    my $val = shift;
    my $worked = 0;
    if($selectAnswers{$val})
    {
        if(substr($selectAnswers{$val},0,1) ne ':')
        {
            $selectAnswers{$val} =~ s/\s/\\s/g;
            print "Selecting '$selectAnswers{$val}' from '$domid'\n";
            $worked = selectsChooseSpecificOption($self, $domid, $selectAnswers{$val});
            if(!$worked)
            {
                print "Couldn't select option: '".$selectAnswers{$val}."' in dropdown '".$val."'\nPlease define it in config\n";
                $attempts{$val}++;
            }
        }
        else
        {
            if($selectAnswers{$val} =~ m/anything/gi)
            {
                $worked = selectsChooseAnyOption($self, $domid);
                if(!$worked)
                {
                    print "Couldn't select option: '".$selectAnswers{$val}."' in dropdown '".$val."'\nPlease define it in config\n";
                    $attempts{$val}++;
                }
            }
            elsif($selectAnswers{$val} =~ m/branches/gi)
            {
                $branchable = 1;
                print "Getting Next Branch\n";
                my $branch = getNextBranch($self, 0);
                $worked = selectsChooseSpecificOption($self, $domid, $branch);
                if(!$worked)
                {
                    print "Couldn't select option: '$branch' in dropdown '".$val."'\nPlease define it in config\n";
                    $attempts{$val}++;
                }
            }
        }
        if($worked)
        {
            my @fin = @{$selectAnswers{"finished"}};
            push(@fin, $val);
            $selectAnswers{"finished"} = \@fin;
        }
        else
        {
            if($attempts{$val} > $attemptMax)
            {
                print "Exceeded $attemptMax attempts on '$val' \nGiving up\n";
                exit;
            }
        }
    }
    else
    {
        print "We've encountered a dropdown list that is not defined:\n'$val'\nReport: '" . $self->{name} . "'\nPlease define it in config\n";
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
    print "Executing: 
    $script\n";
    my $found = $self->{driver}->execute_script($script);
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
    print "Executing:
    $script\n";
    my $found = $self->{driver}->execute_script($script);
    $found += 0;
    return 1 if $found > -1;
    return 0;
}

sub clickPopulateButtons
{
    my ($self) = shift;
    my $script = 
    "
    var doms = document.getElementsByTagName('button');
    var found = 0;
    for(var i=0;i<doms.length;i++)
    {
        var thisaction = doms[i].getAttribute('onClick');
        if(thisaction.match(/reprompt/gi))
        {
            doms[i].getElementsByTagName('input')[0].click();
            found++;
        }
    }
    return found;
    ";
    print "Executing:
    $script\n";
    my $found = $self->{driver}->execute_script($script);
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
    print "Executing:
    $script\n";
    $self->{driver}->execute_script($script);
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
    while($handles->[1])
    {
        if($waiting % 10 == 0)
        {
            $self->{driver}->switch_to_window($handles->[1]);
            $self->takeScreenShot('new_window');
            $self->{driver}->switch_to_window($handles->[0]);
        }
        print "Waiting for new window to finish - These are the handles right now:\n";
        print Dumper($self->{driver}->get_window_handles) . "\n";
        sleep 1;
        $handles = $self->{driver}->get_window_handles;
    }
    print "Final Window Listing\n";
    print Dumper($self->{driver}->get_window_handles) . "\n";
    print "Download should start\n";
}

sub seeIfNewFile
{
    my ($self) = shift;
    my @files = @{readSaveFolder};
    foreach(@files)
    {
        if(!$filesOnDisk{$_})
        {
            print "Detected new file: $_\n";
            return $_;
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
    opendir(DIR,$pwd) or die "Cannot open $pwd\n";
    my @thisdir = readdir(DIR);
    closedir(DIR);
    foreach my $file (@thisdir) 
    {
        if(($file ne ".") and ($file ne ".."))
        {
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



1;