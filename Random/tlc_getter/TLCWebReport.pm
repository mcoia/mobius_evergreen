#!/usr/bin/perl

package TLCWebReport;

use pQuery;
use Try::Tiny;
use Data::Dumper;


use parent TLCWebController;

our %selectAnswers = 
(
    "Location" => ":branches",
    "Active or Owning Holdings Code" => "Active Holdings Code",
    "Titles to Include" => "New and Edited Titles",
    "Include On-The-Fly Items" => "Yes",
    "Sort" => ":anything"

);

our @usedBranches = ();
our $branchable = 0;
our @selectAlled = ();

sub scrape
{
    my ($self) = shift;

    my $reportsPage = $self->SUPER::getToReportSelectionPage();
    if($reportsPage)
    {
        $self->takeScreenShot('filling_selects');
        fillSelects($self);
        print "done\n";
        $self->takeScreenShot('filled_selects');
        print "done with screenshot\n";
        exit;
        
    }
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
                else
                {
                    var is_option_selected = option_element.selected;
                    if(is_option_selected)
                    {
                        allIDs += loops + ',';
                    }
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
    my $thisName;
    foreach(@s)
    {
        if($thisOne && $thisName)
        {
            $sels{$thisOne} = ( "name" => $thisName, "selected" => $_);
            $thisOne = 0;
            $thisName = 0;
        }
        else if($thisOne)
        {
            $thisName = $_;
        }
        else
        {
            $thisOne = $_;
        }
    }
    return \%sels;
}

sub fillSelects
{
    my %sels = %{getSingleSelectIDs($self)};
    while ((my $domid, my $val) = each(%sels))
    {
        fillThisSelect($self, $domid);
    }
    return 1;
}

sub fillThisSelect
{
    my ($self) = shift;
    my $domid = shift;
    if($selectAnswers{$val})
    {
        if(substr($selectAnswers{$val},0,1) ne ':')
        {
            $selectAnswers{$val} =~ s/\s/\\s/g;
            print "Selecting '$selectAnswers{$val}' from '$domid'\n";
            my $worked = selectsChooseSpecificOption($self, $domid, $selectAnswers{$val});
            if(!$worked)
            {
                print "Couldn't select option: '".$selectAnswers{$val}."' in dropdown '".$val."'\nPlease define it in my code TLCWebReport.pm\n";
                $self->takeScreenShot('filled_selects');
                exit;
            }
        }
        else
        {
            if($selectAnswers{$val} =~ m/anything/gi)
            {
                my $worked = selectsChooseAnyOption($self, $domid);
                if(!$worked)
                {
                    print "Couldn't select option: '".$selectAnswers{$val}."' in dropdown '".$val."'\nPlease define it in my code TLCWebReport.pm\n";
                    $self->takeScreenShot('filled_selects');
                    exit;
                }
            }
            else if($selectAnswers{$val} =~ m/branches/gi)
            {
                $branchable = 1;
                my $branch = getNextBranch($self, 0);
                my $worked = selectsChooseSpecificOption($self, $domid, $branch);
                if(!$worked)
                {
                    print "Couldn't select option: '".$selectAnswers{$val}."' in dropdown '".$val."'\nPlease define it in my code TLCWebReport.pm\n";
                    $self->takeScreenShot('filled_selects');
                    exit;
                }
            }
        }
    }
    else
    {
        print "We've encountered a dropdown list that is not defined:\n'$val'\nReport: '" . $self->{name} . "'\nPlease define it in my code TLCWebReport.pm\n";
        $self->takeScreenShot('filled_selects');
        exit;
    }

}

sub selectsGetNonCorrect
{
    
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
                if(option_text.match(/".$option."/g))
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
            if(index > 0)
            {
                var option_text = option_element.text;
                if(!option_text.match(/\-\-\-\-\-\-/g))
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

sub getNextBranch
{
    my ($self) = shift;
    my $justChecking shift;
    
    my $lastBranch = "";
    my @branches = @{$self->{branches}};
    foreach(branches)
    {
        my $thisBranch = $_;
        foreach(@usedBranches)
        {
            if($thisBranch ne $_)
            {
                push @usedBranches($thisBranch) if !justChecking;
                return $thisBranch;
            }
        }
    }
    return 0;
}

sub collectReportData
{
    my ($self) = shift;
    my $dbDate = shift;
    my @frameSearchElements = ('HOME LIBRARY TOTAL CIRCULATION');

    if(handleRecalculate($self))
    {
        if(!$self->switchToFrame(\@frameSearchElements))
        {
            my $randomHash = $self->SUPER::collectReportData($dbDate);

            ## Attempt to correct any unknown branches
            translateShortCodes($self);

            $self->SUPER::normalizeNames();
        }
        else
        {
            return 0;
        }
    }
}

sub handleRecalculate
{
    my ($self) = @_[0];
    print "Clicking Recalculate\n";
    my @frameSearchElements = ('RECAL');
    
    if(!$self->switchToFrame(\@frameSearchElements))
    {   
        my $owning = $self->{driver}->execute_script("
            var doms = document.getElementsByTagName('form');
            for(var i=0;i<doms.length;i++)
            {
                doms[i].submit();
            }
        ");
        sleep 1;
        $self->{driver}->switch_to_frame();
        my $finished = handleReportSelection_processing_waiting($self);
        $self->takeScreenShot('handleRecalculate');
        return $finished;
    }
    else
    {
    print "didn't find RECAL\n";
    $self->takeScreenShot('handleRecalculate_notfound');
        return 0;
    }
}

sub translateShortCodes
{
    my ($self) = shift;

    my $worked = 0;
    if( !$self->{postgresConnector})
    {
        try
        {
            $self->{log}->addLine("Making new DB connection to pg");
            $self->{postgresConnector} = new DBhandler($self->{pgDB},$self->{pgURL},$self->{pgUser},$self->{pgPass},$self->{pgPort},"pg");
            $worked = 1 if($self->{postgresConnector}->getQuote(""));
        }
        catch
        {
            print "ERROR: PG Connection = \n".$@."\n";
            $self->{log}->addLine("ERROR: PG Connection = \n".$@);
            $self->{postgresConnector} = 0;
        };
    }
    else
    {
        $worked = 1 if($self->{postgresConnector}->getQuote(""));
        $self->{log}->addLine("PG Connection = $worked") if $self->{debug};
    }
    if($worked)
    {
        my $randomHash = $self->generateRandomString(12);
        my $query =
        "
        select svb.code_num,svl.code,svbn.name,svbn.branch_id 
        from
        sierra_view.location svl,
        sierra_view.branch svb,
        sierra_view.branch_name svbn
        where
        svbn.branch_id=svb.id and
        svb.code_num=svl.branch_code_num and
        svbn.name is not null and
        btrim(svbn.name) !=''
        ";
        
        $self->{log}->addLine("Connection to PG:\n$query");
        my @results = @{$self->{postgresConnector}->query($query)};

        ## re-using an already existing table to stage our results into.
        $query = "INSERT INTO 
        $self->{prefix}"."_bnl_stage
        (working_hash,owning_lib,borrowing_lib)
        values
        ";
        my @vals = ();
        foreach(@results)
        {
            my @row = @{$_};
            $query .= "( ?, ? , ? ),\n";
            push @vals, ($randomHash, $self->trim(@row[1]), $self->trim(@row[2]));
        }
        $query = substr($query,0,-2);
        $self->doUpdateQuery($query,"translateShortCodes INSERT $self->{prefix}"."_bnl_stage",\@vals);

        # Update the full names from those from postgres but do not get overridden by the branch_shortname_translate table
        $query = "
        UPDATE 
        $self->{prefix}"."_branch branch,
        $self->{prefix}"."_bnl_stage bnl_stage,
        $self->{prefix}"."_cluster cluster
        set
        branch.institution = bnl_stage.borrowing_lib
        WHERE
        cluster.id = branch.cluster and
        bnl_stage.owning_lib = branch.shortname and
        cluster.name = ? and
        branch.shortname not in(select shortname from $self->{prefix}"."_branch_shortname_agency_translate) and
        bnl_stage.working_hash = ?";
        @vals = ($self->{name}, $randomHash);
        $self->doUpdateQuery($query,"translateShortCodes branch.institution = bnl_stage.borrowing_lib UPDATE $self->{prefix}"."_branch",\@vals);
        undef @vals;
    }

    # Update the full names from those that are overridden by the branch_shortname_translate table
    my @vals =  ($self->{name});
    my $query = "
    UPDATE 
    $self->{prefix}"."_branch branch,
    $self->{prefix}"."_branch_shortname_agency_translate shortname_trans,
    $self->{prefix}"."_cluster cluster
    set
    branch.institution = shortname_trans.institution
    WHERE
    cluster.id = branch.cluster and
    cluster.name = ? and
    branch.shortname = shortname_trans.shortname";
    $self->doUpdateQuery($query,"translateShortCodes branch.institution = shortname_trans.institution UPDATE $self->{prefix}"."_branch",\@vals);

}

sub handleReportSelection
{
    my ($self) = @_[0];
    my $selection = @_[1];
    my @frameSearchElements = ('TOTAL circulation', 'Choose a STARTING and ENDING month');
        
    if(!$self->switchToFrame(\@frameSearchElements))
    {   
        my $owning = $self->{driver}->execute_script("
            var doms = document.getElementsByTagName('td');
            var stop = 0;
            for(var i=0;i<doms.length;i++)
            {
                if(!stop)
                {
                    var thisaction = doms[i].innerHTML;

                    if(thisaction.match(/".$selection."/gi))
                    {
                        doms[i].getElementsByTagName('input')[0].click();
                        stop = 1;
                    }
                }
            }
            if(!stop)
            {
                return 'didnt find the button';
            }

            ");
        sleep 1;
        $owning = $self->{driver}->execute_script("
            var doms = document.getElementsByTagName('form');
            for(var i=0;i<doms.length;i++)
            {
                doms[i].submit();
            }
        ");
        sleep 1;
        $self->{driver}->switch_to_frame();
        my $finished = handleReportSelection_processing_waiting($self);
        $self->takeScreenShot('handleReportSelection');
        return $finished;
    }
    else
    {
        return 0;
    }
}

sub handleReportSelection_processing_waiting
{
    my ($self) = @_[0];
    $count = 0;
    my @frameSearchElements = ('This statistical report is calculating');
    while($count < 10)
    {
        my $error = $self->switchToFrame(\@frameSearchElements);
        if(!$error) ## will only exist while server is processing the request
        {
            print "Having to wait for server to process: $count\n";
            my $owning = $self->{driver}->execute_script("
                var doms = document.getElementsByTagName('form');
                for(var i=0;i<doms.length;i++)
                {
                    doms[i].submit();
                }
            ");
            sleep 1;
            $self->{driver}->switch_to_frame();
            $count++;
        }
        else
        {
            return 1;
        }
    }
    return 0;
}

sub handleLandingPage
{
    my ($self) = @_[0];
    my @frameSearchElements = ('Circ Activity', '<b>CIRCULATION<\/b>');
        
    if(!$self->switchToFrame(\@frameSearchElements))
    {

    # JS way
        my $owning = $self->{driver}->execute_script("
            var doms = document.getElementsByTagName('form');
            var stop = 0;
            for(var i=0;i<doms.length;i++)
            {
                if(!stop)
                {
                    var thisaction = doms[i].getAttribute('action');

                    if(thisaction.match(/\\/managerep\\/startviews\\/0\\/d\\/table_1x1/g))
                    {
                        doms[i].submit();
                        stop = 1;
                    }
                }
            }
            ");

        # Selemnium Way
        # my @forms = $self->{driver}->find_elements('//form');
        # foreach(@forms)
        # {
            # $thisForm = $_;
            # if($thisForm->get_attribute("action") =~ /\/managerep\/startviews\/0\/d\/table_1x1/g )
            # {
                # $thisForm->submit();
            # }
        # }
        sleep 2;
        $self->{driver}->switch_to_frame();
        $self->takeScreenShot('handleLandingPage');
        return 1;
    }
    else
    {
        return 0;
    }
}

sub handleCircStatOwningHome
{
    my ($self) = shift;
    my $clickAllActivityFirst = shift|| 0;

    my @frameSearchElements = ('Owning\/Home', 'htcircrep\/owning\/\/o\|\|\|\|\|\/');

    if($clickAllActivityFirst)
    {
        if(!$self->switchToFrame(\@frameSearchElements))
        {   
            my $owning = $self->{driver}->execute_script("
                var doms = document.getElementsByTagName('a');
                var stop = 0;
                for(var i=0;i<doms.length;i++)
                {
                    if(!stop)
                    {
                        var thisaction = doms[i].getAttribute('href');

                        if(thisaction.match(/htcircrep\\/activity\\/\\/a0\\|y1\\|s\\|1\\|\\|/g))
                        {
                            doms[i].click();
                            stop = 1;
                        }
                    }
                }
                if(!stop)
                {
                    return 'didnt find the button';
                }

                ");
                sleep 1;
                $self->{driver}->switch_to_frame();
                $self->takeScreenShot('handleCircStatOwningHome_clickAllActivityFirst');
        }
    }

    if(!$self->switchToFrame(\@frameSearchElements))
    {   
        my $owning = $self->{driver}->execute_script("
            var doms = document.getElementsByTagName('a');
            var stop = 0;
            for(var i=0;i<doms.length;i++)
            {
                if(!stop)
                {
                    var thisaction = doms[i].getAttribute('onClick');

                    if(thisaction.match(/htcircrep\\/owning\\/\\/o\\|\\|\\|\\|\\|\\//g))
                    {
                        doms[i].click();
                        stop = 1;
                    }
                }
            }
            if(!stop)
            {
                return 'didnt find the button';
            }

            ");
        sleep 1;
        $owning = $self->{driver}->execute_script("
            var doms = document.getElementsByTagName('form');
            for(var i=0;i<doms.length;i++)
            {
                doms[i].submit();
            }
        ");
        sleep 1;
        $self->{driver}->switch_to_frame();
        $self->takeScreenShot('handleCircStatOwningHome');
        return 1;
    }
    else
    {
        return 0;
    }
}

sub handleLoginPage
{
    my ($self) = @_[0];
    my $body = $self->{driver}->execute_script("return document.getElementsByTagName('html')[0].innerHTML");
    $body =~ s/[\r\n]//g;
    # $self->{log}->addLine("Body of the HTML: " . Dumper($body));
    if( ($body =~ m/<td>Initials<\/td>/) && ($body =~ m/<td>Password<\/td>/)  )
    {
        my @forms = $self->{driver}->find_elements('//form');
        foreach(@forms)
        {
            $thisForm = $_;
            if($thisForm->get_attribute("action") =~ /\/htcircrep\/\/\-1\/\/VALIDATE/g )
            {
                my $circActivty = $self->{driver}->execute_script("
                var doms = document.getElementsByTagName('input');
                for(var i=0;i<doms.length;i++)
                {
                    var thisaction = doms[i].getAttribute('name');

                    if(thisaction.match(/NAME/g))
                    {
                        doms[i].value = '".$self->{webLogin}."';
                    }
                    if(thisaction.match(/CODE/g))
                    {
                        doms[i].value = '".$self->{webPass}."';
                    }
                }

                ");
                $thisForm->submit();
                sleep 1;
                $self->takeScreenShot('handleLoginPage');
            }
        }
    }
    else
    {
        print "no login page found\n";
        return 0;
    }
    return 1;  # always return true even when it doesn't prompt to login
}



1;