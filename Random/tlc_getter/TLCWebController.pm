#!/usr/bin/perl

package TLCWebController;

use pQuery;
use Try::Tiny;
use Data::Dumper;

our $screenShotStep = 0;
our $maxPageLoops = 10;

sub new
{
    my $class = shift;
    my %phandles = ( "Log On to IBM Cognos Software" => "loginPage", "Public Folders" => "reportSearch", "Search" => "clickSearchResult" );
    my $self = 
    {
        name => shift,
        dbHandler => shift,
        driver => shift,
        screenshotDIR => shift,
        log => shift,
        debug => shift,
        webURL => shift,
        webLogin => shift,
        webPass => shift,
        branches => shift,
        selectAnswers => shift,
        saveFolder => shift,
        error => 0,
        pageHandles => \%phandles
        
    };
    if($self->{name} && $self->{dbHandler} && $self->{driver} && $self->{log})
    {
        
    }
    else
    {
        $self->{error} = 1;
    }
    $screenShotStep = 0;
    bless $self, $class;
    return $self;
}

sub getToReportSelectionPage
{
    my ($self) = @_[0];
    goHome($self);
    if(detectPage($self, 0, 1))
    {
        return 1;
    }
    return 0;
}

sub detectPage
{
    my ($self) = @_[0];
    my $loop = @_[1];
    my $loopUntilReportPage = @_[2];
    $loop++;
    my $title = getTitle($self);
    if(isReportPage($self,$title))
    {
        print "We've arrived at the report page\n";
        return 1;
    }
    my $function = "";
    print "title = $title\n";
    while ((my $key, my $val) = each(%{$self->{pageHandles}}))
    {
        my $len = length($key);
        my $cutTitle = $title;
        $cutTitle = substr($title, 0, $len) if (length($title) > $len);
        $cutTitle = lc $cutTitle;
        my $comp = lc $key;
        $function = $val if($cutTitle eq $comp);
    }
    if($function ne "")
    {
        my $ret = 0;
        $function = "\$ret = $function(\$self);";
        print "Executing: $function\n";
        local $@;
        eval ($function);
        # print "ret came back: $ret\n";
        if($loops < $maxPageLoops && $loopUntilReportPage)
        {
            return detectPage($self, $loop, $loopUntilReportPage);
        }
        elsif($loops >= $maxPageLoops)
        {
            return 0;
        }
        else
        {
            return 1;
        }
    }
    else
    {
        print "This page: '$title' is unknown to us. Cannot proceed\n";
    }
    return 0;
}

sub isReportPage
{
    my ($self) = @_[0];
    my $title = @_[1];
    my $len = length($self->{name});
    my $cutTitle = $title;
    $cutTitle = substr($title, 0, $len) if (length($title) > $len);
    $cutTitle = lc $cutTitle;
    my $comp = lc $self->{name};
    return 1 if($cutTitle eq $comp);
    return 0;
}

sub goHome
{
    my ($self) = @_[0];
    $self->{log}->addLine("Getting " . $self->{webURL});
    $self->{driver}->get($self->{webURL});
    sleep 3;
    waitForPageLoad($self);
    $self->takeScreenShot('home');
}

sub getTitle
{
    my ($self) = @_[0];
    my $head = $self->{driver}->execute_script("return document.head.innerHTML");            
    $head =~ s/[\r\n]//g;
    # $self->{log}->addLine($head);
    $head =~ s/.*?<title>([^<]*)<\/title>.*/$1/g;
    return $head;
}

sub loginPage
{
    my ($self) = @_[0];
    print "Handling Login Page\n";

    my $script = 
    "
    var doms = document.getElementById('CAMUsername');
    doms.value = '".$self->{webLogin}."';
    return 1;
    ";
    $self->{driver}->execute_script($script);
    
    $script = 
    "
    var doms = document.getElementById('CAMPassword');
    doms.value = '".$self->{webPass}."';
    ";
    $self->{driver}->execute_script($script);
    $self->takeScreenShot('login');

    $script = 
    "
    var doms = document.getElementById('cmdOK');
    doms.click();
    ";
    $self->{driver}->execute_script($script);
    # print "finished\n";
    sleep 1;
    $self->takeScreenShot('after_login');
    return 1;
}

sub reportSearch
{
    my ($self) = @_[0];
    print "Handling Report Search\n";

    my $script = 
    "
    var doms = document.getElementById('stext');
    doms.value = '".$self->{name}."';
    var evt = new CustomEvent('keypress');
    evt.which = 13;
    evt.keyCode = 13;
    doms.dispatchEvent(evt);
    return 1;
    ";
    $self->{driver}->execute_script($script);
    
    # print "finished\n";
    sleep 1;
    $self->takeScreenShot('after_search');
    return 1;
}

sub clickSearchResult
{
    my ($self) = @_[0];
    print "Handling Report Search\n";

    my $script = 
    "
    var stop = 0;
    var doms = document.querySelectorAll('td.tableText > div > a');
    for(var i=0;i<doms.length;i++)
    {
        if(!stop)
        {
            var thisaction = doms[i].getAttribute('onClick');

            if(thisaction.match(/MainSearchTurnUrlIntoPostSubmission/gi))
            {
                var linkText = doms[i].innerHTML;
                if(linkText.match(/".$self->{name}."/gi))
                {
                    console.log(linkText);
                    doms[i].click();
                    return 1;
                    stop = 1;
                }
            }
        }
    }
    if(!stop)
    {
        return 0;
    }
    return 0;
    ";
    my $answer = $self->{driver}->execute_script($script);
    if($answer)
    {
        sleep 1;
        $self->takeScreenShot('clicked_report_from_results');
        return 1;
    }
    return 0;
}

sub waitForPageLoad
{
    my ($self) = shift;
    my $done = $self->{driver}->execute_script("return document.readyState === 'complete';");
    # print "Page done: $done\n";
    my $stop = 0;
    my $tries = 0;
    
    while(!$done && !$stop)
    {
        $done = $self->{driver}->execute_script("return document.readyState === 'complete';");
        print "Waiting for Page load check: $done\n";
        $tries++;
        $stop = 1 if $tries > 10;
        $tries++;
        sleep 1;
    }
    return $done;
}

sub takeScreenShot
{
    my ($self) = shift;
    my $action = shift;
    $screenShotStep++;
    waitForPageLoad($self);
    # $self->{log}->addLine("screenshot self: ".Dumper($self));
    # print "ScreenShot: ".$self->{screenshotDIR}."/".$self->{name}."_".$screenShotStep."_".$action.".png\n";
    $self->{driver}->capture_screenshot($self->{screenshotDIR}."/".$self->{name}."_".$screenShotStep."_".$action.".png", {'full' => 1});
}

sub generateRandomString
{
    my ($self) = shift;
	my $length = @_[0];
	my $i=0;
	my $ret="";
	my @letters = ('a','b','c','d','e','f','g','h','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z');
	my $letterl = $#letters;
	my @sym = ('@','#','$');
	my $syml = $#sym;
	my @nums = (1,2,3,4,5,6,7,8,9,0);
	my $nums = $#nums;
	my @all = ([@letters],[@sym],[@nums]);
	while($i<$length)
	{
		#print "first rand: ".$#all."\n";
		my $r = int(rand($#all+1));
		#print "Random array: $r\n";
		my @t = @{@all[$r]};
		#print "rand: ".$#t."\n";
		my $int = int(rand($#t + 1));
		#print "Random value: $int = ".@{$all[$r]}[$int]."\n";
		$ret.= @{$all[$r]}[$int];
		$i++;
	}
	
	return $ret;
}

sub trim
{
    my ($self) = shift;
	my $string = shift;
	$string =~ s/^\s+//;
	$string =~ s/\s+$//;
	return $string;
}

sub DESTROY
{
    my ($self) = @_[0];
    ## call destructor
    # undef $self->{postgresConnector};
}


1;