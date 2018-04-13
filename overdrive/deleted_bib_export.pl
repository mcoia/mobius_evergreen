#!/usr/bin/perl

# These Perl modules are required:
# install Email::MIME
# install Email::Sender::Simple
# install Digest::SHA1

use lib qw(../);
use File::Path qw(make_path remove_tree);
use strict; 
use Loghandler;
use Mobiusutil;
use DBhandler;
use Data::Dumper;
use DateTime;
use utf8;
use email;
use Encode;
use DateTime::Format::Duration;
use XML::Simple;
 
  my $configFile = @ARGV[0];
 if(!$configFile)
 {
	print "Please specify a config file\n";
	exit;
 }

 
 our $xmlconf = "/openils/conf/opensrf.xml";
 our $dbHandler;
 our $outputFile = new Loghandler(@ARGV[1]);
 our $pgTimeLapse = @ARGV[2];
 
 our $mobUtil = new Mobiusutil(); 
 our $conf = $mobUtil->readConfFile($configFile);
 our %conf;
 if($conf)
 {
	%conf = %{$conf};
    if ($conf{"logfile"})
	{
        my $log = new Loghandler($conf->{"logfile"});
		$log->addLogLine(" ---------------- Deleted bib export Starting ---------------- ");
        my @reqs = ("db","dbuser","dbpass","port","successemaillist","fromemail");
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
            
            my $query = "select id from biblio.record_entry where deleted and edit_date > now()-'$pgTimeLapse'::interval";
            my @results = @{$dbHandler->query($query)};
            my $output ='';
            my $records = 0;
            foreach(@results)
            {
                my $row = $_;
                my @row = @{$row};
                $output.=@row[0]."\n";
                $records++;
            }
            if($records)
            {
                $outputFile->truncFile($output);
                
                my @files = ($outputFile->getFileName());
                $log->addLine(Dumper(\@files));
                
                my @tolist = ($conf{"alwaysemail"});
                if(1)  #switch FTP on and off easily
                {
                    local $@;
                    my $dt   = DateTime->now(time_zone => "local"); 	
                    my $fdate = $dt->ymd();
                    
                    my $remoteDirectory = $conf{"ftpremotedir"} || "/";
                    eval{$mobUtil->sendftp($conf{"ftphost"},$conf{"ftplogin"},$conf{"ftppass"},$remoteDirectory,\@files,$log);};
                    if ($@) 
                    {
                        
                        $log->addLogLine("FTP FAILED");
                        my $email = new email($conf{"fromemail"},\@tolist,1,0,\%conf);
                        $email->send("RMO marcive deleted bib FTP FAIL $fdate","I'm just going to apologize right now, I could not FTP the file to ".$conf{"ftphost"}." ! Remote directory: $remoteDirectory\r\n\r\nYou are going to have to do it by hand. Bummer.\r\n\r\nCheck the log located: ".$conf{"logfile"}." and you will know more about why. Please fix this so that I can FTP the file in the future!\r\n\r\n -MOBIUS Perl Squad-");
                        
                        $valid=0;
                    }
                    if($valid)
                    {
                        my $email = new email($conf{"fromemail"},\@tolist,0,1,\%conf);
                        $email->send("MIEV Delete Bib extract $fdate","This process finished\r\n\r\nHere is some information:\r\n\r\nOutput File: \t\t".$outputFile->getFileName()."\r\n$records Record(s)\r\nFTP location: ".$conf{"ftphost"}."\r\nUserID: ".$conf{"ftplogin"}."\r\nFolder: $remoteDirectory\r\n\r\n");
                    }
                }
            }
        }
    }
    else
    {
        print "Config file does not define 'logfile'\n";
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

 
 