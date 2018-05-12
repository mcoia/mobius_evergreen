#!/usr/bin/perl

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
use DateTime;
use utf8;
use Encode;
use LWP::Simple;
use OpenILS::Application::AppUtils;
use DateTime::Format::Duration;
use File::stat;
use XML::Simple;
use Math::Round qw(nhimult);


 our $mobUtil = new Mobiusutil(); 
 
 our $xmlconf = "/openils/conf/opensrf.xml";
 our $log = new Loghandler(@ARGV[0]);
 our $dbHandler;
 our $currentFile;
 our $beforeFile = new Loghandler(@ARGV[1]);
 our $afterFile = new Loghandler(@ARGV[2]);
 our $before;
 our $after;
	
	
    my $dt = time;
    
    $log->truncFile("");
    $log->addLogLine(" ---------------- Script Starting ---------------- ");
    my $errorMessage="";
    
    my @files;
    
    $beforeFile->truncFile("");
    $afterFile->truncFile("");
    
    my %dbconf = %{getDBconnects($xmlconf)};
    $dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
    
    my $inputError = 0;
    
    for my $b (3..$#ARGV)
    {
        my $log = new Loghandler(@ARGV[$b]);
        if(!$log->fileExists())
        {
            $inputError = 1;
            print "Could not locate file: ".@ARGV[$b]."\n";
        }
        else
        {
            push(@files, @ARGV[$b]);
        }
    }
    if($inputError)
    {	
        print "Some of the files do not exist\n";
    }
    else
    {
        if($#files!=-1)
        {
            $| = 1;
            #print Dumper(@files);
            my $cnt = 0;
            my $fcnt = 0;
            my $scnt = 0;
            for my $b(0..$#files)
            {
                $currentFile = $files[$b];                    
                $log->addLogLine("Parsing: ".$files[$b]);
                my $file = MARC::File::USMARC->in($files[$b]);
                while ( my $marc = $file->next() ) 
                {
                    $cnt++;
                    my $importResult = importMARCintoEvergreen($marc);
                    if( $importResult > 0 )
                    {
                        $scnt++ if $importResult==2;
                        # exit if $importResult==1;
                    }
                    else
                    {
                        $fcnt++;
                    }
                    if( $cnt % 100 == 0 )
                    {
                        $beforeFile->appendLine($before);
                        $afterFile->appendLine($after);
                        $after='';
                        $before='';
                        
                        # exit if($cnt > 1000);
                    }
                    print "\rProcessed: $cnt / Failed: $fcnt / no change: $scnt";
                    
                    my $afterProcess = time;
                    my $difference = $afterProcess - $dt;
                    $difference+=1; # add one second to prevent divide by zero
                    my $duration =  nhimult(.1, $difference / 60);
                    my $rps = nhimult(.1, $cnt / $difference);
                    print "    $duration minutes elapsed, $rps bibs/sec                        ";
                }
                $file->close();
                undef $file;
            }
            $log->addLogLine("Processed: $cnt Failed: $fcnt Non-updated no change: $scnt");
        }
        $| = 0;
        print "\nDone\n";
    }
    $log->addLogLine(" ---------------- Script Ending ---------------- ");
	


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
	my $marc = @_[0];
    my $query;
    my $ret = -1;
		
    my $bibid = getsubfield($marc,'901','c');
   
    my $meMARC = getMEMARC($bibid);
    
    if($meMARC == -1)
    {
        $log->addLine("Record $bibid not found in database. File $currentFile");
    }
    else
    {
        $meMARC =~ s/(<leader>.........)./${1}a/;
        $meMARC = MARC::Record->new_from_xml($meMARC);
        my $compResults = compareMARC($meMARC, $marc);
     
        if($compResults)
		{
            my $marcforDB = convertMARCtoXML($marc);
            my @values = ($marcforDB);
            $query = "UPDATE BIBLIO.RECORD_ENTRY SET marc=\$1 WHERE ID=$bibid";
            $log->addLine($query);
            my $res = $dbHandler->updateWithParameters($query,\@values);
            $log->addLine("$bibid\thttp://missourievergreen.org/eg/opac/record/$bibid?expand=marchtml#marchtml\thttp://upgrade.missourievergreen.org/eg/opac/record/$bibid?expand=marchtml#marchtml");
           
            undef $marcforDB;
            undef @values;
            $ret = 1;
        }
        else
        {
            # No change!
            $log->addLine("$bibid\tNO CHANGE");
            $ret = 2;
        }
        
    }
	return $ret;
}

sub compareMARC
{
    my $ret = -1;
    my $first = shift;
    my $second = shift;
    
    my $fcomp = $first->clone();
    my $scomp = $second->clone();
    
    # Strip 901
    
    my $f901 = $fcomp->field('901');
    my @go = ($f901);
    # print "About to convert to DELETE_FIELDS\n";
    $fcomp->delete_fields(@go);
    
    my $f901 = $scomp->field('901');
    my @go = ($f901);
    # print "About to convert to DELETE_FIELDS\n";
    $scomp->delete_fields(@go);
    
    $before .= $fcomp->as_formatted()."\n";
    $after .= decode_utf8($scomp->as_formatted())."\n";
    
    # print "Stripping fcomp\n";
    $fcomp = convertMARCtoXML($fcomp);
    $fcomp =~s/<record>//;
    $fcomp =~s/<\/record>//;
    $fcomp =~s/<\/collection>/<\/record>/;
    $fcomp =~s/<collection/<record  /;
    $fcomp =~s/XMLSchema-instance"/XMLSchema-instance\"  /;
    $fcomp =~s/schema\/MARC21slim.xsd"/schema\/MARC21slim.xsd\"  /;
    
    # print "Stripping scomp\n";
    $scomp = convertMARCtoXML($scomp);
    $scomp =~s/<record>//;
    $scomp =~s/<\/record>//;
    $scomp =~s/<\/collection>/<\/record>/;
    $scomp =~s/<collection/<record  /;
    $scomp =~s/XMLSchema-instance"/XMLSchema-instance\"  /;
    $scomp =~s/schema\/MARC21slim.xsd"/schema\/MARC21slim.xsd\"  /;
    # $before.=$fcomp."\n";
    # $after.=$scomp."\n";
    
    $ret = 0 if($fcomp eq $scomp);
    $ret = 1 if($fcomp ne $scomp);
    undef $fcomp;
    undef $scomp;
    return $ret;
}

sub getMEMARC
{
	my $dbID = @_[0];
	my $query = "SELECT MARC FROM BIBLIO.RECORD_ENTRY WHERE ID=$dbID";
	my @results = @{$dbHandler->query($query)};
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $marc = @row[0];
		return $marc;
	}
	return -1;
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

 
 