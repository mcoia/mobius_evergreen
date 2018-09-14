#!/usr/bin/perl
use lib qw(../);
use MARC::Record;
use MARC::File;
use MARC::File::XML (BinaryEncoding => 'utf8');
use strict; 
use Loghandler;
use Mobiusutil;
use DBhandler;
use Data::Dumper;
use DateTime;
use utf8;
use Encode;
use DateTime;
use LWP::Simple;
use OpenILS::Application::AppUtils;
use DateTime::Format::Duration;
use Digest::SHA1;
use Unicode::Normalize;


my $xmlconf = "/openils/conf/opensrf.xml";

	our $mobUtil = new Mobiusutil();  
	our $log;
    
    # my $marcFile = "/mnt/evergreen/migration/wright/data/marc_og.mrc";
    # my $file = MARC::File::USMARC->in( $marcFile );
    # my $count = 0;
    # while ( my $marc = $file->next() ) 
    # {
        # print "$count";        
        # print $marc->field('001')->data() if $marc->field('001');
        # print "\n";
        # $count++;
    # }
    # $file->close();
    # undef $file;
    
    my $output='<collection xmlns="http://www.loc.gov/MARC21/slim">';
    unlink "/mnt/evergreen/migration/wright/inter/wrc.clean.marc.xml";
    my $outputFile = new Loghandler("/mnt/evergreen/migration/wright/inter/wrc.clean.marc.xml");
    my $cnt = 0;
    my $startingUser = 1;
    my $outterChunkSize = 500;
    
    my $file = "/mnt/evergreen/migration/wright/inter/wrc.clean.marc_messedup.xml";
    

    my $chunk = getChunk($file, $startingUser, $outterChunkSize);
    while($chunk)
    {
        local $@;
        my $test = "";
        eval
        {
            my @chunks = split(/<record>/,$chunk);
            # Remove the first element <collection.....
            shift @chunks;
            # Remove the last element </collection.....
            pop @chunks;
            foreach(@chunks)
            {
                my $xmlchunk = "<record>".$_;
                my $marc = MARC::Record->new_from_xml( $xmlchunk, 'UTF-8', 'USMARC');                
                $test.=$marc->as_xml_record();
                
                $test=~s/<\?xml version="1\.0" encoding="UTF\-8"\?>//g;
                
            }
        };
        if($@)
        {
            print " Couldn't read record $cnt\n";
            print " Breaking it down into single records\n";
            $output .= breakItUP($chunk);
        }
        else
        {
            $output.=$test;
        }
        $cnt+=$outterChunkSize;
        print "$cnt\n";
        $startingUser+=$outterChunkSize;
        $chunk = getChunk($file,$startingUser,$outterChunkSize);
        #print "$chunk\n";
    }
    $outputFile->truncFile($output."</collection>");
    undef $file;
    

sub getChunk
{
    my $file = shift;
    my $starting = shift;
    my $chunkSize = shift;
    
    # print "reading $file\n";
    # print "starting $starting\n";
    # print "chunkSize $chunkSize\n";
    open my $info, $file or die "Could not open $file: $!";
   
    binmode(inputfile, ":utf8");
    
    my $xmlheader = "";
    my $stopReadingHeader = 0;
    my $userCount = 0;
    my $usersAdded = 0;
    my $ret = '';
    
    while( my $line = <$info>)
    {       
        if (!$stopReadingHeader)
        {
            $xmlheader .= $line if( !($line =~ m/<record>/ ) );
            $stopReadingHeader = 1 if($line =~ m/<collection/ );
        }
        $userCount++ if( $line =~ m/<record>/ );
        if( ($stopReadingHeader) && ($usersAdded < $chunkSize ) && ($starting < ($userCount + 1) ) )
        {
            $ret .= $line;
            $usersAdded++ if( $line =~ m/<\/record>/ );
        }
        last if $usersAdded == $chunkSize;
    }
     # print "
# userCount = $userCount
# usersAdded = $usersAdded
# ";
    close($info);
    my $finalret = $xmlheader.$ret;
    $finalret.="</collection>\n" if !($finalret =~ m/<\/collection>/); # only append the closing tag when it's not already there from the source file
    return 0 if $starting > $userCount;
    return $finalret;
}

sub breakItUP
{
    my $alltogether = shift;
    my $output='';
    my $startingUser = 1;
    my $cnt = 0;
    my $chunk = getChunkWithinChunk($alltogether,$startingUser,1);
    while($chunk)
    {
        local $@;
        my $test = "";
        eval
        { 
            my $marc = MARC::Record->new_from_xml( $chunk, 'UTF-8', 'USMARC');
            $test.=$marc->as_xml_record(); 
            $test=~s/<\?xml version="1\.0" encoding="UTF\-8"\?>//g;
        };
        if($@)
        {
            print " Couldn't read record $cnt - dropping it\n";
        }
        else
        {
            $output.=$test;
        }
        $cnt++;
        # print "Breakdown $cnt\n";
        $startingUser++;
        $chunk = getChunkWithinChunk($alltogether,$startingUser,1);
        #print "$chunk\n";
    }
    return $output;
}


sub getChunkWithinChunk
{
    my $file = shift;
    my $starting = shift;
    my $chunkSize = shift;
    
    
    my $xmlheader = "";
    my $stopReadingHeader = 0;
    my $userCount = 0;
    my $usersAdded = 0;
    my $ret = '';
    my @lines = split(/\n/,$file);
    print "lines: ".$#lines."\n";
    # print Dumper(@lines);
    # exit;
    
    foreach( @lines )
    {
        my $line = $_;
        if (!$stopReadingHeader)
        {
            $xmlheader .= $line if( !($line =~ m/<record>/ ) );
            $stopReadingHeader = 1 if($line =~ m/<collection/ );
            # print $xmlheader."\n";
            # print "stopping header\n"  if($line =~ m/<collection/ );
        }
        $userCount++ if( $line =~ m/<record>/ );
        if( ($stopReadingHeader) && ($usersAdded < $chunkSize ) && ($starting < ($userCount + 1) ) )
        {
            $ret .= $line;
            $usersAdded++ if( $line =~ m/<\/record>/ );
        }
        last if $usersAdded == $chunkSize;
    }
     # print "
# userCount = $userCount
# usersAdded = $usersAdded
# ";
    my $finalret = $xmlheader.$ret;
    $finalret.="</collection>\n" if !($finalret =~ m/<\/collection>/); # only append the closing tag when it's not already there from the source file
    return 0 if $starting > $userCount;
    return $finalret;
}



 exit;

 
 