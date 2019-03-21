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
use DateTime;
use utf8;
use Encode;
use LWP::Simple;
use OpenILS::Application::AppUtils;
use DateTime::Format::Duration;

our $importSourceName;
our $importSourceNameDB;
our $log;
 my $configFile = @ARGV[0];
 if(!$configFile)
 {
	print "Please specify a config file\n";
	exit;
 }

 my $mobUtil = new Mobiusutil(); 
 my $conf = $mobUtil->readConfFile($configFile);
 
 if($conf)
 {
	my %conf = %{$conf};
	if ($conf{"logfile"})
	{
		my $dt = DateTime->now(time_zone => "local"); 
		my $fdate = $dt->ymd; 
		my $ftime = $dt->hms;
		my $dateString = "$fdate $ftime";
		$log = new Loghandler($conf->{"logfile"});
		#$log->truncFile("");
		$log->addLogLine(" ---------------- Script Starting ---------------- ");
		my @reqs = ("tempspace","dbhost","db","dbuser","dbpass","port","participants","logfile","sourcename"); 
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
	
		my $dbHandler;
		if($valid)
		{	
			$importSourceName = $conf{"sourcename"};
			$importSourceNameDB = $importSourceName.' script';
			$importSourceNameDB =~ s/\s/\-/g;
			my @shortnames = split(/,/,$conf{"participants"});
			for my $y(0.. $#shortnames)
			{				
				@shortnames[$y]=$mobUtil->trim(@shortnames[$y]);
			}			
			$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});
			my @molib2godbrecords = @{getMolib2goList($dbHandler,$log)};
            print "".$#molib2godbrecords." gathered....done gathering\n";
			my @updatethese;
			foreach(@molib2godbrecords)
			{
				my $marc = @{$_}[1];
				my $id = @{$_}[0];
				$marc =~ s/(<leader>.........)./${1}a/;
				my $marcobject = MARC::Record->new_from_xml($marc);
                # print "Making sure that the 856's have the correct indicators\n";
				
                $marcobject = fix856($marcobject,\@shortnames);
				my $thisXML = convertMARCtoXML($marcobject);
				my $before = substr($marc,index($marc, '<leader>'));
				my $after = substr($thisXML,index($thisXML, '<leader>'));
				if($before ne $after)
				{
					my @temp = ( $id, $thisXML );
					push @updatethese, [@temp];
                    print "adding to update list\n";
					# $log->addLine("These are different now $id");
					# $log->addLine("$marc\r\nbecame\r\n$thisXML");
				}
			}
			foreach(@updatethese)
			{
				my @both = @{$_};
				my $bibid = @both[0];
				my $marc = @both[1];
				my $query = "UPDATE BIBLIO.RECORD_ENTRY SET MARC=\$1 WHERE ID=$bibid";
				my @values = ($marc);
				$dbHandler->updateWithParameters($query,\@values);
				$log->addLine("$bibid\thttp://missourievergreen.org/eg/opac/record/$bibid?query=yellow;qtype=keyword;locg=4;expand=marchtml#marchtml\thttp://mig.missourievergreen.org/eg/opac/record/$bibid?query=yellow;qtype=keyword;locg=157;expand=marchtml#marchtml");
			}
			
		}
		$log->addLogLine(" ---------------- Script Ending ---------------- ");
	}
	else
	{
		print "Config file does not define 'logfile'\n";		
	}
}

sub decidemolib2go856
{
	my $field = @_[0];
	my @sub3 = $field->subfield( '3' );
	my $ind2 = $field->indicator(2);
	foreach(@sub3)
	{
		if(lc($_) eq 'excerpt')
		{
			return 0;
		}
	}
	my @s7 = $field->subfield( '7' );
	if(!@s7)
	{
		return 0;
	}
	else
	{
		my $foundmolib7=0;
		foreach(@s7)
		{
			if($_ eq $importSourceName)
			{
				$foundmolib7=1;
			}
		}
		if(!$foundmolib7)
		{
			return 0;
		}
	}
	return 1;
}

sub getMolib2goList
{
	my $dbHandler = @_[0];
	my $log = @_[1];	
	my @ret;
	my $query = "
	SELECT ID,MARC FROM 
	BIBLIO.RECORD_ENTRY WHERE 
	DELETED IS FALSE AND 
	ID IN(SELECT RECORD FROM ASSET.CALL_NUMBER WHERE LABEL=\$\$##URI##\$\$) 
	AND MARC ~ '<subfield code=\"7\">$importSourceName'
    -- limit 1000
	";
	$log->addLine($query);
	my @results = @{$dbHandler->query($query)};
	my $found=0;
	foreach(@results)
	{
		my @row = @{$_};
		my $prevmarc = @row[1];
		my $id = @row[0];
        print "gathering $id\n";
		my @temp = ($id,$prevmarc);
		push @ret,[@temp];
	}
	return \@ret;
}

sub fix856
{
	my $marc = @_[0];
	my @shortnames = @{@_[1]};
	my @recID = $marc->field('856');
	if(@recID)
	{
		#$marc->delete_fields( @recID );
		for my $rec(0..$#recID)
		{
			#print Dumper(@recID[$rec]);
			my @recordshortnames=();
			my $ismolib2go = decidemolib2go856(@recID[$rec]);			
			if($ismolib2go)
			{
				my $thisField = @recID[$rec];
				my $ind1 = $thisField->indicator(1);
                my $ind2 = $thisField->indicator(2);
                if($ind1 ne '4' || $ind2 ne '0')
                {
                    print "fixing ".$marc->subfield('901',"c")."\n";
                    $thisField->set_indicator(1,'4');
                    $thisField->set_indicator(2,'0');
                }
			}
		}
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

 exit;

 
 