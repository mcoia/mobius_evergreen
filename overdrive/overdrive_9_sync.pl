#!/usr/bin/perl

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

our $importSourceName;
our $importSourceNameDB;
our $importBIBTagName;
our $domainname = '';
our $log;
 my $configFile = @ARGV[0];
 if(!$configFile)
 {
    print "Please specify a config file\n";
    exit;
 }
our $limit = @ARGV[1];
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
        $log = new Loghandler($conf{"logfile"});
        #$log->truncFile("");
        $log->addLogLine(" ---------------- Script Starting ---------------- ");
        my @reqs = ("tempspace","dbhost","db","dbuser","dbpass","port","participants","logfile","sourcename","bibtag");
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
            $importBIBTagName = $conf{"bibtag"};
            $domainname = $conf{"domainname"} || '';
            my @shortnames = split(/,/,$conf{"participants"});
            for my $y(0.. $#shortnames)
            {
                @shortnames[$y]=$mobUtil->trim(@shortnames[$y]);
            }
            $dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});
            my @molib2godbrecords = @{getMolib2goList($dbHandler)};
            # print "done gathering\n";
            my @updatethese;
            foreach(@molib2godbrecords)
            {
                my $marc = @{$_}[1];
                my $id = @{$_}[0];
                $marc =~ s/(<leader>.........)./${1}a/;
                my $marcobject = MARC::Record->new_from_xml($marc);
                # print "adding 9s $id\n";
                $marcobject = add9($marcobject,\@shortnames);
                my $thisXML = convertMARCtoXML($marcobject);
                my $before = substr($marc,index($marc, '<leader>'));
                my $after = substr($thisXML,index($thisXML, '<leader>'));
                if($before ne $after)
                {
                    my @temp = ( $id, $thisXML );
                    push @updatethese, [@temp];
                    print "adding to update list\n";
                    #$log->addLine("These are different now $id");
                    #$log->addLine("$marc\r\nbecame\r\n$thisXML");
                }
            }
            foreach(@updatethese)
            {
                my @both = @{$_};
                my $bibid = @both[0];
                my $marc = @both[1];
                my @urls = @{getAffectedURLs($marc)};
                foreach(@urls)
                {
                    recordSyncToDB($dbHandler,$conf{"participants"},$bibid,$_);
                }
                #$log->addLine("UPDATE BIBLIO.RECORD_ENTRY SET MARC=\$1 WHERE ID=$bibid");
                #$log->addLine($marc);
                my $query = "UPDATE BIBLIO.RECORD_ENTRY SET MARC=\$1 WHERE ID=$bibid";
                my @values = ($marc);
                $dbHandler->updateWithParameters($query,\@values);
                $log->addLine("$bibid\thttp://$domainname/eg/opac/record/$bibid?query=yellow;qtype=keyword;locg=4;expand=marchtml#marchtml\thttp://$domainname/eg/opac/record/$bibid?query=yellow;qtype=keyword;locg=157;expand=marchtml#marchtml");
            }

        }
        $log->addLogLine(" ---------------- Script Ending ---------------- ");
    }
    else
    {
        print "Config file does not define 'logfile'\n";
    }
}

sub getAffectedURLs
{
    my $marc = @_[0];
    my @ret=();
    my $marcobject = MARC::Record->new_from_xml($marc);
    my @recID = $marcobject->field('856');
    if(@recID)
    {
        for my $rec(0..$#recID)
        {
            my $ismolib2go = decidemolib2go856(@recID[$rec]);
            if($ismolib2go)
            {
                my @u = @recID[$rec]->subfield( 'u' );
                push @ret, @u;
            }
        }
    }
    return \@ret;
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
    if($ind2 ne '0')
    {
        return 0;
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
            if($_ eq $importBIBTagName)
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

sub recordSyncToDB
{
    my $dbHandler = @_[0];
    my $shortnames = @_[1];
    my $bibid = @_[2];
    my $url = @_[3];
    my $query = "INSERT INTO e_bib_import.nine_sync(record,nines_synced,url) VALUES(\$1,\$2,\$3)";
    my @values = ($bibid,$shortnames,$url);
    $dbHandler->updateWithParameters($query,\@values);
}

sub getMolib2goList
{
    my $dbHandler = @_[0];
    my @ret;
    my $query = "
    SELECT id,marc FROM
    biblio.record_entry WHERE
    deleted IS FALSE AND
    id IN(SELECT record FROM asset.call_number WHERE label=\$\$##URI##\$\$)
    AND marc ~ '<subfield code=\"7\">$importBIBTagName'
    ";
    $query .=" LIMIT $limit" if($limit);
    $log->addLine($query);
    my @results = @{$dbHandler->query($query)};
    my $found=0;
    foreach(@results)
    {
        my @row = @{$_};
        my $prevmarc = @row[1];
        my $id = @row[0];
        # print "gathering $id\n";
        my @temp = ($id,$prevmarc);
        push @ret,[@temp];
    }
    return \@ret;
}

sub add9
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
                my @ninposes;
                my $poses=0;
                #deleting subfields requires knowledge of what position among all of the subfields they reside.
                #so we have to record at what positions each of the 9's are ahead of time.
                foreach($thisField->subfields())
                {
                    my @f = @{$_};
                    if(@f[0] eq '9')
                    {
                        push (@ninposes, $poses);
                    }
                    $poses++;
                }
                my @nines = $thisField->subfield("9");
                my @delete9s = ();

                for my $t(0.. $#shortnames)
                {
                    my @s7 = @recID[$rec]->subfield( '7' );

                    my @subfields = @recID[$rec]->subfield( '9' );
                    my $shortnameexists=0;
                    for my $subs(0..$#subfields)
                    {
                    #print "Comparing ".@subfields[$subs]. " to ".@shortnames[$t]."\n";
                    push @recordshortnames, @subfields[$subs];
                        if(@subfields[$subs] eq @shortnames[$t])
                        {
                            $shortnameexists=1;
                        }
                    }
                    #print "shortname exists: $shortnameexists\n";
                    if(!$shortnameexists)
                    {
                        #print "adding ".@shortnames[$t]."\n";
                        @recID[$rec]->add_subfields('9'=>@shortnames[$t]);
                    }
                }
                ## clean up 9's that are not in the list
                my $ninePos = 0;
                for my $recshortname(0.. $#recordshortnames)
                {
                    my $thisname = @recordshortnames[$recshortname];
                    my $foundshortname=0;
                    foreach(@shortnames)
                    {
                        if($_ eq $thisname)
                        {
                            $foundshortname=1;
                        }
                    }
                    if(!$foundshortname)
                    {
                        push(@delete9s, @ninposes[$ninePos]);
                    }
                    $ninePos++;
                }
                if($#delete9s > -1)
                {
                    @recID[$rec]->delete_subfield(code => '9', 'pos' => \@delete9s);
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


