#!/usr/bin/perl

# These Perl modules are required:
# install Email::MIME
# install Email::Sender::Simple
# install Digest::SHA1

use lib qw(../);
use strict; 
use Loghandler;
use Mobiusutil;
use DBhandler;
use Data::Dumper;
use Getopt::Long;
use DateTime;
use utf8;
use Encode;
use XML::Simple;
use Email::MIME;

 our $body;
 our $from;
 our $to;
 our $ateo;
 our $stripDate;
 our $mobUtil = new Mobiusutil(); 
 our $xmlconf = "/openils/conf/opensrf.xml";
GetOptions (
"from=s" => \$from,
"to=s" => \$to,
"xmlconfig=s" => \$xmlconf,
"body=s" => \$body,
"ateo=s" => \$ateo,
"stripdate" => \$stripDate
)
or die("Error in command line arguments\n");


 my $bodyDefined = $body || 0;
 
 our $dbHandler;
 my $subject = 'test message';
    print $xmlconf."\n";
    my %dbconf = %{getDBconnects($xmlconf)};
    $dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
    
    my @headers;
    if($ateo)
    {
        my $full;
        print "Getting ateo ID $ateo\n";
        my @results = @{$dbHandler->query("select data from action_trigger.event_output where id=$ateo")};
        
        foreach(@results)
        {
            my @row = @{$_};
            $full = @row[0];
        }
        
        if($full)
        {
            my $collectBody = 0;
            my @lines = split(/\n/,$full);
            
            foreach(@lines)
            {
                if(!$collectBody)
                {
                    my $type = $_;
                    my @tm = split(/:/,$type);
                    $type = @tm[0];
                    $type =~ s/^\s//;
                    $type =~ s/\s$//;
                    my $remainder = @tm[1];
                    print "Parsing Type = $type\n";
                    print "Parsing Remainder = '$remainder'\n";
                    next if ( (lc($type) =~ m/date/) && $stripDate );
                    
                    if( (lc $type) !~ m/subject/)
                    {
                        $remainder = $from if( (lc $type) =~ m/from/ && $from );
                        $remainder = $to if( (lc $type) =~ m/to/ && $to );
                        push (@headers, ($type, $remainder) );
                    }
                    else
                    {
                        $subject = $_;
                        push (@headers, ($type, $remainder));
                        $collectBody = 1;
                    }
                }
                else
                {
                    $body .= $_."\n" if(!$bodyDefined);
                }
            }
        }
    }
    else
    {
        push @headers, ("From", $from);
        push @headers, ("To", $to);
        push @headers, ("Subject", $subject);
    }
       
     $body="\n\n$body" if($bodyDefined);

print "Resulting headers\n";
foreach(@headers)
{
    print $_."\n";
}
print $subject."\n";
# # print $body."\n";
       
       # exit;
        my $message;
	
        $message = Email::MIME->create(
          header_str => [
            @headers
          ],
          attributes => {
            encoding => 'quoted-printable',
            charset  => 'ISO-8859-1',
          },
          body_str => "$body\n");
         my $valid=1;
         if($valid)
         {
            use Email::Sender::Simple qw(sendmail);
            sendmail($message);
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

 
 