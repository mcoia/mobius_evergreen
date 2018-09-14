#!/usr/bin/perl
use lib qw(../);
use File::Path qw(make_path remove_tree);
use strict; 
use Loghandler;
use Mobiusutil;
use DBhandler;
use Data::Dumper;
#use email;
use DateTime;
use utf8;
use Encode;
use DateTime;
use XML::Simple;
use Getopt::Long;

my $configFile=0;
my $xmlconf = "/openils/conf/opensrf.xml";
our $daysrepeat=0;
our $debug=0;
our $reindex=0;



GetOptions (
"daysrepeat=i" => \$daysrepeat,
"config=s" => \$configFile,
"xmlconfig=s" => \$xmlconf,
"debug" => \$debug,
"reindex" => \$reindex
)
or die("Error in command line arguments\nYou can specify
--daysrepeat integer
--config configfilename (required)
--xmlconfig  pathto_opensrf.xml
--debug flag
--reindex flag to only reindex the html directories\n");

if(! -e $xmlconf)
{
	print "I could not find the xml config file: $xmlconf\nYou can specify the path when executing this script --xmlconfig configfilelocation\n";
	exit 0;
}
 if(!$configFile)
 {
	print "Please specify a config file\n";
	exit;
 }

	our $mobUtil = new Mobiusutil();  
	my $conf = $mobUtil->readConfFile($configFile);
	our $log;
	our $dbHandler;
	our %lib_notices;
	our %lib_confs;
	our %system_short_codes;
	our %branch_names;
	our %affectedDirectories;
	our %conf;
  
if($conf)
{
	%conf = %{$conf};	
	if ($conf{"logfile"})
	{
		my $dt = DateTime->now(time_zone => "local"); 
		my $fdate = $dt->ymd; 
		my $ftime = $dt->hms;
		my $dateString = "$fdate $ftime";
		$log = new Loghandler($conf->{"logfile"});
		$log->truncFile("");
		$log->addLogLine(" ---------------- Script Starting ---------------- ");		
		my @reqs = ("logfile","temp_space","path_to_xsl_template","outputroot","xsltproc_binary","fop_binary","path_to_index_html_template","directory_index_file");
		my $valid = 1;		
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
						
			if($reindex)
			{
				reIndex();
				exit;
			}
			
			my $query = "SELECT ated.id,ated.name,
			(SELECT shortname FROM actor.org_unit WHERE id=ated.owner),
			(CASE WHEN lower(delay::TEXT)!~'day' THEN (SELECT delay FROM action_trigger.event_definition WHERE owner=ated.owner AND reactor='MarkItemLost' AND hook='checkout.due' LIMIT 1) else delay end)
			FROM action_trigger.event_definition ated
			WHERE
			(ated.granularity=\$\$".$conf{"granularity_name"}."\$\$
            OR
            (ated.reactor=\$\$ProcessTemplate\$\$ AND ated.hook=\$\$lost.auto\$\$ AND ated.granularity=\$\$long_overdue_to_lost\$\$)
            )and
            id=611";
			$log->addLine($query);
			my $resetDaysRepeat = $daysrepeat;
			my @results = @{$dbHandler->query($query)};	
			foreach(@results)
			{
				my $row = $_;
				my @row = @{$row};
				my $templateID=@row[0];
				my $name=@row[1];
				my $owning_ou=@row[2];
				my $delay=@row[3];
				#$delay =~ s/[\D\s]//g;
				#print "delay: $delay\n";
				$daysrepeat = $resetDaysRepeat;
				while($daysrepeat > -1)
				{
					my @date=@{figureDateString($daysrepeat)};
					$daysrepeat-=1;
					gatherDB($templateID,@date[0]);
					#$log->addLine($data);
					while ((my $internal, my $value ) = each(%lib_notices))
					{
						my $path = $conf{"outputroot"};
						$path =~ s/\/+$//; #remove trailing slashes
						my $system = lc(getSystemShortCode($internal));
						$path.="/$system";
						$affectedDirectories{$path}=1;
						if(!-d $path)
						{
							make_path($path, {
							verbose => $debug,
							mode => 0755,
							});
						}
						if(!-d $conf{"temp_space"})
						{
							make_path($conf{"temp_space"}, {
							verbose => $debug,
							mode => 0755,
							});
						}
						my $tempxmlfile = $mobUtil->chooseNewFileName($conf{"temp_space"},"temp-$internal-".@date[0],"xml");
						my $tempfofile = $mobUtil->chooseNewFileName($conf{"temp_space"},"temp-$internal-".@date[0],"fo");
						my $finalpdffile = $path."/$owning_ou $internal-$name-".@date[0].".pdf";						
						my $fopstandardoutput = $conf{"temp_space"}."/fopstandardoutput.out";
						my $write = new Loghandler($tempxmlfile);
						$write->addLine("<?xml version='1.0' encoding='UTF-8'?>\n<file type='notice' date='".@date[0]."'>$value</file>");
						# gotta escape the space character when working on the bash prompt
						$tempxmlfile =~s/\s/\\ /g;
						$tempfofile =~s/\s/\\ /g;
						$finalpdffile =~s/\s/\\ /g;
						$fopstandardoutput=~s/\s/\\ /g;
						# gotta remove the ampersand character when working on the bash prompt
						$tempxmlfile =~s/&/_/g;
						$tempfofile =~s/&/_/g;
						$finalpdffile =~s/&/_/g;
						$fopstandardoutput=~s/&/_/g;
						# gotta remove the parentheses characters when working on the bash prompt
						$tempxmlfile =~s/\)/_/g;
						$tempfofile =~s/\)/_/g;
						$finalpdffile =~s/\)/_/g;
						$fopstandardoutput=~s/\)/_/g;
						$tempxmlfile =~s/\(/_/g;
						$tempfofile =~s/\(/_/g;
						$finalpdffile =~s/\(/_/g;
						$fopstandardoutput=~s/\(/_/g;
						#print "Unlinking $finalpdffile\n";
						unlink $finalpdffile;
						$log->addLine( $conf{"xsltproc_binary"}." --stringparam gendate \"".@date[1]."\" --stringparam delayvalue \"$delay\" ".$conf{"path_to_xsl_template"}." $tempxmlfile > $tempfofile");
						system($conf{"xsltproc_binary"}." --stringparam gendate \"".@date[1]."\" --stringparam delayvalue \"$delay\" ".$conf{"path_to_xsl_template"}." $tempxmlfile > $tempfofile");
						$log->addLine($conf{"fop_binary"}." $tempfofile $finalpdffile > $fopstandardoutput");
						system($conf{"fop_binary"}." $tempfofile $finalpdffile > $fopstandardoutput");
						if($debug)
						{
							print "Creating $finalpdffile\n";
						}
						if(!$debug)
						{
							#put the regular spaces back into the path
							$tempxmlfile =~s/\\\s/ /g;
							$tempfofile =~s/\\\s/ /g;							
							$fopstandardoutput=~s/\\\s/ /g;
							$log->addLine("Deleting \n$tempxmlfile\n$tempfofile\n$fopstandardoutput");
							unlink $tempxmlfile;
							unlink $tempfofile;
							unlink $fopstandardoutput;
						}
						else
						{
							print "DEBUG - not removing $tempxmlfile\n";
							print "DEBUG - not removing $tempfofile\n";
							print "DEBUG - not removing $fopstandardoutput\n";
						}
					}
					%lib_notices=();
				}
			}
		}
		setupIndexForAffectedDirectories();
		$log->addLogLine(" ---------------- Script End ---------------- ");	
	}
	else
	{
		print "Your config file does not specify a log file (logfile=)\n";
	}
}

sub reIndex
{
	my $path = $conf{"outputroot"};
	opendir(DIR, $path) or die $!;
	while (my $file = readdir(DIR)) 
	{
		if (-d "$path/$file")
		{
			$affectedDirectories{"$path/$file"}=1;
		}
	}
	setupIndexForAffectedDirectories();
}

sub setupIndexForAffectedDirectories
{
	
	while ((my $internal, my $value ) = each(%affectedDirectories))
	{
		my $content='';
		my $total='';
		my $dir = $internal;
		my @files;
		opendir(DIR, $dir) or die $!;
		
		while (my $file = readdir(DIR)) 
		{
			# We only want files
			next unless (-f "$dir/$file");
			# Use a regular expression to find files ending in .pdf
			next unless (lc$file =~ m/\.pdf$/);
			push(@files,$file);
		}
		closedir(DIR);
		
		#Alphabetical order please
		@files = sort @files;
		
		my $branchName = "";
		$content.="<ul>\n";
		foreach(@files)
		{
			my $file = $_;
			# We are relying on the file name to be consistent system_short_code branch_short_code-notice_title-date.pdf
			my @s = split(/\s/,$file);
			my @s2 = split(/-/,@s[1]);
			pop @s2;
			my $bname = getBranchName(join("-",@s2));			
			if($branchName ne $bname)
			{
				$branchName = $bname;
				$content.="<h2>$branchName</h2>";
			}
			$content.="<li><a href=\"$file\">$file</a></li>\n";
		}
		$content.="</ul>\n";
		
		my $template = new Loghandler($conf{"path_to_index_html_template"});
		my @lines = @{$template->readFile()};
		foreach(@lines)
		{
			my $line = $_;
			$line =~ s/\$content/$content/g;
			$total.="$line\n";
		}
		my $indexhtml = new Loghandler($dir."/".$conf{"directory_index_file"});
		$indexhtml->truncFile($total);
	}
}

sub getSystemShortCode
{
	my $branchshortcode = @_[0];
	if(!$system_short_codes{$branchshortcode})
	{
		my $query = "SELECT SHORTNAME FROM ACTOR.ORG_UNIT WHERE ID IN
		(
		SELECT PARENT_OU FROM ACTOR.ORG_UNIT WHERE LOWER(SHORTNAME)=LOWER(\$\$$branchshortcode\$\$)
		)
		";
		$log->addLine($query);
		my @results = @{$dbHandler->query($query)};	
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			$system_short_codes{$branchshortcode} = @row[0];
			return @row[0];
		}
		return "none-$branchshortcode";		
	}
	return $system_short_codes{$branchshortcode};
}

sub getBranchName
{
	my $shortcode = @_[0];
	my $ret = "Unidentified";
	if(!$branch_names{$shortcode})
	{
		my $query = "SELECT NAME FROM ACTOR.ORG_UNIT WHERE LOWER(SHORTNAME)=LOWER(\$\$$shortcode\$\$)";
		$log->addLine($query);
		my @results = @{$dbHandler->query($query)};	
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			$branch_names{$shortcode} = @row[0];
			return @row[0];
		}
		return "Unidentified";
	}
	return $branch_names{$shortcode};
}

sub gatherDB
{
	my $templateID = @_[0];
	my $date = @_[1];
	my $query = "SELECT array_to_string(array_accum(coalesce(data, '')),'') FROM action_trigger.event_output where id in (select template_output from action_trigger.event where event_def = $templateID AND run_time::date = '$date');";	
	$log->addLine($query);
	my @results = @{$dbHandler->query($query)};	
	foreach(@results)
	{
		my $row = $_;
		my @row = @{$row};
		my $data="<?xml version='1.0' encoding='UTF-8'?>\n<file type='notice'>".@row[0]."</file>";
		parseXML($data);
	}
}

sub figureDateString
{
	my $daysback=@_[0];
	my $dt = DateTime->now;   # Stores current date and time as datetime object	
	my $target = $dt->subtract(days=>$daysback);
	my @ret=($target->ymd,$target->mdy);
	return \@ret;	
}

sub is_integer {
   return defined $_[0] && $_[0] =~ /^[+-]?\d+$/;
}			

sub parseXML
{
	my $xmldata = @_[0];
	my $xml = new XML::Simple;
	my %data = %{$xml->XMLin($xmldata)};
	
	$log->addLine(Dumper(%data));
	my $i=0;
	my $t = ref($data{'notice'});
	$log->addLine("Reference = $t");
	$log->addLine(Dumper($data{'notice'}));
	if(ref($data{'notice'}) eq 'ARRAY')
	{
		$log->addLine("It's an Array");
		foreach (@{$data{'notice'}}) 
		{
			my %notice = %{$_};
			my $location=$notice{location}{shortname};
			$lib_notices{$location}.="<notice>".createXML(\%notice,'')."</notice>";		
			$i++;
		}
	}
	elsif(ref($data{'notice'}) eq 'HASH')
	{
		$log->addLine("It's an HASH");		
		$log->addLine("Looping \%{\$data{'notice'}}");
		my %notice = %{$data{'notice'}};
		my $location=$notice{location}{shortname};
		$lib_notices{$location}.="<notice>".createXML(\%notice,'')."</notice>";		
		$i++;
		
	}
}

sub createXML
{
	#print " is ".ref(@_[0])."\n";
	my $test = @_[0];	
	my $xml = @_[1];
	my $element = @_[2];
	my $ref  = ref($test);
	if($ref eq 'HASH')
	{
		#$log->addLine("$element is HASH");
		while ((my $internal, my $value ) = each(%{$test}))
		{
				
			 my $t = substr($xml,(-1*(length("<$internal>"))));			
			my $thisref = ref($value);
			if(substr($xml,(-1*(length("<$internal>")))) ne "<$internal>")
			{
				$xml.="<$internal>";
			}			
			else
			{
				#$log->addLine( "equal <$internal> ending: $t");
			}
			$xml=createXML($value,$xml,$internal);
			if(substr($xml,(-1*(length("</$internal>")))) ne "</$internal>")
			{
				$xml.="</$internal>";
			}
			else
			{
				#$log->addLine( "equal </$internal> ending: $t");
			}			
		}
	}	
	elsif($ref =~ m/ARRAY/g)
	{
		#$log->addLine("$element is ARRAY");
		foreach(@{$test})
		{
			
			# my $t = substr($xml,(-1*(length("<$element>")+1)));
			# print "$element ending: $t\n";
			if(substr($xml,(-1*(length("<$element>")))) ne "<$element>")
			{
				$xml.="<$element>";
			}
			$xml=createXML($_,$xml,$element);
			if(substr($xml,(-1*(length("</$element>")))) ne "</$element>")
			{
				$xml.="</$element>";
			}
			
		}
	}
	else
	{		
		#$log->addLine("$element is $ref");
		my $esc = $test;
		$esc =~ s/&/&amp;/g;
		$xml.="$esc";
	}	
	
	return $xml;
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