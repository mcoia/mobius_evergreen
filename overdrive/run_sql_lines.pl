#!/usr/bin/perl
# 

 
 use Loghandler;
 use DBhandler;
 use Data::Dumper;
 use MARC::Record;
 use MARC::File;
 use feature qw(switch);
 use XML::Simple;
		
 my $sqlfile = @ARGV[0];
 my $logfile = @ARGV[1];
 my $xmlconf = "/openils/conf/opensrf.xml";
 if(!$logfile)
 {
	print "Please specify a log file\n";
	print "usage: ./run_sql_lines sqlfile.sql /tmp/logfile.log\n";
	exit;
 }
 if(!$sqlfile)
 {
	print "Please specify a sql file\n";
	print "usage: ./run_sql_lines sqlfile.sql /tmp/logfile.log\n";
	exit;
 }
 if(! -e $xmlconf)
 {
	print "I could not find the $xmlconf\n";
	print "usage: ./run_sql_lines sqlfile.sql /tmp/logfile.log\n";
	exit;
 }
  if(! -e $sqlfile)
 {
	print "I could not find the $sqlfile\n";
	print "usage: ./run_sql_lines sqlfile.sql /tmp/logfile.log\n";
	exit;
 }


 


my $log = new Loghandler($logfile);
$log->deleteFile();

	
$log->addLogLine(" ---------------- Script Starting ---------------- ");

my %conf = %{getDBconnects($xmlconf,$log)};
my @reqs = ("dbhost","db","dbuser","dbpass","port"); 
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
	my $dbHandler;
	eval{$dbHandler = new DBhandler($conf{"db"},$conf{"dbhost"},$conf{"dbuser"},$conf{"dbpass"},$conf{"port"});};
	if ($@) 
	{
		$log->addLogLine("Could not establish a connection to the database");
		print "Could not establish a connection to the database";
	}
	else
	{
		my @lines = @{parsesql($sqlfile)};
		my $total = $#lines;
		print "Ok - I am connected to:\n".$conf{"dbhost"}."\n".$conf{"db"}."\n".$conf{"dbuser"}."\n".$conf{"dbpass"}."\n".$conf{"port"}."\n"."\n\nWe have $total sql command(s)\n\n";
		my $tcount=1;
		my $rcount=0;
		my $scount=0;
		my $stop=0;
		for my $i (0..$#lines)
		{
			if(!$stop)
			{
				my $underscore=0;
				my $sqlcmd = @lines[$i];
				while($underscore<40){print "_";$underscore++}
				print "\n";
				print "Current: $tcount\tRan: $rcount\tSkipped: $scount\n\n$sqlcmd\n\nRun this?\ny,(n),q: "; 
				my $a =  <STDIN>;			
				chomp $a;
				#print "\"$a\"";
				$log->addLine("$tcount :\n$sqlcmd");
				given( uc $a)
				{
					when('Y')
					{
						$rcount++;
						my $output = runsqlcmd($sqlcmd,$log,$dbHandler);
						print "\n$output\n";
						$log->addLine("$output");
					}
					when('N')
					{
						$scount++;
						print "\nSkipped\n";
						$log->addLine("Skipped");
					}
					when('Q')
					{
						$stop=1;
						print "\nQuit\n";
						$log->addLine("Quit");
					}
					default
					{
						$scount++;
						print "\nSkipped\n";
						$log->addLine("Skipped");
					}
					
				}
				$tcount++;
			}
		}
	}
}
	$log->addLogLine(" ---------------- Script Ending ---------------- ");



sub runsqlcmd
{
	my $ret;
	my $cmd = @_[0];
	$cmd =~ s/^\s+//;
	$cmd =~ s/\s+$//;
	my $log = @_[1];
	my $dbHandler= @_[2];
	my @s = split(/\s/,$cmd);
	
	if(uc(@s[0] ) eq 'UPDATE' || uc(@s[0] ) eq 'INSERT' || uc(@s[0] ) eq 'DELETE' || uc(@s[0] ) eq 'DROP' || uc(@s[0] ) eq 'CREATE' || uc(@s[0] ) eq 'ALTER')
	{
		$ret = $dbHandler->update($cmd);
	}
	elsif(uc(@s[0] ) eq '\\COPY')
	{
		$ret = $dbHandler->copyinput($cmd);
	}
	elsif(uc(@s[0] ) eq '\\I')
	{
		$cmd =~s/\\i\s//g;
		$ret = "Executing: psql < $cmd\n";
		$ret.=system("psql < $cmd");
	}
	else
	{
		my @results = @{$dbHandler->query($cmd)};
		foreach(@results)
		{
			my $row = $_;
			my @row = @{$row};
			foreach(@row)
			{
				$ret.=$_."\t";
			}
			
		}
		
	}
	
	return $ret;
	
}

sub parsesql
{
	my $sql = @_[0];
	print "Reading $sql\n";
	my $sqlfile = new Loghandler($sql);
	my @lines = @{$sqlfile->readFile()};
	my @final;
	my $cmd="";	
	foreach(@lines)
	{
		#print "Line:";
		my $line = $_;
		#print "$line\n";
		$line=~s/^\s+//;
		$line =~ s/\s+$//;
		my $adding=0;
		#print "Line: $line\n";
		if(length($line)>2)
		{	
			if(substr($line,0,2) ne'--')
			{
				if((substr(lc($line),0,5) ne '\echo'))
				{
					if((length($cmd)==0) && (substr(lc($line),0,5) eq '\\copy'))
					{
						push(@final,$line);
						$cmd="";
					}
					elsif((length($cmd)==0) && (substr(lc($line),0,2) eq '\\i'))
					{
						push(@final,$line);
						$cmd="";
					}
					else
					{
						$cmd.=" $line";
						my $subs = substr($line,-1,1);
						#print "Last char: $subs\n";
						if(substr($line,-1,1) eq ';')
						{
							$adding=1;
							#print "Adding $cmd\n";
							push(@final,$cmd);
							$cmd="";
						}
					}
				}
				
			}
			else		
			{
				print "Removing: $line\n";
			}
		}
		
	}
	#my $final;
	#foreach(@final){$final.=$_;	}
	#@final = split(/;/,$final);
	return \@final;
}

sub getDBconnects
{
	my $openilsfile = @_[0];
	my $log = @_[1];
	my $xml = new XML::Simple;
	my $data = $xml->XMLin($openilsfile);
	my %conf;
	$conf{"dbhost"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{host};
	$conf{"db"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{db};
	$conf{"dbuser"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{user};
	$conf{"dbpass"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{pw};
	$conf{"port"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{port};
	#print Dumper(\%conf);
	return \%conf;

}

 exit;