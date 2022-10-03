#!/usr/bin/perl

use lib qw(./);
use Loghandler;
use Mobiusutil;
use Data::Dumper;
use XML::Simple;
use XML::TreeBuilder;
use Getopt::Long;
use DBhandler;
use utf8;
use Encode;

our $schema;
our $mobUtil = new Mobiusutil();
our $log;
our $dbHandler;
our $data_dir;
our $sample;
our $loginvestigationoutput;
our @columns;
our @allRows;

my $xmlconf = "/openils/conf/opensrf.xml";
GetOptions (
"logfile=s" => \$logFile,
"xmlconfig=s" => \$xmlconf,
"data_dir=s" => \$data_dir,
"sample=i" => \$sample,
"schema=s" => \$schema,
)
or die("Error in command line arguments\nYou can specify
--logfile configfilename (required)
--xmlconfig pathtoevergreenopensrf.xml (default /opensrf/conf/opensrf.xml)
--data_dir path to the text file data directory
--schema (eg. m_slmpl)
\n");

if(! -e $xmlconf)
{
	print "I could not find the xml config file: $xmlconf\nYou can specify the path when executing this script --xmlconfig configfilelocation\n";
	exit 0;
}
if(!$logFile)
{
	print "Please specify a log file\n";
	exit;
}
if(!$schema)
{
	print "Please specify an Evergreen DB schema to dump the data to\n";
	exit;
}

	$log = new Loghandler($logFile);
	$log->truncFile("");
	$log->addLogLine(" ---------------- Script Starting ---------------- ");		

	my %dbconf = %{getDBconnects($xmlconf)};
	$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
    my $dh;
    opendir($dh, $data_dir) || die "Dan't open the directory $data_dir";
    print "opened folder $data_dir\n";
    my @dots;
    while (my $file = readdir($dh)) 
    {
        print "Checking $file\n";
        push @dots, $file if ( !( $file =~ m/^\./) && -f "$data_dir$file" && ( $file =~ m/\.migdat$/i) )
    }
    closedir $dh;
    
    foreach(@dots)
    {
        my $tablename = $_;
        $tablename =~ s/\.migdat//gi;
        $tablename =~ s/\s/_/g;
        my $file = $data_dir.''.$_;
        my $insertFile = new Loghandler($data_dir.''.$_.'.insert');
        $insertFile->deleteFile();
        my $outputText = "";
        print $mobUtil->boxText("Loading '$file'","#","|",1);

        open(my $fh,"<",$file) || die "error $!\n";
        while(<$fh>)
        {
            my $line = $_;
            $line =~ s/\n//g;
            $line =~ s/\r//g;
            $outputText .= "$line\n";
        }
        $outputText = substr($outputText,0,-1); # remove the last return
        print $mobUtil->boxText("Cleaning line Spans '$file'","#","|",1);
        $outputText = cleanLineSpans($outputText);
        my @lines = split(/\n/, $outputText);
        setupTable(\@lines,$tablename,$insertFile);
        undef $outputText;
    }
    
	
	
	$log->addLogLine(" ---------------- Script End ---------------- ");

sub setupTable
{
	my @lines = @{@_[0]};
	my $tablename = @_[1];
    my $insertFile = @_[2];
    my $insertString = '';
	
    my $emptyHeaderName = 'ghost';
    my $header = shift @lines;
    $log->addLine($header);
    my $delimiter = figureDelimiter($header);
    
    my @cols = split(/$delimiter/,$header);
    $log->appendLine($_) foreach(@cols);
    my %colTracker = ();
    for my $i (0.. $#cols)
	{
        @cols[$i] =~ s/[\.\/\s\$!\-\(\)]/_/g;
        @cols[$i] =~ s/\_{2,50}/_/g;
        @cols[$i] =~ s/\_$//g;
        @cols[$i] =~ s/&//g;
        @cols[$i] =~ s/"//g;
        @cols[$i] =~ s/,//g;
        @cols[$i] =~ s/\*//g;
        
        # Catch those naughty columns that don't have anything left to give
        $emptyHeaderName.='t' if(length(@cols[$i]) == 0);
        @cols[$i]=$emptyHeaderName if(length(@cols[$i]) == 0);
        my $int = 1;
        my $base = @cols[$i];
        while($colTracker{@cols[$i]}) #Fix duplicate column names
        {
            @cols[$i] = $base."_".$int;
            $int++;
        }
        $colTracker{@cols[$i]} = 1;
	}
	print "Gathering $tablename....";
	$log->addLine(Dumper(\@cols));
    $insertString.= join("\t",@cols);
    $insertString.="\n";
	print $#lines." rows\n";
	
	
	#drop the table
	my $query = "DROP TABLE IF EXISTS $schema.$tablename";
	$log->addLine($query);
	$dbHandler->update($query);
	
	#create the table
	$query = "CREATE TABLE $schema.$tablename (";
	$query.=$_." TEXT," for @cols;
	$query=substr($query,0,-1).")";
	$log->addLine($query);
	$dbHandler->update($query);
	
	if($#lines > -1)
	{
		#insert the data
		$query = "INSERT INTO $schema.$tablename (";
		$query.=$_."," for @cols;
		$query=substr($query,0,-1).")\nVALUES\n";
        my $count = 0;
		foreach(@lines)
		{
            last if ( $sample && ($count > $sample) );
            # ensure that there is at least one tab
            if($_ =~ m/$delimiter/)
            {
                # $log->appendLine($_) if $count > 15000;
                my @thisrow = split(/$delimiter/,$_);
                my $thisline = '';
                my $valcount = 0;
                # if(@thisrow[0] =~ m/2203721731/)
                # {
                $query.="(";
                for(@thisrow)
                {
                    if($valcount < scalar @cols)
                    {
                        my $value = $_;
                        #add period on trialing $ signs
                        #print "$value -> ";
                        $value =~ s/\$$/\$\./;
                        $value =~ s/\n//;
                        $value =~ s/\r//;
                        # $value = NFD($value);
                        $value =~ s/[\x{80}-\x{ffff}]//go;
                        $thisline.=$value;
                        $insertString.=$value."\t";
                        #print "$value\n";
                        $query.='$data$'.$value.'$data$,';
                        $valcount++;
                    }
                }
                # pad columns for lines that are too short
                my $pad = $#cols - $#thisrow - 1;
                for my $i (0..$pad)
                {
                    $thisline.='$$$$,';
                    $query.='$$$$,';
                    $insertString.="\t";
                }
                $insertString = substr($insertString,0,-1)."\n";
                # $log->addLine( "final line $thisline");
                $query=substr($query,0,-1)."),\n";
                $count++;
                if( $count % 5000 == 0)
                {
                    $insertFile->addLine($insertString);
                    $insertString='';
                    $query=substr($query,0,-2)."\n";
                    $loginvestigationoutput.="select count(*),$_ from $schema.$tablename group by $_ order by $_\n" for @cols;
                    print "Inserted ".$count." Rows into $schema.$tablename\n";
                    $log->addLine($query);
                    $dbHandler->update($query);
                    $query = "INSERT INTO $schema.$tablename (";
                    $query.=$_."," for @cols;
                    $query=substr($query,0,-1).")\nVALUES\n";
                }
                # }
            }
		}
		$query=substr($query,0,-2)."\n";
		$loginvestigationoutput.="select count(*),$_ from $schema.$tablename group by $_ order by $_\n" for @cols;
		print "Inserted ".$count." Rows into $schema.$tablename\n";
        
		$log->addLine($query);
		$dbHandler->update($query);
        $insertFile->addLine($insertString);
	}
	else
	{
		print "Empty dataset for $tablename \n";
		$log->addLine("Empty dataset for $tablename");
	}
}

sub cleanLineSpans
{
    my $fileContents = shift;
    my $finalout = '';
    my $errorout = '';
    my $delimiter = figureDelimiter($fileContents);
    my @lines = split(/\n/, $fileContents);
    print "lines: $#lines\n";
    my $header = @lines[0];
    $header = $mobUtil->trim($header);
    my @headers = split(/$delimiter/,$header);
    my $headerCount = $#headers;
    print "Header count = $headerCount , delimiter: '$delimiter'\n";
    return $fileContents if($headerCount == 0); # if there is only one column, this code doesn't work
    my $i = 1;
    my $previousLine = "";
    while($i <= $#lines)
    {
        my @thisLine = @{readFullLine(\@lines, $i, $delimiter, $headerCount)};
        # print "LINE:\n";
        # print Dumper(\@thisLine);
        $i = @thisLine[1];
        if(@thisLine[0])
        {
            $finalout .= @thisLine[0] . "\n";
        }
        else
        {
            print "Error on line $i\n";
            $log->addLine($finalout);
            return 0;
        }
        $i++;
        # exit if $i > 6;
    }
    $finalout = "$header\n" . substr($finalout,0,-1);
    return $finalout;
}

sub figureDelimiter
{
    my $fileContents = shift;
    my @lines = split(/\n/, $fileContents);
    my $commas = 0;
    my $tabs = 0;
    my $loops = 0;
    foreach(@lines)
    {
        my @split = split(/,/,$_);
        $commas+=$#split;
        @split = split(/\t/,$_);
        $tabs+=$#split;
        last if ($loops > 100);
        $loops++;
    }
    my $delimiter = $commas > $tabs ? "," : "\t";
    return $delimiter;
}

sub readFullLine
{
    my $tlines = shift;
    my $i = shift;
    my $delimiter = shift;
    my $headerCount = shift;
    my @ret;
    my @lines = @{$tlines};
    # print Dumper($tlines);
    my $line = readLineCorrectly(@lines[$i], $delimiter);
    my @datas = split(/$delimiter/,$line);
    # print "First read through:\n" . Dumper($line);
    # print "Headercount = $headerCount\nDatacolumns: ".$#datas."\n";
    $i++;
    while( ($headerCount > $#datas) && ($i < $#lines + 1) )
    {
        print "looping through more lines to get more columns\n";
        my $lookingForTerminator = !isLastElementComplete(@datas[$#datas], $delimiter);
        print "We are looking for terminator\n" if($lookingForTerminator);
        
        $line = readLineCorrectly(@lines[$i], $delimiter, $lookingForTerminator);
        my @tdatas = split(/$delimiter/,$line);
        @datas[$#datas] .= shift @tdatas if($lookingForTerminator);
        foreach(@tdatas)
        {
            push (@datas, $mobUtil->trim($_));
        }
        # print "Final line so far:\n";
        # print Dumper(\@datas);
        # print "Headercount = $headerCount\nDatacolumns: ".$#datas."\n";
        # my $temp = $i + 1;
        # print "Next line: '" . @lines[$temp] . "'\n";
        if( ($i == $#lines) && ($headerCount > $#datas) ) ## Second to last line of the file, TLC likes to trim the last null columns on the last line
        {
            # print "We are on the second to last line of the file\n";
            if(length($mobUtil->trim(@lines[$i])) == 0 ) # pad the last line with blank delimiters until we've reached a complete line
            {
                 while($headerCount > $#datas)
                 {
                    push (@datas,'');
                 }
            }
        }
        $i++;
        undef $lookingForTerminator;
    }
    $i--;
    if ( ($headerCount + 1 == $#datas) && (length($mobUtil->trim(@datas[$#datas])) == 0) ) #handles the case when TLC puts blank columns on the end
    {
        # print "Popping last element from the line\n";
        pop @datas;
    }
    if($headerCount == $#datas)
    {
        my $retString = "";
        foreach(@datas)
        {
            $retString .= $_ . $delimiter;
        }
        $retString = substr($retString, 0, -1);
        # Make sure the last element of the last line is finished
        while(!isLastElementComplete($retString,$delimiter))
        {
            if($#lines > $i)
            {
                $i++;
                my $frag = readLineCorrectly(@lines[$i], $delimiter);
                my @tdatas = split(/$delimiter/,$frag);
                foreach(@tdatas)
                {
                    $retString .= " " . $mobUtil->trim($_);
                }
            }
            else #we've encountered the bottom of the file and we still have an unterminiated string. Just end it here and be done
            {
                $retString .= '"';
            }
        }
        @ret = ($retString, $i);
    }
    else # This ended up reading some un-even number of columns
    {
        @ret = (0, $i);
    }
    return \@ret;
}

sub readLineCorrectly
{
    my $line = shift;
    my $delimiter = shift;
    my $lookingForTerminator = shift || 0;
    my $ret = "";
    # can't use split because it ignores zero length fields, so we do it by hand
    my @info = @{getDelimitedLine($line,$delimiter,$lookingForTerminator)};

    # print "Starting with looking = $lookingForTerminator\n";
    # print Dumper(\@info);
    foreach(@info)
    {
        # $ret .= ' ' if($lookingForTerminator);
        my $elem = $_;
        $lookingForTerminator = !isLastElementComplete(($lookingForTerminator ? '#' : '' ) . $elem, $delimiter);
        $ret .= $elem;
        if(!$lookingForTerminator)
        {
            # print "Added delimiter after '$elem'\n";
            $ret .= $delimiter;
        }
    }
    $ret = substr($ret,0,-1) if(substr($ret,0,-1) eq $delimiter);
    # my @prin = split(/$delimiter/,$ret);
    # print "looking = $lookingForTerminator\nreturning\n'$ret'\n";
    # print Dumper(\@prin);
    return $ret;
}

sub getDelimitedLine
{
    my $line = shift;
    my $delimiter = shift;
    my $middleOfField = shift || 0;
    # my @ret = split(/$delimiter/,$line,-1);  # This doesn't work because these files (sometimes) will put another delimiter at the end of each line
    # print Dumper(\@ret);
    # exit;
    # return \@ret;
    my @info = ();
    my @each = split(//,$line);
    my $ret = "";
    
    foreach(@each)
    {
        if( ($_ eq $delimiter) && !$middleOfField)
        {
            $ret = " " if(length($ret) == 0); #pad space for empty columns so we can use split function later
            # print "Pushing '$ret'\n";
            push(@info, $ret);
            $ret = "";
        }
        else
        {
            if( $_ eq '"' ) # this field is qoute wrapped
            {
                $middleOfField = !$middleOfField;
            }
            if( ($_ eq $delimiter) && $middleOfField)
            {
                $ret .= ''; #don't introduce the delimiter in the middle of the data. Removing those for our purposes.
            }
            else
            {
                $ret .= $_;
            }
        }
    }
    $ret = " " if(length($ret) == 0); #pad space for empty columns so we can use split function later
    push(@info, $ret);
    return \@info;
}

sub isLastElementComplete
{
    my $line = shift;
    my $delimiter = shift;
    my $ret = "";
    my @info = split(/$delimiter/,$line);
    my $lastElement = pop @info;
    $lastElement =~ s/""//g; # Double quotes are escapes - remove them for this string terminator exercise
    my @lastElementChars = split(//,$lastElement);
    # print "Last element: $lastElement\n";
    # print "Last line first/last Char: '".@lastElementChars[0]."' / '".@lastElementChars[$#lastElementChars]."'\n";
    return 0 if(@lastElementChars[0] && @lastElementChars[0] eq '"' && $#lastElementChars == 0 ); # Only one character long and it's a " mark
    return 0 if(@lastElementChars[0] && @lastElementChars[0] eq '"' && @lastElementChars[$#lastElementChars] ne '"');
    return 1;
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