#!/usr/bin/perl


use Loghandler;
use Mobiusutil;
use Data::Dumper;

 
my $inputError = 0;
my @files;

for my $b (0..$#ARGV)
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
	print "Usage ./correct_tlc_output.pl  inputfile1 inputfile2 inputfile3 ... ... ... \n";
}
else
{
	foreach(@files)
	{	
		my $file = $_;
		my $path;
		my $baseFileName;
		my $fExtension;
		my $originalFileName;
		my $errorFileName;
		my @sp = split('/',$file);
		# foreach(@sp)
		# {
			# $path.=$_.'/';
		# }
		#$path=substr($path,0,( (length(@sp[$#sp]))*-1) -1);
		$path=substr($file,0,( (length(@sp[$#sp]))*-1) );
		print "lastE = ".@sp[$#sp]."\n";
		my @fsp = split('\.',@sp[$#sp]);
		$baseFileName = @fsp[0];
		$fExtension = @fsp[1];
		$originalFileName = $baseFileName."_org.".$fExtension;		
		$errorFileName = $baseFileName."_error.".$fExtension;
print "path = $path  Base = $baseFileName  Orgname = $originalFileName  Errorname = $errorFileName\n";
		my $fhandle = new Loghandler($file);
		$fhandle->copyFile($path.$originalFileName);
		my $finalout;
		my $errorout;
		my @lines = @{$fhandle->readFile()};
		my $header = @lines[0];
		chomp $header;
		my $orgout="$header\n";
		$finalout="$header\n";
		print "$finalout";
		my @headers = @{breakTabs($header)};
		my $headerCount = @headers[0];
print "Header count: $headerCount\n";
		my $i=1;
		while($i<=$#lines)
		{
			my $thislineFinal;
			my $line = @lines[$i];
			chomp $line;
			
			my $thislineFinal=$line;
			$orgout.=$line."\n";
			my $orglinebeforefastforward=$orgout;
			my @info = @{breakTabs($line)};
			my $count = @info[0];
			my @spl = @{@info[1]};
			my $originalPos=$i;
			#continue to append the following line until the number of tabs are equal to the header row
			#or until the total tabs exceeds the header row (error will throw)
			while($headerCount>$count && $i<=$#lines )
			{
			my @cha = split('',$thislineFinal);
			
			#Get rid of the weird character at the end of these TLC lines
				$thislineFinal=substr($thislineFinal,0,-1);
			#see what the second to last one looks like
			#print "last character= \"".@cha[$#cha]."\"\n";
				$i++;
				$line = @lines[$i];
				chomp $line;
				@info = @{breakTabs($line)};				
				$orgout.="$line\n";
				$count+=@info[0];
				$thislineFinal.=$line;				
			}
			if($headerCount==$count)
			{
				$finalout.=$thislineFinal."\n";
			}
			else			
			{
			#start back at the original row and work down again for the next loop
				$i=$originalPos;
				$line = @lines[$i];
				$errorout.="Error Line $i\n".$line;
print "Errors with ".@sp[$#sp]." $i\n";
				$orgout=$orglinebeforefastforward;
			}
			$i++;
		}
		my $fh = new Loghandler($path.$originalFileName);
		#$fh->truncFile($orgout);
		if(length($errorout)>0)
		{
			$fh = new Loghandler($path.$errorFileName);
			$fh->truncFile($errorout);
		}
		$fhandle->deleteFile();
		chomp $finalout;
		$finalout=substr($finalout,0,-1);
		my @asdf = split('',$finalout);
		print "Last char:'".@asdf[$#asdf]."'\n";
		$fhandle->appendLine($finalout);
	}
}

sub breakTabs
{
	my $line = @_[0];
	my @spl = split('\t',$line);
	my @ret;
	push(@ret,$#spl);
	push(@ret,[@spl]);
	
	return \@ret;
}
