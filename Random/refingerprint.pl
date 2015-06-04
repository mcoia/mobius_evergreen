#!/usr/bin/perl


use Loghandler;
use Mobiusutil;
use Data::Dumper;

 
my $inputError = 0;
my @files;

my @keepcols=
(
#1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
);
my $totalcols = $#keepcols;
my %keepcolsmap = map { $_ => 1 } @keepcols;

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
	print "Usage ./refingerprint.pl  inputfile1 inputfile2 inputfile3 ... ... ... \n";
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
		my $i=0;
		while($i<=$#lines)
		{
			my $thislineFinal;
			my $line = @lines[$i];
			chomp $line;
			$line=substr($line,0,-1);
			my @info = split('\t',$line);
			my $temp = @info[$#info];
			
			#print "last char: '$temp'\n";
			my $colpos=1;
			my $colsadded=0;
			my $sortval = @info[2];
			while(length($sortval)<17)
			{
				$sortval = '0'.$sortval;
			}
			@info[0] = $sortval;
			foreach(@info)
			{	
				$thislineFinal.=$_."\t";
				$colsadded++;
				$colpos++;
			}
			chomp $thislineFinal;
			$thislineFinal=substr($thislineFinal,0,-1);
			while($colsadded<$totalcols+1)
			{
				print "only $colsadded and needed $totalcols\n";
				$thislineFinal.="\t";
				$colsadded++;
			}
			$finalout.=$thislineFinal."\n";
			$i++;
		}	
		my $fh = new Loghandler($path.$baseFileName.".$fExtension");
		$fh->deleteFile();
		chomp $finalout;
		#$finalout=substr($finalout,0,-1);
		$fh->appendLine($finalout);
		
	}
}
