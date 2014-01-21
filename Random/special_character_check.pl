#!/usr/bin/perl
# 
# Example Configure file:
# 
use lib qw(../);
 use strict; 
 use Loghandler;
 use Mobiusutil;
 use utf8;
 use Encode;
 use encoding 'utf8', Filter => 1;
 
 #use warnings;
 #use diagnostics; 
 
 my $file = @ARGV[0];
 my $logr = new Loghandler($file);
 my @lines = @{$logr->readFile()};
 my $count=0;
 my $found=0;
 foreach(@lines)
 {
	my $test = $_;
	
	$count++;
	if($test =~ m/[\x80-\x{FFFF}]/)
	{
		$found++;
		my @sp = split(/ /,$test);
		my $out;
		foreach(@sp)
		{			
			my $ch = $_;
			if($ch =~ m/[\x80-\x{FFFF}]/)
			{
				$out.=" $ch";
			}			
		}

		print "Line:  $count\n words: $out\n";
	
	}
}

print "Found $found / $count lines with special characters\n";
 
 exit;