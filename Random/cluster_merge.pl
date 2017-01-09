#!/usr/bin/perl


use lib qw(../);
use DateTime;
use Data::Dumper;
use Loghandler;
use DBhandler;
use Mobiusutil;
#use strict;
#use warnings;
use LWP::UserAgent;





our $questdbHandler = new DBhandler("iii","74.217.200.60","mobiusdbs","password","1032");
our $lancedbHandler = new DBhandler("iii","74.217.200.58","mobiusdbs","password","1032");
our $egdb = new DBhandler("evergreen","192.168.11.34","evergreen","password","5432");
setupEGTables();
$egdb->update("TRUNCATE seekdestroy.quest");
$egdb->update("TRUNCATE seekdestroy.lance");
get001s("seekdestroy.quest",$questdbHandler);
get001s("seekdestroy.lance",$lancedbHandler);



sub get001s
{
	my $egtable = @_[0];
	my $db = @_[1];
	
	my $limit = 100000;
	my $offset = 0;
	my $querytemplate = "SELECT field_content,record_num,record_type_code,varfield_type_code from sierra_view.varfield_view where marc_tag='001' order by id limit !!thelimit!! offset !!theoffset!!";
	
	my $yeild = 1;
	while($yeild > -1)
	{
		my $query = $querytemplate;
		$query =~ s/!!thelimit!!/$limit/g;
		$query =~ s/!!theoffset!!/$offset/g;
		#print $query."\n";
		my @results = @{$db->query($query)};
		$yeild = $#results;
		print "offset $offset received $yeild\n";
		if($yeild > -1)
		{
			my $insertQuery = "INSERT INTO $egtable (id001,record_num,record_type_code,varfield_type_code)
			values
			";
			foreach(@results)
			{
				my @row = @{$_};
				$insertQuery.="(\$\$".@row[0]."\$\$, ".@row[1].", \$\$".@row[2]."\$\$, \$\$".@row[3]."\$\$),\n";
			}
			$insertQuery=substr($insertQuery,0,-2);
			#print "inserting";
			$egdb->update($insertQuery);
			$offset+=$limit;
		}
	}
}


sub setupEGTables
{
		$query = "CREATE TABLE seekdestroy.quest(
		id001 text,record_num integer,record_type_code text,varfield_type_code text)";
		$egdb->update($query);
		$query = "CREATE TABLE seekdestroy.lance(
		id001 text,record_num integer,record_type_code text,varfield_type_code text)";
		$egdb->update($query);
}


