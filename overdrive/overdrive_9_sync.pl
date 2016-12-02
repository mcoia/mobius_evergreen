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
			setupSchema($dbHandler);
			my @molib2godbrecords = @{getMolib2goList($dbHandler,$log)};
			my @updatethese;
			foreach(@molib2godbrecords)
			{
				my $marc = @{$_}[1];
				my $id = @{$_}[0];
				$marc =~ s/(<leader>.........)./${1}a/;
				my $marcobject = MARC::Record->new_from_xml($marc);
				$marcobject = add9($marcobject,\@shortnames);
				my $thisXML = convertMARCtoXML($marcobject);
				my $before = substr($marc,index($marc, '<leader>'));
				my $after = substr($thisXML,index($thisXML, '<leader>'));
				if($before ne $after)
				{
					my @temp = ( $id, $thisXML );
					push @updatethese, [@temp];
					#$log->addLine("These are different now $id");
					#$log->addLine("$marc\r\nbecame\r\n$thisXML");
				}
			}
			foreach(@updatethese)
			{
				my @both = @{$_};
				my $bibid = @both[0];
				my $marc = @both[1];
				my @urls = @{getAffectedURLs($marc,$log)};
				foreach(@urls)
				{
					recordSyncToDB($dbHandler,$conf{"participants"},$bibid,$_);
				}
				removeOldCallNumberURI($bibid,$dbHandler);				
				#$log->addLine("UPDATE BIBLIO.RECORD_ENTRY SET MARC=\$1 WHERE ID=$bibid");
				#$log->addLine($marc);
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

sub getAffectedURLs
{
	my $marc = @_[0];
	my $log = @_[1];
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

sub recordSyncToDB
{
	my $dbHandler = @_[0];	
	my $shortnames = @_[1];
	my $bibid = @_[2];	
	my $url = @_[3];
	my $query = "INSERT INTO MOLIB2GO.NINE_SYNC(RECORD,NINES_SYNCED,URL) VALUES(\$1,\$2,\$3)";
	my @values = ($bibid,$shortnames,$url);
	$dbHandler->updateWithParameters($query,\@values);
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
	";
	$log->addLine($query);
	my @results = @{$dbHandler->query($query)};
	my $found=0;
	foreach(@results)
	{
		my @row = @{$_};
		my $prevmarc = @row[1];
		my $id = @row[0];
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

sub removeOldCallNumberURI
{
	my $bibid = @_[0];
	my $dbHandler = @_[1];
	my $query = "
	DELETE FROM asset.uri_call_number_map WHERE call_number in 
	(
		SELECT id from asset.call_number WHERE record = $bibid AND label = \$\$##URI##\$\$
	)
	";

	$dbHandler->update($query);
	$query = "
	DELETE FROM asset.uri_call_number_map WHERE call_number in 
	(
		SELECT id from asset.call_number WHERE  record = $bibid AND label = \$\$##URI##\$\$
	)";

	$dbHandler->update($query);
	$query = "
	DELETE FROM asset.uri WHERE id not in
	(
		SELECT uri FROM asset.uri_call_number_map
	)";

	$dbHandler->update($query);
	$query = "
	DELETE FROM asset.call_number WHERE  record = $bibid AND label = \$\$##URI##\$\$
	";

	$dbHandler->update($query);
	$query = "
	DELETE FROM asset.call_number WHERE  record = $bibid AND label = \$\$##URI##\$\$
	";

	$dbHandler->update($query);

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

sub setupSchema
{
	my $dbHandler = @_[0];
	my $query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'molib2go'";
	my @results = @{$dbHandler->query($query)};
	if($#results==-1)
	{
		$query = "CREATE SCHEMA molib2go";
		$dbHandler->update($query);	
		$query = "CREATE TABLE molib2go.job
		(
		id bigserial NOT NULL,
		start_time timestamp with time zone NOT NULL DEFAULT now(),
		last_update_time timestamp with time zone NOT NULL DEFAULT now(),
		status text default 'processing',	
		current_action text,
		current_action_num bigint default 0,
		CONSTRAINT job_pkey PRIMARY KEY (id)
		  )";		  
		$dbHandler->update($query);
		$query = "CREATE TABLE molib2go.item_reassignment(
		id serial,
		copy bigint,
		prev_bib bigint,
		target_bib bigint,
		change_time timestamp default now(),
		job  bigint NOT NULL,
		CONSTRAINT item_reassignment_fkey FOREIGN KEY (job)
		REFERENCES molib2go.job (id) MATCH SIMPLE
		)";
		$dbHandler->update($query);
		$query = "CREATE TABLE molib2go.bib_marc_update(
		id serial,
		record bigint,
		prev_marc text,
		changed_marc text,
		new_record boolean NOT NULL DEFAULT false,
		change_time timestamp default now(),
		job  bigint NOT NULL,
		CONSTRAINT bib_marc_update_fkey FOREIGN KEY (job)
		REFERENCES molib2go.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE molib2go.bib_merge(
		id serial,
		leadbib bigint,
		subbib bigint,
		change_time timestamp default now(),
		job  bigint NOT NULL,
		CONSTRAINT bib_merge_fkey FOREIGN KEY (job)
		REFERENCES molib2go.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);
		$query = "CREATE TABLE molib2go.undedupe(
		id serial,
		oldleadbib bigint,
		undeletedbib bigint,
		undeletedbib_electronic_score bigint,
		undeletedbib_marc_score bigint,
		moved_call_number bigint,
		change_time timestamp default now(),
		job  bigint NOT NULL,
		CONSTRAINT undedupe_fkey FOREIGN KEY (job)
		REFERENCES molib2go.job (id) MATCH SIMPLE)";
		$dbHandler->update($query);	
		$query = "CREATE TABLE molib2go.nine_sync(
		id serial,
		record bigint,
		nines_synced text,
		url text,
		change_time timestamp default now())";
		$dbHandler->update($query);
	}
}

 exit;

 
 