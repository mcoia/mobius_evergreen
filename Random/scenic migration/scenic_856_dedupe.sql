-- SCLENDS bibliographic dedupe routine
--
-- Copyright 2010-2011 Equinox Software, Inc.
-- Author: Galen Charlton <gmc@esilibrary.com>
-- 
-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2, or (at your option)
-- any later version.
-- 
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
-- 
-- You should have received a copy of the GNU General Public License
-- along with this program.  If not, see <http://www.gnu.org/licenses/>.
--
--
-- This implements a bibliographic deduplication routine based
-- on criteria and an algorithm specified by the South Carolina
-- State Library on behalf of the SC LENDS consortium.  This work
-- was sponsored by SC LENDS, whose impetus is gratefully
-- acknowledged.  Portions of this script were subseqently expanded
-- based on the advice of the Indiana State Library on the behalf
-- of the Evergreen Indiana project.


-- This script has been expanded to merge the 856 fields of the duplicates
-- ADDED:
-- m_dedupe.updateboth()
-- m_dedupe.melt856s
-- m_dedupe.update_lead
-- m_dedupe.update_sub
-- Added 2 columns to merge_map
-- MOBIUS 04-08-2013


-- DROP SCHEMA m_dedupe CASCADE;
-- schema to store the dedupe routine and intermediate data
CREATE SCHEMA m_dedupe;

CREATE TYPE mig_isbn_match AS (norm_isbn TEXT, norm_title TEXT, qual TEXT, bibid BIGINT);


-- function to calculate the normalized ISBN and title match keys
-- and the bibliographic portion of the quality score.  The normalized
-- ISBN key consists of the set of 020$a and 020$z normalized as follows:
--  * numeric portion of the ISBN converted to ISBN-13 format
--
-- The normalized title key is taken FROM the 245$a with the nonfiling
-- characters and leading and trailing whitespace removed, ampersands
-- converted to ' and ', other punctuation removed, and the text converted
-- to lowercase.
--
-- The quality score is a 19-digit integer computed by concatenating
-- counts of various attributes in the MARC records; see the get_quality
-- routine for details.
--
CREATE OR REPLACE FUNCTION m_dedupe.get_isbn_match_key (bib_id BIGINT, marc TEXT) RETURNS SETOF mig_isbn_match AS $func$
		use strict;
		use warnings;

		use MARC::Record;
		use MARC::File::XML (BinaryEncoding => 'utf8');
		use Business::ISBN;
		use Loghandler;
		use Data::Dumper;

		binmode(STDERR, ':bytes');
		binmode(STDOUT, ':utf8');
		binmode(STDERR, ':utf8');

		my $logf = new Loghandler("/tmp/log.log");

		#$logf->addLine('Script running.....');

		my $get_quality = sub {
			my $marc = shift;
			my $logf = shift;
			
			my $score = 0;
			$score+= score($marc,2,100,400,$logf,'245');
			$score+= score($marc,1,1,150,$logf,'100');
			$score+= score($marc,1,1.1,150,$logf,'110');
			$score+= score($marc,0,50,200,$logf,'6..');
			$score+= score($marc,0,50,100,$logf,'02.');
			
			$score+= score($marc,0,100,200,$logf,'246');
			$score+= score($marc,0,100,100,$logf,'130');
			$score+= score($marc,0,100,100,$logf,'010');
			$score+= score($marc,0,100,200,$logf,'490');
			$score+= score($marc,0,10,50,$logf,'830');
			
			$score+= score($marc,1,.5,50,$logf,'300');
			$score+= score($marc,0,1,100,$logf,'7..');
			$score+= score($marc,2,2,100,$logf,'50.');
			$score+= score($marc,2,2,100,$logf,'52.');
			
			$score+= score($marc,2,.5,200,$logf,'51.', '53.', '54.', '55.', '56.', '57.', '58.');

			return $score;
		};


		my ($bibid, $xml) = @_;


		$xml =~ s/(<leader>.........)./${1}a/;
		my $marc;
		eval {
			$marc = MARC::Record->new_from_xml($xml);
		};
		if ($@) {
		$logf->addLine("could not parse $bibid: $@");
			#elog("could not parse $bibid: $@\n");
			import MARC::File::XML (BinaryEncoding => 'utf8');
			return;
		}
		#$logf->addLine("Success Parse $bibid: $@");
		my @f245 = $marc->field('245');
		return unless @f245; # must have 245
		my $norm_title = norm_title($f245[0]);
		return unless $norm_title ne '';

		my @isbns = $marc->field('020');
		return unless @isbns; # must have at least 020

		my $qual = $get_quality->($marc, $logf);

#		$logf->addLine("quality = $qual");

		my @norm_isbns = norm_isbns(\@isbns, $logf);
		#$logf->addLine("I recieved these isbns from subroutine: ".$#norm_isbns);
		foreach my $isbn (@norm_isbns) {
		#$logf->addLine("$isbn, $norm_title, $qual, $bibid");
			return_next({ norm_isbn => $isbn, norm_title => $norm_title, qual => $qual, bibid => $bibid });
		}
		return undef;


		sub score
		{
			my ($marc) = shift;
			my ($type) = shift;
			my ($weight) = shift;
			my ($cap) = shift;
			my ($logf) = shift;
			my @tags = @_;
			my $ou = Dumper(@tags);
			$logf->addLine("Tags: $ou\n\nType: $type\nWeight: $weight\nCap: $cap");
			my $score = 0;			
			if($type == 0) #0 is field count
			{
				$logf->addLine("Calling count_field");
				$score = count_field($marc,$logf,\@tags);
			}
			elsif($type == 1) #1 is length of field
			{
				$logf->addLine("Calling field_length");
				$score = field_length($marc,$logf,\@tags);
			}
			elsif($type == 2) #2 is subfield count
			{
				$logf->addLine("Calling count_subfield");
				$score = count_subfield($marc,$logf,\@tags);
			}
			$score = $score * $weight;
			if($score > $cap)
			{
				$score = $cap;
			}
			$score = int($score);
			$logf->addLine("Weight and cap applied\nScore is: $score");
			return $score;
		}
		
		sub count_subfield
		{
			my ($marc) = $_[0];
			my $logf = $_[1];
			my @tags = @{$_[2]};
			my $total = 0;
			$logf->addLine("Starting count_subfield");
			foreach my $tag (@tags) 
			{
				my @f = $marc->field($tag);
				foreach my $field (@f)
				{
					my @subs = $field->subfields();
					my $ou = Dumper(@subs);
					$logf->addLine($ou);
					if(@subs)
					{
						$total += scalar(@subs);
					}
				}
			}
			$logf->addLine("Total Subfields: $total");
			return $total;
			
		}	
		
		sub count_field 
		{
			my ($marc) = $_[0];
			my $logf = $_[1];
			my @tags = @{$_[2]};
			my $total = 0;
			foreach my $tag (@tags) 
			{
				my @f = $marc->field($tag);
				$total += scalar(@f);
			}
			return $total;
		}

		sub field_length 
		{
			my ($marc) = $_[0];
			my $logf = $_[1];
			my @tags = @{$_[2]};

			my @f = $marc->field(@tags[0]);
			return 0 unless @f;
			my $len = length($f[0]->as_string);
			my $ou = Dumper(@f);
			$logf->addLine($ou);
			$logf->addLine("Field Length: $len");
			return $len;
		}

		sub norm_title {
			my $f245 = shift;
			my $sfa = $f245->subfield('a');
			return '' unless defined $sfa;
			my $nonf = $f245->indicator(2);
			$nonf = '0' unless $nonf =~ /^\d$/;
			if ($nonf == 0) {
				$sfa =~ s/^a //i;
				$sfa =~ s/^an //i;
				$sfa =~ s/^the //i;
			} else {
				$sfa = substr($sfa, $nonf);
			}
			$sfa =~ s/&/ and /g;
			$sfa = lc $sfa;
			$sfa =~ s/\[large print\]//;
			$sfa =~ s/[[:punct:]]//g;
			$sfa =~ s/^\s+//;
			$sfa =~ s/\s+$//;
			$sfa =~ s/\s+/ /g;
			return $sfa;
		}

		sub norm_isbns {
			my @isbns = @{$_[0]};
			my $logf = $_[1];
		#$logf->addLine("SUBROUTINE: I recieved these isbns: ".$#isbns);
			my %uniq_isbns = ();
			foreach my $field (@isbns) {
				my $sfa = $field->subfield('a');
		#$logf->addLine("I got this ISBN $sfa");
				my $norm = norm_isbn($sfa, $logf);
		#$logf->addLine("Normalize ISBN = $norm");
				$uniq_isbns{$norm}++ unless $norm eq '';

				my $sfz = $field->subfield('z');
				$norm = norm_isbn($sfz, $logf);
				$uniq_isbns{$norm}++ unless $norm eq '';
			}
			return sort(keys %uniq_isbns);
		}

		sub norm_isbn {
			my $str = $_[0];
			my $logf = $_[1];
			my $norm = '';
			return '' unless defined $str;
		#added because our test data only has 1 digit
			#return $str;
			

			$str =~ s/-//g;
			$str =~ s/^\s+//;
			$str =~ s/\s+$//;
			$str =~ s/\s+//g;
			$str = lc $str;
			my $isbn;
			if ($str =~ /^(\d{12}[0-9-x])/) {
				$isbn = $1;
				$norm = $isbn;
			} elsif ($str =~ /^(\d{9}[0-9x])/) {
				$isbn =  Business::ISBN->new($1);
				my $isbn13 = $isbn->as_isbn13;
				$norm = lc($isbn13->as_string);
				$norm =~ s/-//g;
			}
			return $norm;
		}
$func$ LANGUAGE PLPERLU;


-- Setup trigger to update marc xml cells on m_scenic.s_856_fix when the xml on biblio.record_entry is updated
DROP FUNCTION m_dedupe.updateboth() CASCADE;
CREATE FUNCTION m_dedupe.updateboth() RETURNS trigger AS $updateboth$
    BEGIN
		UPDATE m_scenic.s_856_fix SET lead_marc = NEW.marc WHERE lead_bibid = NEW.id;
		UPDATE m_scenic.s_856_fix SET sub_marc = NEW.marc WHERE sub_bibid = NEW.id;
        RETURN NEW;
    END;
$updateboth$ LANGUAGE plpgsql;


-- Setup Custom 856 copy from duplicated record
DROP TYPE  eight56s_melt CASCADE; 
CREATE TYPE eight56s_melt AS (bibid BIGINT, marc TEXT);


CREATE OR REPLACE FUNCTION m_dedupe.melt856s(bib_id BIGINT,marc_primary TEXT, sub_bib_id BIGINT, marc_secondary TEXT) RETURNS SETOF eight56s_melt AS $functwo$
		use strict;
		use warnings;

		use MARC::Record;
		use MARC::File::XML (BinaryEncoding => 'utf8');
		use Business::ISBN;
		use Loghandler;
		use Data::Dumper;
		use utf8;

		binmode(STDERR, ':bytes');
		binmode(STDOUT, ':utf8');
		binmode(STDERR, ':utf8');

		my $logf = new Loghandler("/tmp/log.log");


		$logf->addLine("*********************Started new function*********************");


		my ($bibid, $xml, $bibid2, $xml2) = @_;
		$logf->addLine("$bibid, $bibid2");


		$xml =~ s/(<leader>.........)./${1}a/;
		$xml2 =~ s/(<leader>.........)./${1}a/;
		my $marc;
		my $marc2;
		eval {
			$marc = MARC::Record->new_from_xml($xml);
			$marc2 = MARC::Record->new_from_xml($xml2);
		};
		if ($@) {
		$logf->addLine("could not parse $bibid: $@");
		$logf->addLine("could not parse $bibid2: $@");
			import MARC::File::XML (BinaryEncoding => 'utf8');
			return;
		}

		my @eight56s = $marc->field("856");
		my @eight56s_2 = $marc2->field("856");
		my @eights;
		my $original856 = $#eight56s + 1;
		
#LOGGING
#	$logf->addLine("First 856's (DB ID: $bibid)\n{");
#		foreach(@eight56s)
#		{
#		$logf->addLine("\t{");
#			@eights = $_->subfield('u');
#				$logf->addLine("\tu fields:");
#				foreach(@eights)
#				{
#					$logf->addLine("\t\t$_");
#				}
#				@eights = $_->subfield('z');
#				$logf->addLine("\tz fields:");
#				foreach(@eights)
#				{
#					$logf->addLine("\t\t$_");
#				}
#				@eights = $_->subfield('9');
#				$logf->addLine("\t9 fields:");
#				foreach(@eights)
#				{
#					$logf->addLine("\t\t$_");
#				}
#		$logf->addLine("\t}");
#		}
#		$logf->addLine("}\nSecond 856's (DB ID: $bibid2)\n{");		
#		foreach(@eight56s_2)
#		{
#		$logf->addLine("\t{");
#			@eights = $_->subfield('u');
#				$logf->addLine("\tu fields:");
#				foreach(@eights)
#				{
#					$logf->addLine("\t\t$_");
#				}
#				@eights = $_->subfield('z');
#				$logf->addLine("\tz fields:");
#				foreach(@eights)
#				{
#					$logf->addLine("\t\t$_");
#				}
#				@eights = $_->subfield('9');
#				$logf->addLine("\t9 fields:");
#				foreach(@eights)
#				{
#					$logf->addLine("\t\t$_");
#				}
#		$logf->addLine("\t}");
#		}
#		$logf->addLine("}");	
# ENDING LOGGING
		@eight56s = (@eight56s,@eight56s_2);

		my %urls;  


		foreach(@eight56s)
		{
			my $thisField = $_;
			
			# Just read the first $u and $z
			my $u = $thisField->subfield("u");
			my $z = $thisField->subfield("z");
		#$logf->addLine("I got u = $u and z = $z");
			
			if($u) #needs to be defined because its the key
			{
				if(!$urls{$u})
				{
					$urls{$u} = $thisField;
				}
				else
				{
					my @nines = $thisField->subfield("9");
					my $otherField = $urls{$u};
					my @otherNines = $otherField->subfield("9");
					my $otherZ = $otherField->subfield("z");
					if($#nines >-1 && $#otherNines>-1)
					{
						if(!$otherZ)
						{
		#				$logf->addLine("z didnt exist");
							if($z)
							{
								$otherField->add_subfields('z'=>$z);
		#						$logf->addLine("it exists here, so im adding it to og");
							}
						}
						foreach(@nines)
						{
							my $looking = $_;
							my $found = 0;
							foreach(@otherNines)
							{
		#					$logf->addLine("Searching for $looking");
								if($looking eq $_)
								{
									$found=1;
								}
							}
							if($found==0)
							{
		#					$logf->addLine("Didnt find $looking so adding it to og");
								$otherField->add_subfields('9' => $looking);
							}
						}
						$urls{$u} = $otherField;
					}
				}
			}
		}
		
		my $finalCount = scalar keys %urls;
		if($original856 != $finalCount)
		{
			$logf->addLine("There is a difference here!");
		}
		
		my $dump1=Dumper(\%urls);
#		$logf->addLine("$dump1");
#		$logf->addLine("Melted\n{");
		my @remove = $marc->field('856');
		$logf->addLine("Removing ".$#remove." 856 records");
		$marc->delete_fields(@remove);


		while ((my $internal, my $mvalue ) = each(%urls))
			{
#LOGGING METHODS
#			$logf->addLine("\t{");
#				@eights = $mvalue->subfield('u');
#				$logf->addLine("\tu fields:");
#				foreach(@eights)
#				{
#					$logf->addLine("\t\t$_");
#				}
#				@eights = $mvalue->subfield('z');
#				$logf->addLine("\tz fields:");
#				foreach(@eights)
#				{
#					$logf->addLine("\t\t$_");
#				}
#				@eights = $mvalue->subfield('9');
#				$logf->addLine("\t9 fields:");
#				foreach(@eights)
#				{
#					$logf->addLine("\t\t$_");
#				}
#				$logf->addLine("\t}");
#LOGGING METHODS ENDING

#This code was to change the indicator but that turned out to be bad
#my $nfield = MARC::Field->new(856, '4', '1','f'=>'temp');
#my @allsubs = $mvalue->subfields();
#foreach(@allsubs)
#{
#	my @thissub = @{$_};
#	$nfield->add_subfields(@thissub[0],@thissub[1]);
#}
#$nfield->delete_subfield(code => 'f'); */

				$marc->insert_grouped_field( $mvalue );
#				$logf->addLine("Inserted 856 back in");
			}
#		$logf->addLine("}");

# Compare the 2 marc records for debugging
#		my $mobutil = new Mobiusutil();
#		my @errors = @{$mobutil->compare2MARCObjects($marc,$marc2)};
#						my $errors;
#						foreach(@errors)
#						{
#							$errors.= $_."\r\n";
#						}
		#$logf->addLine("$errors");
		my $returning = $marc->as_xml_record();
		$returning =~ s/\n//g;
		$returning =~ s/\<\?xml version="1.0" encoding="UTF-8"\?\>//g;
		#$logf->addLine("$returning");


		return_next({ bibid => $bibid, marc => $returning });
		
$logf->addLine("*********************Ended new function*********************");
		return undef;

$functwo$ LANGUAGE PLPERLU;


DROP FUNCTION update_lead(dedupeid BIGINT, thisbidid BIGINT);
CREATE OR REPLACE FUNCTION m_dedupe.update_lead(dedupeid BIGINT, thisbidid BIGINT) RETURNS text AS $functhree$	
BEGIN
	UPDATE biblio.record_entry bre SET marc=
		(
			SELECT marc FROM
			(
				SELECT (a.melt856s::eight56s_melt).marc AS marc,(a.melt856s::eight56s_melt).bibid as bibid
				FROM (		
				SELECT m_dedupe.melt856s(
				mm.lead_bibid, mm.lead_marc,
				mm.sub_bibid, mm.sub_marc )
				FROM 
				 m_scenic.s_856_fix mm WHERE id=dedupeid
				) as a
			) as b WHERE b.bibid=bre.id
		)
		WHERE bre.id = thisbidid;
		UPDATE m_scenic.s_856_fix mm SET lead_marc=
		(
			SELECT marc FROM biblio.record_entry
			 as bre WHERE bre.id=thisbidid
		)
		WHERE lead_bibid = thisbidid;
--Return nothing because the work has been done
RETURN '';
END;
$functhree$ LANGUAGE plpgsql;

DROP FUNCTION update_sub(dedupeid BIGINT, thisbidid BIGINT);
CREATE OR REPLACE FUNCTION m_dedupe.update_sub(dedupeid BIGINT, thisbidid BIGINT) RETURNS text AS $funcfour$	
BEGIN
	UPDATE biblio.record_entry bre SET marc=
		(
			SELECT marc FROM
			(
				SELECT (a.melt856s::eight56s_melt).marc AS marc,(a.melt856s::eight56s_melt).bibid as bibid
				FROM (		
				SELECT m_dedupe.melt856s(
				mm.sub_bibid, mm.sub_marc,
				mm.lead_bibid, mm.lead_marc )
				FROM 
				 m_scenic.s_856_fix mm WHERE id=dedupeid
				) as a
			) as b WHERE b.bibid=bre.id
		)
		WHERE bre.id = thisbidid;
--Return nothing because the work has been done
RETURN '';
END;
$funcfour$ LANGUAGE plpgsql;

drop table m_scenic.s_856_fix;
CREATE TABLE m_scenic.s_856_fix
(
  lead_bibid bigint,
  sub_bibid bigint,
  lead_marc text,
  sub_marc text,
  id serial NOT NULL,
  done boolean DEFAULT false
);

INSERT INTO m_scenic.s_856_fix(lead_bibid,sub_bibid) select lead,sub from 
m_scenic.merge where 
sub in(select record from asset.call_number where label like'%##URI##%');


-- Fill the new columns with the marc xml from record_entry
UPDATE m_scenic.s_856_fix SET lead_marc = (SELECT marc FROM biblio.record_entry where id=lead_bibid);
UPDATE m_scenic.s_856_fix SET sub_marc = (SELECT marc FROM biblio.record_entry where id=sub_bibid);


-- Wipe out all of the 856 data because we are about to replace it all with merged $u $z $9 info
 DELETE FROM asset.uri_call_number_map WHERE call_number in 
(
	SELECT id from asset.call_number WHERE record in
	(SELECT lead_bibid from m_scenic.s_856_fix) AND label = '##URI##'
);

DELETE FROM asset.uri_call_number_map WHERE call_number in 
(
	SELECT id from asset.call_number WHERE record in
	(SELECT sub_bibid from m_scenic.s_856_fix) AND label = '##URI##'
);

DELETE FROM asset.uri WHERE id not in
(
	SELECT uri FROM asset.uri_call_number_map
);

DELETE FROM asset.call_number WHERE record in
	(SELECT lead_bibid from m_scenic.s_856_fix) AND label = '##URI##';
	
DELETE FROM asset.call_number WHERE record in
	(SELECT sub_bibid from m_scenic.s_856_fix) AND label = '##URI##'; 


-- Create a trigger to fire when the marc xml is updated
-- This will update the m_scenic.s_856_fix.lead_xml and m_scenic.s_856_fix.sub_xml
-- which will be nice when there are many to one merges	
CREATE TRIGGER updateboth AFTER UPDATE ON biblio.record_entry
    FOR EACH ROW EXECUTE PROCEDURE updateboth();
	


-- Activate the 2 marc update functions
-- This will update the marc xml on both soon-to-be-deleted records and the winning record

SELECT * FROM 
(SELECT m_dedupe.update_lead(
mm.id,mm.lead_bibid)
from m_scenic.s_856_fix mm) as a;


-- Perhaps this is not required because this is supposedly the soon-to-be-deleted record
-- But just for assurance! But at a cost of time!
--SELECT * FROM 
--(SELECT m_dedupe.update_sub(
--mm.id,mm.sub_bibid)
--from m_scenic.s_856_fix mm) as a;
-- Get rid of the trigger because we only needed it for the updating marc xml
DROP FUNCTION m_dedupe.updateboth() CASCADE;

