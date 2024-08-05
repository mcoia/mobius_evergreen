
-- 
-- DROP TABLE IF EXISTS mymig.monograph_part_conversion;
-- DROP TABLE IF EXISTS mymig.monograph_part_conversion_map;
-- DROP FUNCTION IF EXISTS mymig.monograph_part_get_current_job();
-- DROP FUNCTION IF EXISTS mymig.monograph_part_update_current_job();
-- DROP FUNCTION IF EXISTS mymig.monograph_part_conversion_error_generator();
-- DROP TABLE IF EXISTS mymig.monograph_part_conversion_job;
--

CREATE TABLE IF NOT EXISTS mymig.monograph_part_conversion_job
(
id bigserial NOT NULL,
start_time timestamp with time zone NOT NULL DEFAULT now(),
last_update_time timestamp with time zone NOT NULL DEFAULT now(),
status text default 'processing',    
current_action text,
current_action_num bigint default 0,
CONSTRAINT job_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS mymig.monograph_part_conversion
(
id bigserial NOT NULL,
original_label text,
new_label text,
res_query text,
manual boolean DEFAULT FALSE,
job bigint,
CONSTRAINT mymig_monograph_part_conversion_job FOREIGN KEY (job) REFERENCES mymig.monograph_part_conversion_job (id) MATCH SIMPLE
);

CREATE TABLE IF NOT EXISTS mymig.monograph_part_conversion_manual
(
original_label text,
new_label text
);

CREATE TABLE IF NOT EXISTS mymig.monograph_part_conversion_map
(
copy bigint,
record bigint,
acpm bigint,
original_label text,
new_label text,

job bigint,
CONSTRAINT mymig_monograph_part_conversion_map_job FOREIGN KEY (job) REFERENCES mymig.monograph_part_conversion_job (id) MATCH SIMPLE
);


CREATE INDEX IF NOT EXISTS mymig_monograph_part_conversion_map_record_idx
  ON mymig.monograph_part_conversion_map
  USING btree
  (record);


CREATE OR REPLACE FUNCTION mymig.monograph_part_get_current_job() RETURNS BIGINT AS
$func$
DECLARE

 job bigint;
 
BEGIN
    SELECT INTO job MAX(id) FROM mymig.monograph_part_conversion_job;
RETURN job;
END;
$func$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION mymig.monograph_part_update_current_job(step_text TEXT, finished boolean = FALSE) RETURNS TEXT AS
$func$
DECLARE
 status_text TEXT := 'processing';
 cjob BIGINT;
BEGIN

SELECT INTO cjob mymig.monograph_part_get_current_job();

    IF finished THEN
    status_text = 'complete';
    END IF;

    UPDATE mymig.monograph_part_conversion_job
    SET
    last_update_time = now(),
    status = status_text,
    current_action = step_text,
    current_action_num = current_action_num + 1
    WHERE
    id = cjob;
RETURN step_text;
END;
$func$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION mymig.monograph_part_conversion_error_generator(message TEXT = 'There was an error')
RETURNS boolean AS
$body$
BEGIN
    RAISE '%', message;
    RETURN FALSE;
END;
$body$
LANGUAGE plpgsql;


\set ON_ERROR_STOP on



-- start a new job

INSERT INTO mymig.monograph_part_conversion_job(status,current_action)
VALUES ('starting','Starting up...');

-- try to read any manual converted items
SELECT mymig.monograph_part_update_current_job('truncating mymig.monograph_part_conversion_manual');
TRUNCATE mymig.monograph_part_conversion_manual;

-- SELECT mymig.monograph_part_update_current_job('Reading file /mnt/evergreen/tmp/monograph_part_manual.csv');
-- \COPY mymig.monograph_part_conversion_manual(original_label,new_label) FROM /mnt/evergreen/tmp/monograph_part_manual.csv

-- Clean it up
SELECT mymig.monograph_part_update_current_job('Trimming white space from mymig.monograph_part_conversion_manual');
UPDATE mymig.monograph_part_conversion_manual
SET
new_label = btrim(new_label),
original_label = btrim(original_label);

SELECT mymig.monograph_part_update_current_job('Removing starting/trailing quote marks from mymig.monograph_part_conversion_manual');
-- remove quotation marks from the beginning and end
UPDATE mymig.monograph_part_conversion_manual
SET
new_label = REGEXP_REPLACE(new_label,'^"*(.*?)"*','\1','g'),
original_label = REGEXP_REPLACE(original_label,'^"*(.*?)"*','\1','g');

SELECT mymig.monograph_part_update_current_job('Trimming white space from mymig.monograph_part_conversion_manual');
UPDATE mymig.monograph_part_conversion_manual
SET
new_label = btrim(new_label),
original_label = btrim(original_label);

SELECT mymig.monograph_part_update_current_job('Deleting rows that are null or empty from mymig.monograph_part_conversion_manual');
DELETE FROM mymig.monograph_part_conversion_manual
WHERE
new_label IS NULL OR  new_label ='';



INSERT INTO
mymig.monograph_part_conversion
(original_label,new_label,res_query,job)
 
select label,(case when (label~'\-$' and "regexp_replace"!~'\-$') then "regexp_replace"||'-' else "regexp_replace" end),res_query,mymig.monograph_part_get_current_job()
from
(

-- Volume language

-- Vol. XX
select
label,
regexp_replace(
regexp_replace(
regexp_replace(
regexp_replace(
regexp_replace(
regexp_replace(label,'^\(?v[^abtsn\.\s,]*[\.\s,]+([^\.\s,]+)([^\)]*)\)?.*$','Vol. \1\2','gi'),
'\([^\)]+\)','','gi'),
'[\(\)]','','gi'),
'[&/]','-','gi'),
'\-$','','gi'),
'(\d{1,3})/(\d{4})','\1, \2','gi')
, 'Vol. XX' as res_query

from 
biblio.monograph_part
where 
label~*'^\(?v[^\.\s,]*[\.\s,]+[^\.\s,\)]+[\)\.\s]?$'
and
(
label~*'^\(?v[\s\.]'
or
label~*'^\(?vol\.'
or
label~*'^\(?vol[^\:]'
or
label~*'^\(?volume'
)

union all

-- VXX
select
label,regexp_replace(regexp_replace(btrim(label),'^^v\.*([^\-,\.\s/\(\)]*)$','Vol. \1','gi'),'&','-','g')
, 'VXX' as res_query
from 
biblio.monograph_part
where
btrim(label) ~'[^\s]'
and
btrim(label)~*'^v\.*[^\-,\.\s/\(\)]*$'
and btrim(label)!~*'^volume$'
and btrim(label)!~*'^vol$'
and
(
btrim(label)~*'^v'
or
label~*'^\(?v\.'
or
label~*'^\(?vol\.'
or
label~*'^\(?vol[^\:]'
or
label~*'^\(?volume'
)

union all

-- Vol X,(for rows starting with {digits}th ed)
select
label,regexp_replace(label,'^\s?\(?(\d+[tnrs][hdt]).*v[^\s\.,]*[\.\s]+([^\s\.,]+)\s?$','\1 ed., Vol. \2','gi')
, 'Vol X,(for rows starting with {digits}th) ed' as res_query
from 
biblio.monograph_part
where 
label~*'v[^\s\.,]*[\.\s]'
and
label!~*'p[^\s\.,]*[\.\s]'
and
label!~*'n[^\s\.,]*[\.\s]'
and
label!~*'se[^\s\.,]*[\.\s]'
and
label~*'^\s?\(?\d+[tnrs][hdt]\s+ed.*v[^\s\.,]*[\.\s]+[^\s\.,]+\s?$'

union all

-- Vol X,(for rows starting with {digits}th Series)
select
label,regexp_replace(label,'^\s?\(?(\d+[tnrs][hdt]).*v[^\s\.,]*[\.\s]+([^\s\.,]+)\s?$','\1 Series, Vol. \2','gi')
, 'Vol X,(for rows starting with {digits}th) Series' as res_query
from 
biblio.monograph_part
where 
label~*'v[^\s\.,]*[\.\s]'
and
label!~*'p[^\s\.,]*[\.\s]'
and
label!~*'n[^\s\.,]*[\.\s]'
and
label~*'^\s?\(?\d+[tnrs][hdt]\s+ser.*v[^\s\.,]*[\.\s]+[^\s\.,]+\s?$'

union all

-- Vol. X, YYYY
select
label,regexp_replace(label,'^v[^\d]*(\d+)\s+(\d{4})$','Vol. \1, \2','gi')
, 'Vol. X, YYYY' as res_query
from 
biblio.monograph_part
where 
label~'\d'
and
label~*'^v[^\d]*\d+\s+\d{4}$'

union all

-- Vol. X, YYYY-YYYY
select
label,regexp_replace(label,'^v[^\d]*(\d+)\s+(\d{4})[\-/](\d{4})$','Vol. \1, \2-\3','gi')
, 'Vol. X, YYYY-YYYY' as res_query
from 
biblio.monograph_part
where 
label~'\d'
and
label~*'^v[^\d]*\d+\s+\d{4}[\-/]\d{4}$'

union all

-- Vol. X, YYYY-YYYY (from YYYY/YYYY v.x)
select
label,regexp_replace(label,'^\s*(\d{4})[\\/\-](\d{4})[\s\.,\-]+v[^\s\.,]*[\s\.,]+(.+)$','Vol. \3, \1-\2','gi')
, ' Vol. X, YYYY-YYYY (from YYYY/YYYY v.x)' as res_query
from 
biblio.monograph_part
where 
label~'\d'
and
label~*'^\s*\d{4}[\\/\-]\d{4}[\s\.,\-]+v[^\s\.,]*[\s\.,]+.+$'

union all

-- Vol. X, YYYY-YYYY (from YYYY/YY v.x)
select
label,regexp_replace(label,'^\s*(\d{2})(\d{2})[\\/\-](\d{2})[\s\.,\-]+v[^\s\.,]*[\s\.,]+([^\-\s\.]+).*$','Vol. \4, \1\2-\1\3','gi')
, 'Vol. X, YYYY-YYYY (from YYYY/YY v.x)' as res_query
from 
biblio.monograph_part
where 
label~'\d'
and
label~*'^\s*\d{4}[\\/\-]\d{2}[\s\.,\-]+v[^\s\.,]*[\s\.,]+.+$'

union all

-- Vol. X, YYYY {season}
select
label,initcap(regexp_replace(label,'^v[^\d]*(\d+)\s+(\d{4})[\s\:]([afws][uaip][tlnmr][ultmi][men]?[nrg]?)\-?$','Vol. \1, \2:\3','gi'))
, 'Vol. X, YYYY {season}' as res_query
from 
biblio.monograph_part
where 
label~'\d'
and
label~*'^v[^\d]*\d+\s+\d{4}'
and
(
label~*'autumn' or
label~*'fall' or
label~*'winter' or
label~*'summer' or
label~*'spring'
)

union all

-- Vol. X, YYYY {season/season}
select
label,initcap(regexp_replace(label,'^v[^\d]*(\d+\.?\d*)\s+(\d{4})\s+([^\d]+)/([^\d]+)$','Vol. \1, \2:\3/\4','gi'))
, 'Vol. X, YYYY {season/season}' as res_query
from 
biblio.monograph_part
where 
label~'\d'
and
label~'[/]'
and
label~*'^v[^\d]*\d+\.?\d*\s+\d{4}\s+[^\d]+/[^\d]+$'
and
(
label~*'autumn' or
label~*'fall' or
label~*'winter' or
label~*'summer' or
label~*'spring'
)

union all

-- YYYY Vol. XX
select
label,regexp_replace(label,'^\(?(\d{4})[\\/\:\.,\s]+v[^\d]*[\s\.\-]+([^\s\.\-,]+)\s?$','Vol. \2, \1','gi')
, 'YYYY Vol. XX' as res_query
from 
biblio.monograph_part
where 
label~*'^\(?\d{4}[\\/\:\.,\s]+v[^\d]*[\s\.\-]+[^\s\.\-,]+\s?$'

union all

-- "v. 1, disc 1-4"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:]+):?[,\.\s\-]+d[^,\.\s]*[,\.\s\-]+([^,\.\s\-]+)[,\.\s\-]+([^,\.\s\-]+)\s?$','Vol. \1, Disc \2-\3','gi')
, 'v. 1, disc 1-4' as res_query
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:]+:?[,\.\s\-]+d[^,\.\s]*[,\.\s\-]+[^,\.\s\-]+[,\.\s\-]+[^,\.\s\-]+\s?$'

union all

-- "v.1, 1897-1942"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[:;,\.\s\-]+(\d{4})[,\.\s\-\\/]+(\d{4})\s?$','Vol. \1, \2-\3','gi')
, 'v.1, 1897-1942' as res_query
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[:;,\.\s\-]+\d{4}[,\.\s\-\\/]+\d{4}\s?$'

union all

-- "v.1/2 1913/1989"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[/:;,\-]+([^,\.\s\-:/;]+)[/:;,\.\s\-]+(\d{4})[,\.\s\-\\/]+(\d{4})\s?$','Vol. \1-\2, \3-\4','gi')
, 'v.1/2 1913/1989' as res_query
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[/:;,\-]+[^,\.\s\-:/;]+[/:;,\.\s\-]+\d{4}[,\.\s\-\\/]+\d{4}\s?$'

union all

-- "v.5, no. 4 1988"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+no?[,\.\s\-:/;]+([^,\.\s\-:/;]+)[,\.\s\-:/;\(\)]+(\d{4})[\(\)\s]?$','Vol. \1, No. \2, \3','gi')
, 'v.5, no. 4 1988' as res_query
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+no?[,\.\s\-:/;]+[^,\.\s\-:/;]+[,\.\s\-:/;\(\)]+\d{4}[\(\)\s]?$'

union all

-- "v.1 A-C"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+([^\s\-&])[\s\-&]+([^\s\-&])\s?$','Vol. \1 \2-\3','gi')
, 'v.1 A-C' as res_query
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+[^\s\-&][\s\-&]+[^\s\-&]\s?$'

union all

-- "v. 1 No. 1"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+no[\.\s,\-]+([^,\.\s\-:/;]+)\s?$','Vol. \1, No. \2','gi')
, 'v. 1 No. 1' as res_query
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+no[\.\s,\-]+[^,\.\s\-:/;]+\s?$'

union all

-- "v. 1/pt. 2"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+pt[\.\s,\-]+([^,\.\s\-:/;]+)\s?$','Vol. \1, Part \2','gi')
, 'v. 1/pt. 2' as res_query
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+pt[\.\s,\-]+[^,\.\s\-:/;]+\s?$'

union all

-- "pt.2/v.1"
select
label,regexp_replace(label,'^pt[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+v[\.\s,\-]+([^,\.\s\-:/;]+)\s?$','Vol. \2, Part \1','gi')
, 'pt.2/v.1' as res_query
from 
biblio.monograph_part
where 
label~*'^pt[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+v[\.\s,\-]+[^,\.\s\-:/;]+\s?$'

union all

-- "v.10 c.1"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+c\.[\.\s,\-]*([^,\.\s\-:/;]+)\s?$','Vol. \1, Copy \2','gi')
, 'v.10 c.1' as res_query
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+c\.[\.\s,\-]*[^,\.\s\-:/;]+\s?$'

union all

-- "Vol. 1,pt2 "
select
label,regexp_replace(label,'^vol[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+pt[\.\s,\-]*([^,\.\s\-:/;]+)\s?$','Vol. \1, Part \2','gi')
, 'Vol. 1,pt2' as res_query
from 
biblio.monograph_part
where 
label~*'^vol[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+pt[\.\s,\-]*[^,\.\s\-:/;]+\s?$'

union all

-- "v.1 1974/75"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+\(?(\d{2})(\d{2})[/\-]\(?(\d{2})\)?\s?$','Vol. \1, \2\3-\2\4','gi')
, 'v.1 1974/75' as res_query
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+\(?\d{4}[/\-]\(?\d{1,2}\)?\s?$'

union all




-- disk language

-- Disc X
select
label,regexp_replace(label,'^d[^\d]*(\d*)(.*)$','Disc \1','gi')
, 'Disc X' as res_query
from 
biblio.monograph_part
where 
label~'\d'
and
label!~'&'
and
label!~','
and
label!~'\-'
and
(
label~*'^d\.\s*\d*$'
or
label~*'^dis[^\s]*\s*?\d*\s*$'
)
and
label!~*'season'

union all

-- Disc X- {no number}
select
label,regexp_replace(label,'^dis[csk]*[^\d]*(\d*)\-+.*','Disc \1','gi')
, 'Disc X- {no number}' as res_query
from 
biblio.monograph_part
where 
label~'\d'
and
label!~'&'
and
label!~','
and
label~'\-'
and
(
label~*'^dis[csk]*[^\d]*\d*\-+[^\dd*]'
)
and
label!~*'season'


union all

-- Disc X-Y
select
label,regexp_replace(label,'^dis[csk]*[^\d]*(\d+)[\-&\s]+(\d+)\s*','Disc \1-\2','gi')
, 'Disc X-Y' as res_query
from 
biblio.monograph_part
where
label!~'\d{4}' and
label~*'^dis[csk]*[^\d]*\d+[\-&\s]+\d+\s*$'

union all

-- Disc X-Disc Y
select
label,regexp_replace(label,'^dis[csk]*[\-&\s,]*(\d*)[\-&\s,]*dis[csk]*[\-&\s,]*(\d*).*','Disc \1-\2','gi')
, 'Disc X-Disc Y' as res_query
from 
biblio.monograph_part
where 
label~'\d'
and
label~'\-'
and
(
label~*'^dis[csk]*[\-&\s,]*\d*[\-&\s,]*dis[csk]*[\-&\s,]*\d.*$'
)
and
label!~*'season'

union all

-- DVD {anything} Disc X
select
label,regexp_replace(label,'^dvd[^\d]+dis[csk]*[\-&\s,]+(\d*)$','Disc \1','gi')
, 'DVD {anything} Disc X' as res_query
from 
biblio.monograph_part
where 
label~'\d'
and
label~*'^dvd[^\d]*\d*.*$'
and
label!~*'season'

union all

-- season X Disc Y
select
label,regexp_replace(label,'^.*?season[^\d]*([\d]+).*?dis[cks]*[^\d]*([\d]+)$','Season \1, Disc \2','gi')
, 'season X Disc Y' as res_query
from 
biblio.monograph_part
where 
btrim(label)~*'^.*?season[^\d]*[\d]+.*?dis[cks]*[^\d]*[\d]+$'
and
label!~*'vol'
and
label!~*'part'

union all

-- season X Disc Y-Z
select
label,regexp_replace(label,'^.*?season[^\d]*([\d]+).*?dis[cks]*[^\d]*([\d]+)[\s\-&\.,]+(\d+)$','Season \1, Disc \2-\3','gi')
, 'season X Disc Y-Z' as res_query
from 
biblio.monograph_part
where 
btrim(label)~*'^.*?season[^\d]*[\d]+.*?dis[cks]*[^\d]*[\d]+[\s\-&\.,]+\d+$'
and
label!~*'vol'
and
label!~*'part'

union all

-- season W Disc X-Y-Z -> Season \1, Disc \2-\4'
select
label,regexp_replace(label,'^.*?season[^\d]*([\d]+).*?dis[cks]*[^\d]*([\d]+)[\s\-&\.,]+(\d+)[\s\-&\.,]+(\d+)$','Season \1, Disc \2-\4','gi')
, 'season W Disc X-Y-Z -> Season \1, Disc \2-\4' as res_query
from 
biblio.monograph_part
where 
btrim(label)~*'^.*?season[^\d]*[\d]+.*?dis[cks]*[^\d]*[\d]+[\s\-&\.,]+\d+[\s\-&\.,]+\d+$'
and
label!~*'vol'
and
label!~*'part'

union all



-- Year

-- Xth YYYY/YYYY -> YYYY-YYYY
select
label,regexp_replace(label,'^\s?\(?\d+[tnrs][hdt].*(\d{4})[\s,\.\\/\-]+(\d{4})\s?$','\1-\2','gi')
, 'Xth YYYY/YYYY -> YYYY-YYYY' as res_query
from 
biblio.monograph_part
where
label!~*'v[^\s\.,]*[\.\s]'
and
label!~*'p[^\s\.,]*[\.\s]'
and
label!~*'n[^\s\.,]*[\.\s]'
and
label~*'^\s?\(?\d+[tnrs][hdt].*\d{4}[\s,\.\\/\-]+\d{4}\s?$'
and
label!~*'^\s?\(?\d+[tnrs][hdt].*\d{4}[\s,\.\\/\-]+\d{4}[\s,\.\\/\-]\d{4}[\s,\.\\/\-]+\d{4}\s?$'

union all

-- Xth YYYY/YYYY - YYYY/YYYY -> YYYY-YYYY
select
label,regexp_replace(label,'^\s?\(?\d+[tnrs][hdt][^\d]+(\d{4})[\s,\.\\/\-]+(\d{4})[\s,\.\\/\-](\d{4})[\s,\.\\/\-]+(\d{4})\s?$','\1-\4','gi')
, 'Xth YYYY/YYYY - YYYY/YYYY -> YYYY-YYYY' as res_query
from 
biblio.monograph_part
where
label!~*'v[^\s\.,]*[\.\s]'
and
label!~*'p[^\s\.,]*[\.\s]'
and
label!~*'n[^\s\.,]*[\.\s]'
and
label~*'^\s?\(?\d+[tnrs][hdt][^\d]+\d{4}[\s,\.\\/\-]+\d{4}[\s,\.\\/\-]\d{4}[\s,\.\\/\-]+\d{4}\s?$'

union all

-- YYYY/YYYY - YYYY/YYYY -> YYYY-YYYY   (from YYYY/YY-YYYY/YY)
select
label,regexp_replace(label,'^\s?\(?(\d{4})[\s,\.\\/\-]+\d{2}[\s,\.\\/\-](\d{2})\d{2}[\s,\.\\/\-]+(\d{2})\s?$','\1-\2\3','gi')
, 'YYYY/YYYY - YYYY/YYYY -> YYYY-YYYY   (from YYYY/YY-YYYY/YY)' as res_query
from 
biblio.monograph_part
where
label!~*'v[^\s\.,]*[\.\s]'
and
label!~*'p[^\s\.,]*[\.\s]'
and
label!~*'n[^\s\.,]*[\.\s]'
and
label~*'^\s?\(?\d{4}[\s,\.\\/\-]+\d{2}[\s,\.\\/\-]\d{4}[\s,\.\\/\-]+\d{2}\s?$'

union all

-- YYYY/YYYY - YYYY/YYYY -> YYYY-YYYY   (from YYYY/YYYY-YYYY/YYYY)
select
label,regexp_replace(label,'^\s?\(?(\d{4})[\s\.\\/\-]+\d{4}[\s\.\\/\-]\d{4}[\s\.\\/\-]+(\d{4})\s?$','\1-\2','gi')
, 'YYYY/YYYY - YYYY/YYYY -> YYYY-YYYY   (from YYYY/YYYY-YYYY/YYYY)' as res_query
from 
biblio.monograph_part
where
label!~*'v[^\s\.,]*[\.\s]'
and
label!~*'p[^\s\.,]*[\.\s]'
and
label!~*'n[^\s\.,]*[\.\s]'
and
label~*'^\s?\(?\d{4}[\s\.\\/\-]+\d{4}[\s\.\\/\-]\d{4}[\s\.\\/\-]+\d{4}\s?$'

union all

-- YYYY-YYYY
select
label,btrim(regexp_replace(label,'^\(?(\d{4})[/\-\\&\s]+\(?(\d{4})\)?\s*([^\s\-\.]?)[\s\.\-\:/\\]?$','\1-\2 \3','gi'))
, 'YYYY-YYYY' as res_query
from 
biblio.monograph_part
where 
label~*'^\(?\d{4}[/\-\\&\s]+\(?\d{4}\)?\s*[^\s\-\.]?[\s\.\-\:/\\]?$'

union all

-- YYYY-YY {optional qualifier}
select
label,regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[\\/\-]\(?(\d{2})\)?\s+\(?([^\s\)]+)\)?\-?\s?$','\1\2-\1\3 \4','gi')
, 'YYYY-YY {optional qualifier}' as res_query
from 
biblio.monograph_part
where 
label !~*'v\.'
and
label !~*'supp?l?'
and
label !~*'pt\.'
and
label!~*'autumn' and
label!~*'fall' and
label!~*'winter' and
label!~*'summer' and
label!~*'spring'
and
(
label!~*'jan' and
label!~*'feb' and
label!~*'mar' and
label!~*'apr' and
label!~*'may' and
label!~*'jun' and
label!~*'jul' and
label!~*'aug' and
label!~*'sep' and
label!~*'oct' and
label!~*'nov' and
label!~*'dec'
)
and
label~*'^\(?\d{4}[/\-]\(?\d{2}\)?\s+[^\s]+\s?$'

union all

-- YYYY-YYYYY {optional qualifier}
select
label,regexp_replace(label,'^\s?\(?(\d{4})[\\/\-]\(?(\d{4})\)?\s+\(?([^\s\)]+)\)?\-?\s?$','\1-\2 \3','gi')
, 'YYYY-YYYYY {optional qualifier}' as res_query
from 
biblio.monograph_part
where 
label !~*'v\.'
and
label !~*'supp?l?\.'
and
label !~*'pt\.'
and
label!~*'autumn' and
label!~*'fall' and
label!~*'winter' and
label!~*'summer' and
label!~*'spring'
and
(
label!~*'jan' and
label!~*'feb' and
label!~*'mar' and
label!~*'apr' and
label!~*'may' and
label!~*'jun' and
label!~*'jul' and
label!~*'aug' and
label!~*'sep' and
label!~*'oct' and
label!~*'nov' and
label!~*'dec'
)
and
label~*'^\(?\d{4}[/\-]\(?\d{4}\)?\s+[^\s]+\s?$'
and
label!~*'^\(?\d{4}[/\-]\(?\d{4}\)?\s+\d{4}[/\-]\(?\d{4}\)?.*' -- remove eg. '2004/2005 2004/2005', handled in another query

union all

-- YYYY {month}
select
label,initcap(
concat(
regexp_replace(label,'^\(?(\d{4})[\\/,\s\:]*\(?([^\d\.\(/\-,\s\:]+)\.?\s?\)?$','\1:','gi'),
to_char(
    to_date(regexp_replace(label,'^\(?(\d{4})[\\/,\s\:]*\(?([^\d\.\(/\-,\s\:]+)\.?\s?\)?$','\2','gi'),'Mon'),
    'Mon'
    )
))
, 'YYYY {month}' as res_query
from 
biblio.monograph_part
where 
label~*'^\(?\d{4}[\\/,\s\:]*\(?[^\d\.\(/\-,\s\:]+\.?\s?\)?$'
and

label!~*'autumn' and
label!~*'fall' and
label!~*'winter' and
label!~*'summer' and
label!~*'spring'

and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

-- YYYY/MM {month}
select
label,initcap(
concat(
regexp_replace(label,'^\(?(\d{4})[/\-]\(?\d{1,2}\)?\s+\(?([^\s\)/\.\-]+)\)?\s?$','\1:','gi'),
to_char(
    to_date(regexp_replace(label,'^\(?(\d{4})[/\-]\(?\d{1,2}\)?\s+\(?([^\s\)/\.\-]+)\)?\s?$','\2','gi'),'Mon'),
    'Mon'
    )
))
, 'YYYY/MM {month}' as res_query
from 
biblio.monograph_part
where 
label~*'^\(?\d{4}[/\-]\(?\d{1,2}\)?\s+\(?[^\s\)/\.\-]+\)?\s?$'
and

label!~*'autumn' and
label!~*'fall' and
label!~*'winter' and
label!~*'summer' and
label!~*'spring'

and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

-- YYYY/MM {month} / {month}
select
label,initcap(
concat(
regexp_replace(label,'^\(?(\d{2})(\d{2})[/\-]+\(?(\d{1,2})\)?\s+\(?([^\s\)/\.\-]+)[\)?\s?\\/]?([^\s\)/\.\-]+)\)?\s?$','\1\2:','gi'),
to_char(
    to_date(regexp_replace(label,'^\(?(\d{2})(\d{2})[/\-]+\(?(\d{1,2})\)?\s+\(?([^\s\)/\.\-]+)[\)?\s?\\/]?([^\s\)/\.\-]+)\)?\s?$','\4','gi'),'Mon'),
    'Mon'
    ),
regexp_replace(label,'^\(?(\d{2})(\d{2})[/\-]+\(?(\d{1,2})\)?\s+\(?([^\s\)/\.\-]+)[\)?\s?\\/]?([^\s\)/\.\-]+)\)?\s?$','-\1\3:','gi'),
to_char(
    to_date(regexp_replace(label,'^\(?(\d{2})(\d{2})[/\-]+\(?(\d{1,2})\)?\s+\(?([^\s\)/\.\-]+)[\)?\s?\\/]?([^\s\)/\.\-]+)\)?\s?$','\5','gi'),'Mon'),
    'Mon'
    )
))
, 'YYYY/MM {month} / {month}' as res_query
from 
biblio.monograph_part
where 
label~*'^\(?\d{4}[/\-]\(?\d{1,2}\)?\s+\(?[^\s\)/\.\-]+[\\/\-]+[^\s\)/\.\-]+\)?\s?$'
and

label!~*'autumn' and
label!~*'fall' and
label!~*'winter' and
label!~*'summer' and
label!~*'spring'

and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

-- YYYY {month-month}
select
label,initcap(
concat(
regexp_replace(label,'^\(?(\d{4})\s?\:?\(?([^\d\.\(/\-,\s]+)\.?[\-/\\]+([^\d\.\-/\)]+)\.?\s?\)?$','\1:','gi'),
to_char(
    to_date(regexp_replace(label,'^\(?(\d{4})\s?\:?\(?([^\d\.\(/\-,\s]+)\.?[\-/\\]+([^\d\.\-/\)]+)\.?\s?\)?$','\2','gi'),'Mon'),
    'Mon'
    ),
    '-',
to_char(
    to_date(regexp_replace(label,'^\(?(\d{4})\s?\:?\(?([^\d\.\(/\-,\s]+)\.?[\-/\\]+([^\d\.\-/\)]+)\.?\s?\)?$','\3','gi'),'Mon'),
    'Mon'
    )
))
, 'YYYY {month-month}' as res_query
from 
biblio.monograph_part
where 
label~*'^\(?\d{4}\s?\:?\(?[^\d\.\(/\-,\s]+\.?[\-/\\]+[^\d\.\-/]+\.?\s?\)?$'
and

label!~*'autumn' and
label!~*'fall' and
label!~*'winter' and
label!~*'summer' and
label!~*'spring'

and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

-- YYYY {month-month} (from YYYY month day - month day)
select
label,initcap(
concat(
regexp_replace(label,'^\(?(\d{4})[\s\:\.]+([^\s\:\.]+)[\s\:\.]+(\d+)[\s\:\.]?\-\s*([^\s\:\.]+)[\s\:\.]+(\d+).*$','\1:','gi'),
to_char(
    to_date(regexp_replace(label,'^\(?(\d{4})[\s\:\.]+([^\s\:\.]+)[\s\:\.]+(\d+)[\s\:\.]?\-\s*([^\s\:\.]+)[\s\:\.]+(\d+).*$','\2','gi'),'Mon'),
    'Mon'),
regexp_replace(label,'^\(?(\d{4})[\s\:\.]+([^\s\:\.]+)[\s\:\.]+(\d+)[\s\:\.]?\-\s*([^\s\:\.]+)[\s\:\.]+(\d+).*$',' \3 - \1:','gi'),
to_char(
    to_date(regexp_replace(label,'^\(?(\d{4})[\s\:\.]+([^\s\:\.]+)[\s\:\.]+(\d+)[\s\:\.]?\-\s*([^\s\:\.]+)[\s\:\.]+(\d+).*$','\4','gi'),'Mon'),
    'Mon'),
regexp_replace(label,'^\(?(\d{4})[\s\:\.]+([^\s\:\.]+)[\s\:\.]+(\d+)[\s\:\.]?\-\s*([^\s\:\.]+)[\s\:\.]+(\d+).*$',' \5','gi')
))
, 'YYYY {month-month} (from YYYY month day - month day)' as res_query
from 
biblio.monograph_part
where 
label~*'^\(?\d{4}[\s\:\.]+[^\s\:\.]+[\s\:\.]+\d+[\s\:\.]?\-\s*[^\s\:\.]+[\s\:\.]+\d+.*$'
and

label!~*'autumn' and
label!~*'fall' and
label!~*'winter' and
label!~*'summer' and
label!~*'spring'

and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all




-- No. Language

-- No. X YYYY
select
label,regexp_replace(label,'no\.?\s?(\d+);?\s+\(?(\d\d\d\d)\)?$','No. \1, \2','gi')
, 'No. X YYYY' as res_query
from 
biblio.monograph_part
where 
label~'\d{4}'
and
label~'\s'
and
label!~'[\\/\-\:,]'
and
label~'^no'
and
label~*'^no\.?\s?\d+;?\s+\(?\d\d\d\d\)?$'

union all

-- No. X YYYY:{season}
select
label,initcap(regexp_replace(label,'no\.?\s?(\d+);?\s+\(?(\d\d\d\d)\)?.*([afws][uaip][tlnmr][ultmi][men]?[nrg]?)$','No. \1, \2:\3','gi'))
, 'No. X YYYY:{season}' as res_query
from 
biblio.monograph_part
where 
label~'\d{4}'
and
label~'\s'
and
label!~'[\\/\-]'
and
label~'^no'
and
label~*'^no\.?\s?\d+;?\s+\(?\d\d\d\d\)?.+$'
and
(
label~*'autumn' or
label~*'fall' or
label~*'winter' or
label~*'summer' or
label~*'spring'
)

union all

-- No. X YYYY:{season/season}
select
label,initcap(regexp_replace(label,'no\.?\s?(\d+);?\s+\(?(\d\d\d\d)\)?\s?\(?[\d/]*\)?\s?([^\d\)]+)\)?$','No. \1, \2:\3','gi'))
, 'No. X YYYY:{season/season}' as res_query
from 
biblio.monograph_part
where 
label~'\d{4}'
and
label~'\s'
and
label~'[/]'
and
label~'^no'
and
label~*'^no\.?\s?\d+;?\s+\(?\d\d\d\d\)?.+$'
and
(
label~*'autumn' or
label~*'fall' or
label~*'winter' or
label~*'summer' or
label~*'spring'
)

union all

-- No. X YYYY:{month}
select
label,initcap(
concat(
regexp_replace(label,'no\.?\s?(\d+);?\s+\(?(\d\d\d\d)\)?\s?\(?\:?[\d/]*\)?\s?([^\d\)\.\:,]+)\)?\.?,?$','No. \1, \2:','gi'),
to_char(
    to_date(regexp_replace(label,'no\.?\s?(\d+);?\s+\(?(\d\d\d\d)\)?\s?\(?\:?[\d/]*\)?\s?([^\d\)\.\:,]+)\)?\.?,?$','\3','gi'),'Mon'),
    'Mon'
    )
))
, 'No. X YYYY:{month}' as res_query
from 
biblio.monograph_part
where 
label~'\d{4}'
and
label~'\s'
and
label!~'[/\-]'
and
label~'^no'
and
label~*'^no\.?\s?\d+;?\s+\(?\d\d\d\d\)?.*[^\d]+$'
and

label!~*'autumn' and
label!~*'fall' and
label!~*'winter' and
label!~*'summer' and
label!~*'spring'

and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

-- No. X YYYY:{month} but where the month is mentioned first instead of second
select
label,initcap(
concat(
regexp_replace(label,'^no\.?\s?(\d+);?\s+\(?([^\d\.]+)\)?\.*\s+\(?(\d\d\d\d)\)?.*$','No. \1, \3:','gi'),
to_char(
    to_date(
        regexp_replace(label,'^no\.?\s?(\d+);?\s+\(?([^\d\.]+)\)?\.*\s+\(?(\d\d\d\d)\)?.*$','\2','gi'),'Mon'),
        'Mon')
))
, 'No. X YYYY:{month} but where the month is mentioned first instead of second' as res_query
from 
biblio.monograph_part
where 
label~'\d{4}'
and
label~'\s'
and
label!~'[/\-]'
and
label~'^no'
and
label~*'^no\.?\s?\d+;?\s+\(?[^\d\s]+\s+\(?\d\d\d\d\)?.*$'
and

label!~*'autumn' and
label!~*'fall' and
label!~*'winter' and
label!~*'summer' and
label!~*'spring'

and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

-- No. X Vol. YYYY:{month} (starting with XX/YY)
select
label,initcap(
concat(
regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+(\d{4})[\:,\.\s]*\(?\d*\)?[\:,\.\s]*([^\.\s/]+)\.?$','Vol. \1, No. \2, \3:','gi'),
to_char(
    to_date(regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+(\d{4})[\:,\.\s]*\(?\d*\)?[\:,\.\s]*([^\.\s/]+)\.?$','\4','gi'),'Mon'),
    'Mon'
    )
))
, 'No. X Vol. YYYY:{month} (starting with XX/YY)' as res_query
from 
biblio.monograph_part
where 
label~'\d{4}'
and
label~*'^\s?\d{1,3}/+\d{1,3}[\s\.,]+\d{4}[\:,\.\s]*\(?\d*\)?[\:,\.\s]*[^\.\s/]+\.?$'
and
label!~*'autumn' and
label!~*'fall' and
label!~*'winter' and
label!~*'summer' and
label!~*'spring'

and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

-- No. X Vol. YYYY:{month-month} (starting with XX/YY)
select
label,initcap(
concat(
regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+(\d{4})[\:,\.\s]+([^\(\.\s/]+)[\:,\.\s/]+([^\.\s/]+)\.?$','Vol. \1, No. \2, \3:','gi'),
to_char(
    to_date(regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+(\d{4})[\:,\.\s]+([^\(\.\s/]+)[\:,\.\s/]+([^\.\s/]+)\.?$','\4','gi'),'Mon'),
    'Mon'
    ),
regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+(\d{4})[\:,\.\s]+([^\(\.\s/]+)[\:,\.\s/]+([^\.\s/]+)\.?$',' - \3:','gi'),
to_char(
    to_date(regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+(\d{4})[\:,\.\s]+([^\(\.\s/]+)[\:,\.\s/]+([^\.\s/]+)\.?$','\5','gi'),'Mon'),
    'Mon'
    )
))
, 'No. X Vol. YYYY:{month-month} (starting with XX/YY)' as res_query
from 
biblio.monograph_part
where 
label~'\d{4}'
and
label~*'^\s?\d{1,3}/+\d{1,3}[\s\.,]+\d{4}[\:,\.\s]+[^\(\.\s/]+[\:,\.\s/]+[^\.\s/]+\.?$'
and
label!~*'autumn' and
label!~*'fall' and
label!~*'winter' and
label!~*'summer' and
label!~*'spring'

and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

-- No. X Vol. YYYY:{month} (starting with XX/YY month YYYY)
select
label,initcap(
concat(
regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+([^\d\s\-/\\]+)[\s\.,]+(\d{4})\.?$','Vol. \1, No. \2, \4:','gi'),
to_char(
    to_date(
        regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+([^\d\s\-/\\]+)[\s\.,]+(\d{4})\.?$','\3','gi'),'Mon'),
        'Mon'
    )
))
, 'No. X Vol. YYYY:{month} (starting with XX/YY month YYYY)' as res_query
from 
biblio.monograph_part
where 
label~'\d{4}'
and
label~*'^\s?\d{1,3}/+\d{1,3}[\s\.,]+[^\d\s\-/\\]+[\s\.,]+\d{4}\.?$'
and
label!~*'autumn' and
label!~*'fall' and
label!~*'winter' and
label!~*'summer' and
label!~*'spring'

and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

-- No. X YYYY:{month-month}
select
label,initcap(
concat(
regexp_replace(label,'no\.?\s?(\d+);?\s+\(?(\d\d\d\d)\)?\s?\(?\:?[\d/]*\)?\s?([^\d\)\.\:,/]+)[\)\.,/\-]+([^\d\)\.\:,/]+)\.?$','No. \1, \2:','gi'),
to_char(
    to_date(regexp_replace(label,'no\.?\s?(\d+);?\s+\(?(\d\d\d\d)\)?\s?\(?\:?[\d/]*\)?\s?([^\d\)\.\:,/]+)[\)\.,/\-]+([^\d\)\.\:,/]+)\.?$','\3','gi'),'Mon'),
    'Mon'
    ),
regexp_replace(label,'no\.?\s?(\d+);?\s+\(?(\d\d\d\d)\)?\s?\(?\:?[\d/]*\)?\s?([^\d\)\.\:,/]+)[\)\.,/\-]+([^\d\)\.\:,/]+)\.?$',' - \2:','gi'),
to_char(
    to_date(regexp_replace(label,'no\.?\s?(\d+);?\s+\(?(\d\d\d\d)\)?\s?\(?\:?[\d/]*\)?\s?([^\d\)\.\:,/]+)[\)\.,/\-]+([^\d\)\.\:,/]+)\.?$','\4','gi'),'Mon'),
    'Mon'
    )
))
, 'No. X YYYY:{month-month}' as res_query
from 
biblio.monograph_part
where 
label~'\d{4}'
and
label~'\s'
and
label~'[/\-\\]'
and
label~'^no'
and
label~*'^no\.?\s?\d+;?\s+\(?\d\d\d\d\)?.+$'
and
label!~*'\d\d\d\d/\d\d\d\d'
and
label!~*'\d[\-/]\d\d\d\d'
and

label!~*'autumn' and
label!~*'fall' and
label!~*'winter' and
label!~*'summer' and
label!~*'spring'

and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

-- No. X YYYY:{month-month} (with beginning format XX/YY month-month YYYY)
select
label,initcap(
concat(
regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+([^\d\s\-/\\]+)[\s\-/\\]+([^\d\s\-/\\]+)[\s\.,]+(\d{4})\.?$','Vol. \1, No. \2, \5:','gi'),
to_char(
    to_date(regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+([^\d\s\-/\\]+)[\s\-/\\]+([^\d\s\-/\\]+)[\s\.,]+(\d{4})\.?$','\3','gi'),'Mon'),
    'Mon'
    ),
regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+([^\d\s\-/\\]+)[\s\-/\\]+([^\d\s\-/\\]+)[\s\.,]+(\d{4})\.?$',' - \5:','gi'),
to_char(
    to_date(regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+([^\d\s\-/\\]+)[\s\-/\\]+([^\d\s\-/\\]+)[\s\.,]+(\d{4})\.?$','\4','gi'),'Mon'),
    'Mon'
    )
))
, 'No. X YYYY:{month-month} (with beginning format XX/YY month-month YYYY)' as res_query
from 
biblio.monograph_part
where 
label~'\d{4}'
and
label~*'^\s?\d{1,3}/+\d{1,3}[\s\.,]+[^\d\s\-/\\]+[\s\-/\\]+[^\d\s\-/\\]+[\s\.,]+\d{4}\.?$'
and
label!~*'autumn' and
label!~*'fall' and
label!~*'winter' and
label!~*'summer' and
label!~*'spring'

and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

-- No. X
select
label,regexp_replace(label,'no[\.,]?\s*(\d+);?\s*$','No. \1','gi')
, 'No. X' as res_query
from 
biblio.monograph_part
where 
label~*'^no[\.,]\s*\d+;?\s*$'

union all

-- No. X-Y
select
label,regexp_replace(label,'no[\.,]?\s*(\d+)\s?\-\s?(\d+)$','No. \1-\2','gi')
, 'No. X-Y' as res_query
from 
biblio.monograph_part
where 
label~*'^no[\.,]?\s*\d+\s?\-\s?\d+$'

union all

-- X of Y -> X
select
label,regexp_replace(label,'^\s?#?(\d+)\s*of\s*\d+$','\1','gi')
, 'X of Y -> X' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?#?\d+\s*of\s*\d+$'

union all

-- #X
select
label,regexp_replace(label,'^\(?\s?#\s?(\d+)[\)\s]?$','No. \1','gi')
, '#X' as res_query
from 
biblio.monograph_part
where 
label~*'^\(?\s?#\s?\d+[\)\s]?$'

union all

-- X (1 or 2 digit bare numbers)
select
label,regexp_replace(label,'^\s*(\d{1,2})[\)\s]?$','Vol. \1','gi')
, 'X (1 or 2 digit bare numbers)' as res_query
from 
biblio.monograph_part
where 
label~*'^\s*\d{1,2}[\)\s]?$'

union all


-- Part Language

-- "pt. 1"
select
label,regexp_replace(label,'^[\(\s]?pte?[\.\s,]*([^\\/\.\s,\-]+)[\s\-]*$','Part \1','gi')
, 'pt. 1' as res_query
from 
biblio.monograph_part
where 
label~*'^[\(\s]?pte?[\.\s,]*[^\\/\.\s,\-]+[\s\-]*$'

union all

-- "pt. X-Y"
select
label,regexp_replace(label,'^[\(\s]?pte?[\.\s,]+([^&\\/\s,\-]+)[&\\/\s,\-]+([^&\\/\s,\-]+)[\-\s/\\\.]*$','Part \1-\2','gi')
, 'pt. X-Y' as res_query
from 
biblio.monograph_part
where 
label~*'^[\(\s]?pte?[\.\s,]+[^&\\/\s,\-]+[&\\/\s,\-]+[^&\\/\s,\-]+[\-\s/\\\.]*$'
and
label!~*'\d{4}'
and
label!~*'v\.'
and
label!~*'no\.'

union all

-- "pt.1 1972" 
select
label,regexp_replace(label,'^[\(\s]?pt[\.\s]+([^\\/\.\s,\-]+)[\\/\.\s,]+(\d{4})\s?$','Part \1, \2','gi')
, 'pt.1 1972' as res_query
from 
biblio.monograph_part
where 
label~*'^[\(\s]?pt[\.\s]+[^\\/\.\s,\-]+[\\/\.\s,]+\d{4}\s?$'

union all

-- Part X, Vol. X (for rows starting with numeric values only)
select
label,regexp_replace(label,'^[\(\s]?(\d{1,3})\s+v[^\d]*(\d+)$','Vol. \2, Part \1','gi')
, 'Part X, Vol. X (for rows starting with numeric values only)' as res_query
from 
biblio.monograph_part
where 
label~*'^[\(\s]?\d{1,3}\s+v[^\d]*\d+$'

union all

-- Part X, Vol. X (for rows starting with pt)
select
label,regexp_replace(label,'^[\(\s]?pt\.?\s?(\d+)\,?\s+v[^\d]*(\d+)$','Vol. \2, Part \1','gi')
, 'Part X, Vol. X (for rows starting with pt)' as res_query
from 
biblio.monograph_part
where 
label~*'^[\(\s]?pt\.?\s?\d+\,?\s+v[^\d]*\d+$'

union all

-- Part X, Vol. X (for rows starting with v)
select
label,regexp_replace(label,'^[\(\s]?v[^\s\.,]*[\s\.,]+([^\s\.,]+)[\s\.,]+p[^\s\.,]*[\s\.,]+([^\s\.,]+)\.?\s?,?$','Vol. \1, Part \2','gi')
, 'Part X, Vol. X (for rows starting with v)' as res_query
from 
biblio.monograph_part
where 
label~*'^[\(\s]?v[^\s\.,]*[\s\.,]+[^\s\.,]+[\s\.,]+p[^\s\.,]*[\s\.,]+[^\s\.,]+\.?\s?,?$'

union all

-- Part X, Vol. X (for rows starting with {digits}th)
select
label,regexp_replace(label,'^\s?\(?\d+[tnrs][hdt].*p[^\s\.,]*[\.\s]+([^\s,\.\\/\-])+[\.\s]+v[^\s\.,]*[\.\s]+([^\s,\.\\/\-]+)\s?$','Vol. \2, Part \1','gi')
, 'Part X, Vol. X (for rows starting with {digits}th)' as res_query
from 
biblio.monograph_part
where 
label~*'v[^\s\.,]*[\.\s]'
and
label~*'p[^\s\.,]*[\.\s]'
and
label!~*'n[^\s\.,]*[\.\s]'
and
label~*'^\s?\(?\d+[tnrs][hdt].*p[^\s\.,]*[\.\s]+[^\s,\.\\/\-]+[\.\s]+v[^\s\.,]*[\.\s]+[^\s,\.\\/\-]+\s?$'

union all

-- Part X, No. Y, Vol. Z (for rows starting with v)
select
label,regexp_replace(label,'^[\(\s]?v[^\s\.,]*[\s\.,]+([^\s\.,]+)[\s\.,]+p[^\s\.,]*[\s\.,]+([^\s\.,]+)[\s\.,]n[^\s\.,]*[\s\.,]+([^\s\.,]+).*$','Vol. \1, No. \3, Part \2','gi')
, 'Part X, No. Y, Vol. Z (for rows starting with v)' as res_query
from 
biblio.monograph_part
where 
label~*'^[\(\s]?v[^\s\.,]*[\s\.,]+[^\s\.,]+[\s\.,]+p[^\s\.,]*[\s\.,]+[^\s\.,]+[\s\.,]n[^\s\.,]*[\s\.,]+[^\s\.,]+.*$'

union all

-- Part X, Vol. X, YYYY (for rows starting with v)
select
label,regexp_replace(label,'^[\(\s]?v[^\s\.,]*[\s\.,]+([^\s\.,]+)[\s\.,]+p[^\s\.,]*[\s\.,]+([^\s\.,]+)[\s\.,](\d{4})\.?\s?,?$','Vol. \1, Part \2, \3','gi')
, 'Part X, Vol. X, YYYY (for rows starting with v)' as res_query
from 
biblio.monograph_part
where 
label~*'^[\(\s]?v[^\s\.,]*[\s\.,]+[^\s\.,]+[\s\.,]+p[^\s\.,]*[\s\.,]+[^\s\.,]+[\s\.,]\d{4}\.?\s?,?$'

union all

-- Part X, Vol. X, YYYY-YYYY (for rows starting with v)
select
label,regexp_replace(label,'^[\(\s]?v[^\s\.,]*[\s\.,]+([^\s\.,]+)[\s\.,]+p[^\s\.,]*[\s\.,]+([^\s\.,]+)[\s\.,](\d{4})[\\/\-]+(\d{4})\.?\s?,?$','Vol. \1, Part \2, \3-\4','gi')
, 'Part X, Vol. X, YYYY-YYYY (for rows starting with v)' as res_query
from 
biblio.monograph_part
where 
label~*'^[\(\s]?v[^\s\.,]*[\s\.,]+[^\s\.,]+[\s\.,]+p[^\s\.,]*[\s\.,]+[^\s\.,]+[\s\.,]\d{4}[\\/\-]+\d{4}\.?\s?,?$'

union all

-- Part X, YYYY
select
label,regexp_replace(regexp_replace(label,'^\s?(\d{4})\s+pt?\.\s?(\d+)([/\-]?\d*).*$','Part \2\3, \1','gi'),'/','-','gi')
, 'Part X, YYYY' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{4}\)?\s+pt?\..*$'

union all

-- Part X, YYYY-YYYY
select
label,regexp_replace(label,'^\s?\(?(\d{4})[\)\-/]+(\d{4})[\,\.\:]?\s+pt\.\s?([^\s]+).*$','Part \3, \1-\2','gi')
, 'Part X, YYYY-YYYY' as res_query
from 
biblio.monograph_part
where 
label~*'\spt\.'
and
label~*'^\s?\(?\d{4}[\)\-/]+\d{4}[\,\.\:]?\s+pt\.\s?.*$'

union all

-- Part X, YYYY-YYYY (from YYYY/YY)
select
label,regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[\)\-/]+(\d{1,2})[\,\.\:]?\s+pt\.\s?([^\s]+).*$','Part \4, \1\2-\1\3','gi')
, 'Part X, YYYY-YYYY (from YYYY/YY)' as res_query
from 
biblio.monograph_part
where 
label~*'\spt\.'
and
label~*'^\s?\(?\d{4}[\)\-/]+\d{1,2}[\,\.\:]?\s+pt\.\s?.*$'

union all




-- Series language

-- Series X
select
label,regexp_replace(label,'^\s?\(?ser[^,\.\:]?i?e?s?[,\.\:\s]+([^\s,\.\\/\-]+)[,\s]?$','Series \1','gi')
, 'Series X' as res_query
from 
biblio.monograph_part
where 
label!~*'[,\.\:\s]+v[^,\.\:]*[,\.\:\s]+'
and
label~*'^\s?\(?ser[^,\.\:]?i?e?s?[,\.\:\s]+[^\s,\.\\/\-]+[,\s]?$'

union all

-- Series X-Y
select
label,regexp_replace(label,'^\s?\(?ser[^,\.\:]?i?e?s?[,\.\:\s]+(\d+)[\-/\\]+(\d+)[,\s]?$','Series \1-\2','gi')
, 'Series X-Y' as res_query
from 
biblio.monograph_part
where 
label!~*'[,\.\:\s]+v[^,\.\:]*[,\.\:\s]+'
and
label~*'^\s?\(?ser[^,\.\:]?i?e?s?[,\.\:\s]+\d+[\-/\\]+\d+[,\s]?$'

union all

-- Series X, Vol. Y
select
label,regexp_replace(label,'^\s?\(?ser[^,\.\:]?i?e?s?[,\.\:\s]+([^\s,\.\\/\-]+)[\s,\.\\/\-]+v[^,\.\:]*[,\.\:\s]+([^\s,]+)[,\s]?$','Series \1, Vol. \2','gi')
, 'Series X, Vol. Y' as res_query
from 
biblio.monograph_part
where 
label~*'[,\.\:\s]+v[^,\.\:]*[,\.\:\s]+'
and
label~*'^\s?\(?ser[^,\.\:]?i?e?s?[,\.\:\s]+[^\s,\.\\/\-]+[\s,\.\\/\-]+v[^,\.\:]*[,\.\:\s]+[^\s,]+[,\s]?$'

union all

-- Part Z, Series X, Vol. Y
select
label,regexp_replace(label,'^\s?\(?ser[^,\.\:]?i?e?s?[,\.\:\s]+([^\s,\.\\/\-]+)[\s,\.\\/\-]+v[^,\.\:]*[,\.\:\s]+([^\s,]+)[,\.\:\s]+p[^,\.\:]*[,\.\:\s]+([^\s,]+)$','Series \1, Vol. \2, Part \3','gi')
, 'Part Z, Series X, Vol. Y' as res_query
from 
biblio.monograph_part
where 
label~*'[,\.\:\s]+v[^,\.\:]*[,\.\:\s]+'
and
label~*'[,\.\:\s]+p[^,\.\:]*[,\.\:\s]+'
and
label~*'^\s?\(?ser[^,\.\:]?i?e?s?[,\.\:\s]+[^\s,\.\\/\-]+[\s,\.\\/\-]+v[^,\.\:]*[,\.\:\s]+[^\s,]+[,\.\:\s]+p[^,\.\:]*[,\.\:\s]+[^\s,]+$'

union all

-- Xrd Series
select
label,regexp_replace(label,'^\s?\(?(\d+[tnrs][hdt])[\s\.\-]+ser[^,\.\:]?i?e?s?[,\.\:\s]?$','\1 Series','gi')
, 'Xrd Series' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d+[tnrs][hdt][\s\.\-]+ser[^,\.\:]?i?e?s?[,\.\:\s]?$'

union all

-- Xrd Series, Vol. Y
select
label,regexp_replace(label,'^\s?\(?(\d+[tnrs][hdt])[\s\.\-]+ser[^,\.\:]?i?e?s?[,\.\:\s]+v[^,\.\:]*[,\.\:\s]+([^\s,]+)\s?$','\1 Series, Vol. \2','gi')
, ' Xrd Series, Vol. Y' as res_query
from 
biblio.monograph_part
where 
label~*'[,\.\:\s]+v[^,\.\:]*[,\.\:\s]+'
and
label~*'^\s?\(?\d+[tnrs][hdt][\s\.\-]+ser[^,\.\:]?i?e?s?[,\.\:\s]+v[^,\.\:]*[,\.\:\s]+[^\s,]+\s?$'

union all

-- Xrd Series, Vol. Y YYYY
select
label,regexp_replace(label,'^\s?\(?(\d+[tnrs][hdt])[\s\.\-]+ser[^,\.\:]?i?e?s?[,\.\:\s]+v[^,\.\:]*[,\.\:\s]+([^\s,]+)[,\.\:\s]+(\d{4})\s?$','\1 Series, Vol. \2, \3','gi')
, 'Xrd Series, Vol. Y YYYY' as res_query
from 
biblio.monograph_part
where 
label~*'[,\.\:\s]+v[^,\.\:]*[,\.\:\s]+'
and
label~*'^\s?\(?\d+[tnrs][hdt][\s\.\-]+ser[^,\.\:]?i?e?s?[,\.\:\s]+v[^,\.\:]*[,\.\:\s]+[^\s,]+[,\.\:\s]+\d{4}\s?$'

union all

-- Xrd Series, Vol. Y YYYY-YYYY
select
label,regexp_replace(label,'^\s?\(?(\d+[tnrs][hdt])[\s\.\-]+ser[^,\.\:]?i?e?s?[,\.\:\s]+v[^,\.\:]*[,\.\:\s]+([^\s,]+)[,\.\:\s]+(\d{4})[,\.\:\s\-]+(\d{4})\s?$','\1 Series, Vol. \2, \3-\4','gi')
, 'Xrd Series, Vol. Y YYYY-YYYY' as res_query
from 
biblio.monograph_part
where 
label~*'[,\.\:\s]+v[^,\.\:]*[,\.\:\s]+'
and
label~*'^\s?\(?\d+[tnrs][hdt][\s\.\-]+ser[^,\.\:]?i?e?s?[,\.\:\s]+v[^,\.\:]*[,\.\:\s]+[^\s,]+[,\.\:\s]+\d{4}[,\.\:\s\-]+\d{4}\s?$'

union all





-- suppl. language

-- YYYY-YYYY Suppl (digit)?
select
label,btrim(regexp_replace(label,'^\s?\(?(\d{4})[/,\.\:\-\s]+(\d{4})[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]?)$','\1-\2 Suppl. \3','gi'))
, 'YYYY-YYYY Suppl (digit)?' as res_query
from 
biblio.monograph_part
where 
label~*'suppl'
and
label~*'^\s?\(?\d{4}[/,\.\:\-\s]+\d{4}[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?[^,\.\:\-\s]?$'

union all

-- YYYY/YY Suppl (digit)?
select
label,btrim(regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[/,\.\:\-\s]+(\d{2})[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]?)$','\1\2-\1\3 Suppl. \4','gi'))
, 'YYYY/YY Suppl (digit)?' as res_query
from 
biblio.monograph_part
where 
label~*'suppl'
and
label~*'^\s?\(?\d{4}[/,\.\:\-\s]+\d{2}[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?[^,\.\:\-\s]?$'

union all

-- YYYY/YY Suppl (digit)?
select
label,btrim(regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[/,\.\:\-\s]+(\d{2})[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]?)$','\1\2-\1\3 Suppl. \4','gi'))
, 'YYYY/YY Suppl (digit)?' as res_query
from 
biblio.monograph_part
where 
label~*'suppl'
and
label~*'^\s?\(?\d{4}[/,\.\:\-\s]+\d{2}[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?[^,\.\:\-\s]?$'

union all

-- YYYY/YY-YYYY/YY Suppl (digit)?
select
label,btrim(regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[/,\.\:\-\s]+(\d{2})[/,\.\:\-\s](\d{2})(\d{2})[/,\.\:\-\s]+(\d{2})[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]?)$','\1\2-\1\3 - \4\5-\4\6 Suppl. \7','gi'))
, 'YYYY/YY-YYYY/YY Suppl (digit)?' as res_query
from 
biblio.monograph_part
where 
label~*'suppl'
and
label~*'^\s?\(?\d{4}[/,\.\:\-\s]+\d{2}[/,\.\:\-\s]\d{4}[/,\.\:\-\s]+\d{2}[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?[^,\.\:\-\s]?$'

union all

-- Supp X YYYY
select
label,regexp_replace(label,'^\s?\(?suppl[\.ement]*[,\.\:\-\s]+([^,\.\:\-\s/]+)[,\.\:\-\s]+(\d{4})\s?$','\2 Suppl. \1','gi')
, 'Supp X YYYY' as res_query
from 
biblio.monograph_part
where 
label~*'suppl'
and
label~*'^\s?\(?suppl[\.ement]*[,\.\:\-\s]+[^,\.\:\-\s/]+[,\.\:\-\s]+\d{4}\s?$'
and
label != regexp_replace(label,'^\s?\(?suppl[\.ement]*[,\.\:\-\s]+([^,\.\:\-\s/]+)[,\.\:\-\s]+(\d{4})\s?$','\2 Suppl. \1','gi')

union all

-- Supp YYYY
select
label,regexp_replace(label,'^\s?\(?suppl[\.ement]*[,\.\:\-\s]+(\d{4})\s?$','\1 Suppl.','gi')
, 'Supp YYYY' as res_query
from 
biblio.monograph_part
where 
label~*'suppl'
and
label~*'^\s?\(?suppl[\.ement]*[,\.\:\-\s]+\d{4}\s?$'

union all

-- Supp X YYYY/YYYY
select
label,btrim(regexp_replace(label,'^\s?\(?suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]+)[,\.\:\-\s]+(\d{4})[/,\.\:\-\s]+(\d{4})[,\.\:\-\s]?$','\2-\3 Suppl. \1','gi'))
, 'Supp X YYYY/YYYY' as res_query
from 
biblio.monograph_part
where 
label~*'suppl'
and
label~*'^\s?\(?suppl[\.ement]*[,\.\:\-\s]?[^,\.\:\-\s]+[,\.\:\-\s]+\d{4}[/,\.\:\-\s]+\d{4}[,\.\:\-\s]?$'

union all

-- YYYY Suppl (digit)?
select
label,btrim(regexp_replace(label,'^\s?\(?(\d{4})[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]?)$','\1 Suppl. \2','gi'))
, 'YYYY Suppl (digit)?' as res_query
from 
biblio.monograph_part
where 
label~*'suppl'
and
label~*'^\s?\(?\d{4}[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?[^,\.\:\-\s]?$'

union all

-- YYYY Suppl (digit)? YYYY
select
label,btrim(regexp_replace(label,'^\s?\(?(\d{4})[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]?)[,\.\:\-\s]+(\d{4})\s?$','\1-\3 Suppl. \2','gi'))
, 'YYYY Suppl (digit)? YYYY' as res_query
from 
biblio.monograph_part
where 
label~*'suppl'
and
label~*'^\s?\(?\d{4}[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?[^,\.\:\-\s]?[,\.\:\-\s]+\d{4}\s?$'

union all

-- YYYY : month Suppl (digit)?
select
label,btrim(
concat(
regexp_replace(label,'^\s?\(?(\d{4})[,\.\:\-\s]+([^\d\.]+)[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]?)\s?$','\1:','gi'),
to_char(
    to_date(regexp_replace(label,'^\s?\(?(\d{4})[,\.\:\-\s]+([^\d\.]+)[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]?)\s?$','\2','gi'),'Mon'),
    'Mon'
    ),
regexp_replace(label,'^\s?\(?(\d{4})[,\.\:\-\s]+([^\d\.]+)[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]?)\s?$',' Suppl. \3','gi')
))
, 'YYYY : month Suppl (digit)?' as res_query
from 
biblio.monograph_part
where 
label~*'suppl'
and
label~*'^\s?\(?\d{4}[,\.\:\-\s]+[^\d\.]+[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?[^,\.\:\-\s]?\s?$'
and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

-- month YYYY Suppl (digit)?
select
label,btrim(regexp_replace(label,'^\s?\(?([^\d\.]+)[,\.\:\-\s]+(\d{4})[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]?)\s?$','\2:\1 Suppl. \3','gi'))
, 'month YYYY Suppl (digit)?' as res_query
from 
biblio.monograph_part
where 
label~*'suppl'
and
label~*'^\s?\(?[^\d\.]+[,\.\:\-\s]+\d{4}[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?[^,\.\:\-\s]?\s?$'
and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

-- Suppl (digit)? YYYY:month
select
label,btrim(
concat(
regexp_replace(label,'^\s?\(?suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]?)[,\.\:\-\s]+(\d{4})[,\.\:\-\s]+([^\d\.]+)[,\.\:\-\s]?$','\2:','gi'),
to_char(
    to_date(regexp_replace(label,'^\s?\(?suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]?)[,\.\:\-\s]+(\d{4})[,\.\:\-\s]+([^\d\.]+)[,\.\:\-\s]?$','\3','gi'),'Mon'),
    'Mon'
    ),
regexp_replace(label,'^\s?\(?suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]?)[,\.\:\-\s]+(\d{4})[,\.\:\-\s]+([^\d\.]+)[,\.\:\-\s]?$',' Suppl. \1','gi')
))
, 'Suppl (digit)? YYYY:month' as res_query
from 
biblio.monograph_part
where 
label~*'suppl'
and
label~*'^\s?\(?suppl[\.ement]*[,\.\:\-\s]?[^,\.\:\-\s]?[,\.\:\-\s]+\d{4}[,\.\:\-\s]+[^\d\.]+[,\.\:\-\s]?$'
and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)


union all

-- Just a date language

-- Standard DD-MM-YY
select
label,
to_char(
to_date(
regexp_replace(label,'^\s?\(?(\d{2})[\\/\-]+(\d{2})[\\/\-]+(\d{2})[,\.\:\-\s]?','\1\2\3','gi'),
'MMDDYY'),
'YYYY:Mon DD'
)
, 'Standard DD-MM-YY' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{2}[\\/\-]+\d{2}[\\/\-]+\d{2}[,\.\:\-\s]?$'

union all

-- "1889 June-1890 June" 
select
label,
concat(
regexp_replace(label,'^\s?\(?(\d{4})[\\/\s\:]+([^\d\.]{3,12})[\.\\/\-\s]+(\d{4})[\\/\s\:]+([^\d\.]{3,12})[,\.\:\-\s]?$','\1:','gi'),
to_char(
    to_date(regexp_replace(label,'^\s?\(?(\d{4})[\\/\s\:]+([^\d\.]{3,12})[\.\\/\-\s]+(\d{4})[\\/\s\:]+([^\d\.]{3,12})[,\.\:\-\s]?$','\2','gi'),'Mon'),
    'Mon'
    ),
regexp_replace(label,'^\s?\(?(\d{4})[\\/\s\:]+([^\d\.]{3,12})[\.\\/\-\s]+(\d{4})[\\/\s\:]+([^\d\.]{3,12})[,\.\:\-\s]?$',' - \3:','gi'),
to_char(
    to_date(regexp_replace(label,'^\s?\(?(\d{4})[\\/\s\:]+([^\d\.]{3,12})[\.\\/\-\s]+(\d{4})[\\/\s\:]+([^\d\.]{3,12})[,\.\:\-\s]?$','\4','gi'),'Mon'),
    'Mon'
    )
)
, '1889 June-1890 June' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{4}[\\/\s\:]+[^\d\.]{3,12}[\.\\/\-\s]+\d{4}[\\/\s\:]+[^\d\.]{3,12}[,\.\:\-\s]?$'
and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)
and
label!~*'autumn' and
label!~*'fall' and
label!~*'winter' and
label!~*'summer' and
label!~*'spring'

union all

-- "1906 Nov. 15-1907 Nov. 1"
select
label,regexp_replace(label,'^\s?\(?(\d{4})[\\/\s\:]+([^\d\.]{3,12})[\.\\/\-\s]+(\d{4})[\\/\s\:]+([^\d\.]{3,12})[,\.\:\-\s]?$','\1:\2 - \3:\4','gi')
, '1906 Nov. 15-1907 Nov. 1' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{4}[\\/\s\:]+[^\d\.]{3,12}[\.\\/\-\s]+\d{1,2}[\.\\/\-\s]+\d{4}[\\/\s\:]+[^\d\.]{3,12}[,\.\:\-\s]+\d{1,2}[\s\.\-]?$'
and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

-- "10 1977 Aug."
select
label,regexp_replace(label,'^\s?\(?(\d{1,3})[\\/\s\:]+(\d{4})[\.\\/\-\s]*([^\d\.]*)[\s\.\-]?','Vol. \1, \2:\3','gi')
, '10 1977 Aug.' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{1,3}[\\/\s\:]+\d{4}[\.\\/\-\s]*[^\d\.]*[\s\.\-]?$'
and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

-- "10 1977"
select
label,regexp_replace(label,'^\s?\(?(\d{1,3})[\\/\s\:]+(\d{4})[\s\.\-]?','Vol. \1, \2','gi')
, '10 1977' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{1,3}[\\/\s\:]+\d{4}[\s\.\-]?$'

union all

-- "1/6"
select
label,regexp_replace(label,'^\s?\(?([^9]?\d{1,2})[\\/\s\:\-\.]+(\d{1,3})[\s\.\-]?$','Vol. \1, No. \2','gi')
, '1/6' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?[^9]?\d{1,2}[\\/\s\:\-\.]+\d{1,3}[\s\.\-]?$'

union all

-- "1/4-1/5"
select
label,regexp_replace(label,'^\s?\(?([^9]?\d{1,2})[\\/\s\:]+([^9]?\d{1,2})[\s\.\-/\\]+([^9]?\d{1,2})[\\/\s\:]+(\d{1,3})[\s\.\-]?$','Vol. \1, No. \2 - Vol. \3, No. \4','gi')
, '1/4-1/5' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?[^9]?\d{1,2}[\\/\s\:]+[^9]?\d{1,2}[\s\.\-/\\]+[^9]?\d{1,2}[\\/\s\:]+\d{1,3}[\s\.\-]?$'

union all

-- "958/1-"
select
label,to_char(to_date(regexp_replace(label,'^\s?\(?(9\d{1,2})[\\/\s\:\-\.]+(\d{1,3})[\s\.\-]?$','1\1-\2','gi'),'YYYY-MM'),'YYYY:Mon')
, '958/1-' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?9\d{1,2}[\\/\s\:\-\.]+\d{1,3}[\s\.\-]?$'

union all

-- "988 June 1988"
select
label,
concat(
regexp_replace(label,'^\s?\(?9\d{1,2}[\\/\s\:\-\.]+([^\d\.]{3,12})[\s\.\-]+(\d{4})[\s\.\-]?$','\2:','gi'),
to_char(
    to_date(regexp_replace(label,'^\s?\(?9\d{1,2}[\\/\s\:\-\.]+([^\d\.]{3,12})[\s\.\-]+(\d{4})[\s\.\-]?$','\1','gi'),'Mon'),
    'Mon'
    )
)
, '988 June 1988' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?9\d{1,2}[\\/\s\:\-\.]+[^\d\.]{3,12}[\s\.\-]+\d{4}[\s\.\-]?$'

union all

-- "988/7-989/8 Jul.1988/Aug.1989"
select
label,
concat(
to_char(
to_date(
regexp_replace(label,'^\s?\(?(9\d{2})[\\/\s\:\-\.]+(\d{1,2})[\s\.\-]+9\d{2}[\\/\s\:\-\.]+\d{1,2}[\s\.\-]+[^\d\.]{3,12}[\s\.\:]+\d{4}[\\/\s\:\-\.]+[^\d\.]{3,12}.*$','1\1-\2','gi')
,'YYYY-MM'),
'YYYY:Mon'),
' - ',
to_char(
to_date(
regexp_replace(label,'^\s?\(?9\d{2}[\\/\s\:\-\.]+\d{1,2}[\s\.\-]+(9\d{2})[\\/\s\:\-\.]+(\d{1,2})[\s\.\-]+[^\d\.]{3,12}[\s\.\:]+\d{4}[\\/\s\:\-\.]+[^\d\.]{3,12}.*$','1\1-\2','gi')
,'YYYY-MM'),
'YYYY:Mon')
)
, '988/7-989/8 Jul.1988/Aug.1989' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?9\d{2}[\\/\s\:\-\.]+\d{1,2}[\\/\s\.\-]+9\d{2}[\\/\s\:\-\.]+\d{1,2}[\\/\s\.\-]+[^\d\.]{3,12}[\\/\s\.\:]+\d{4}[\\/\s\:\-\.]+[^\d\.]{3,12}.*$'

union all

-- "1923/YY" where YY <= 12 and subtraction between the two years is less than 15
select
label,regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1\2-\1\3','gi')
, '"1923/YY" where YY <= 12 and subtraction between the two years is less than 15' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{4}[\\/\s\:\-\.]+\d{2}[\s\.\-]?$'
and
regexp_replace(label,'^\s?\(?(\d{4})[\\/\s\:\-\.]+\d{2}[\s\.\-]?$','\1','gi')::numeric < 1990  --- year needs to be less than 1990
and
regexp_replace(label,'^\s?\(?\d{4}[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1','gi')::numeric between 1 and 12  --- Looks like a month number
and
(
regexp_replace(label,'^\s?\(?(\d{2})\d{2}[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1\2','gi')::numeric - 
regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[\\/\s\:\-\.]+\d{2}[\s\.\-]?$','\1\2','gi')::numeric
 ) between 1 and 15 --- But if it were a year number, and subtracted from the previous year - close enough to make it a year range instead of a month number

union all

-- "1923/YY" where YY <= 12 and subtraction between the two years is less than 5
select
label,regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1\2-\1\3','gi')
, '"1923/YY" where YY <= 12 and subtraction between the two years is less than 5' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{4}[\\/\s\:\-\.]+\d{2}[\s\.\-]?$'
and
regexp_replace(label,'^\s?\(?(\d{4})[\\/\s\:\-\.]+\d{2}[\s\.\-]?$','\1','gi')::numeric > 1989  --- year needs to be greater than 1989
and
regexp_replace(label,'^\s?\(?\d{4}[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1','gi')::numeric between 1 and 12  --- Looks like a month number
and
(
regexp_replace(label,'^\s?\(?(\d{2})\d{2}[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1\2','gi')::numeric - 
regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[\\/\s\:\-\.]+\d{2}[\s\.\-]?$','\1\2','gi')::numeric
 ) between 1 and 4 --- But if it were a year number, and subtracted from the previous year - close enough to make it a year range instead of a month number
 
union all

-- "1923/YY" where YY <= 12 and subtraction between the two years is > 4  (it's a month)
select
label,to_char(to_date(regexp_replace(label,'^\s?\(?(\d{4})[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1-\2','gi'),'YYYY-MM'),'YYYY:Mon')
, '"1923/YY" where YY <= 12 and subtraction between the two years is > 4  (its a month)' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{4}[\\/\s\:\-\.]+\d{2}[\s\.\-]?$'
and
regexp_replace(label,'^\s?\(?(\d{4})[\\/\s\:\-\.]+\d{2}[\s\.\-]?$','\1','gi')::numeric > 1989  --- year needs to be greater than 1989
and
regexp_replace(label,'^\s?\(?\d{4}[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1','gi')::numeric between 1 and 12  --- Looks like a month number
and
(
(
regexp_replace(label,'^\s?\(?(\d{2})\d{2}[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1\2','gi')::numeric - 
regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[\\/\s\:\-\.]+\d{2}[\s\.\-]?$','\1\2','gi')::numeric
 ) < 1 --- But if it were a year number, and subtracted from the previous year - Too far apart for it to be a year range - it's a month
or
(
regexp_replace(label,'^\s?\(?(\d{2})\d{2}[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1\2','gi')::numeric - 
regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[\\/\s\:\-\.]+\d{2}[\s\.\-]?$','\1\2','gi')::numeric
 ) > 4 --- But if it were a year number, and subtracted from the previous year - Too far apart for it to be a year range - it's a month
)
 
union all

-- "1923/YY" where YY <= 12 and subtraction between the two years is > 15  (it's a month)
select
label,to_char(to_date(regexp_replace(label,'^\s?\(?(\d{4})[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1-\2','gi'),'YYYY-MM'),'YYYY:Mon')
, '"1923/YY" where YY <= 12 and subtraction between the two years is > 15  (its a month)' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{4}[\\/\s\:\-\.]+\d{2}[\s\.\-]?$'
and
regexp_replace(label,'^\s?\(?(\d{4})[\\/\s\:\-\.]+\d{2}[\s\.\-]?$','\1','gi')::numeric < 1990  --- year needs to be less than 1989
and
regexp_replace(label,'^\s?\(?\d{4}[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1','gi')::numeric between 1 and 12  --- Looks like a month number
and
(
(
regexp_replace(label,'^\s?\(?(\d{2})\d{2}[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1\2','gi')::numeric - 
regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[\\/\s\:\-\.]+\d{2}[\s\.\-]?$','\1\2','gi')::numeric
 ) < 1 --- But if it were a year number, and subtracted from the previous year - Too far apart for it to be a year range - it's a month
or
 (
regexp_replace(label,'^\s?\(?(\d{2})\d{2}[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1\2','gi')::numeric - 
regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[\\/\s\:\-\.]+\d{2}[\s\.\-]?$','\1\2','gi')::numeric
 ) > 15 --- But if it were a year number, and subtracted from the previous year - Too far apart for it to be a year range - it's a month
)
 
union all

-- "1923/X" just one digit after the /
select
label,to_char(to_date(regexp_replace(label,'^\s?\(?(\d{4})[\\/\s\:\-\.]+(\d)[\s\.\-]?$','\1-\2','gi'),'YYYY-MM'),'YYYY:Mon')
, '"1923/X" just one digit after the /' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{4}[\\/\s\:\-\.]+\d[\s\.\-]?$'

union all

-- "1923/XX" two digits after the / ( > 12 )
select
label,regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1\2-\1\3','gi')
, '"1923/XX" two digits after the / ( > 12 )' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{4}[\\/\s\:\-\.]+\d{2}[\s\.\-]?$'
and
regexp_replace(label,'^\s?\(?\d{4}[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1','gi')::numeric > 12  --- Looks like a year
group by 1,2

union all

-- "1998 (09) Sep"
select
label,
concat(
regexp_replace(label,'^\s?\(?(\d{4})[\s\./\\\d\(\)]?\(\d+\)\s+([^\s\.\d\\/]{3,5})[\)\s]?$','\1:','gi'),
to_char(
    to_date(regexp_replace(label,'^\s?\(?(\d{4})[\s\./\\\d\(\)]?\(\d+\)\s+([^\s\.\d\\/]{3,5})[\)\s]?$','\2','gi'),'Mon'),
    'Mon')
)
, '1998 (09) Sep' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{4}[\s\./\\\d\(\)]?\(\d+\)\s+[^\s\.\d\\/]{3,5}[\)\s]?$'
and
(
label~*'jan' or
label~*'feb' or
label~*'mar' or
label~*'apr' or
label~*'may' or
label~*'jun' or
label~*'jul' or
label~*'aug' or
label~*'sep' or
label~*'oct' or
label~*'nov' or
label~*'dec'
)

union all

--"1998/4 Winter"
select
label,
initcap(
concat(
to_char(
to_date(
regexp_replace(label,'^\s?\(?(\d{4})[\s\./\\\(\)]+(\d)[\.,\\/\s\(]+[^\s\.\d\\/]{3,12}[\)\s]?$','\1-\2','gi'),
'YYYY-MM'),
'YYYY:Mon'),
regexp_replace(label,'^\s?\(?\d{4}[\s\./\\\(\)]+\d[\.,\\/\s\(]+([^\s\.\d\\/\)]{3,12})[\)\s]?$',' (\1)','gi')
)
)
, '1998/4 Winter' as res_query
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{4}[\s\./\\\(\)]+\d[\.,\\/\s\(]+[^\s\.\d\\/]{3,12}[\)\s]?$'
and
(
label~*'autumn' or
label~*'fall' or
label~*'winter' or
label~*'summer' or
label~*'spring'
)

union all

-- Leftover odds and ends stuff like Bk. sup
-- bk X
select
label,regexp_replace(label,'^[\(\s]?bks?[\.\s]*([^\\/\.\s,\-]+)[\s\-]*$','Book \1','gi')
, 'bk X' as res_query
from 
biblio.monograph_part
where 
label~*'^[\(\s]?bks?[\.\s]*[^\\/\.\s,\-]+[\s\-]*$'

union all

-- bk X-Y
select
label,regexp_replace(label,'^[\(\s]?bks?[\.\s]*([^\\/\.\s,\-]+)[\s\-]+([^\\/\.\s,\-]+)[\s\-]*$','Book \1-\2','gi')
, 'bk X-Y' as res_query
from 
biblio.monograph_part
where 
label~*'^[\(\s]?bks?[\.\s]*[^\\/\.\s,\-]+[\s\-]+[^\\/\.\s,\-]+[\s\-]*$'
and
label!~'\d{4}'

union all

-- sup X
select
label,regexp_replace(label,'^[\(\s]?sup?[\.\s]+([^\\/\.\s,\-]+)[\s\-]*$','Suppl. \1','gi')
, 'sup X' as res_query
from 
biblio.monograph_part
where 
label~*'^[\(\s]?sup?p?[\.\s]+[^\\/\.\s,\-]+[\s\-]*$'
and
label!~'\d{4}'

union all

-- sup YYYY
select
label,regexp_replace(label,'^[\(\s]?sup?[\.\s]+(\d{4})[\s\-]*$','\1 Suppl.','gi')
, 'sup YYYY' as res_query
from 
biblio.monograph_part
where 
label~*'^[\(\s]?sup?[\.\s]+\d{4}[\s\-]*$'
and
label~'\d{4}'

union all

-- YYYY sup
select
label,regexp_replace(label,'^[\(\s]?(\d{4})[\.\s\|\\/]+sup?[\.\s]+[\s\-]*$','\1 Suppl.','gi')
, 'YYYY sup' as res_query
from 
biblio.monograph_part
where 
label~*'^[\(\s]?\d{4}[\.\s\|\\/]+sup?[\.\s]+[\s\-]*$'

union all

-- "pt.1 1960/1962"
select
label,regexp_replace(label,'^[\(\s]?pt[\.\s\|\\/]+([^\s,\.]+)[\s,\.]+(\d{4})[\\/\.\s]+(\d{4})[\s\-]*$','Part \1, \2-\3','gi')
, 'pt.1 1960/1962' as res_query
from 
biblio.monograph_part
where 
label~*'^[\(\s]?pt[\.\s\|\\/]+[^\s,\.]+[\s,\.]+\d{4}[\\/\.\s]+\d{4}[\s\-]*$'


)
as a


group by 1,2,3
order by length(a.label),1,2;


-- clean a little bit
select mymig.monograph_part_update_current_job('Trimming mymig.monograph_part_conversion.new_label');
UPDATE
mymig.monograph_part_conversion
SET new_label = btrim(new_label)
WHERE 
job = mymig.monograph_part_get_current_job() AND
new_label != btrim(new_label);


-- Append the manual stuff (if any)
SELECT mymig.monograph_part_update_current_job('Appending any manual conversions into mymig.monograph_part_conversion');
INSERT INTO
mymig.monograph_part_conversion
(original_label,new_label,manual,job)
SELECT 
original_label,new_label,true,mymig.monograph_part_get_current_job()
FROM
mymig.monograph_part_conversion_manual;


SELECT mymig.monograph_part_update_current_job('Removing exact duplicates from mymig.monograph_part_conversion');
-- Eliminate exact duplicates
DELETE FROM mymig.monograph_part_conversion mmpc
WHERE
id in(
SELECT id
FROM 
(
SELECT MIN(id) as id, original_label,new_label,job
FROM
mymig.monograph_part_conversion
GROUP BY original_label,new_label,job
HAVING COUNT(*) > 1) b
);

-- do it twice to be sure
SELECT mymig.monograph_part_update_current_job('Removing exact duplicates from mymig.monograph_part_conversion');
-- Eliminate exact duplicates
DELETE FROM mymig.monograph_part_conversion mmpc
WHERE
id in(
SELECT id
FROM 
(
SELECT MIN(id) as id, original_label,new_label,job
FROM
mymig.monograph_part_conversion
GROUP BY original_label,new_label,job
HAVING COUNT(*) > 1) b
);


-- remove conversions that don't make any changes
SELECT mymig.monograph_part_update_current_job('Removing conversions that dont make any changes');
-- Eliminate exact duplicates
DELETE FROM mymig.monograph_part_conversion mmpc
WHERE
original_label = new_label
AND
job = mymig.monograph_part_get_current_job();

-- And now the trouble starts
-- might have two entries that map the same old value to two different new values 
-- this is an execution killer

-- look for duplicates
select mymig.monograph_part_update_current_job('Looking for duplicates in the map - this can terminate execution');
SELECT CASE WHEN a.count > 1 THEN mymig.monograph_part_conversion_error_generator('The conversion map contains conflicting conversions') ELSE TRUE END
FROM
(
SELECT original_label,count(*)
FROM
mymig.monograph_part_conversion
WHERE
job = mymig.monograph_part_get_current_job()
GROUP by 1
HAVING count(*) > 1
) as a;

-- Record the map with the affected copies
select mymig.monograph_part_update_current_job('Create the final map with bre.record and asset.copy.id');
INSERT INTO mymig.monograph_part_conversion_map
(copy,record,acpm,original_label,new_label,job)
SELECT
acpm.target_copy,
bmp.record,
acpm.id,
bmp.label,
mmpc.new_label,
mymig.monograph_part_get_current_job()
FROM
biblio.monograph_part bmp
JOIN asset.copy_part_map acpm ON (bmp.id = acpm.part)
JOIN mymig.monograph_part_conversion mmpc ON (mmpc.original_label = bmp.label AND acpm.part = bmp.id AND mmpc.job=mymig.monograph_part_get_current_job())
-- LEFT JOIN mymig.monograph_part_conversion_map mmpcm ON ( acpm.target_copy = mmpcm.copy AND bmp.record=mmpcm.record and mmpcm.job=mymig.monograph_part_get_current_job())
WHERE
NOT bmp.deleted
-- AND mmpcm.copy IS NULL
;


ROLLBACK;
-- now we have all the stuff we need to start making the conversion in production
BEGIN;

-- Create new monograph labels that don't exist yet
select mymig.monograph_part_update_current_job('Inserting biblio.monograph_part new part labels that do not already exist');
INSERT INTO biblio.monograph_part(record,label)
SELECT
DISTINCT mmpcm.record,mmpcm.new_label
FROM
mymig.monograph_part_conversion_map mmpcm
LEFT JOIN biblio.monograph_part bmp ON (bmp.label = mmpcm.new_label AND bmp.record = mmpcm.record )
WHERE
mmpcm.job = mymig.monograph_part_get_current_job() AND
bmp is null;

-- assign the copies to the new part where appropriate
select mymig.monograph_part_update_current_job('Updating copies to point to the new monograph parts');
UPDATE asset.copy_part_map acpm
SET
part = bmp.id
FROM
biblio.monograph_part bmp,
mymig.monograph_part_conversion_map mmpcm
WHERE
mmpcm.job = mymig.monograph_part_get_current_job() AND
mmpcm.acpm = acpm.id AND
mmpcm.copy = acpm.target_copy AND
mmpcm.record = bmp.record AND
mmpcm.new_label = bmp.label;

-- delete unused labels when we can
select mymig.monograph_part_update_current_job('Deleting old/unused bmp labels where we can');
UPDATE
biblio.monograph_part bmp_outter
SET
deleted = TRUE
FROM
biblio.monograph_part bmp
JOIN mymig.monograph_part_conversion_map mmpcm ON ( bmp.record=mmpcm.record AND mmpcm.original_label = bmp.label AND mmpcm.job = mymig.monograph_part_get_current_job() )
LEFT JOIN asset.copy_part_map acpm ON ( acpm.part=bmp.id )
WHERE
bmp_outter.id=bmp.id AND
acpm.id IS NULL;

select mymig.monograph_part_update_current_job('Committing transaction');

COMMIT;

select mymig.monograph_part_update_current_job('Done',true);
-- 



-- Make a report for those labels that were not converted or touched
-- select
-- bmp.label,string_agg(bmp.record::text,',')
-- from
-- biblio.monograph_part bmp
-- left join mymig.monograph_part_conversion mmpc on(bmp.label=mmpc.original_label)
-- left join mymig.monograph_part_conversion mmpc2 on(bmp.label=mmpc2.new_label)
-- where
-- mmpc.original_label is null and
-- mmpc2.original_label is null and
-- not bmp.deleted
-- group by 1
-- order by length(bmp.label)