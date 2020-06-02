/* 
create table mymig.monograph_part_conversion
(current_val text,new_val text)


insert into
mymig.monograph_part_conversion
(current_val,new_val)



 */

 
select label,(case when (label~'\-$' and "regexp_replace"!~'\-$') then "regexp_replace"||'-' else "regexp_replace" end)
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
from 
biblio.monograph_part
where
btrim(label) ~'[^\s]'
and
btrim(label)~*'^v\.*[^\-,\.\s/\(\)]*$'
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

-- Vol X,(for rows starting with {digits}th)
select
label,regexp_replace(label,'^\s?\(?\d+[tnrs][hdt].*v[^\s\.,]*[\.\s]+([^\s\.,]+)\s?$','Vol. \1','gi')
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
label~*'^\s?\(?\d+[tnrs][hdt].*v[^\s\.,]*[\.\s]+[^\s\.,]+\s?$'

union all

-- Vol. X, YYYY
select
label,regexp_replace(label,'^v[^\d]*(\d+)\s+(\d{4})$','Vol. \1, \2','gi')
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
from 
biblio.monograph_part
where 
label~*'^\(?\d{4}[\\/\:\.,\s]+v[^\d]*[\s\.\-]+[^\s\.\-,]+\s?$'

union all

-- "v. 1, disc 1-4"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:]+):?[,\.\s\-]+d[^,\.\s]*[,\.\s\-]+([^,\.\s\-]+)[,\.\s\-]+([^,\.\s\-]+)\s?$','Vol. \1, Disc \2-\3','gi')
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:]+:?[,\.\s\-]+d[^,\.\s]*[,\.\s\-]+[^,\.\s\-]+[,\.\s\-]+[^,\.\s\-]+\s?$'

union all

-- "v.1, 1897-1942"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[:;,\.\s\-]+(\d{4})[,\.\s\-\\/]+(\d{4})\s?$','Vol. \1, \2-\3','gi')
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[:;,\.\s\-]+\d{4}[,\.\s\-\\/]+\d{4}\s?$'

union all

-- "v.1/2 1913/1989"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[/:;,\-]+([^,\.\s\-:/;]+)[/:;,\.\s\-]+(\d{4})[,\.\s\-\\/]+(\d{4})\s?$','Vol. \1-\2, \3-\4','gi')
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[/:;,\-]+[^,\.\s\-:/;]+[/:;,\.\s\-]+\d{4}[,\.\s\-\\/]+\d{4}\s?$'

union all

-- "v.5, no. 4 1988"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+no?[,\.\s\-:/;]+([^,\.\s\-:/;]+)[,\.\s\-:/;\(\)]+(\d{4})[\(\)\s]?$','Vol. \1, No. \2, \3','gi')
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+no?[,\.\s\-:/;]+[^,\.\s\-:/;]+[,\.\s\-:/;\(\)]+\d{4}[\(\)\s]?$'

union all

-- "v.1 A-C"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+([^\s\-&])[\s\-&]+([^\s\-&])\s?$','Vol. \1 \2-\3','gi')
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+[^\s\-&][\s\-&]+[^\s\-&]\s?$'

union all

-- "v. 1 No. 1"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+no[\.\s,\-]+([^,\.\s\-:/;]+)\s?$','Vol. \1, No. \2','gi')
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+no[\.\s,\-]+[^,\.\s\-:/;]+\s?$'

union all

-- "v. 1/pt. 2"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+pt[\.\s,\-]+([^,\.\s\-:/;]+)\s?$','Vol. \1, Part \2','gi')
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+pt[\.\s,\-]+[^,\.\s\-:/;]+\s?$'

union all

-- "pt.2/v.1"
select
label,regexp_replace(label,'^pt[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+v[\.\s,\-]+([^,\.\s\-:/;]+)\s?$','Vol. \2, Part \1','gi')
from 
biblio.monograph_part
where 
label~*'^pt[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+v[\.\s,\-]+[^,\.\s\-:/;]+\s?$'

union all

-- "v.10 c.1"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+c\.[\.\s,\-]*([^,\.\s\-:/;]+)\s?$','Vol. \1, Copy \2','gi')
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+c\.[\.\s,\-]*[^,\.\s\-:/;]+\s?$'

union all

-- "Vol. 1,pt2 "
select
label,regexp_replace(label,'^vol[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+pt[\.\s,\-]*([^,\.\s\-:/;]+)\s?$','Vol. \1, Part \2','gi')
from 
biblio.monograph_part
where 
label~*'^vol[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+pt[\.\s,\-]*[^,\.\s\-:/;]+\s?$'

union all

-- "v.1 1974/75"
select
label,regexp_replace(label,'^v[\.\s,\-]+([^,\.\s\-:/;]+)[,\.\s\-:/;]+\(?(\d{2})(\d{2})[/\-]\(?(\d{2})\)?\s?$','Vol. \1, \2\3-\2\4','gi')
from 
biblio.monograph_part
where 
label~*'^v[\.\s,\-]+[^,\.\s\-:/;]+[,\.\s\-:/;]+\(?\d{4}[/\-]\(?\d{1,2}\)?\s?$'

union all




-- disk language

-- Disc X
select
label,regexp_replace(label,'^d[^\d]*(\d*)(.*)$','Disc \1','gi')
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
from 
biblio.monograph_part
where
label!~'\d{4}' and
label~*'^dis[csk]*[^\d]*\d+[\-&\s]+\d+\s*$'

union all

-- Disc X-Disc Y
select
label,regexp_replace(label,'^dis[csk]*[\-&\s,]*(\d*)[\-&\s,]*dis[csk]*[\-&\s,]*(\d*).*','Disc \1-\2','gi')
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
from 
biblio.monograph_part
where 
label~'\d'
and
label~*'^dvd[^\d]*\d*.*$'
and
label!~*'season'

union all

-- DVD {anything} Disc X-Y
select
label,regexp_replace(label,'^dvd.*dis[csk]*[\-&\s,]+(\d*)[\-&\s,]+(\d*)$','Disc \1-\2','gi')
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
from 
biblio.monograph_part
where 
label~*'^\(?\d{4}[/\-\\&\s]+\(?\d{4}\)?\s*[^\s\-\.]?[\s\.\-\:/\\]?$'

union all

-- YYYY-YY {optional qualifier}
select
label,regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[\\/\-]\(?(\d{2})\)?\s+\(?([^\s\)]+)\)?\-?\s?$','\1\2-\1\3 \4','gi')
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
label~*'^\(?\d{4}[/\-]\(?\d{2}\)?\s+[^\s]+\s?$'

union all

-- YYYY-YYYYY {optional qualifier}
select
label,regexp_replace(label,'^\s?\(?(\d{4})[\\/\-]\(?(\d{4})\)?\s+\(?([^\s\)]+)\)?\-?\s?$','\1-\2 \3','gi')
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

union all

-- YYYY {month}
select
label,initcap(regexp_replace(label,'^\(?(\d{4})[\\/,\s\:]*\(?([^\d\.\(/\-,\s\:]+)\.?\s?\)?$','\1:\2','gi'))
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
label,initcap(regexp_replace(label,'^\(?(\d{4})[/\-]\(?\d{1,2}\)?\s+\(?([^\s\)/\.\-]+)\)?\s?$','\1:\2','gi'))
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
label,initcap(regexp_replace(label,'^\(?(\d{2})(\d{2})[/\-]+\(?(\d{1,2})\)?\s+\(?([^\s\)/\.\-]+)[\)?\s?\\/]?([^\s\)/\.\-]+)\)?\s?$','\1\2:\4-\1\3:\5','gi'))
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
label,initcap(regexp_replace(label,'^\(?(\d{4})\s?\:?\(?([^\d\.\(/\-,\s]+)\.?[\-/\\]+([^\d\.\-/\)]+)\.?\s?\)?$','\1:\2-\3','gi'))
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
label,initcap(regexp_replace(label,'^\(?(\d{4})[\s\:\.]+([^\s\:\.]+)[\s\:\.]+(\d+)[\s\:\.]?\-\s*([^\s\:\.]+)[\s\:\.]+(\d+).*$','\1:\2 \3 - \1:\4 \5','gi'))
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
label,initcap(regexp_replace(label,'no\.?\s?(\d+);?\s+\(?(\d\d\d\d)\)?\s?\(?\:?[\d/]*\)?\s?([^\d\)\.\:,]+)\)?\.?,?$','No. \1, \2:\3','gi'))
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
label,initcap(regexp_replace(label,'^no\.?\s?(\d+);?\s+\(?([^\d\.]+)\)?\.*\s+\(?(\d\d\d\d)\)?.*$','No. \1, \3:\2','gi'))
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
label,initcap(regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+(\d{4})[\:,\.\s]*\(?\d*\)?[\:,\.\s]*([^\.\s/]+)\.?$','Vol. \1, No. \2, \3:\4','gi'))
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
label,initcap(regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+(\d{4})[\:,\.\s]+([^\(\.\s/]+)[\:,\.\s/]+([^\.\s/]+)\.?$','Vol. \1, No. \2, \3:\4 - \3:\5','gi'))
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
label,initcap(regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+([^\d\s\-/\\]+)[\s\.,]+(\d{4})\.?$','Vol. \1, No. \2, \4:\3','gi'))
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
label,initcap(regexp_replace(label,'no\.?\s?(\d+);?\s+\(?(\d\d\d\d)\)?\s?\(?\:?[\d/]*\)?\s?([^\d\)\.\:,/]+)[\)\.,/\-]+([^\d\)\.\:,/]+)\.?$','No. \1, \2:\3 - \2:\4','gi'))
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
label,initcap(regexp_replace(label,'^\s?(\d{1,3})/+(\d{1,3})[\s\.,]+([^\d\s\-/\\]+)[\s\-/\\]+([^\d\s\-/\\]+)[\s\.,]+(\d{4})\.?$','Vol. \1, No. \2, \5:\3 - \5:\4','gi'))
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
from 
biblio.monograph_part
where 
label~*'^no[\.,]\s*\d+;?\s*$'

union all

-- No. X-Y
select
label,regexp_replace(label,'no[\.,]?\s*(\d+)\s?\-\s?(\d+)$','No. \1-\2','gi')
from 
biblio.monograph_part
where 
label~*'^no[\.,]?\s*\d+\s?\-\s?\d+$'

union all

-- X of Y -> X
select
label,regexp_replace(label,'^\s?#?(\d+)\s*of\s*\d+$','\1','gi')
from 
biblio.monograph_part
where 
label~*'^\s?#?\d+\s*of\s*\d+$'

union all

-- #X
select
label,regexp_replace(label,'^\(?\s?#\s?(\d+)[\)\s]?$','No. \1','gi')
from 
biblio.monograph_part
where 
label~*'^\(?\s?#\s?\d+[\)\s]?$'

union all

-- X (1 or 2 digit bare numbers)
select
label,regexp_replace(label,'^\s*(\d{1,2})[\)\s]?$','Vol. \1','gi')
from 
biblio.monograph_part
where 
label~*'^\s*\d{1,2}[\)\s]?$'

union all


-- Part Language

-- "pt. 1"
select
label,regexp_replace(label,'^[\(\s]?pte?[\.\s,]*([^\\/\.\s,\-]+)[\s\-]*$','Part \1','gi')
from 
biblio.monograph_part
where 
label~*'^[\(\s]?pte?[\.\s,]*[^\\/\.\s,\-]+[\s\-]*$'

union all

-- "pt. X-Y"
select
label,regexp_replace(label,'^[\(\s]?pte?[\.\s,]+([^&\\/\s,\-]+)[&\\/\s,\-]+([^&\\/\s,\-]+)[\-\s/\\\.]*$','Part \1-\2','gi')
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
from 
biblio.monograph_part
where 
label~*'^[\(\s]?pt[\.\s]+[^\\/\.\s,\-]+[\\/\.\s,]+\d{4}\s?$'

union all

-- Part X, Vol. X (for rows starting with numeric values only)
select
label,regexp_replace(label,'^[\(\s]?(\d{1,3})\s+v[^\d]*(\d+)$','Part \1, Vol. \2','gi')
from 
biblio.monograph_part
where 
label~*'^[\(\s]?\d{1,3}\s+v[^\d]*\d+$'

union all

-- Part X, Vol. X (for rows starting with pt)
select
label,regexp_replace(label,'^[\(\s]?pt\.?\s?(\d+)\,?\s+v[^\d]*(\d+)$','Part \1, Vol. \2','gi')
from 
biblio.monograph_part
where 
label~*'^[\(\s]?pt\.?\s?\d+\,?\s+v[^\d]*\d+$'

union all

-- Part X, Vol. X (for rows starting with v)
select
label,regexp_replace(label,'^[\(\s]?v[^\s\.,]*[\s\.,]+([^\s\.,]+)[\s\.,]+p[^\s\.,]*[\s\.,]+([^\s\.,]+)\.?\s?,?$','Vol. \1, Part \2','gi')
from 
biblio.monograph_part
where 
label~*'^[\(\s]?v[^\s\.,]*[\s\.,]+[^\s\.,]+[\s\.,]+p[^\s\.,]*[\s\.,]+[^\s\.,]+\.?\s?,?$'

union all

-- Part X, Vol. X (for rows starting with {digits}th)
select
label,regexp_replace(label,'^\s?\(?\d+[tnrs][hdt].*p[^\s\.,]*[\.\s]+([^\s,\.\\/\-])+[\.\s]+v[^\s\.,]*[\.\s]+([^\s,\.\\/\-]+)\s?$','Vol. \2, Part \1','gi')
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
from 
biblio.monograph_part
where 
label~*'^[\(\s]?v[^\s\.,]*[\s\.,]+[^\s\.,]+[\s\.,]+p[^\s\.,]*[\s\.,]+[^\s\.,]+[\s\.,]n[^\s\.,]*[\s\.,]+[^\s\.,]+.*$'

union all

-- Part X, Vol. X, YYYY (for rows starting with v)
select
label,regexp_replace(label,'^[\(\s]?v[^\s\.,]*[\s\.,]+([^\s\.,]+)[\s\.,]+p[^\s\.,]*[\s\.,]+([^\s\.,]+)[\s\.,](\d{4})\.?\s?,?$','Vol. \1, Part \2, \3','gi')
from 
biblio.monograph_part
where 
label~*'^[\(\s]?v[^\s\.,]*[\s\.,]+[^\s\.,]+[\s\.,]+p[^\s\.,]*[\s\.,]+[^\s\.,]+[\s\.,]\d{4}\.?\s?,?$'

union all

-- Part X, Vol. X, YYYY-YYYY (for rows starting with v)
select
label,regexp_replace(label,'^[\(\s]?v[^\s\.,]*[\s\.,]+([^\s\.,]+)[\s\.,]+p[^\s\.,]*[\s\.,]+([^\s\.,]+)[\s\.,](\d{4})[\\/\-]+(\d{4})\.?\s?,?$','Vol. \1, Part \2, \3-\4','gi')
from 
biblio.monograph_part
where 
label~*'^[\(\s]?v[^\s\.,]*[\s\.,]+[^\s\.,]+[\s\.,]+p[^\s\.,]*[\s\.,]+[^\s\.,]+[\s\.,]\d{4}[\\/\-]+\d{4}\.?\s?,?$'

union all

-- Part X, YYYY
select
label,regexp_replace(regexp_replace(label,'^\s?(\d{4})\s+pt?\.\s?(\d+)([/\-]?\d*).*$','Part \2\3, \1','gi'),'/','-','gi')
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{4}\)?\s+pt?\..*$'

union all

-- Part X, YYYY-YYYY
select
label,regexp_replace(label,'^\s?\(?(\d{4})[\)\-/]+(\d{4})[\,\.\:]?\s+pt\.\s?([^\s]+).*$','Part \3, \1-\2','gi')
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
from 
biblio.monograph_part
where 
label~*'[,\.\:\s]+v[^,\.\:]*[,\.\:\s]+'
and
label~*'^\s?\(?ser[^,\.\:]?i?e?s?[,\.\:\s]+[^\s,\.\\/\-]+[\s,\.\\/\-]+v[^,\.\:]*[,\.\:\s]+[^\s,]+[,\s]?$'

union all

-- Part Z, Series X, Vol. Y
select
label,regexp_replace(label,'^\s?\(?ser[^,\.\:]?i?e?s?[,\.\:\s]+([^\s,\.\\/\-]+)[\s,\.\\/\-]+v[^,\.\:]*[,\.\:\s]+([^\s,]+)[,\.\:\s]+p[^,\.\:]*[,\.\:\s]+([^\s,]+)$','Series \1, Vol. \2,Part \3','gi')
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
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d+[tnrs][hdt][\s\.\-]+ser[^,\.\:]?i?e?s?[,\.\:\s]?$'

union all

-- Xrd Series, Vol. Y
select
label,regexp_replace(label,'^\s?\(?(\d+[tnrs][hdt])[\s\.\-]+ser[^,\.\:]?i?e?s?[,\.\:\s]+v[^,\.\:]*[,\.\:\s]+([^\s,]+)\s?$','\1 Series, Vol. \2','gi')
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
from 
biblio.monograph_part
where 
label~*'suppl'
and
label~*'^\s?\(?\d{4}[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?[^,\.\:\-\s]?[,\.\:\-\s]+\d{4}\s?$'

union all

-- YYYY : month Suppl (digit)?
select
label,btrim(regexp_replace(label,'^\s?\(?(\d{4})[,\.\:\-\s]+([^\d\.]+)[,\.\:\-\s]+suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]?)\s?$','\1:\2 Suppl. \3','gi'))
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
label,btrim(regexp_replace(label,'^\s?\(?suppl[\.ement]*[,\.\:\-\s]?([^,\.\:\-\s]?)[,\.\:\-\s]+(\d{4})[,\.\:\-\s]+([^\d\.]+)[,\.\:\-\s]?$','\2:\3 Suppl. \1','gi'))
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
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{2}[\\/\-]+\d{2}[\\/\-]+\d{2}[,\.\:\-\s]?$'

union all

-- "1889 June-1890 June" 
select
label,regexp_replace(label,'^\s?\(?(\d{4})[\\/\s\:]+([^\d\.]{3,12})[\.\\/\-\s]+(\d{4})[\\/\s\:]+([^\d\.]{3,12})[,\.\:\-\s]?$','\1:\2 - \3:\4','gi')
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

union all

-- "1906 Nov. 15-1907 Nov. 1"
select
label,regexp_replace(label,'^\s?\(?(\d{4})[\\/\s\:]+([^\d\.]{3,12})[\.\\/\-\s]+(\d{4})[\\/\s\:]+([^\d\.]{3,12})[,\.\:\-\s]?$','\1:\2 - \3:\4','gi')
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
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{1,3}[\\/\s\:]+\d{4}[\s\.\-]?$'

union all

-- "1/6"
select
label,regexp_replace(label,'^\s?\(?([^9]?\d{1,2})[\\/\s\:\-\.]+(\d{1,3})[\s\.\-]?$','Vol. \1, No. \2','gi')
from 
biblio.monograph_part
where 
label~*'^\s?\(?[^9]?\d{1,2}[\\/\s\:\-\.]+\d{1,3}[\s\.\-]?$'

union all

-- "1/4-1/5"
select
label,regexp_replace(label,'^\s?\(?([^9]?\d{1,2})[\\/\s\:]+([^9]?\d{1,2})[\s\.\-/\\]+([^9]?\d{1,2})[\\/\s\:]+(\d{1,3})[\s\.\-]?$','Vol. \1, No. \2 - Vol. \3, No. \4','gi')
from 
biblio.monograph_part
where 
label~*'^\s?\(?[^9]?\d{1,2}[\\/\s\:]+[^9]?\d{1,2}[\s\.\-/\\]+[^9]?\d{1,2}[\\/\s\:]+\d{1,3}[\s\.\-]?$'

union all

-- "958/1-"
select
label,to_char(to_date(regexp_replace(label,'^\s?\(?(9\d{1,2})[\\/\s\:\-\.]+(\d{1,3})[\s\.\-]?$','1\1-\2','gi'),'YYYY-MM'),'YYYY:Mon')
from 
biblio.monograph_part
where 
label~*'^\s?\(?9\d{1,2}[\\/\s\:\-\.]+\d{1,3}[\s\.\-]?$'

union all

-- "988 June 1988"
select
label,regexp_replace(label,'^\s?\(?9\d{1,2}[\\/\s\:\-\.]+([^\d\.]{3,12})[\s\.\-]+(\d{4})[\s\.\-]?$','\2:\1','gi')
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
from 
biblio.monograph_part
where 
label~*'^\s?9\d{2}[\\/\s\:\-\.]+\d{1,2}[\\/\s\.\-]+9\d{2}[\\/\s\:\-\.]+\d{1,2}[\\/\s\.\-]+[^\d\.]{3,12}[\\/\s\.\:]+\d{4}[\\/\s\:\-\.]+[^\d\.]{3,12}.*$'

union all

-- "1923/YY" where YY <= 12 and subtraction between the two years is less than 15
select
label,regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1\2-\1\3','gi')
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
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{4}[\\/\s\:\-\.]+\d[\s\.\-]?$'

union all

-- "1923/XX" two digits after the / ( > 12 )
select
label,regexp_replace(label,'^\s?\(?(\d{2})(\d{2})[\\/\s\:\-\.]+(\d{2})[\s\.\-]?$','\1\2-\1\3','gi')
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
label,regexp_replace(label,'^\s?\(?(\d{4})[\s\./\\\d\(\)]?\(\d+\)\s+([^\s\.\d\\/]{3,5})[\)\s]?$','\1:\2','gi')
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

-- "1992/6 (June)"
select
label,to_char(to_date(regexp_replace(label,'^\s?\(?(\d{4})[\s\./\\\(\)]+(\d)[\.,\\/\s\(]+[^\s\.\d\\/]{3,12}[\)\s]?$','\1-\2','gi'),'YYYY-MM'),'YYYY:Mon')
from 
biblio.monograph_part
where 
label~*'^\s?\(?\d{4}[\s\./\\\(\)]+\d[\.,\\/\s\(]+[^\s\.\d\\/]{3,12}[\)\s]?$'
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
from 
biblio.monograph_part
where 
label~*'^[\(\s]?bks?[\.\s]*[^\\/\.\s,\-]+[\s\-]*$'

union all

-- bk X-Y
select
label,regexp_replace(label,'^[\(\s]?bks?[\.\s]*([^\\/\.\s,\-]+)[\s\-]+([^\\/\.\s,\-]+)[\s\-]*$','Book \1-\2','gi')
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
from 
biblio.monograph_part
where 
label~*'^[\(\s]?\d{4}[\.\s\|\\/]+sup?[\.\s]+[\s\-]*$'

union all

-- "pt.1 1960/1962"
select
label,regexp_replace(label,'^[\(\s]?pt[\.\s\|\\/]+([^\s,\.]+)[\s,\.]+(\d{4})[\\/\.\s]+(\d{4})[\s\-]*$','Part \1, \2-\3','gi')
from 
biblio.monograph_part
where 
label~*'^[\(\s]?pt[\.\s\|\\/]+[^\s,\.]+[\s,\.]+\d{4}[\\/\.\s]+\d{4}[\s\-]*$'


)
as a


group by 1,2
order by length(a.label),1,2

