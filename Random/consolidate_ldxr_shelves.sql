BEGIN;
DROP TABLE IF EXISTS newshelves;
CREATE table newshelves (
name text,
holdable boolean,
hold_verify boolean,
opac_visible boolean,
circulate boolean,
checkin_alert boolean
);

--populate little_dixie.csv from: https://docs.google.com/spreadsheets/d/1FzcJzEwJNdNbaWZr-K1khcq7R8SnG552v9EmSzHvD2M/edit?usp=sharing
\copy newshelves(name,holdable,hold_verify,opac_visible,circulate,checkin_alert) from /mnt/evergreen/tmp/little_dixie.csv
--COPY 46
COMMIT;

select acl.name, count(*) from asset.copy_location acl, newshelves ns where owning_lib in (select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou!=1) and 
  acl.name = ns.name and not acl.deleted
  group by 1 order by 1 ;

BEGIN;
--make to-be-consolidated branch-level shelves have consistent settings:
UPDATE asset.copy_location acl
  SET holdable = ns.holdable,
    hold_verify = ns.hold_verify,
    opac_visible = ns.opac_visible,
    circulate = ns.circulate,
    checkin_alert = ns.checkin_alert
  FROM newshelves ns
  WHERE
  owning_lib in (select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou!=1) and 
  acl.name = ns.name and not deleted;
--UPDATE 180

select * from asset.copy_location where not deleted and owning_lib = 137;
--21 rows (on upgrade)

--create system-level shelves for Little Dixie.
INSERT INTO asset.copy_location(name,owning_lib,holdable,hold_verify,opac_visible,circulate,checkin_alert) 
  select distinct name,137,holdable,hold_verify,opac_visible,circulate,checkin_alert 
  from newshelves ns;
--INSERT 0 46 (on upgrade)

select * from asset.copy_location where not deleted and owning_lib = 137;
--67 rows (mig1)

    --look for branch-level shelf names which aren't system-level shelf names (should be zero now)
    select distinct name from asset.copy_location where not deleted and 
    owning_lib in (select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou!=1) and 
    name not in 
     (select distinct name from asset.copy_location where not deleted and 
     owning_lib in (select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou=1) 
     );
     --0 rows (mig1)
     --0 rows (upgrade)

    --look for duplicate system-level shelf names (should be zero)
    select name, count(*) from asset.copy_location where not deleted and owning_lib in 
      (select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou=1)
      group by 1 having count(1) > 1 order by 2 desc;


--system-level shelving map
drop table if exists ld_shelves;
create table ld_shelves as 
    select name, id as oldid from asset.copy_location where name in 
    (select name from asset.copy_location where owning_lib in (select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou=1)) 
    and id not in 
    (select id from asset.copy_location where owning_lib in (select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou=1)) 
    and owning_lib in (select id from actor.org_unit where shortname ~ 'LDXR') 
    and not deleted order by 1;
--SELECT 184

--add new ID's to map
alter table ld_shelves add column newid integer;
UPDATE ld_shelves lds 
  SET newid = a.newid
  FROM 
   (select name,id as newid from asset.copy_location acl where 
    owning_lib in (select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou=1)) a
  WHERE
  a.name=lds.name;
--UPDATE 184


--update item shelving locations for Little Dixie items.
UPDATE asset.copy ac
  SET location = lds.newid
  FROM ld_shelves lds
  WHERE 
  lds.oldid = ac.location and 
  ac.circ_lib in (137,138,139,140,141) and 
  not deleted
  and
  ac.location in(
    select id from asset.copy_location 
      where 
      owning_lib in(
          select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou!=1
          )
      );

--UPDATE 180935

--update circulation shelving locations for Little Dixie items.
UPDATE action.circulation acirc
  SET copy_location = lds.newid
  FROM ld_shelves lds
  WHERE 
      lds.oldid = acirc.copy_location and 
      acirc.circ_lib in (137,138,139,140,141) 
    and
    acirc.copy_location in(
      select id from asset.copy_location 
        where 
        owning_lib in(
            select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou!=1
            )
        );
--UPDATE 672999

    select location from asset.copy 
      WHERE 
      not deleted and 
      circ_lib in (137,138,139,140,141) and 
      location in(
      select id from asset.copy_location 
        where 
        owning_lib in(
            select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou!=1
            )
        )
     ; 
    select distinct ac.location as ac_location, lds.newid as lds_newid, acl.id as acl_id, acl.name, ac.circ_lib,ac.id as ac_id from 
    ld_shelves lds, 
    asset.copy_location acl, 
        asset.copy ac
      WHERE not ac.deleted and 
      ac.circ_lib in (137,138,139,140,141) and  
      ac.location = acl.id and 
      acl.name = lds.name 
    and
    ac.location in(
      select id from asset.copy_location 
        where 
        owning_lib in(
            select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou!=1
            )
        )
       ;
COMMIT;
