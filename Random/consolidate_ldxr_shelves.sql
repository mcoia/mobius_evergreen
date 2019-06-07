BEGIN;


-- Make system shelving locations

INSERT INTO asset.copy_location(name,owning_lib) 
  select distinct name,137
  from asset.copy_location where owning_lib in(select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou!=1) and
  not deleted and
  lower(name) not in
  (select lower(name) from asset.copy_location where owning_lib in(select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou=1) and not deleted);
  
-- Flip a few of the shelves based upon the libraries wishes:
-- https://docs.google.com/spreadsheets/d/1FzcJzEwJNdNbaWZr-K1khcq7R8SnG552v9EmSzHvD2M/edit#gid=0

UPDATE asset.copy_location set
holdable='f'
where
owning_lib=137 and
name in ('Internet','Main Library','Office','Ready Reference') and
not deleted;

UPDATE asset.copy_location set
opac_visible='f'
where
owning_lib=137 and
name in ('Special Reference') and
not deleted;

UPDATE asset.copy_location set
opac_visible='t'
where
owning_lib=137 and
name in ('Staff Reference') and
not deleted;

select acl.name, count(*) from asset.copy_location acl, newshelves ns where owning_lib in (select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou!=1) and 
  acl.name = ns.name and not acl.deleted
  group by 1 order by 1 ;

  
  
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

create temp table ld_shelves as 
    select 
    acl_branch.id as old_id,
    acl_sys.id as new_id
    from 
    asset.copy_location acl_sys,
    asset.copy_location acl_branch
    where 
    acl_branch.owning_lib in
    (select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou!=1) 
    and
    acl_sys.owning_lib in
    (select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou=1)
    and
    not acl_sys.deleted and
    acl_sys.name=acl_branch.name;
--SELECT 184

-- See how the map looks

select acl_new.id,acl_new.name,acl_old.name,acl_old.id
from
asset.copy_location acl_new,
asset.copy_location acl_old,
ld_shelves lds
where
lds.new_id=acl_new.id and
lds.old_id=acl_old.id;


-- Change the name temporarily for the old shelving locations
UPDATE asset.copy_location acl
set name=name||'_old'
where
id in(select old_id from ld_shelves);

--update item shelving locations for Little Dixie items.
UPDATE asset.copy ac
  SET location = lds.new_id
  FROM ld_shelves lds
  WHERE 
  lds.old_id = ac.location;

--UPDATE 180935

--update circulation shelving locations for Little Dixie items.
UPDATE action.circulation acirc
  SET copy_location = lds.new_id
  FROM ld_shelves lds
  WHERE 
      lds.old_id = acirc.copy_location ;
--UPDATE 672999

    select distinct location from asset.copy 
      WHERE 
      not deleted and 
      circ_lib in (select id from actor.org_unit where shortname ~ 'LDXR') and 
      location in(
      
        select old_id from ld_shelves
        )
     ; 
     
         select distinct location from asset.copy 
      WHERE 
      not deleted and 
      circ_lib in (select id from actor.org_unit where shortname ~ 'LDXR') and 
      location in(select id from asset.copy_location where not deleted and owning_lib in(select id from actor.org_unit where shortname ~ 'LDXR' and parent_ou!=1))
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
       
       UPDATE asset.copy_location set deleted='t' where id in(select old_id from ld_shelves);
       
COMMIT;
