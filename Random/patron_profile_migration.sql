

CREATE TABLE IF NOT EXISTS m_coo.patron_profile_move (id serial, usr bigint, home_ou bigint, home_ou_system bigint, profile bigint,new_profile bigint,usr_dob date,home_ou_system_name text, old_profile_name text,new_profile_name text);

CREATE INDEX m_coo_patron_profile_move_idx
  ON m_coo.patron_profile_move
  USING btree
  (usr);

  CREATE INDEX m_coo_patron_profile_move_home_ou_idx
  ON m_coo.patron_profile_move
  USING btree
  (home_ou);
  
  CREATE INDEX m_coo_patron_profile_move_profile_idx
  ON m_coo.patron_profile_move
  USING btree
  (profile);
  
  
  CREATE INDEX m_coo_patron_profile_move_new_profile_idx
  ON m_coo.patron_profile_move
  USING btree
  (new_profile);
  
truncate   m_coo.patron_profile_move;
  
\echo Cumberland
----------------------------
-- Cumberland
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(58) and
pgt_new.id=26 and
au.home_ou in(207, 208, 209, 210, 211, 212, 213, 214, 219, 220, 221);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(30,57) and
pgt_new.id=32 and
au.home_ou in(207, 208, 209, 210, 211, 212, 213, 214, 219, 220, 221);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(56) and
pgt_new.id=78 and
au.home_ou in(207, 208, 209, 210, 211, 212, 213, 214, 219, 220, 221);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(26) and
pgt_new.id=27 and
au.home_ou in(207, 208, 209, 210, 211, 212, 213, 214, 219, 220, 221);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(59) and
pgt_new.id=30 and
au.home_ou in(207, 208, 209, 210, 211, 212, 213, 214, 219, 220, 221)
and au.id not in(select usr from m_coo.patron_profile_move);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(40) and
pgt_new.id=42 and
au.home_ou in(207, 208, 209, 210, 211, 212, 213, 214, 219, 220, 221)
and au.id not in(select usr from m_coo.patron_profile_move);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(38) and
pgt_new.id=27 and
au.home_ou in(207, 208, 209, 210, 211, 212, 213, 214, 219, 220, 221)
and au.id not in(select usr from m_coo.patron_profile_move);





 \echo Appalachian 
----------------------------
-- Appalachian
----------------------------
insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile=36 and
pgt_new.id=78 and
au.home_ou in(126, 128, 132, 133, 137, 139);

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile=38 and
pgt_new.id=30 and
au.home_ou in(126, 128, 132, 133, 137, 139);

\echo BHM 
----------------------------
-- BHM
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile=36 and
pgt_new.id=25 and
au.home_ou in(236, 237, 238, 239, 240, 241, 242, 244, 245, 246, 247);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile=38 and
pgt_new.id=26 and
au.home_ou in(236, 237, 238, 239, 240, 241, 242, 244, 245, 246, 247);

\echo BHM DOB SPECIFIC
----------------------------
-- BHM
-- DOB SPECIFIC
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile=22 and
pgt_new.id=25 and
au.home_ou in(236, 237, 238, 239, 240, 241, 242, 244, 245, 246, 247)
and 
(au.dob < now() - '18 years'::interval or
au.dob is null
);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile=22 and
pgt_new.id=27 and
au.home_ou in(236, 237, 238, 239, 240, 241, 242, 244, 245, 246, 247)
and au.dob between (now() - '18 years'::interval) and (now() - '13 years'::interval);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile=22 and
pgt_new.id=26 and
au.home_ou in(236, 237, 238, 239, 240, 241, 242, 244, 245, 246, 247)
and au.dob > now() - '13 years'::interval;


\echo Bladen

----------------------------
-- Bladen
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile=36 and
pgt_new.id=25 and
au.home_ou in(336, 337, 338, 339, 340);

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile=38 and
pgt_new.id=30 and
au.home_ou in(336, 337, 338, 339, 340);


\echo Buncombe
----------------------------
-- Buncombe
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(41,64) and
pgt_new.id=27 and
au.home_ou in(336, 337, 338, 339, 340);

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(39,21,36) and
pgt_new.id=25 and
au.home_ou in(111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(38) and
pgt_new.id=26 and
au.home_ou in(111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124);

\echo Buncombe DOB SPECIFIC
----------------------------
-- Buncombe
-- DOB SPECIFIC
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(40) and
pgt_new.id=78 and
au.home_ou in(111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124)
and 
(au.dob < now() - '18 years'::interval or
au.dob is null
);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(40) and
pgt_new.id=32 and
au.home_ou in(111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124)
and 
au.dob between (now() - '18 years'::interval) and (now() - '13 years'::interval);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(40) and
pgt_new.id=30 and
au.home_ou in(111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124)
and 
au.dob > now() - '13 years'::interval;


\echo Caldwell
----------------------------
-- Caldwell
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(36) and
pgt_new.id=25 and
au.home_ou in(188, 189, 190, 194);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(38) and
pgt_new.id=26 and
au.home_ou in(188, 189, 190, 194);


\echo Cleveland

----------------------------
-- Cleveland
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(39) and
pgt_new.id=25 and
au.home_ou in(2, 4, 101, 103);

\echo Cleveland DOB SPECIFIC
----------------------------
-- Cleveland
-- DOB SPECIFIC
----------------------------


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(16) and
pgt_new.id=25 and
au.home_ou in(2, 4, 101, 103)
and 
(au.dob < now() - '18 years'::interval or
au.dob is null
);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(16) and
pgt_new.id=27 and
au.home_ou in(2, 4, 101, 103)
and 
au.dob between (now() - '18 years'::interval) and (now() - '13 years'::interval);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(16) and
pgt_new.id=26 and
au.home_ou in(2, 4, 101, 103)
and 
au.dob > now() - '13 years'::interval;



\echo Mauney DOB SPECIFIC
----------------------------
-- Mauney
-- DOB SPECIFIC
----------------------------


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(16) and
pgt_new.id=25 and
au.home_ou in(102)
and 
(au.dob < now() - '13 years'::interval or
au.dob is null
);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(16) and
pgt_new.id=26 and
au.home_ou in(102)
and 
au.dob > now() - '13 years'::interval;




\echo Davidson
----------------------------
-- Davidson
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(28) and
pgt_new.id=26 and
au.home_ou in(104, 105, 106, 107, 108, 109, 110);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(21) and
pgt_new.id=25 and
au.home_ou in(104, 105, 106, 107, 108, 109, 110);


\echo Davie
----------------------------
-- Davie
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(38) and
pgt_new.id=26 and
au.home_ou in(165, 166, 167);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(41) and
pgt_new.id=27 and
au.home_ou in(165, 166, 167);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(36) and
pgt_new.id=25 and
au.home_ou in(165, 166, 167);


\echo Davie DOB SPECIFIC
----------------------------
-- Davie
-- DOB SPECIFIC
----------------------------


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(40) and
pgt_new.id=78 and
au.home_ou in(165, 166, 167)
and 
(au.dob < now() - '18 years'::interval or
au.dob is null
);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(40) and
pgt_new.id=32 and
au.home_ou in(165, 166, 167)
and 
au.dob between (now() - '18 years'::interval) and (now() - '13 years'::interval);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(40) and
pgt_new.id=30 and
au.home_ou in(165, 166, 167)
and 
au.dob > now() - '13 years'::interval;


\echo Fontana
----------------------------
-- Fontana
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(41) and
pgt_new.id=27 and
au.home_ou in(155, 157, 158, 159, 160, 161, 162, 163, 164);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(38) and
pgt_new.id=26 and
au.home_ou in(155, 157, 158, 159, 160, 161, 162, 163, 164);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(36) and
pgt_new.id=25 and
au.home_ou in(155, 157, 158, 159, 160, 161, 162, 163, 164);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(58) and
pgt_new.id=26 and
au.home_ou in(155, 157, 158, 159, 160, 161, 162, 163, 164);



\echo Fontana DOB SPECIFIC
----------------------------
-- Fontana
-- DOB SPECIFIC
----------------------------


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(40) and
pgt_new.id=78 and
au.home_ou in(155, 157, 158, 159, 160, 161, 162, 163, 164)
and 
(au.dob < now() - '18 years'::interval or
au.dob is null
);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(40) and
pgt_new.id=32 and
au.home_ou in(155, 157, 158, 159, 160, 161, 162, 163, 164)
and 
au.dob between (now() - '18 years'::interval) and (now() - '13 years'::interval);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(40) and
pgt_new.id=30 and
au.home_ou in(155, 157, 158, 159, 160, 161, 162, 163, 164)
and 
au.dob > now() - '13 years'::interval;



\echo Forsyth 
----------------------------
-- Forsyth
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(62) and
pgt_new.id=32 and
au.home_ou in(222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 250);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(41) and
pgt_new.id=27 and
au.home_ou in(222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 250);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(57) and
pgt_new.id=30 and
au.home_ou in(222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 250);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(56) and
pgt_new.id=78 and
au.home_ou in(222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 250);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(36) and
pgt_new.id=25 and
au.home_ou in(222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 250);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(38) and
pgt_new.id=26 and
au.home_ou in(222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 250);



\echo Franklin
----------------------------
-- Franklin
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(36) and
pgt_new.id=25 and
au.home_ou in(141, 142, 144, 150, 151, 154);


insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(38) and
pgt_new.id=26 and
au.home_ou in(141, 142, 144, 150, 151, 154);


\echo Granville
----------------------------
-- Granville
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(36) and
pgt_new.id=25 and
au.home_ou in(341, 342, 343, 344, 345, 346, 347, 348);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(38) and
pgt_new.id=26 and
au.home_ou in(341, 342, 343, 344, 345, 346, 347, 348);

\echo Harnett
----------------------------
-- Harnett
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(36) and
pgt_new.id=25 and
au.home_ou in(263, 264, 265, 266, 267, 268, 269, 270, 349);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(38) and
pgt_new.id=26 and
au.home_ou in(263, 264, 265, 266, 267, 268, 269, 270, 349);



\echo Haywood
----------------------------
-- Haywood
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(36) and
pgt_new.id=25 and
au.home_ou in(173, 174, 175, 176, 177, 178);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(38) and
pgt_new.id=26 and
au.home_ou in(173, 174, 175, 176, 177, 178);




\echo Iredell
----------------------------
-- Iredell
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(36) and
pgt_new.id=25 and
au.home_ou in(305, 306, 307, 308, 372);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(38) and
pgt_new.id=26 and
au.home_ou in(305, 306, 307, 308, 372);



\echo Johnston
----------------------------
-- Johnston
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(36) and
pgt_new.id=25 and
au.home_ou in(313, 314, 315, 316, 317, 318, 319, 320, 321, 322, 323);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(38) and
pgt_new.id=26 and
au.home_ou in(313, 314, 315, 316, 317, 318, 319, 320, 321, 322, 323);



\echo Lee
----------------------------
-- Lee
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(36) and
pgt_new.id=25 and
au.home_ou in(169, 170, 171);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(38) and
pgt_new.id=26 and
au.home_ou in(169, 170, 171);


\echo McDowell
----------------------------
-- McDowell
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(40) and
pgt_new.id=78 and
au.home_ou in(195, 196, 197);


\echo Neuse
----------------------------
-- Neuse
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(36) and
pgt_new.id=25 and
au.home_ou in(288, 289, 290, 291, 292, 293, 294, 295, 296);


\echo Perry
----------------------------
-- Perry
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(36) and
pgt_new.id=25 and
au.home_ou in(369, 370);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(38) and
pgt_new.id=26 and
au.home_ou in(369, 370);


\echo Rockingham
----------------------------
-- Rockingham
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(56) and
pgt_new.id=78 and
au.home_ou in(253, 254, 257, 258, 259, 260, 261);



\echo Wayne
----------------------------
-- Wayne
----------------------------

insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(36) and
pgt_new.id=25 and
au.home_ou in(183, 184, 185, 186, 187);



insert into  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
from
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
where
au.home_ou=aou_branch.id and
aou_branch.parent_ou=aou_sys.id and
pgt_old.id=au.profile and
au.profile in(38) and
pgt_new.id=26 and
au.home_ou in(183, 184, 185, 186, 187);














-- MAKE SURE WE DIDN'T STAGE TWO OF THE SAME PATRON

-- select usr,count(*) from  m_coo.patron_profile_move group by 1 having count(*) > 1;





-- Finally perform the migration
begin;


commit;