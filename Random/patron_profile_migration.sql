
DROP TABLE IF EXISTS m_coo.patron_profile_move;
CREATE TABLE IF NOT EXISTS m_coo.patron_profile_move (
id serial,
usr bigint,
home_ou bigint,
home_ou_system bigint,
profile bigint,
new_profile bigint,
usr_dob date,
home_ou_system_name text,
old_profile_name text,
new_profile_name text,
done boolean default false,
default_map boolean default false
 );

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

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(58) AND
pgt_new.id=26 AND
au.home_ou in(207, 208, 209, 210, 211, 212, 213, 214, 219, 220, 221);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(30,57) AND
pgt_new.id=32 AND
au.home_ou in(207, 208, 209, 210, 211, 212, 213, 214, 219, 220, 221);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(56) AND
pgt_new.id=78 AND
au.home_ou in(207, 208, 209, 210, 211, 212, 213, 214, 219, 220, 221);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36) AND
pgt_new.id=25 AND
au.home_ou in(207, 208, 209, 210, 211, 212, 213, 214, 219, 220, 221);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(26) AND
pgt_new.id=27 AND
au.home_ou in(207, 208, 209, 210, 211, 212, 213, 214, 219, 220, 221);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(59) AND
pgt_new.id=30 AND
au.home_ou in(207, 208, 209, 210, 211, 212, 213, 214, 219, 220, 221);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(40) AND
pgt_new.id=42 AND
au.home_ou in(207, 208, 209, 210, 211, 212, 213, 214, 219, 220, 221);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38) AND
pgt_new.id=27 AND
au.home_ou in(207, 208, 209, 210, 211, 212, 213, 214, 219, 220, 221);





 \echo Appalachian 
----------------------------
-- Appalachian
----------------------------
INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile=36 AND
pgt_new.id=78 AND
au.home_ou in(126, 128, 132, 133, 137, 139);

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile=38 AND
pgt_new.id=30 AND
au.home_ou in(126, 128, 132, 133, 137, 139);

\echo BHM 
----------------------------
-- BHM
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile=36 AND
pgt_new.id=25 AND
au.home_ou in(236, 237, 238, 239, 240, 241, 242, 244, 245, 246, 247);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile=38 AND
pgt_new.id=26 AND
au.home_ou in(236, 237, 238, 239, 240, 241, 242, 244, 245, 246, 247);

\echo BHM DOB SPECIFIC
----------------------------
-- BHM
-- DOB SPECIFIC
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile=22 AND
pgt_new.id=25 AND
au.home_ou in(236, 237, 238, 239, 240, 241, 242, 244, 245, 246, 247)
AND 
(au.dob < now() - '18 years'::interval or
au.dob is null
);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile=22 AND
pgt_new.id=27 AND
au.home_ou in(236, 237, 238, 239, 240, 241, 242, 244, 245, 246, 247)
AND au.dob between (now() - '18 years'::interval) AND (now() - '13 years'::interval);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile=22 AND
pgt_new.id=26 AND
au.home_ou in(236, 237, 238, 239, 240, 241, 242, 244, 245, 246, 247)
AND au.dob > now() - '13 years'::interval;


\echo Bladen

----------------------------
-- Bladen
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile=36 AND
pgt_new.id=25 AND
au.home_ou in(336, 337, 338, 339, 340);

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile=38 AND
pgt_new.id=30 AND
au.home_ou in(336, 337, 338, 339, 340);


\echo Buncombe
----------------------------
-- Buncombe
----------------------------


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(41,64) AND
pgt_new.id=27 AND
au.home_ou in(111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124);

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(39,21,36) AND
pgt_new.id=25 AND
au.home_ou in(111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38) AND
pgt_new.id=26 AND
au.home_ou in(111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124);

\echo Buncombe DOB SPECIFIC
----------------------------
-- Buncombe
-- DOB SPECIFIC
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(40) AND
pgt_new.id=78 AND
au.home_ou in(111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124)
AND 
(au.dob < now() - '18 years'::interval or
au.dob is null
);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(40) AND
pgt_new.id=32 AND
au.home_ou in(111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124)
AND 
au.dob between (now() - '18 years'::interval) AND (now() - '13 years'::interval);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(40) AND
pgt_new.id=30 AND
au.home_ou in(111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124)
AND 
au.dob > now() - '13 years'::interval;


\echo Caldwell
----------------------------
-- Caldwell
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36) AND
pgt_new.id=25 AND
au.home_ou in(188, 189, 190, 194);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38) AND
pgt_new.id=26 AND
au.home_ou in(188, 189, 190, 194);


\echo Cleveland

----------------------------
-- Cleveland
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(39) AND
pgt_new.id=25 AND
au.home_ou in(2, 4, 101, 103);

\echo Cleveland DOB SPECIFIC
----------------------------
-- Cleveland
-- DOB SPECIFIC
----------------------------


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(16) AND
pgt_new.id=25 AND
au.home_ou in(2, 4, 101, 103)
AND 
(au.dob < now() - '18 years'::interval or
au.dob is null
);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(16) AND
pgt_new.id=27 AND
au.home_ou in(2, 4, 101, 103)
AND 
au.dob between (now() - '18 years'::interval) AND (now() - '13 years'::interval);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(16) AND
pgt_new.id=26 AND
au.home_ou in(2, 4, 101, 103)
AND 
au.dob > now() - '13 years'::interval;



\echo Mauney DOB SPECIFIC
----------------------------
-- Mauney
-- DOB SPECIFIC
----------------------------


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(16) AND
pgt_new.id=25 AND
au.home_ou in(102)
AND 
(au.dob < now() - '13 years'::interval or
au.dob is null
);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(16) AND
pgt_new.id=26 AND
au.home_ou in(102)
AND 
au.dob > now() - '13 years'::interval;




\echo Davidson
----------------------------
-- Davidson
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(28) AND
pgt_new.id=26 AND
au.home_ou in(104, 105, 106, 107, 108, 109, 110);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(21) AND
pgt_new.id=25 AND
au.home_ou in(104, 105, 106, 107, 108, 109, 110);


\echo Davie
----------------------------
-- Davie
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38) AND
pgt_new.id=26 AND
au.home_ou in(165, 166, 167);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(41) AND
pgt_new.id=27 AND
au.home_ou in(165, 166, 167);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36) AND
pgt_new.id=25 AND
au.home_ou in(165, 166, 167);


\echo Davie DOB SPECIFIC
----------------------------
-- Davie
-- DOB SPECIFIC
----------------------------


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(40) AND
pgt_new.id=78 AND
au.home_ou in(165, 166, 167)
AND 
(au.dob < now() - '18 years'::interval or
au.dob is null
);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(40) AND
pgt_new.id=32 AND
au.home_ou in(165, 166, 167)
AND 
au.dob between (now() - '18 years'::interval) AND (now() - '13 years'::interval);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(40) AND
pgt_new.id=30 AND
au.home_ou in(165, 166, 167)
AND 
au.dob > now() - '13 years'::interval;


\echo Fontana
----------------------------
-- Fontana
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(41) AND
pgt_new.id=27 AND
au.home_ou in(155, 157, 158, 159, 160, 161, 162, 163, 164);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38) AND
pgt_new.id=26 AND
au.home_ou in(155, 157, 158, 159, 160, 161, 162, 163, 164);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36) AND
pgt_new.id=25 AND
au.home_ou in(155, 157, 158, 159, 160, 161, 162, 163, 164);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(58) AND
pgt_new.id=26 AND
au.home_ou in(155, 157, 158, 159, 160, 161, 162, 163, 164);



\echo Fontana DOB SPECIFIC
----------------------------
-- Fontana
-- DOB SPECIFIC
----------------------------


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(40) AND
pgt_new.id=78 AND
au.home_ou in(155, 157, 158, 159, 160, 161, 162, 163, 164)
AND 
(au.dob < now() - '18 years'::interval or
au.dob is null
);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(40) AND
pgt_new.id=32 AND
au.home_ou in(155, 157, 158, 159, 160, 161, 162, 163, 164)
AND 
au.dob between (now() - '18 years'::interval) AND (now() - '13 years'::interval);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(40) AND
pgt_new.id=30 AND
au.home_ou in(155, 157, 158, 159, 160, 161, 162, 163, 164)
AND 
au.dob > now() - '13 years'::interval;



\echo Forsyth 
----------------------------
-- Forsyth
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(62) AND
pgt_new.id=32 AND
au.home_ou in(222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 250);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(41) AND
pgt_new.id=27 AND
au.home_ou in(222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 250);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(57) AND
pgt_new.id=30 AND
au.home_ou in(222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 250);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(56) AND
pgt_new.id=78 AND
au.home_ou in(222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 250);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36) AND
pgt_new.id=25 AND
au.home_ou in(222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 250);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38) AND
pgt_new.id=26 AND
au.home_ou in(222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 250);



\echo Franklin
----------------------------
-- Franklin
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36) AND
pgt_new.id=25 AND
au.home_ou in(141, 142, 144, 150, 151, 154);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38) AND
pgt_new.id=26 AND
au.home_ou in(141, 142, 144, 150, 151, 154);


\echo Granville
----------------------------
-- Granville
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36) AND
pgt_new.id=25 AND
au.home_ou in(341, 342, 343, 344, 345, 346, 347, 348);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38) AND
pgt_new.id=26 AND
au.home_ou in(341, 342, 343, 344, 345, 346, 347, 348);

\echo Harnett
----------------------------
-- Harnett
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36) AND
pgt_new.id=25 AND
au.home_ou in(263, 264, 265, 266, 267, 268, 269, 270, 349);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38) AND
pgt_new.id=26 AND
au.home_ou in(263, 264, 265, 266, 267, 268, 269, 270, 349);



\echo Haywood
----------------------------
-- Haywood
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36) AND
pgt_new.id=25 AND
au.home_ou in(173, 174, 175, 176, 177, 178);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38) AND
pgt_new.id=26 AND
au.home_ou in(173, 174, 175, 176, 177, 178);




\echo Iredell
----------------------------
-- Iredell
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36) AND
pgt_new.id=25 AND
au.home_ou in(305, 306, 307, 308, 372);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38) AND
pgt_new.id=26 AND
au.home_ou in(305, 306, 307, 308, 372);



\echo Johnston
----------------------------
-- Johnston
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36) AND
pgt_new.id=25 AND
au.home_ou in(313, 314, 315, 316, 317, 318, 319, 320, 321, 322, 323);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38) AND
pgt_new.id=26 AND
au.home_ou in(313, 314, 315, 316, 317, 318, 319, 320, 321, 322, 323);



\echo Lee
----------------------------
-- Lee
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36) AND
pgt_new.id=25 AND
au.home_ou in(169, 170, 171);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38) AND
pgt_new.id=26 AND
au.home_ou in(169, 170, 171);


\echo McDowell
----------------------------
-- McDowell
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(40) AND
pgt_new.id=78 AND
au.home_ou in(195, 196, 197);


\echo Neuse
----------------------------
-- Neuse
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36) AND
pgt_new.id=25 AND
au.home_ou in(288, 289, 290, 291, 292, 293, 294, 295, 296);


\echo Perry
----------------------------
-- Perry
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36) AND
pgt_new.id=25 AND
au.home_ou in(369, 370);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38) AND
pgt_new.id=26 AND
au.home_ou in(369, 370);


\echo Rockingham
----------------------------
-- Rockingham
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(56) AND
pgt_new.id=78 AND
au.home_ou in(253, 254, 257, 258, 259, 260, 261);



\echo Wayne
----------------------------
-- Wayne
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36) AND
pgt_new.id=25 AND
au.home_ou in(183, 184, 185, 186, 187);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38) AND
pgt_new.id=26 AND
au.home_ou in(183, 184, 185, 186, 187);





\echo Defaults
----------------------------
-- Defaults
----------------------------

INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name,default_map )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name,true
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(36,52,16,46,39,21) AND
pgt_new.id=25 AND
au.id in
(
select au.id
FROM
actor.usr au left join m_coo.patron_profile_move mppm on au.id=mppm.usr,
permission.grp_tree pgt
WHERE
mppm.usr is null AND
au.profile in (36, 56, 52, 16, 34, 17, 38, 57, 44, 22, 58, 59, 31, 60, 61, 46, 29, 39, 21, 28, 40, 41, 64, 62)
);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name,default_map )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name,true
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(56,22) AND
pgt_new.id=78 AND
au.id in
(
select au.id
FROM
actor.usr au left join m_coo.patron_profile_move mppm on au.id=mppm.usr,
permission.grp_tree pgt
WHERE
mppm.usr is null AND
au.profile in (36, 56, 52, 16, 34, 17, 38, 57, 44, 22, 58, 59, 31, 60, 61, 46, 29, 39, 21, 28, 40, 41, 64, 62)
);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name,default_map )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name,true
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(17,34,29) AND
pgt_new.id=37 AND
au.id in
(
select au.id
FROM
actor.usr au left join m_coo.patron_profile_move mppm on au.id=mppm.usr,
permission.grp_tree pgt
WHERE
mppm.usr is null AND
au.profile in (36, 56, 52, 16, 34, 17, 38, 57, 44, 22, 58, 59, 31, 60, 61, 46, 29, 39, 21, 28, 40, 41, 64, 62)
);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name,default_map )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name,true
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(38,28) AND
pgt_new.id=26 AND
au.id in
(
select au.id
FROM
actor.usr au left join m_coo.patron_profile_move mppm on au.id=mppm.usr,
permission.grp_tree pgt
WHERE
mppm.usr is null AND
au.profile in (36, 56, 52, 16, 34, 17, 38, 57, 44, 22, 58, 59, 31, 60, 61, 46, 29, 39, 21, 28, 40, 41, 64, 62)
);




INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name,default_map )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name,true
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(57,44,58,59,31) AND
pgt_new.id=30 AND
au.id in
(
select au.id
FROM
actor.usr au left join m_coo.patron_profile_move mppm on au.id=mppm.usr,
permission.grp_tree pgt
WHERE
mppm.usr is null AND
au.profile in (36, 56, 52, 16, 34, 17, 38, 57, 44, 22, 58, 59, 31, 60, 61, 46, 29, 39, 21, 28, 40, 41, 64, 62)
);



INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name,default_map )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name,true
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(60,61,62) AND
pgt_new.id=32 AND
au.id in
(
select au.id
FROM
actor.usr au left join m_coo.patron_profile_move mppm on au.id=mppm.usr,
permission.grp_tree pgt
WHERE
mppm.usr is null AND
au.profile in (36, 56, 52, 16, 34, 17, 38, 57, 44, 22, 58, 59, 31, 60, 61, 46, 29, 39, 21, 28, 40, 41, 64, 62)
);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name,default_map )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name,true
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(40) AND
pgt_new.id=42 AND
au.id in
(
select au.id
FROM
actor.usr au left join m_coo.patron_profile_move mppm on au.id=mppm.usr,
permission.grp_tree pgt
WHERE
mppm.usr is null AND
au.profile in (36, 56, 52, 16, 34, 17, 38, 57, 44, 22, 58, 59, 31, 60, 61, 46, 29, 39, 21, 28, 40, 41, 64, 62)
);


INSERT INTO  m_coo.patron_profile_move (usr, home_ou , home_ou_system, profile ,new_profile ,usr_dob ,home_ou_system_name, old_profile_name ,new_profile_name,default_map )
select
au.id,au.home_ou,aou_sys.id,au.profile,pgt_new.id,au.dob,aou_sys.name,pgt_old.name,pgt_new.name,true
FROM
actor.usr au,
actor.org_unit aou_sys,
actor.org_unit aou_branch,
permission.grp_tree pgt_old,
permission.grp_tree pgt_new
WHERE
au.home_ou=aou_branch.id AND
aou_branch.parent_ou=aou_sys.id AND
pgt_old.id=au.profile AND
au.profile in(41,64) AND
pgt_new.id=27 AND
au.id in
(
select au.id
FROM
actor.usr au left join m_coo.patron_profile_move mppm on au.id=mppm.usr,
permission.grp_tree pgt
WHERE
mppm.usr is null AND
au.profile in (36, 56, 52, 16, 34, 17, 38, 57, 44, 22, 58, 59, 31, 60, 61, 46, 29, 39, 21, 28, 40, 41, 64, 62)
);





-- MAKE SURE WE DIDN'T STAGE TWO OF THE SAME PATRON

-- select usr,count(*) FROM  m_coo.patron_profile_move group by 1 having count(*) > 1;




CREATE OR REPLACE FUNCTION m_coo.patron_migration_update()
RETURNS void AS
$bodyy$
DECLARE
updatecount          INT := 0;
totalcount           INT := 0;
totalupdated         INT := 0;
BEGIN

SELECT INTO totalcount count(*) FROM m_coo.patron_profile_move;

LOOP
   WITH updated_rows AS
    (
        UPDATE actor.usr au SET profile = mppm.new_profile
        FROM m_coo.patron_profile_move mppm
        WHERE
        au.id = mppm.usr AND
        au.id in
        (
            SELECT usr FROM (SELECT id,usr FROM m_coo.patron_profile_move mppm WHERE not done order by id LIMIT 100000) as a
        )
        returning au.id
    )

 SELECT INTO updatecount count(*) FROM updated_rows;
 
 totalupdated = totalupdated + updatecount;
 
 RAISE NOTICE 'Updated % / %',totalupdated,totalcount;
 
 UPDATE m_coo.patron_profile_move SET done = TRUE
 WHERE
 id in(select id FROM m_coo.patron_profile_move WHERE not done ORDER BY id LIMIT 100000);
 
   IF updatecount = 0 THEN EXIT;
   END IF;
END LOOP;
END;
$bodyy$
LANGUAGE 'plpgsql';




-- turn off the auditor trigger

ALTER TABLE actor.usr DISABLE TRIGGER audit_actor_usr_update_trigger;

-- Finally perform the migration
BEGIN;

SELECT * FROM m_coo.patron_migration_update();

COMMIT;

-- turn on the auditor trigger
ALTER TABLE actor.usr ENABLE TRIGGER audit_actor_usr_update_trigger;

DROP FUNCTION m_coo.patron_migration_update();


\echo FINAL DATABASE REMOVALS
----------------------------
-- FINAL DATABASE REMOVALS
----------------------------


-- delete any matrix limit sets referencing the old permission groups
delete from config.circ_matrix_limit_set_map where matchpoint in
(
select id from config.circ_matrix_matchpoint where grp
in
(36,56,52,16,34,17,38,57,44,22,58,59,31,60,61,46,29,39,21,28,40,41,64,62) and not active
);
-- DELETE 19

-- Remove any lingering GPTs
delete from permission.grp_penalty_threshold a where grp in(36,56,52,16,34,17,38,57,44,22,58,59,31,60,61,46,29,39,21,28,40,41,64,62);
-- DELETE 7

-- remove hold policies
delete from config.hold_matrix_matchpoint where usr_grp in
(36,56,52,16,34,17,38,57,44,22,58,59,31,60,61,46,29,39,21,28,40,41,64,62) and not active;
-- DELETE 20

-- remove circ policies
delete from config.circ_matrix_matchpoint 
where grp
in
(36,56,52,16,34,17,38,57,44,22,58,59,31,60,61,46,29,39,21,28,40,41,64,62) and not active
-- DELETE 54

-- finally - eliminate permission groups
delete from permission.grp_tree where id in
(36,56,52,16,34,17,38,57,44,22,58,59,31,60,61,46,29,39,21,28,40,41,64,62);


-- Clean up auxillary hold/circ policies that are disabled as per 
-- https://3.basecamp.com/3986049/buckets/8536501/messages/1393633949

delete from config.hold_matrix_matchpoint where not active;
-- DELETE 8

delete from config.hold_matrix_matchpoint where not active;
-- DELETE 8