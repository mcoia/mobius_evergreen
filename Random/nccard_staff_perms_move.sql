
-- Populate staging tables

create table m_coo.staff_perm_stage(
grp_id integer,
perm integer,
depth integer,
grantable boolean,
new_set boolean default true
);

-- Insert the old set
insert into m_coo.staff_perm_stage
(grp_id,perm,depth,grantable,new_set)
select 
id,perm,depth,grantable,'f'
from
permission.grp_perm_map where grp in(select id from permission.grp_descendants(3)
where
id not in(13,14,15,23)
)
;

-- Create the new set from DEV server
/* select concat('(',grp,',',perm,',',depth,',''',grantable,'''),')
from permission.grp_perm_map where grp in(select id from permission.grp_descendants(3)
where
id not in(13,14,15,23)
) */

-- Insert the new set
insert into m_coo.staff_perm_stage
(grp_id,perm,depth,grantable)
values
(6,1006,0,'f'),
(45,464,0,'f'),
(45,395,1,'f'),
(45,156,1,'f'),
(45,417,1,'f'),
(45,404,1,'f'),
(45,438,1,'f'),
(45,345,1,'f'),
(45,310,0,'f'),
(45,300,1,'f'),
(45,147,1,'f'),
(45,350,0,'f'),
(45,296,1,'f'),
(45,287,0,'f'),
(45,16,0,'f'),
(45,170,1,'f'),
(45,160,1,'f'),
(45,302,0,'f'),
(45,530,0,'f'),
(45,151,1,'f'),
(45,415,1,'f'),
(45,396,1,'f'),
(45,393,1,'f'),
(45,229,1,'f'),
(45,412,1,'f'),
(45,426,1,'f'),
(45,451,1,'f'),
(45,305,0,'f'),
(45,145,0,'f'),
(45,449,1,'f'),
(45,255,0,'f'),
(45,462,0,'f'),
(45,522,0,'f'),
(45,576,0,'f'),
(45,578,0,'f'),
(45,78,0,'f'),
(45,169,1,'f'),
(45,148,1,'f'),
(45,164,1,'f'),
(45,154,1,'f'),
(45,392,1,'f'),
(45,303,0,'f'),
(45,88,0,'f'),
(45,15,0,'f'),
(45,280,0,'f'),
(45,17,0,'f'),
(45,286,0,'f'),
(45,351,0,'f'),
(45,385,0,'f'),
(45,282,1,'f'),
(45,150,1,'f'),
(45,416,1,'f'),
(45,521,0,'f'),
(45,172,0,'f'),
(45,291,1,'f'),
(45,304,0,'f'),
(45,152,1,'f'),
(45,283,1,'f'),
(45,155,1,'f'),
(45,285,1,'f'),
(45,284,0,'f'),
(45,206,0,'f'),
(45,211,1,'f'),
(45,212,1,'f'),
(45,225,1,'f'),
(45,293,1,'f'),
(45,294,1,'f'),
(45,248,1,'f'),
(45,188,1,'f'),
(45,190,0,'f'),
(45,420,1,'f'),
(45,439,1,'f'),
(45,58,1,'f'),
(45,173,0,'f'),
(45,257,0,'f'),
(45,463,0,'f'),
(45,536,0,'f'),
(45,577,0,'f'),
(45,450,1,'f'),
(45,161,1,'f'),
(45,158,1,'f'),
(45,166,1,'f'),
(45,397,1,'f'),
(45,346,1,'f'),
(45,478,0,'f'),
(45,313,1,'f'),
(45,316,1,'f'),
(45,319,1,'f'),
(45,400,1,'f'),
(45,394,1,'f'),
(45,85,0,'f'),
(45,167,0,'f'),
(4,547,1,'f'),
(4,485,1,'f'),
(4,548,1,'f'),
(4,398,1,'f'),
(4,436,1,'f'),
(4,168,1,'f'),
(4,488,1,'f'),
(4,62,1,'f'),
(4,474,0,'f'),
(4,314,1,'f'),
(4,64,1,'f'),
(4,493,1,'f'),
(4,487,1,'f'),
(4,87,1,'f'),
(4,159,1,'f'),
(4,566,1,'f'),
(4,318,1,'f'),
(4,163,1,'f'),
(4,512,1,'f'),
(4,490,1,'f'),
(4,564,1,'f'),
(4,191,1,'f'),
(4,56,1,'f'),
(4,504,1,'f'),
(4,489,1,'f'),
(4,469,1,'f'),
(4,471,1,'f'),
(4,157,1,'f'),
(4,477,1,'f'),
(4,542,2,'f'),
(4,500,0,'f'),
(4,162,1,'f'),
(4,523,2,'f'),
(4,470,1,'f'),
(4,491,0,'f'),
(4,492,0,'f'),
(4,153,0,'f'),
(4,472,1,'f'),
(4,149,1,'f'),
(4,171,1,'f'),
(4,189,1,'f'),
(4,498,1,'f'),
(4,484,1,'f'),
(4,18,1,'f'),
(4,19,1,'f'),
(4,20,1,'f'),
(4,21,1,'f'),
(4,23,1,'f'),
(4,193,1,'f'),
(4,194,1,'f'),
(4,195,1,'f'),
(4,277,1,'f'),
(4,479,1,'f'),
(4,423,1,'f'),
(4,483,1,'f'),
(4,495,1,'f'),
(4,482,1,'f'),
(4,311,0,'f'),
(4,481,1,'f'),
(4,391,1,'f'),
(4,455,0,'f'),
(4,309,0,'f'),
(4,486,1,'f'),
(4,518,0,'f'),
(4,414,1,'f'),
(4,312,1,'f'),
(4,437,1,'f'),
(4,84,1,'f'),
(4,541,2,'f'),
(4,317,0,'f'),
(4,60,1,'f'),
(5,185,1,'f'),
(5,180,0,'f'),
(5,96,1,'f'),
(5,22,2,'f'),
(5,25,0,'f'),
(5,120,0,'f'),
(5,348,0,'f'),
(5,47,2,'f'),
(5,176,1,'f'),
(5,178,1,'f'),
(5,376,1,'f'),
(5,46,1,'f'),
(5,98,1,'f'),
(5,516,1,'f'),
(5,347,1,'f'),
(5,95,1,'f'),
(5,384,1,'f'),
(5,177,0,'f'),
(5,179,0,'f'),
(5,11,0,'f'),
(5,388,1,'f'),
(5,36,1,'f'),
(5,532,0,'f'),
(5,380,1,'f'),
(5,549,1,'f'),
(5,86,1,'f'),
(5,105,0,'f'),
(5,55,1,'f'),
(5,7,0,'f'),
(5,104,0,'f'),
(5,83,0,'f'),
(5,585,0,'f'),
(5,100,0,'f'),
(5,181,1,'f'),
(5,184,1,'f'),
(5,182,0,'f'),
(5,183,1,'f'),
(5,186,1,'f'),
(5,281,1,'f'),
(5,381,1,'f'),
(5,475,1,'f'),
(5,480,1,'f'),
(5,103,0,'f'),
(5,5,0,'f'),
(5,6,0,'f'),
(5,101,0,'f'),
(5,565,0,'f'),
(5,94,1,'f'),
(5,1004,0,'f'),
(5,50,2,'f'),
(5,589,0,'f'),
(5,586,0,'f'),
(5,121,0,'f'),
(5,106,0,'f'),
(5,97,1,'f'),
(12,57,1,'f'),
(12,340,1,'f'),
(12,93,1,'f'),
(12,273,1,'f'),
(12,473,1,'f'),
(12,526,1,'f'),
(12,343,1,'f'),
(12,328,1,'f'),
(12,336,1,'f'),
(12,42,1,'f'),
(12,533,1,'f'),
(12,322,1,'f'),
(12,275,1,'f'),
(12,329,1,'f'),
(12,408,1,'f'),
(12,554,1,'f'),
(12,146,1,'f'),
(12,54,1,'f'),
(12,122,1,'f'),
(12,126,1,'f'),
(12,1011,1,'f'),
(12,58,1,'f'),
(12,390,1,'f'),
(12,204,1,'f'),
(12,52,1,'f'),
(12,60,1,'f'),
(12,127,1,'f'),
(12,123,1,'f'),
(12,1013,1,'f'),
(12,553,1,'f'),
(12,321,1,'f'),
(12,335,1,'f'),
(9,138,1,'f'),
(9,344,1,'f'),
(9,110,1,'f'),
(9,276,0,'f'),
(9,99,1,'f'),
(9,409,1,'f'),
(9,116,1,'f'),
(9,114,1,'f'),
(9,203,1,'f'),
(9,563,1,'f'),
(9,514,1,'f'),
(9,115,1,'f'),
(9,587,1,'f'),
(9,256,0,'f'),
(9,419,0,'f'),
(9,207,0,'f'),
(9,269,0,'f'),
(9,230,0,'f'),
(9,503,0,'f'),
(9,102,1,'f'),
(9,387,0,'f'),
(9,509,1,'f'),
(9,535,1,'f'),
(9,199,1,'f'),
(9,118,1,'f'),
(9,198,1,'f'),
(9,524,1,'f'),
(9,133,1,'f'),
(9,564,1,'f'),
(9,132,1,'f'),
(9,139,1,'f'),
(9,117,1,'f'),
(9,517,1,'f'),
(9,515,1,'f'),
(9,508,0,'f'),
(9,136,0,'f'),
(9,140,0,'f'),
(9,507,0,'f'),
(9,510,1,'f'),
(9,137,0,'f'),
(9,584,1,'f'),
(9,31,1,'f'),
(9,202,1,'f'),
(9,1049,1,'f'),
(9,24,1,'f'),
(9,583,1,'f'),
(9,566,1,'f'),
(9,349,1,'f'),
(9,134,0,'f'),
(9,135,0,'f'),
(9,144,1,'f'),
(9,249,0,'f'),
(3,80,0,'f'),
(3,81,0,'f'),
(3,82,0,'f'),
(3,91,0,'f'),
(3,92,0,'f'),
(3,14,0,'f'),
(3,49,0,'f'),
(3,175,0,'f'),
(3,259,0,'f'),
(3,9,0,'f'),
(3,29,0,'f'),
(3,258,0,'f'),
(3,253,0,'f'),
(3,33,0,'f'),
(3,34,0,'f'),
(3,26,0,'f'),
(3,75,0,'f'),
(3,70,0,'f'),
(3,201,0,'f'),
(3,200,0,'f'),
(3,43,0,'f'),
(3,2,0,'f'),
(3,39,0,'f'),
(3,37,0,'f'),
(3,38,0,'f'),
(3,89,0,'f'),
(3,40,0,'f'),
(3,79,1,'f'),
(3,383,1,'f'),
(3,69,1,'f'),
(3,142,0,'f'),
(3,141,0,'f'),
(3,30,0,'f'),
(3,41,0,'f'),
(3,45,0,'f'),
(3,48,0,'f'),
(3,74,0,'f'),
(3,76,0,'f'),
(3,28,0,'f'),
(3,77,0,'f'),
(3,501,0,'f'),
(3,143,0,'f'),
(3,109,0,'f'),
(3,13,0,'f'),
(3,192,0,'f'),
(7,1006,0,'f'),
(10,270,0,'f'),
(10,173,0,'f'),
(10,346,1,'f'),
(10,111,1,'f'),
(10,196,1,'f'),
(10,401,1,'f'),
(10,378,1,'f'),
(10,377,1,'f'),
(10,169,1,'f'),
(10,542,1,'f'),
(10,433,1,'f'),
(10,155,1,'f'),
(10,210,1,'f'),
(10,222,1,'f'),
(10,53,1,'f'),
(10,160,1,'f'),
(10,446,1,'f'),
(10,156,1,'f'),
(10,61,1,'f'),
(10,527,1,'f'),
(10,161,1,'f'),
(10,119,1,'f'),
(10,166,1,'f'),
(10,459,1,'f'),
(10,150,1,'f'),
(10,68,1,'f'),
(10,320,1,'f'),
(10,472,1,'f'),
(10,149,1,'f'),
(10,171,1,'f'),
(10,541,2,'f'),
(10,113,1,'f'),
(10,112,1,'f'),
(10,531,1,'f'),
(10,379,1,'f'),
(10,164,1,'f'),
(10,540,1,'f'),
(10,282,1,'f'),
(10,151,1,'f'),
(10,147,1,'f'),
(10,67,1,'f'),
(10,51,1,'f'),
(10,59,1,'f'),
(10,525,1,'f'),
(10,168,1,'f'),
(10,291,1,'f'),
(10,152,1,'f'),
(10,148,1,'f'),
(10,63,1,'f'),
(10,65,1,'f'),
(10,27,1,'f'),
(10,167,1,'f'),
(10,300,1,'f'),
(10,154,1,'f'),
(10,261,1,'f'),
(10,272,1,'f'),
(10,163,1,'f'),
(10,309,1,'f'),
(10,157,1,'f'),
(10,162,1,'f'),
(10,523,2,'f'),
(10,153,0,'f'),
(10,538,0,'f'),
(10,455,0,'f'),
(10,474,0,'f'),
(10,389,0,'f'),
(10,477,0,'f'),
(10,189,0,'f'),
(10,479,0,'f'),
(10,498,0,'f'),
(10,360,0,'f'),
(10,476,0,'f'),
(10,382,0,'f'),
(10,158,1,'f'),
(35,1006,0,'f'),
(76,1006,0,'f')
;


-- Staging staff accounts

create table m_coo.actor_usr_staff_perm_stage
(
usr_id bigint,
old_profile bigint,
new_profile bigint,
new_secondary_profile bigint,
new_grp_name1 text,  -- unused - for comparison
new_grp_name2 text,  -- unused - for comparison
usrname text         -- unused - for comparison
);

create table m_coo.permission_usr_grp_map_staff_perm_stage
(
usr_id bigint,
grp_id bigint
);

create table m_coo.permission_usr_perm_map_staff_perm_stage
(
usr_id bigint,
perm_id bigint,
depth integer,
grantable boolean default false
);


-- Insert the spreadsheet

\copy m_coo.actor_usr_staff_perm_stage(usr_id, new_profile, new_secondary_profile , new_grp_name1, new_grp_name2, usrname) from /mnt/evergreen/tmp/Final_SPP_Assignments.csv

update m_coo.actor_usr_staff_perm_stage mausps set old_profile=b.profile
from
actor.usr b
where
b.id=mausps.usr_id;

-- Double check to make sure that the spreadsheet is correct
-- SHOULD BE NONE
select count(*) from 
m_coo.actor_usr_staff_perm_stage mausps,
actor.usr au
where
au.id=mausps.usr_id and
au.usrname!=mausps.usrname;

-- Double check some other stuff
-- SHOULD BE NONE
select count(*) from 
m_coo.actor_usr_staff_perm_stage mausps,
permission.grp_tree pgt
where
pgt.id=mausps.new_profile and
pgt.name!=mausps.new_grp_name1;

-- Double check some other stuff
-- SHOULD BE NONE
select count(*) from 
m_coo.actor_usr_staff_perm_stage mausps,
permission.grp_tree pgt
where
pgt.id=mausps.new_secondary_profile and
pgt.name!=mausps.new_grp_name2;


truncate m_coo.permission_usr_perm_map_staff_perm_stage;
truncate m_coo.permission_usr_grp_map_staff_perm_stage;

ROLLBACK;
BEGIN;


-- record the secondary permission assignments
insert into m_coo.permission_usr_grp_map_staff_perm_stage(usr_id ,grp_id )
select
usr,grp from 
permission.usr_grp_map;


-- record the one-off permissions
insert into m_coo.permission_usr_perm_map_staff_perm_stage(usr_id ,perm_id ,depth ,grantable )
select usr,perm,depth,grantable from
permission.usr_perm_map;

-- record the original profile ID for everyone affected
insert into m_coo.actor_usr_staff_perm_stage (usr_id ,old_profile,new_profile ,usrname)
select id,profile,5,usrname from
actor.usr where
profile in (
select id from permission.grp_descendants(3)
where
id not in(13,14,15,23)
)
and
id not in(select usr_id from m_coo.actor_usr_staff_perm_stage);


-- Remove all permission maps for staff
delete from permission.grp_perm_map where grp in(
select grp_id from
m_coo.staff_perm_stage
);

-- Create the new permissions for staff
insert into permission.grp_perm_map(grp,perm,depth,grantable)
select grp_id,perm,depth,grantable from m_coo.staff_perm_stage where new_set;

-- Blanket everyone to Circulator
UPDATE actor.usr SET profile = 5 WHERE profile in (
select id from permission.grp_descendants(3)
where
id not in(13,14,15,23)
);

commit;

begin;
-- remove all secondary permissions
truncate permission.usr_grp_map;

-- remove all one-off
truncate permission.usr_perm_map;

-- Set the new profile for those that need it
update actor.usr au set profile=mausps.new_profile
from 
m_coo.actor_usr_staff_perm_stage mausps
where
mausps.usr_id=au.id and
au.profile!=mausps.new_profile and
mausps.new_profile is not null;

-- Make the secondary permissions for those that need it
insert into permission.usr_grp_map (usr,grp)
select usr_id,new_secondary_profile
from
m_coo.actor_usr_staff_perm_stage mausps
where
new_secondary_profile is not null;


-- Move the two acq groups to delete tree
update permission.grp_tree set parent=76
where
id in(6,7);

commit;

