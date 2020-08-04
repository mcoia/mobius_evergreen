
DROP DATABASE evergreen;
cd /home/opensrf/repos/Evergreen && perl Open-ILS/src/support-scripts/eg_db_config --service all --create-database --create-schema  --user evergreen --password localdbpass --hostname ipaddress --port 5432 --database evergreen --admin-user admin --admin-pass admin-pass

-- get started by removing any old stuff so it won't interfere with new stuff going in.
\echo "lets gets started..."
DROP SCHEMA IF EXISTS m_staging_schema CASCADE;

-- Create the migration scheme and use the migration tools to build it up
\echo "create the staging schema"
CREATE SCHEMA m_staging_schema;
\i /mnt/evergreen/migration/migration-tools/sql/base/base.sql
\echo "init/build"
select migration_tools.init('m_staging_schema');

update m_staging_schema.config set value = 
concat(
value,
',money.check_payment,',
'money.account_adjustment,',
'actor.usr_message,',
'actor.org_unit,',
'actor.org_unit_closed,',
'actor.org_address,',
'actor.workstation,',
'permission.usr_work_ou_map,',
'permission.grp_perm_map,',
'permission.grp_tree,',
'permission.grp_penalty_threshold,',
'biblio.record_entry,',
'biblio.monograph_part,',
'asset.copy_part_map,',
'asset.copy_tag,',
'asset.copy_tag_copy_map,',
'asset.copy_alert,',
'config.rule_circ_duration,',
'config.rule_max_fine,',
'config.rule_recurring_fine,',
'config.circ_limit_set,',
'config.circ_limit_set_circ_mod_map,',
'config.circ_matrix_limit_set_map,',
'config.circ_matrix_matchpoint,',
'config.copy_tag_type,',
'config.hold_matrix_matchpoint,',
'config.composite_attr_entry_definition,',
'config.coded_value_map,',
'config.billing_type,',
'config.copy_status,',
'config.sms_carrier,',
'config.z3950_source,',
'config.z3950_index_field_map,',
'config.z3950_attr,',
'config.non_cataloged_type,',
'config.copy_alert_type,',
'reporter.template,',
'reporter.template_folder,',
'reporter.output_folder,',
'reporter.report_folder,',
'action.in_house_use,',
'action.non_cat_in_house_use,',
'action.non_cataloged_circulation,',
'action_trigger.event_definition,',
'action_trigger.event_params,',
'action_trigger.environment,',
'container.biblio_record_entry_bucket,',
'container.biblio_record_entry_bucket_item,',
'container.biblio_record_entry_bucket_item_note,',
'container.biblio_record_entry_bucket_note'



)
 where key='production_tables';
select migration_tools.build('m_staging_schema');


-- import data from sierra into staging tables
#as root
cd /mnt/evergreen/migration/ozark
time ./migrate_evergreen.pl --logfile log/evergreendump_prod.log --schema m_staging_schema --evergreendatabase evergreen --evergreenhost remotedbip --evergreenport 5432 --evergreenlogin evergreen --evergreenpass  remoteevergreenpassword --evergreenlocationcodes ORL
real    69m16.002s
user    64m38.416s




-- Bibs with items - Do this seperately with a .sql file and in a screen. Takes 9 hours.
update m_staging_schema.biblio_record_entry_legacy
set
creator=1,
editor=1,
source=2,
quality=l_quality::integer,
create_date=l_create_date::timestamp,
edit_date=l_edit_date::timestamp,
active=l_active::boolean,
deleted=l_deleted::boolean,
tcn_source=l_tcn_source::text,
tcn_value=id::text,
marc=l_marc::text,
last_xact_id=l_last_xact_id::text,
owner=1,
share_depth=l_share_depth::integer
where
source is null;


rollback;
begin;
insert into biblio.record_entry
select * from m_staging_schema.biblio_record_entry
where
id in
(
select id from m_staging_schema.biblio_record_entry_legacy mbre
where
mbre.l_id in(

select l_record from m_staging_schema.asset_copy_legacy

union all
        
select mahrl.l_target from 
m_staging_schema.action_hold_request_legacy mahrl where mahrl.l_hold_type='T'

union all
        
select mcbrebil.l_target_biblio_record_entry
from
m_staging_schema.container_biblio_record_entry_bucket_item_legacy mcbrebil

)
)
and id not in(select id from biblio.record_entry)
;

commit;

real    599m37.026s






-- temp for test migration because I manually created the system level org unit
update m_staging_schema.actor_org_unit_legacy maoul set id=101 where l_shortname='ORL';

rollback;
--- Make the org units
begin;

insert into actor.org_unit(
id, 
  parent_ou ,
  ou_type ,
  shortname ,
  name ,
  email ,
  phone ,
  opac_visible,
  fiscal_calendar)
  
  select id,
  l_parent_ou::integer ,
  l_ou_type::integer,
  l_shortname ,
  l_name ,
  l_email ,
  l_phone ,
  l_opac_visible::boolean,
  l_fiscal_calendar::integer
  from
  m_staging_schema.actor_org_unit_legacy where l_parent_ou='1' and l_shortname not in(select shortname from actor.org_unit);
  
  update m_staging_schema.actor_org_unit_legacy maoul set parent_ou=parent.id
  from
  m_staging_schema.actor_org_unit_legacy parent
  where
  parent.l_parent_ou='1' and
  maoul.l_ou_type='3' and
  parent.l_id=maoul.l_parent_ou;
  
  
insert into actor.org_unit(
id, 
  parent_ou ,
  ou_type ,
  shortname ,
  name ,
  email ,
  phone ,
  opac_visible,
  fiscal_calendar)
  
  select id,
  parent_ou,
  l_ou_type::integer,
  l_shortname ,
  l_name ,
  l_email ,
  l_phone ,
  l_opac_visible::boolean,
  l_fiscal_calendar::integer
  from
  m_staging_schema.actor_org_unit_legacy where l_parent_ou!='1' and l_shortname not in(select shortname from actor.org_unit);
  
  commit;



-- setup org unit addresses

update m_staging_schema.actor_org_address_legacy maoal
set 
address_type = l_address_type,
street1 = l_street1,
street2 = l_street2,
city = l_city,
county = l_county,
state = l_state,
country = l_country,
post_code = l_post_code,
san = l_san;

-- Get the new ID's
update m_staging_schema.actor_org_address_legacy maoal
set
org_unit=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
maoul.l_id = maoal.l_org_unit;


rollback;
begin;
insert into actor.org_address
select * from m_staging_schema.actor_org_address;

update actor.org_unit aou
set 
ill_address = maoal.id
  from
  m_staging_schema.actor_org_address_legacy maoal
  where
  maoal.org_unit=aou.id;
  
update actor.org_unit aou
set 
holds_address = maoal.id
  from
  m_staging_schema.actor_org_address_legacy maoal
  where
   maoal.org_unit=aou.id;
  
  update actor.org_unit aou
set 
  mailing_address = maoal.id
  from
  m_staging_schema.actor_org_address_legacy maoal
  where
   maoal.org_unit=aou.id;
   
   
  update actor.org_unit aou
set 
  billing_address = maoal.id
  from
  m_staging_schema.actor_org_address_legacy maoal
  where
   maoal.org_unit=aou.id;
  
commit;


-- Org unit hours
rollback;
begin;
update
m_staging_schema.actor_hours_of_operation mahoo
set id = maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
maoul.l_id=mahoo.id;

insert into actor.hours_of_operation
select 
id::integer,
dow_0_open::time,
dow_0_close::time,
dow_1_open::time,
dow_1_close::time,
dow_2_open::time,
dow_2_close::time,
dow_3_open::time,
dow_3_close::time,
dow_4_open::time,
dow_4_close::time,
dow_5_open::time,
dow_5_close::time,
dow_6_open::time,
dow_6_close::time
from
m_staging_schema.actor_hours_of_operation;

commit;

-- Coded value map

rollback;
begin;


insert into config.coded_value_map(id,ctype,code,value,opac_visible,search_label,is_simple)
select
id,
l_ctype,l_code,l_value,l_opac_visible::boolean,l_search_label,l_is_simple::boolean
from
m_staging_schema.config_coded_value_map_legacy mcvm
where
mcvm.l_value='Playaway';

update m_staging_schema.config_composite_attr_entry_definition_legacy mccaedl set coded_value=mccvml.id
from
m_staging_schema.config_coded_value_map_legacy mccvml
where
mccvml.l_id=mccaedl.l_coded_value;

update m_staging_schema.config_composite_attr_entry_definition_legacy mccaedl set definition=l_definition;


insert into
config.composite_attr_entry_definition
select * from m_staging_schema.config_composite_attr_entry_definition;

commit;


-- circ mods

/* see what's up */

select * from m_staging_schema.circ_modifier where code not in(select code from config.circ_modifier);

-- All new 
begin;

insert into config.circ_modifier
select
 code,
  name,
  description,
  sip2_media_type,
  magnetic_media::boolean,
  avg_wait_time::interval
  from
  m_staging_schema.circ_modifier where code not in(select code from config.circ_modifier);

  commit;
  
  
-- circ rules
-- circ limit sets

-- get the old id translation
update m_staging_schema.config_circ_limit_set_legacy mcclsl set
name = mcclsl.l_name,
owning_lib=maoul.id,
items_out=l_items_out::integer,
depth=mcclsl.l_depth::integer,
global=mcclsl.l_global::boolean,
description = mcclsl.l_description
from
m_staging_schema.actor_org_unit_legacy maoul
where
mcclsl.l_owning_lib = maoul.l_id;

-- get the consortium ones
update m_staging_schema.config_circ_limit_set_legacy mcclsl set
name = mcclsl.l_name,
owning_lib=mcclsl.l_owning_lib::integer,
items_out=l_items_out::integer,
depth=mcclsl.l_depth::integer,
global=mcclsl.l_global::boolean,
description = mcclsl.l_description
where
mcclsl.owning_lib is null;

begin;
insert into config.circ_limit_set
select * from  m_staging_schema.config_circ_limit_set;

commit;
  
-- m_staging_schema.config_circ_limit_set_circ_mod_map_legacy

update m_staging_schema.config_circ_limit_set_circ_mod_map_legacy mcclscmml
set
limit_set= mcclsl.id,
circ_mod=l_circ_mod
from
m_staging_schema.config_circ_limit_set_legacy mcclsl
where
mcclsl.l_id=mcclscmml.l_limit_set;
  
begin;
insert into config.circ_limit_set_circ_mod_map
select * from m_staging_schema.config_circ_limit_set_circ_mod_map;

commit;

-- m_staging_schema.config_rule_circ_duration

update m_staging_schema.config_rule_circ_duration_legacy mcrcdl
set id = treal.id
from
config.rule_circ_duration treal
where
treal.name = mcrcdl.l_name;

update m_staging_schema.config_rule_circ_duration_legacy
set
name=l_name,
extended= l_extended::interval,
normal = l_normal::interval,
shrt = l_shrt::interval,
max_renewals = l_max_renewals::integer;

begin;
insert into config.rule_circ_duration
select * from m_staging_schema.config_rule_circ_duration
where name not in(select name from config.rule_circ_duration);

commit;

-- m_staging_schema.config_rule_max_fine_legacy
update m_staging_schema.config_rule_max_fine_legacy mcrmfl set 
id = treal.id
from
config.rule_max_fine treal
where
mcrmfl.l_name=treal.name;

update m_staging_schema.config_rule_max_fine_legacy mcrmfl 
set 
name = l_name,
amount = l_amount::numeric(6,2),
is_percent = l_is_percent::boolean;

begin;
insert into config.rule_max_fine
select * from m_staging_schema.config_rule_max_fine
where name not in(select name from config.rule_max_fine);

commit;

-- m_staging_schema.config_rule_recurring_fine_legacy

update m_staging_schema.config_rule_recurring_fine_legacy mcrrfl
set
id = treal.id
from
config.rule_recurring_fine treal
where
treal.name=mcrrfl.l_name;

update m_staging_schema.config_rule_recurring_fine_legacy mcrrfl
set
  name=l_name,
  high=l_high::numeric(6,2),
  normal=l_normal::numeric(6,2),
  low=l_low::numeric(6,2),
  recurrence_interval=l_recurrence_interval::interval,
  grace_period=l_grace_period::interval;

begin;
insert into config.rule_recurring_fine
select * from m_staging_schema.config_rule_recurring_fine
where name not in(select name from config.rule_recurring_fine);

commit;

-- m_staging_schema.asset_copy_location_legacy

update m_staging_schema.asset_copy_location_legacy macll
set id=1 where l_name='Stacks';


update m_staging_schema.asset_copy_location_legacy macll
set
owning_lib = maoul.id
from 
m_staging_schema.actor_org_unit_legacy maoul
where
maoul.l_id=macll.l_owning_lib;


update m_staging_schema.asset_copy_location_legacy macll set
name=l_name::text,
holdable=l_holdable::boolean,
hold_verify=l_hold_verify::boolean,
opac_visible=l_opac_visible::boolean,
circulate=l_circulate::boolean,
label_prefix=l_label_prefix::text,
label_suffix=l_label_suffix::text,
checkin_alert=l_checkin_alert::boolean,
deleted=l_deleted::boolean,
url=l_url::text;

rollback;
begin;
insert into asset.copy_location
select * from m_staging_schema.asset_copy_location where name!='Stacks';

commit;


-- permissions
-- match the old and new names together and get the new IDs for real
update m_staging_schema.permission_grp_tree_legacy mpgtl
set
id = treal.id
from
permission.grp_tree treal
where
treal.name=mpgtl.l_name;

update m_staging_schema.permission_grp_tree_legacy mpgtl
set
parent = treal.id
from
m_staging_schema.permission_grp_tree_legacy treal
where
treal.l_id=mpgtl.l_parent;


update m_staging_schema.permission_grp_tree_legacy
set 
name=l_name::text,
usergroup=l_usergroup::boolean,
perm_interval=l_perm_interval::interval,
description=l_description::text,
application_perm=l_application_perm::text,
hold_priority=l_hold_priority::integer;

rollback;
begin;
insert into permission.grp_tree
select * from m_staging_schema.permission_grp_tree mpgt
where
id in
(
select mpgtl.id from 
m_staging_schema.permission_grp_tree_legacy mpgtl,
m_staging_schema.actor_usr_legacy moaul
where
moaul.l_profile=mpgtl.l_id 

union all

select mpgtl.id from 
m_staging_schema.permission_grp_tree_legacy mpgtl,
m_staging_schema.config_circ_matrix_matchpoint_legacy mchmml
where
mchmml.l_grp=mpgtl.l_id 

union all

select mpgtl.id from 
m_staging_schema.permission_grp_tree_legacy mpgtl,
m_staging_schema.config_hold_matrix_matchpoint_legacy mchmml
where
mchmml.l_usr_grp=mpgtl.l_id
)
and
mpgt.id not in(select id from permission.grp_tree);

commit;
UPDATE_PATRON_PRIMARY_CARD
UPDATE_PATRON_ACTIVE_CARD
UPDATE_PATRON_CLAIM_NEVER_CHECKED_OUT_COUNT

-- Perm map

update m_staging_schema.permission_grp_perm_map_legacy
set
perm=l_perm::integer,
depth=l_depth::integer,
grantable=l_grantable::boolean;

update m_staging_schema.permission_grp_perm_map_legacy mpgpml
set
grp=mpgtl.id
from
m_staging_schema.permission_grp_tree_legacy mpgtl
where
mpgpml.l_grp=mpgtl.l_id;

begin;
insert into permission.perm_list(code,description)
values
('PATRON_EXCEEDS_LONGOVERDUE_COUNT.override','Allow staff to override checkout long overdue failure');

commit;
-- catch the odd "PATRON_EXCEEDS_LONGOVERDUE_COUNT.override"
update m_staging_schema.permission_grp_perm_map_legacy mpgpml
set
perm=ppl.id
from
permission.perm_list ppl
where
ppl.code='PATRON_EXCEEDS_LONGOVERDUE_COUNT.override' and
mpgpml.perm=1008;


rollback;
begin;
insert into permission.grp_perm_map
select * from m_staging_schema.permission_grp_perm_map where 
grp::text||'____'||perm::text not in(select grp::text||'____'||perm::text from permission.grp_perm_map) and
grp in(select id from permission.grp_tree);

commit;


--m_staging_schema.config_circ_matrix_matchpoint_legacy

update m_staging_schema.config_circ_matrix_matchpoint_legacy
set
active=l_active::boolean,
circ_modifier=l_circ_modifier::text,
marc_type=l_marc_type::text,
marc_form=l_marc_form::text,
marc_bib_level=l_marc_bib_level::text,
marc_vr_format=l_marc_vr_format::text,
ref_flag=l_ref_flag::boolean,
juvenile_flag=l_juvenile_flag::boolean,
is_renewal=l_is_renewal::boolean,
usr_age_lower_bound=l_usr_age_lower_bound::interval,
usr_age_upper_bound=l_usr_age_upper_bound::interval,
item_age=l_item_age::interval,
circulate=l_circulate::boolean,
hard_due_date=l_hard_due_date::integer,
renewals=l_renewals::integer,
grace_period=l_grace_period::interval,
script_test=l_script_test::text,
total_copy_hold_ratio=l_total_copy_hold_ratio::double precision,
available_copy_hold_ratio=l_available_copy_hold_ratio::double precision,
description=l_description::text;



update m_staging_schema.config_circ_matrix_matchpoint_legacy
set
grp=maoul.id
from
m_staging_schema.permission_grp_tree_legacy maoul
where
l_grp = maoul.l_id;

update m_staging_schema.config_circ_matrix_matchpoint_legacy
set
copy_circ_lib=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
l_copy_circ_lib = maoul.l_id;

update m_staging_schema.config_circ_matrix_matchpoint_legacy
set
copy_owning_lib=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
l_copy_owning_lib = maoul.l_id;


update m_staging_schema.config_circ_matrix_matchpoint_legacy
set
user_home_ou=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
l_user_home_ou = maoul.l_id;

update m_staging_schema.config_circ_matrix_matchpoint_legacy
set
org_unit=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
l_org_unit = maoul.l_id;

update m_staging_schema.config_circ_matrix_matchpoint_legacy
set
duration_rule=maoul.id
from
m_staging_schema.config_rule_circ_duration_legacy maoul
where
l_duration_rule = maoul.l_id;

update m_staging_schema.config_circ_matrix_matchpoint_legacy
set
recurring_fine_rule=maoul.id
from
m_staging_schema.config_rule_recurring_fine_legacy maoul
where
l_recurring_fine_rule = maoul.l_id;

update m_staging_schema.config_circ_matrix_matchpoint_legacy
set
max_fine_rule=maoul.id
from
m_staging_schema.config_rule_max_fine_legacy maoul
where
l_max_fine_rule = maoul.l_id;

update m_staging_schema.config_circ_matrix_matchpoint_legacy
set
copy_location=maoul.id
from
m_staging_schema.asset_copy_location_legacy maoul
where
l_copy_location = maoul.l_id;


begin;
insert into config.circ_matrix_matchpoint
select * from 
m_staging_schema.config_circ_matrix_matchpoint;

commit;







-- m_staging_schema.config_hold_matrix_matchpoint_legacy

update m_staging_schema.config_hold_matrix_matchpoint_legacy set
active=l_active::boolean,
strict_ou_match=l_strict_ou_match::boolean,
circ_modifier=l_circ_modifier::text,
marc_type=l_marc_type::text,
marc_form=l_marc_form::text,
marc_bib_level=l_marc_bib_level::text,
marc_vr_format=l_marc_vr_format::text,
juvenile_flag=l_juvenile_flag::boolean,
ref_flag=l_ref_flag::boolean,
item_age=l_item_age::interval,
holdable=l_holdable::boolean,
distance_is_from_owner=l_distance_is_from_owner::boolean,
transit_range=l_transit_range::integer,
max_holds=l_max_holds::integer,
include_frozen_holds=l_include_frozen_holds::boolean,
stop_blocked_user=l_stop_blocked_user::boolean,
age_hold_protect_rule=l_age_hold_protect_rule::integer,
description=l_description::text;


update m_staging_schema.config_hold_matrix_matchpoint_legacy mchmml set
user_home_ou=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mchmml.l_user_home_ou=maoul.l_id;

update m_staging_schema.config_hold_matrix_matchpoint_legacy mchmml set
request_ou=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mchmml.l_request_ou=maoul.l_id;

update m_staging_schema.config_hold_matrix_matchpoint_legacy mchmml set
pickup_ou=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mchmml.l_pickup_ou=maoul.l_id;

update m_staging_schema.config_hold_matrix_matchpoint_legacy mchmml set
item_owning_ou=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mchmml.l_item_owning_ou=maoul.l_id;

update m_staging_schema.config_hold_matrix_matchpoint_legacy mchmml set
item_circ_ou=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mchmml.l_item_circ_ou=maoul.l_id;

update m_staging_schema.config_hold_matrix_matchpoint_legacy mchmml set
usr_grp=maoul.id
from
m_staging_schema.permission_grp_tree_legacy maoul
where
mchmml.l_usr_grp=maoul.l_id;

update m_staging_schema.config_hold_matrix_matchpoint_legacy mchmml set
requestor_grp=maoul.id
from
m_staging_schema.permission_grp_tree_legacy maoul
where
mchmml.l_requestor_grp=maoul.l_id;

-- get rid of that temp rule
update m_staging_schema.config_hold_matrix_matchpoint_legacy mchmml set
active=false where l_id in('145','318','447');


-- Fix the missing CONS for user home library
update m_staging_schema.config_hold_matrix_matchpoint_legacy mchmml set
user_home_ou=1
where l_user_home_ou = '1';


rollback;
begin;
insert into config.hold_matrix_matchpoint
select * from m_staging_schema.config_hold_matrix_matchpoint mchmml
where
mchmml.active;

commit;

-- m_staging_schema.config_circ_matrix_limit_set_map_legacy
update m_staging_schema.config_circ_matrix_limit_set_map_legacy
set
fallthrough=l_fallthrough::boolean,
active=l_active::boolean;

update m_staging_schema.config_circ_matrix_limit_set_map_legacy mccmlsml
set
matchpoint=mchmml.id
from
m_staging_schema.config_circ_matrix_matchpoint_legacy mchmml
where
mccmlsml.l_matchpoint=mchmml.l_id;

update m_staging_schema.config_circ_matrix_limit_set_map_legacy mccmlsml
set
limit_set=mchmml.id
from
m_staging_schema.config_circ_limit_set_legacy mchmml
where
mccmlsml.l_limit_set=mchmml.l_id;

rollback;
begin;
insert into config.circ_matrix_limit_set_map
select * from m_staging_schema.config_circ_matrix_limit_set_map;

commit;



-- call numbers and Items

alter table m_staging_schema.asset_copy_legacy add column egid bigint;

update m_staging_schema.asset_copy_legacy macl set egid = mbrel.id
from
m_staging_schema.biblio_record_entry_legacy mbrel
where
mbrel.l_id=macl.l_record;


update m_staging_schema.asset_copy_legacy
set
create_date=l_create_date::timestamp,
edit_date=l_edit_date::timestamp,
copy_number=l_copy_number::integer,
loan_duration=l_loan_duration::integer,
fine_level=l_fine_level::integer,
age_protect=l_age_protect::integer,
circulate=l_circulate::boolean,
deposit=l_deposit::boolean,
ref=l_ref::boolean,
holdable=l_holdable::boolean,
deposit_amount=l_deposit_amount::numeric(6,2),
price=l_price::numeric(8,2),
barcode=l_barcode::text,
circ_modifier=l_circ_modifier::text,
circ_as_type=l_circ_as_type::text,
dummy_title=l_dummy_title::text,
dummy_author=l_dummy_author::text,
alert_message=l_alert_message::text,
opac_visible=l_opac_visible::boolean,
deleted=l_deleted::boolean,
floating=l_floating::integer,
dummy_isbn=l_dummy_isbn::text,
status_changed_time=l_status_changed_time::timestamp,
active_date=l_active_date::timestamp,
mint_condition=l_mint_condition::boolean,
cost=l_cost::numeric(8,2);


update m_staging_schema.asset_copy_legacy macl
set
circ_lib=maoul.id
from 
m_staging_schema.actor_org_unit_legacy maoul
where
macl.l_circ_lib=maoul.l_id;

-- Catch the rest as Ironton
update m_staging_schema.asset_copy_legacy macl
set
circ_lib=(select id from actor.org_unit where lower(name)~'ironton')
where
circ_lib is null;

update m_staging_schema.asset_copy_legacy macl
set
location=macll.id
from 
m_staging_schema.asset_copy_location_legacy macll
where
macl.l_location=macll.l_id;


TRUNCATE m_staging_schema.asset_call_number;

	\echo "create the staging call number table"
	INSERT INTO m_staging_schema.asset_call_number ( 
	label, record, owning_lib, creator, editor
	) SELECT DISTINCT
	l_label,
	egid,
	circ_lib, 
	1,  -- Admin
	1 -- Admin
	FROM m_staging_schema.asset_copy_legacy AS i WHERE egid <> -1 AND egid 
	IN (SELECT id FROM biblio.record_entry) ORDER BY 1,2,3;


	--link call number labels to asset.copy
	\echo "linking call numbers to asset.copy"
	UPDATE m_staging_schema.asset_copy_legacy AS i SET call_number = COALESCE(
	(SELECT c.id FROM m_staging_schema.asset_call_number AS c WHERE label = l_label AND record = egid AND owning_lib = circ_lib),
	-1
	);

	-- Report copies that did not get mapped to a call number
	select count(*) from m_staging_schema.asset_copy_legacy where call_number=-1 and not deleted;

	\echo "delete call numbers without copies"
	DELETE FROM m_staging_schema.asset_call_number
	WHERE id NOT in (select call_number FROM m_staging_schema.asset_copy);

    
-- set some default values

UPDATE m_staging_schema.asset_copy_legacy SET 
  creator = 1, 
  editor = 1;


BEGIN;
\echo inserting call numbers
INSERT INTO asset.call_number SELECT * FROM m_staging_schema.asset_call_number;
\echo inserting copies
INSERT INTO asset.copy SELECT * FROM m_staging_schema.asset_copy;
COMMIT;    
    
-- Monograph parts
update m_staging_schema.biblio_monograph_part_legacy
set
label=l_label::text,
deleted=l_deleted::boolean;


update m_staging_schema.biblio_monograph_part_legacy mbmpl
set 
record=mbrel.id
from
m_staging_schema.biblio_record_entry_legacy mbrel
where
mbmpl.l_record=mbrel.l_id;

-- see if we have nulls

select * from m_staging_schema.biblio_monograph_part where record is null;
-- Only 81 and many of them were deleted and all of them look like magazines


rollback;
begin;
insert into biblio.monograph_part(id,label,deleted,record)
select id,label,deleted,record from 
m_staging_schema.biblio_monograph_part where record is not null;

commit;
    
update m_staging_schema.asset_copy_part_map_legacy macpml
set
target_copy=macl.id
from
m_staging_schema.asset_copy_legacy macl
where
macpml.l_target_copy=macl.l_id;
    
  
update m_staging_schema.asset_copy_part_map_legacy macpml
set
part=mbmpl.id
from
m_staging_schema.biblio_monograph_part_legacy mbmpl
where
macpml.l_part=mbmpl.l_id;

rollback;
begin;
insert into asset.copy_part_map
select * from m_staging_schema.asset_copy_part_map where part in(select id from biblio.monograph_part);

commit;
   

-- Copy tags
-- THERE ARE ZERO

-- copy notes

update m_staging_schema.asset_copy_note_legacy
set
creator = 1,
create_date=l_create_date::timestamp,
pub=l_pub::boolean,
title=l_title::text,
value=l_value::text;
   
update m_staging_schema.asset_copy_note_legacy macnl
set
owning_copy=macl.id
from
m_staging_schema.asset_copy_legacy macl
where
macnl.l_owning_copy=macl.l_id;


begin;
insert into asset.copy_note
select * from m_staging_schema.asset_copy_note;

commit;


-- asset stat cat

update m_staging_schema.asset_stat_cat_legacy mascl
set
opac_visible=l_opac_visible::boolean,
name=l_name::text,
required=l_required::boolean,
sip_field=l_sip_field::character(2),
sip_format=l_sip_format::text,
checkout_archive=l_checkout_archive::boolean;

update m_staging_schema.asset_stat_cat_legacy mascl
set
owner=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mascl.l_owner = maoul.l_id;

update m_staging_schema.asset_stat_cat_legacy mascl set owner = 1 where l_owner='1';

rollback;
begin;
insert into asset.stat_cat
select * from m_staging_schema.asset_stat_cat;

commit;


update m_staging_schema.asset_stat_cat_entry_legacy
set
value=l_value::text;

update m_staging_schema.asset_stat_cat_entry_legacy mascel
set
stat_cat=mascl.id
from
m_staging_schema.asset_stat_cat_legacy mascl
where
mascel.l_stat_cat=mascl.l_id;


update m_staging_schema.asset_stat_cat_entry_legacy mascel
set
owner=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mascel.l_owner = maoul.l_id;

-- The others are consortium level
update m_staging_schema.asset_stat_cat_entry_legacy mascel
set
owner=1
where owner is null;

begin;
insert into asset.stat_cat_entry
select * from m_staging_schema.asset_stat_cat_entry;

commit;


update m_staging_schema.asset_stat_cat_entry_copy_map_legacy mascecml
set
stat_cat=mascel.id
from
m_staging_schema.asset_stat_cat_legacy mascel
where
mascecml.l_stat_cat=mascel.l_id;

update m_staging_schema.asset_stat_cat_entry_copy_map_legacy mascecml
set
stat_cat_entry=mascel.id
from
m_staging_schema.asset_stat_cat_entry_legacy mascel
where
mascecml.l_stat_cat_entry=mascel.l_id;

update m_staging_schema.asset_stat_cat_entry_copy_map_legacy mascecml
set
owning_copy=macl.id
from
m_staging_schema.asset_copy_legacy macl
where
mascecml.l_owning_copy=macl.l_id;

begin;
insert into asset.stat_cat_entry_copy_map
select * from m_staging_schema.asset_stat_cat_entry_copy_map;

commit;



-- Patrons    

-- Bring back SSN option in config.identification_type 
insert into config.identification_type (id,name)
values (2,'SSN');


update m_staging_schema.actor_usr_legacy
set
usrname=l_usrname::text,
email=l_email::text,
passwd=l_passwd::text,
standing=l_standing::integer,
ident_type=l_ident_type::integer,
ident_value=l_ident_value::text,
ident_type2=l_ident_type2::integer,
ident_value2=l_ident_value2::text,
net_access_level=l_net_access_level::integer,
photo_url=l_photo_url::text,
prefix=l_prefix::text,
first_given_name=l_first_given_name::text,
second_given_name=l_second_given_name::text,
family_name=l_family_name::text,
suffix=l_suffix::text,
alias=l_alias::text,
day_phone=l_day_phone::text,
evening_phone=l_evening_phone::text,
other_phone=l_other_phone::text,
dob=l_dob::date,
active=l_active::boolean,
master_account=l_master_account::boolean,
super_user=l_super_user::boolean,
barred=l_barred::boolean,
deleted=l_deleted::boolean,
juvenile=l_juvenile::boolean,
usrgroup=l_usrgroup::integer,
claims_returned_count=l_claims_returned_count::integer,
credit_forward_balance=l_credit_forward_balance::numeric(6,2),
last_xact_id=l_last_xact_id::text,
alert_message=l_alert_message::text,
create_date=l_create_date::timestamp,
expire_date=l_expire_date::timestamp,
claims_never_checked_out_count=l_claims_never_checked_out_count::integer,
last_update_time=l_last_update_time::timestamp;


update m_staging_schema.actor_usr_legacy maul
set
profile=mpgt.id
from m_staging_schema.permission_grp_tree_legacy mpgt
where
maul.l_profile=mpgt.l_id;

update m_staging_schema.actor_usr_legacy maul
set
home_ou=maoul.id
from m_staging_schema.actor_org_unit_legacy maoul
where
maul.l_home_ou=maoul.l_id;



update m_staging_schema.actor_card_legacy macl
set
barcode=l_barcode::text,
active=l_active::boolean;

update m_staging_schema.actor_card_legacy macl
set
usr=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
macl.l_usr=maul.l_id;


update m_staging_schema.actor_usr_legacy maul
set
card=macl.id
from m_staging_schema.actor_card_legacy macl
where
maul.l_card=macl.l_id;

update m_staging_schema.actor_usr_legacy maul
set
mailing_address=maual.id
from m_staging_schema.actor_usr_address_legacy maual
where
maul.l_mailing_address=maual.l_id;

update m_staging_schema.actor_usr_legacy maul
set
billing_address=maual.id
from m_staging_schema.actor_usr_address_legacy maual
where
maul.l_billing_address=maual.l_id;


update m_staging_schema.patron_passwd mpp
set
usr=maul.id::text
from
m_staging_schema.actor_usr_legacy maul
where
mpp.usr=maul.l_id;


update m_staging_schema.actor_usr_address_legacy maual
set
valid=l_valid::boolean,
within_city_limits=l_within_city_limits::boolean,
address_type=l_address_type::text,
street1=l_street1::text,
street2=l_street2::text,
city=l_city::text,
county=l_county::text,
state=l_state::text,
country=l_country::text,
post_code=l_post_code::text,
pending=l_pending::boolean,
replaces=l_replaces::integer;


update m_staging_schema.actor_usr_address_legacy maual
set
usr=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
maual.l_usr=maul.l_id;

-- Fix orlsip
update m_staging_schema.actor_usr_legacy set mailing_address=null,billing_address=null where usrname='orlsip';

rollback;
begin;
insert into actor.usr
select * from m_staging_schema.actor_usr;

insert into actor.usr_address
select * from m_staging_schema.actor_usr_address where usr is not null;

insert into actor.passwd(
usr,
  salt,
  passwd,
  passwd_type,
  create_date,
  edit_date)
  select 
usr::integer,
salt::text,
passwd::text,
passwd_type::text,
create_date::timestamp,
edit_date::timestamp
from
m_staging_schema.patron_passwd;

insert into actor.card
select * from m_staging_schema.actor_card;

commit;


-- Bibs, items, patrons are done. Now patron mapped bits

update m_staging_schema.actor_usr_standing_penalty_legacy
set
standing_penalty=l_standing_penalty::integer,
set_date=l_set_date::timestamp,
stop_date=l_stop_date::timestamp,
note=l_note::text;

update m_staging_schema.actor_usr_standing_penalty_legacy mauspl
set
org_unit=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mauspl.l_org_unit = maoul.l_id;

-- The rest are consortium
update m_staging_schema.actor_usr_standing_penalty_legacy mauspl
set
org_unit=1
where org_unit is null;


update m_staging_schema.actor_usr_standing_penalty_legacy mauspl
set
usr=maoul.id
from
m_staging_schema.actor_usr_legacy maoul
where
mauspl.l_usr = maoul.l_id;


update m_staging_schema.actor_usr_standing_penalty_legacy mauspl
set
staff=maoul.id
from
m_staging_schema.actor_usr_legacy maoul
where
mauspl.l_staff = maoul.l_id;

rollback;
begin;
-- There is a custom penalty
insert into config.standing_penalty(name,label,block_list,staff_alert)
values('PATRON_EXCEEDS_FINES_FOR_HOLDS','Patron exceeds fine threshold for placing holds','HOLD','t');

update m_staging_schema.actor_usr_standing_penalty_legacy auspl
set
standing_penalty=csp.id
from
config.standing_penalty csp
where
auspl.standing_penalty=102 and
csp.name='PATRON_EXCEEDS_FINES_FOR_HOLDS';

insert into actor.usr_standing_penalty
select * from m_staging_schema.actor_usr_standing_penalty;

commit;


-- Skipping patron stat cat for ORL

/* update m_staging_schema.actor_stat_cat_legacy mascl
set
name=l_name::text,
opac_visible=l_opac_visible::boolean,
usr_summary=l_usr_summary::boolean,
sip_field=l_sip_field::character(2),
sip_format=l_sip_format::text,
checkout_archive=l_checkout_archive::boolean,
required=l_required::boolean,
allow_freetext=l_allow_freetext::boolean;

update m_staging_schema.actor_stat_cat_legacy mascl
set
owner=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mascl.l_owner = maoul.l_id;
 */

 
 -- Patron notes
 
update m_staging_schema.actor_usr_note_legacy
set
create_date=l_create_date::timestamp,
pub=l_pub::boolean,
title=l_title::text,
value=l_value::text;

update m_staging_schema.actor_usr_note_legacy maunl
set
usr=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
maunl.l_usr = maul.l_id;

update m_staging_schema.actor_usr_note_legacy maunl
set
creator=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
maunl.l_creator = maul.l_id;

-- Catch the rest as admin
update m_staging_schema.actor_usr_note_legacy maunl
set
creator=1
where creator is null;

begin;
insert into actor.usr_note
select * from m_staging_schema.actor_usr_note;

commit;


update m_staging_schema.actor_usr_message_legacy
set
title=l_title::text,
message=l_message::text,
create_date=l_create_date::timestamp,
deleted=l_deleted::boolean,
read_date=l_read_date::timestamp;
 
update m_staging_schema.actor_usr_message_legacy mauml
set
usr=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
mauml.l_usr = maul.l_id;

update m_staging_schema.actor_usr_message_legacy mauml
set
sending_lib=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mauml.l_sending_lib = maoul.l_id;

rollback;
begin;
insert into actor.usr_message
select * from m_staging_schema.actor_usr_message where usr is not null and sending_lib is not null;

commit;


-- Circulations
-- workstations

update m_staging_schema.actor_workstation_legacy set 
name=l_name::text;

update m_staging_schema.actor_workstation_legacy oawl
set 
owning_lib=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
oawl.l_owning_lib=maoul.l_id;

begin;
insert into actor.workstation
select * from m_staging_schema.actor_workstation where owning_lib is not null;

commit;

  
  
update m_staging_schema.action_circulation_legacy
set
xact_start=l_xact_start::timestamp,
xact_finish=l_xact_finish::timestamp,
unrecovered=l_unrecovered::boolean,
renewal_remaining=l_renewal_remaining::integer,
grace_period=l_grace_period::interval,
due_date=l_due_date::timestamp,
stop_fines_time=l_stop_fines_time::timestamp,
checkin_time=l_checkin_time::timestamp,
create_time=l_create_time::timestamp,
duration=l_duration::interval,
fine_interval=l_fine_interval::interval,
recurring_fine=l_recurring_fine::numeric(6,2),
max_fine=l_max_fine::numeric(6,2),
phone_renewal=l_phone_renewal::boolean,
desk_renewal=l_desk_renewal::boolean,
opac_renewal=l_opac_renewal::boolean,
duration_rule=l_duration_rule::text,
recurring_fine_rule=l_recurring_fine_rule::text,
max_fine_rule=l_max_fine_rule::text,
stop_fines=l_stop_fines::text,
checkin_scan_time=l_checkin_scan_time::timestamp;


update m_staging_schema.action_circulation_legacy macl
set
usr=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
macl.l_usr = maul.l_id;


update m_staging_schema.action_circulation_legacy macl
set
circ_staff=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
macl.l_circ_staff = maul.l_id;

update m_staging_schema.action_circulation_legacy macl
set
checkin_staff=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
macl.l_checkin_staff = maul.l_id;


update m_staging_schema.action_circulation_legacy macl
set
target_copy=maul.id
from
m_staging_schema.asset_copy_legacy maul
where
macl.l_target_copy = maul.l_id;

update m_staging_schema.action_circulation_legacy macl
set
circ_lib=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
macl.l_circ_lib=maoul.l_id;

update m_staging_schema.action_circulation_legacy macl
set
checkin_lib=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
macl.l_checkin_lib=maoul.l_id;

update m_staging_schema.action_circulation_legacy macl
set
workstation=maoul.id
from
m_staging_schema.actor_workstation_legacy maoul,
actor.workstation aw
where
aw.id=maoul.id and
macl.l_workstation=maoul.l_id;

update m_staging_schema.action_circulation_legacy macl
set
checkin_workstation=maoul.id
from
m_staging_schema.actor_workstation_legacy maoul,
actor.workstation aw
where
aw.id=maoul.id and
macl.l_checkin_workstation=maoul.l_id;


-- Fake the workstation to something close
update m_staging_schema.action_circulation_legacy macl
set
workstation=(select id from actor.workstation aw where aw.owning_lib=macl.circ_lib limit 1)
where
macl.workstation is null and l_workstation is not null;

update m_staging_schema.action_circulation_legacy macl
set
checkin_workstation=(select id from actor.workstation aw where aw.owning_lib=macl.circ_lib limit 1)
where
macl.checkin_workstation is null and l_checkin_workstation is not null;

-- Fake the circ_staff and checkin_staff to something close
update m_staging_schema.action_circulation_legacy macl
set
circ_staff=(select id from actor.usr where home_ou in(select id from actor.org_unit where lower(name)~'ozark') and profile in(select id from permission.grp_tree where name~'Circulation Ad') limit 1)
where
macl.circ_staff is null and 
target_copy is not null;

update m_staging_schema.action_circulation_legacy macl
set
checkin_staff=(select id from actor.usr where home_ou in(select id from actor.org_unit where lower(name)~'ozark') and profile in(select id from permission.grp_tree where name~'Circulation Ad') limit 1)
where
checkin_staff is null and
macl.checkin_workstation is not null and 
target_copy is not null;




update m_staging_schema.action_circulation_legacy macl
set
parent_circ=maoul.id
from
m_staging_schema.action_circulation_legacy maoul
where
macl.l_parent_circ=maoul.l_id and
maoul.circ_lib is not null and
maoul.circ_staff is not null and
maoul.target_copy is not null;

update m_staging_schema.action_circulation_legacy macl
set
copy_location=macll.id
from
m_staging_schema.asset_copy_location_legacy macll
where
macl.l_copy_location=macll.l_id;


-- billing pieces
update m_staging_schema.config_billing_type_legacy mcbtl
set
name=l_name::text,
default_price=l_default_price::numeric(6,2);


update m_staging_schema.config_billing_type_legacy mcbtl
set
owner=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mcbtl.l_owner=maoul.l_id;

update m_staging_schema.config_billing_type_legacy mcbtl
set
owner=1
where 
name not in(select name from config.billing_type where owner =1) and owner is null;

-- Need to be sure and insert billing types that are referenced in m_staging_schema.money_billing_legacy
update m_staging_schema.config_billing_type_legacy mcbtl
set
id=treal.id
from
config.billing_type treal
where 
mcbtl.name=treal.name and
mcbtl.id!=treal.id;

-- Lost Items was changed to Lost Materials
update m_staging_schema.config_billing_type_legacy mcbtl
set
id=treal.id
from
config.billing_type treal
where 
mcbtl.name='Lost Items' and
treal.name='Lost Materials' and
mcbtl.id!=treal.id
;


begin;
insert into config.billing_type
select * from m_staging_schema.config_billing_type where id not in(select id from config.billing_type);

commit;




-- billing
update m_staging_schema.money_billing_legacy
set
billing_ts=l_billing_ts::timestamp,
voided=l_voided::boolean,
void_time=l_void_time::timestamp,
amount=l_amount::numeric(6,2),
billing_type=l_billing_type::text,
note=l_note::text,
create_date=l_create_date::timestamp,
period_start=l_period_start::timestamp,
period_end=l_period_end::timestamp;

update m_staging_schema.money_billing_legacy mmbl
set
xact=macl.id
from
m_staging_schema.action_circulation_legacy macl
where
mmbl.l_xact=macl.l_id;

update m_staging_schema.money_billing_legacy mmbl
set
xact=macl.id
from
m_staging_schema.money_grocery_legacy macl
where
mmbl.l_xact=macl.l_id;

update m_staging_schema.money_billing_legacy mmbl
set
voider=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
mmbl.l_voider=maul.l_id;

update m_staging_schema.money_billing_legacy mmbl
set
btype=mcbtl.id
from
m_staging_schema.config_billing_type_legacy mcbtl
where
mmbl.l_btype=mcbtl.l_id;


-- check payment
update m_staging_schema.money_check_payment_legacy
set
payment_ts=l_payment_ts::timestamp,
voided=l_voided::boolean,
amount=l_amount::numeric(6,2),
note=l_note::text,
amount_collected=l_amount_collected::numeric(6,2),
check_number=l_check_number;

update m_staging_schema.money_check_payment_legacy mmbl
set
xact=macl.id
from
m_staging_schema.action_circulation_legacy macl
where
mmbl.l_xact=macl.l_id;

update m_staging_schema.money_check_payment_legacy mmbl
set
xact=macl.id
from
m_staging_schema.money_grocery_legacy macl
where
mmbl.l_xact=macl.l_id;

update m_staging_schema.money_check_payment_legacy mmbl
set
accepting_usr=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
mmbl.l_accepting_usr=maul.l_id;

update m_staging_schema.money_check_payment_legacy mmbl
set
cash_drawer=maul.id
from
m_staging_schema.actor_workstation_legacy maul
where
mmbl.l_cash_drawer=maul.l_id;

-- cash payment
update m_staging_schema.money_cash_payment_legacy
set
payment_ts=l_payment_ts::timestamp,
voided=l_voided::boolean,
amount=l_amount::numeric(6,2),
note=l_note::text,
amount_collected=l_amount_collected::numeric(6,2);

update m_staging_schema.money_cash_payment_legacy mmbl
set
xact=macl.id
from
m_staging_schema.action_circulation_legacy macl
where
mmbl.l_xact=macl.l_id;

update m_staging_schema.money_cash_payment_legacy mmbl
set
xact=macl.id
from
m_staging_schema.money_grocery_legacy macl
where
mmbl.l_xact=macl.l_id;

update m_staging_schema.money_cash_payment_legacy mmbl
set
accepting_usr=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
mmbl.l_accepting_usr=maul.l_id;

update m_staging_schema.money_cash_payment_legacy mmbl
set
cash_drawer=maul.id
from
m_staging_schema.actor_workstation_legacy maul
where
mmbl.l_cash_drawer=maul.l_id;

-- forgive payment
update m_staging_schema.money_forgive_payment_legacy
set
payment_ts=l_payment_ts::timestamp,
voided=l_voided::boolean,
amount=l_amount::numeric(6,2),
note=l_note::text,
amount_collected=l_amount_collected::numeric(6,2);

update m_staging_schema.money_forgive_payment_legacy mmbl
set
xact=macl.id
from
m_staging_schema.action_circulation_legacy macl
where
mmbl.l_xact=macl.l_id;

update m_staging_schema.money_forgive_payment_legacy mmbl
set
xact=macl.id
from
m_staging_schema.money_grocery_legacy macl
where
mmbl.l_xact=macl.l_id;

update m_staging_schema.money_forgive_payment_legacy mmbl
set
accepting_usr=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
mmbl.l_accepting_usr=maul.l_id;


-- account adjustment payment
update m_staging_schema.money_account_adjustment_legacy
set
payment_ts=l_payment_ts::timestamp,
voided=l_voided::boolean,
amount=l_amount::numeric(6,2),
note=l_note::text,
amount_collected=l_amount_collected::numeric(6,2);

update m_staging_schema.money_account_adjustment_legacy mmaal
set
billing=mmbl.id
from
m_staging_schema.money_billing_legacy mmbl
where
mmaal.l_billing=mmbl.l_id;

update m_staging_schema.money_account_adjustment_legacy mmbl
set
xact=macl.id
from
m_staging_schema.action_circulation_legacy macl
where
mmbl.l_xact=macl.l_id;

update m_staging_schema.money_account_adjustment_legacy mmbl
set
xact=macl.id
from
m_staging_schema.money_grocery_legacy macl
where
mmbl.l_xact=macl.l_id;

update m_staging_schema.money_account_adjustment_legacy mmbl
set
accepting_usr=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
mmbl.l_accepting_usr=maul.l_id;



-- Default to admin
update m_staging_schema.money_forgive_payment_legacy mmbl
set
accepting_usr=1 where accepting_usr is null;

-- Default to admin
update m_staging_schema.money_cash_payment_legacy mmbl
set
accepting_usr=1 where accepting_usr is null;

-- Default to admin
update m_staging_schema.money_check_payment_legacy mmbl
set
accepting_usr=1 where accepting_usr is null;

-- Default to admin
update m_staging_schema.money_account_adjustment_legacy mmbl
set
accepting_usr=1 where accepting_usr is null;


-- money grocery
update m_staging_schema.money_grocery_legacy
set
xact_start=l_xact_start::timestamp,
xact_finish=l_xact_finish::timestamp,
unrecovered=l_unrecovered::boolean,
billing_location=l_billing_location::integer,
note=l_note::text;

update m_staging_schema.money_grocery_legacy mmgl
set
usr=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
mmgl.l_usr=maul.l_id;

select left(xact_start::date::text,4),count(*) from
m_staging_schema.action_circulation
where target_copy is not null and circ_lib is not null and circ_staff is not null
group by 1
order by 1;


-- in house
update m_staging_schema.action_in_house_use_legacy
set
use_time=l_use_time::timestamp;

update m_staging_schema.action_in_house_use_legacy maihul
set
item=macl.id
from
m_staging_schema.asset_copy_legacy macl
where
maihul.l_item=macl.l_id;

update m_staging_schema.action_in_house_use_legacy maihul
set
staff=macl.id
from
m_staging_schema.actor_usr_legacy macl
where
maihul.l_staff=macl.l_id;

update m_staging_schema.action_in_house_use_legacy maihul
set
org_unit=macl.id
from
m_staging_schema.actor_org_unit_legacy macl
where
maihul.l_org_unit=macl.l_id;



rollback;
begin;
insert into action.circulation
select * from m_staging_schema.action_circulation where target_copy is not null and circ_lib is not null and circ_staff is not null;

insert into money.grocery
select * from m_staging_schema.money_grocery where usr is not null;

insert into money.billing
select * from m_staging_schema.money_billing where xact is not null;

insert into money.forgive_payment
select * from m_staging_schema.money_forgive_payment where xact is not null;

insert into money.check_payment
select * from m_staging_schema.money_check_payment where xact is not null;

insert into money.cash_payment
select * from m_staging_schema.money_cash_payment where xact is not null;

insert into money.account_adjustment
select * from m_staging_schema.money_account_adjustment where xact is not null;


insert into action.in_house_use
select * from m_staging_schema.action_in_house_use where org_unit is not null and staff is not null and item is not null;


commit;


-- copy status
update m_staging_schema.config_copy_status_legacy
set
name=l_name::text,
holdable=l_holdable::boolean,
opac_visible=l_opac_visible::boolean,
copy_active=l_copy_active::boolean,
restrict_copy_delete=l_restrict_copy_delete::boolean,
is_available=l_is_available::boolean;

update m_staging_schema.config_copy_status_legacy mccsl
set
id=treal.id
from
config.copy_status treal
where
treal.name=mccsl.name;

begin;
insert into config.copy_status
select * from m_staging_schema.config_copy_status where id not in(select id from config.copy_status);

commit;
  

update m_staging_schema.asset_copy_legacy macl
set
status=mccsl.id
from
m_staging_schema.config_copy_status_legacy mccsl
where
macl.l_status=mccsl.l_id;


begin;
update asset.copy ac
set 
status=macl.status
from
m_staging_schema.asset_copy_legacy macl
where
macl.id=ac.id and
ac.status!=macl.status;

commit;


-- SMS carrier
update m_staging_schema.config_sms_carrier_legacy
set
region=l_region::text,
name=l_name::text,
email_gateway=l_email_gateway::text,
active=l_active::boolean;

update m_staging_schema.config_sms_carrier_legacy mcscl
set
id=treal.id
from
config.sms_carrier treal
where
treal.name=mcscl.name;

begin;
insert into config.sms_carrier
select * from m_staging_schema.config_sms_carrier where id not in(select id from config.sms_carrier);

commit;


-- holds
update m_staging_schema.action_hold_request_legacy
set
request_time=l_request_time::timestamp,
capture_time=l_capture_time::timestamp,
fulfillment_time=l_fulfillment_time::timestamp,
checkin_time=l_checkin_time::timestamp,
return_time=l_return_time::timestamp,
prev_check_time=l_prev_check_time::timestamp,
expire_time=l_expire_time::timestamp,
cancel_time=l_cancel_time::timestamp,
cancel_cause=l_cancel_cause::integer,
cancel_note=l_cancel_note::text,
selection_depth=l_selection_depth::integer,
hold_type=l_hold_type::text,
holdable_formats=l_holdable_formats::text,
phone_notify=l_phone_notify::text,
email_notify=l_email_notify::boolean,
sms_notify=l_sms_notify::text,
frozen=l_frozen::boolean,
thaw_date=l_thaw_date::timestamp,
shelf_time=l_shelf_time::timestamp,
cut_in_line=l_cut_in_line::boolean,
mint_condition=l_mint_condition::boolean,
shelf_expire_time=l_shelf_expire_time::timestamp,
behind_desk=l_behind_desk::boolean,
target=l_target::bigint;


update m_staging_schema.action_hold_request_legacy mahrl
set
sms_carrier=mcscl.id
from
m_staging_schema.config_sms_carrier_legacy mcscl
where
mahrl.l_sms_carrier=mcscl.l_id;

update m_staging_schema.action_hold_request_legacy mahrl
set
pickup_lib=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mahrl.l_pickup_lib=maoul.l_id;

update m_staging_schema.action_hold_request_legacy mahrl
set
current_shelf_lib=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mahrl.l_current_shelf_lib=maoul.l_id;

update m_staging_schema.action_hold_request_legacy mahrl
set
request_lib=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mahrl.l_request_lib=maoul.l_id;

update m_staging_schema.action_hold_request_legacy mahrl
set
selection_ou=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mahrl.l_selection_ou=maoul.l_id;

update m_staging_schema.action_hold_request_legacy mahrl
set
fulfillment_lib=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mahrl.l_fulfillment_lib=maoul.l_id;

update m_staging_schema.action_hold_request_legacy mahrl
set
current_copy=maoul.id
from
m_staging_schema.asset_copy_legacy maoul
where
mahrl.l_current_copy=maoul.l_id;

update m_staging_schema.action_hold_request_legacy mahrl
set
fulfillment_staff=maoul.id
from
m_staging_schema.actor_usr_legacy maoul
where
mahrl.l_fulfillment_staff=maoul.l_id;

update m_staging_schema.action_hold_request_legacy mahrl
set
requestor=maoul.id
from
m_staging_schema.actor_usr_legacy maoul
where
mahrl.l_requestor=maoul.l_id;

update m_staging_schema.action_hold_request_legacy mahrl
set
usr=maoul.id
from
m_staging_schema.actor_usr_legacy maoul
where
mahrl.l_usr=maoul.l_id;

-- Fill in the DNA
update m_staging_schema.action_hold_request_legacy mahrl
set
requestor=usr
where requestor is null and usr is not null;

update m_staging_schema.action_hold_request_legacy mahrl
set
request_lib=pickup_lib
where request_lib is null and pickup_lib is not null;

update m_staging_schema.action_hold_request_legacy mahrl
set
request_lib=au.home_ou
from
actor.usr au
where
au.id=mahrl.usr and 
mahrl.request_lib is null;

update m_staging_schema.action_hold_request_legacy mahrl
set
pickup_lib=au.home_ou
from
actor.usr au
where
au.id=mahrl.usr and 
mahrl.pickup_lib is null;

update m_staging_schema.action_hold_request_legacy mahrl
set
selection_ou=pickup_lib
where selection_ou is null and pickup_lib is not null;


-- can't migrate metarecord holds so convert them if possible
update m_staging_schema.action_hold_request_legacy mahrl
set
target=current_copy,
hold_type='C'
where
hold_type='M' and
current_copy is not null;

-- remove the rest
update m_staging_schema.action_hold_request_legacy mahrl
set
target=null
where
hold_type='M' and
current_copy is null;


-- attempt to match parts
update m_staging_schema.action_hold_request_legacy mahrl
set
target=macpml.part
from
m_staging_schema.asset_copy_part_map_legacy macpml
where
mahrl.l_target=macpml.l_part and
mahrl.hold_type='P' and
current_copy is null;

-- make the others copy level
update m_staging_schema.action_hold_request_legacy mahrl
set
target=current_copy,
hold_type='C'
where
hold_type='P' and
current_copy is not null;

-- other part level holds can't come over
update m_staging_schema.action_hold_request_legacy mahrl
set
target=null
where
hold_type='P' and
current_copy is null and
target not in(select part from m_staging_schema.asset_copy_part_map_legacy);

-- Volume level
update m_staging_schema.action_hold_request_legacy mahrl
set
target=current_copy,
hold_type='C'
where
hold_type='V' and
current_copy is not null;

-- wipe the rest
update m_staging_schema.action_hold_request_legacy mahrl
set
target=null
where
hold_type='V' and
current_copy is null;

-- Title level
update m_staging_schema.action_hold_request_legacy mahrl
set
target=mbrel.id
from
m_staging_schema.biblio_record_entry_legacy mbrel
where
mahrl.l_target=mbrel.l_id and
mahrl.hold_type='T';

update m_staging_schema.action_hold_request_legacy mahrl
set
target=current_copy,
hold_type='C'
where
hold_type='T' and
current_copy is not null and
l_target not in(select l_id from m_staging_schema.biblio_record_entry_legacy mbrel);



-- can't connect the other title levels
update m_staging_schema.action_hold_request_legacy mahrl
set
target=null
where
hold_type='T' and
current_copy is null and l_target not in(select l_id from m_staging_schema.biblio_record_entry_legacy mbrel);

-- legacy Copy level
update m_staging_schema.action_hold_request_legacy mahrl
set
target=macl.id
from
m_staging_schema.asset_copy_legacy macl
where
macl.l_id=mahrl.l_target and
mahrl.l_hold_type='C';

-- Convert legacy copy level holds to title level holds where the copies are out of scope
update m_staging_schema.action_hold_request_legacy mahrl
set
target=mbrel.id,
hold_type='T',
capture_time=null,
current_copy=null
from
m_staging_schema.biblio_record_entry_legacy mbrel
where
mahrl.l_record=mbrel.l_id and
l_hold_type='C' and
l_target not in(select macl.l_id from m_staging_schema.asset_copy_legacy macl);


-- Report those special holds to the library
select distinct au.usrname,mahrl.capture_time,mahrl.fulfillment_time,mahrl.cancel_time
from
actor.usr au,
m_staging_schema.action_hold_request_legacy mahrl
where
mahrl.usr=au.id and
mahrl.l_hold_type='C' and
mahrl.hold_type='T' and
mahrl.fulfillment_time is null
order by 2 desc





begin;
insert into action.hold_request
select * from m_staging_schema.action_hold_request where target is not null;


-- make transits if needs be
insert into action.hold_transit_copy(target_copy,source,dest,copy_status,hold)
select
ahr.current_copy,
ac.circ_lib,
ahr.pickup_lib,
8,
ahr.id
from
action.hold_request ahr,
asset.copy ac
where
ac.status=6 and
ac.id=ahr.current_copy and
ahr.capture_time is not null and
ahr.fulfillment_time is null;




commit;


-- Make a report of patrons and holds that could not come over
select
aou_au.name,
au.usrname,
acard.barcode,
au.first_given_name,
au.family_name,
mahrl.request_time::date,
mahrl.fulfillment_time::date,
mahrl.cancel_time::date,
mahrl.l_target,
mahrl.hold_type,
maul.l_id
from
actor.usr au,
actor.card acard,
actor.org_unit aou_au,
m_staging_schema.action_hold_request_legacy mahrl,
m_staging_schema.actor_usr_legacy maul
where
maul.l_id=mahrl.l_usr and
au.id=mahrl.usr and
aou_au.id=au.home_ou and
acard.usr=au.id and
acard.active and
mahrl.target is null
order by 6 desc,5


-- and this way to run it back to ME


-- Make a report of patrons and holds that could not come over
select
mahrl.request_time::date,
mahrl.l_target,
mahrl.l_hold_type,
maul.l_id,
mahrl.l_id
from
actor.usr au,
actor.card acard,
actor.org_unit aou_au,
m_staging_schema.action_hold_request_legacy mahrl,
m_staging_schema.actor_usr_legacy maul
where
mahrl.cancel_time is null and
mahrl.fulfillment_time is null and
maul.l_id=mahrl.l_usr and
au.id=mahrl.usr and
aou_au.id=au.home_ou and
acard.usr=au.id and
acard.active and
mahrl.target is null
order by 4 desc;


-- Import data back to ME
drop table m_ray.missing_holds;

create table m_ray.missing_holds(request_time text,target text, hold_type text, usr text, ahrid text);
\copy m_ray.missing_holds from /mnt/evergreen/migration/orl_missing_holds.csv

-- Now run this report for ORL to create by hand (on ME databaes)

select
au.usrname,
acard.barcode,
ahr.hold_type,
'',
ahr.request_time,
(select string_agg(value,', ') from metabib.title_field_entry where source=outtersource.id),
(select string_agg(value,', ') from metabib.author_field_entry where source=outtersource.id)
from
action.hold_request ahr,
actor.usr au,
actor.card acard,
m_ray.missing_holds mmh,
biblio.record_entry outtersource
where
acard.active and
ahr.usr=au.id and
acard.usr=au.id and
mmh.ahrid::numeric=ahr.id and
ahr.hold_type='T' and
outtersource.id=ahr.target

union all

select
au.usrname,
acard.barcode,
ahr.hold_type,
acn.label,
ahr.request_time,
(select string_agg(value,', ') from metabib.title_field_entry where source=outtersource.id),
(select string_agg(value,', ') from metabib.author_field_entry where source=outtersource.id)
from
action.hold_request ahr,
actor.usr au,
actor.card acard,
m_ray.missing_holds mmh,
biblio.record_entry outtersource,
asset.call_number acn
where
acard.active and
ahr.usr=au.id and
acard.usr=au.id and
mmh.ahrid::numeric=ahr.id and
ahr.hold_type='V' and
outtersource.id=acn.record and
acn.id=ahr.target

union all

select
au.usrname,
acard.barcode,
ahr.hold_type,
ahr.holdable_formats::text,
ahr.request_time,
(select string_agg(value,', ') from metabib.title_field_entry where source=outtersource.id),
(select string_agg(value,', ') from metabib.author_field_entry where source=outtersource.id)
from
action.hold_request ahr,
actor.usr au,
actor.card acard,
m_ray.missing_holds mmh,
biblio.record_entry outtersource,
metabib.metarecord mm
where
acard.active and
ahr.usr=au.id and
acard.usr=au.id and
mmh.ahrid::numeric=ahr.id and
ahr.hold_type='M' and
outtersource.id=(select source from metabib.metarecord_source_map where metarecord=mm.id limit 1) and
mm.id=ahr.target

union all

select
au.usrname,
acard.barcode,
ahr.hold_type,
bmp.label,
ahr.request_time,
(select string_agg(value,', ') from metabib.title_field_entry where source=outtersource.id),
(select string_agg(value,', ') from metabib.author_field_entry where source=outtersource.id)
from
action.hold_request ahr,
actor.usr au,
actor.card acard,
m_ray.missing_holds mmh,
biblio.record_entry outtersource,
biblio.monograph_part bmp
where
acard.active and
ahr.usr=au.id and
acard.usr=au.id and
mmh.ahrid::numeric=ahr.id and
ahr.hold_type='P' and
outtersource.id=bmp.record and
bmp.id=ahr.target

union all

select
au.usrname,
acard.barcode,
ahr.hold_type,
'',
ahr.request_time,
(select string_agg(value,', ') from metabib.title_field_entry where source=outtersource.id),
(select string_agg(value,', ') from metabib.author_field_entry where source=outtersource.id)
from
action.hold_request ahr,
actor.usr au,
actor.card acard,
m_ray.missing_holds mmh,
biblio.record_entry outtersource,
asset.copy ac,
asset.call_number acn
where
acard.active and
ahr.usr=au.id and
acard.usr=au.id and
mmh.ahrid::numeric=ahr.id and
ahr.hold_type='C' and
outtersource.id=acn.record and
acn.id=ac.call_number and
ac.id=ahr.target

;





-- Report templates
-- Going with owner 1
-- ME Reports user ID = 789464
-- mobiusadmin user ID = 10497
update m_staging_schema.reporter_template_folder_legacy
set
create_time=l_create_time::timestamp,
name=l_name::text,
shared=l_shared::boolean;


update m_staging_schema.reporter_template_folder_legacy mrtfl
set
share_with=mrtfl_parent.id
from
m_staging_schema.actor_org_unit_legacy mrtfl_parent
where
mrtfl.l_share_with=mrtfl_parent.l_id;

update m_staging_schema.reporter_template_folder_legacy mrtfl
set
parent=mrtfl_parent.id
from
m_staging_schema.reporter_template_folder_legacy mrtfl_parent
where
mrtfl.l_parent=mrtfl_parent.l_id;

update m_staging_schema.reporter_template_folder_legacy mrtfl
set
owner=1 where l_owner in('789464','10497');

update m_staging_schema.reporter_template_folder_legacy mrtfl
set
owner=mrtfl_parent.id
from
m_staging_schema.actor_usr_legacy mrtfl_parent
where
mrtfl.l_owner=mrtfl_parent.l_id;

update m_staging_schema.reporter_template_folder_legacy
set share_with=1 where owner=1 and shared;

update m_staging_schema.reporter_report_folder_legacy
set
create_time=l_create_time::timestamp,
name=l_name::text,
shared=l_shared::boolean;

update m_staging_schema.reporter_report_folder_legacy mrtfl
set
share_with=mrtfl_parent.id
from
m_staging_schema.actor_org_unit_legacy mrtfl_parent
where
mrtfl.l_share_with=mrtfl_parent.l_id;

update m_staging_schema.reporter_report_folder_legacy mrrfl
set
parent=mrrfl_parent.id
from
m_staging_schema.reporter_report_folder_legacy mrrfl_parent
where
mrrfl.l_parent=mrrfl_parent.l_id;

update m_staging_schema.reporter_report_folder_legacy mrrfl
set
owner=mrrfl_parent.id
from
m_staging_schema.actor_usr_legacy mrrfl_parent
where
mrrfl.l_owner=mrrfl_parent.l_id;


update m_staging_schema.reporter_output_folder_legacy
set
create_time=l_create_time::timestamp,
name=l_name::text,
shared=l_shared::boolean;

update m_staging_schema.reporter_output_folder_legacy mrtfl
set
share_with=mrtfl_parent.id
from
m_staging_schema.actor_org_unit_legacy mrtfl_parent
where
mrtfl.l_share_with=mrtfl_parent.l_id;

update m_staging_schema.reporter_output_folder_legacy mrrfl
set
parent=mrrfl_parent.id
from
m_staging_schema.reporter_output_folder_legacy mrrfl_parent
where
mrrfl.l_parent=mrrfl_parent.l_id;

update m_staging_schema.reporter_output_folder_legacy mrrfl
set
owner=mrrfl_parent.id
from
m_staging_schema.actor_usr_legacy mrrfl_parent
where
mrrfl.l_owner=mrrfl_parent.l_id;


update m_staging_schema.reporter_template_legacy
set
create_time=l_create_time::timestamp,
name=l_name::text,
description=l_description::text,
data=l_data::text;


update m_staging_schema.reporter_template_legacy mrtl
set
folder=mrtfl.id
from
m_staging_schema.reporter_template_folder_legacy mrtfl
where
mrtl.l_folder=mrtfl.l_id;

update m_staging_schema.reporter_template_legacy mrtl
set
owner=1
where l_owner in('789464','10497');

update m_staging_schema.reporter_template_legacy mrtl
set
owner=mrtfl.id
from
m_staging_schema.actor_usr_legacy mrtfl
where
mrtl.l_owner=mrtfl.l_id;

rollback;
begin;
insert into reporter.template_folder
select * from m_staging_schema.reporter_template_folder where owner is not null and id not in(select id from reporter.template_folder);

insert into reporter.template
select * from m_staging_schema.reporter_template where owner is not null and folder is not null and id not in(select id from reporter.template);


insert into reporter.output_folder
select * from m_staging_schema.reporter_output_folder where owner is not null and id not in(select id from reporter.output_folder);
  
  insert into reporter.report_folder
select * from m_staging_schema.reporter_report_folder where owner is not null and id not in(select id from reporter.report_folder);



commit;





-- Working location
update m_staging_schema.permission_usr_work_ou_map_legacy mpuwoml
set
usr =maul.id
from 
m_staging_schema.actor_usr_legacy maul
where
mpuwoml.l_usr=maul.l_id;

update m_staging_schema.permission_usr_work_ou_map_legacy mpuwoml
set
work_ou=maul.id
from 
m_staging_schema.actor_org_unit_legacy maul
where
mpuwoml.l_work_ou=maul.l_id;
  
 rollback; 
begin;
insert into permission.usr_work_ou_map
select * from m_staging_schema.permission_usr_work_ou_map where usr is not null and work_ou is not null;


commit;



-- action triggers

update m_staging_schema.action_trigger_event_definition_legacy
set
active=l_active::boolean,
name=l_name::text,
hook=l_hook::text,
validator=l_validator::text,
reactor=l_reactor::text,
cleanup_success=l_cleanup_success::text,
cleanup_failure=l_cleanup_failure::text,
delay=l_delay::interval,
max_delay=l_max_delay::interval,
repeat_delay=l_repeat_delay::interval,
opt_in_setting=l_opt_in_setting::text,
delay_field=l_delay_field::text,
group_field=l_group_field::text,
template=l_template::text,
granularity=l_granularity::text,
message_template=l_message_template::text,
message_usr_path=l_message_usr_path::text,
message_library_path=l_message_library_path::text,
message_title=l_message_title::text,
retention_interval=l_retention_interval::interval,
usr_field=l_usr_field::text;


update m_staging_schema.action_trigger_event_definition_legacy matedl
set
owner=maul.id
from
m_staging_schema.actor_org_unit_legacy maul
where
matedl.l_owner=maul.l_id;

update m_staging_schema.action_trigger_event_definition_legacy matedl
set
owner=1
where owner is null;


update m_staging_schema.action_trigger_event_definition_legacy matedl
set
id=treal.id
from
action_trigger.event_definition treal
where
 treal.owner||'_'||treal.hook||'_'||treal.validator||'_'||treal.reactor||'_'||treal.delay||treal.delay_field = 
 matedl.owner||'_'||matedl.hook||'_'||matedl.validator||'_'||matedl.reactor||'_'||matedl.delay||matedl.delay_field;

 
update m_staging_schema.action_trigger_event_definition_legacy matedl
set
id=treal.id
from
action_trigger.event_definition treal
where
 treal.owner||'_'||treal.name = 
 matedl.owner||'_'||matedl.name;

 
 -- Environment
 update m_staging_schema.action_trigger_environment_legacy
 set
path=l_path::text,
collector=l_collector::text,
label=l_label::text;

 update m_staging_schema.action_trigger_environment_legacy matel
 set
event_def=matedl.id
from
m_staging_schema.action_trigger_event_definition_legacy matedl
where
matel.l_event_def=matedl.l_id;

update m_staging_schema.action_trigger_environment_legacy matel
set
id=treal.id
from
action_trigger.environment treal
where
matel.path=treal.path and
matel.label=treal.label;


-- event params
 update m_staging_schema.action_trigger_event_params_legacy
 set
param=l_param::text,
value=l_value::text;

 update m_staging_schema.action_trigger_event_params_legacy matel
 set
event_def=matedl.id
from
m_staging_schema.action_trigger_event_definition_legacy matedl
where
matel.l_event_def=matedl.l_id;

update m_staging_schema.action_trigger_event_params_legacy matel
set
id=treal.id
from
action_trigger.event_params treal
where
matel.param=treal.param and
matel.value=treal.value;

 
begin;
insert into action_trigger.event_definition
select * from m_staging_schema.action_trigger_event_definition where id not in(select id from action_trigger.event_definition) and name!='PO HTML';
 
update action_trigger.event_definition set granularity='Daily' where granularity~'Daily' and  granularity!='Daily';

insert into action_trigger.event_params
select * from m_staging_schema.action_trigger_event_params where id not in(select id from action_trigger.event_params);

insert into action_trigger.environment
select * from m_staging_schema.action_trigger_environment where id not in(select id from action_trigger.environment) and event_def is not null;


commit;




-- Patron settings and org settings
update m_staging_schema.patron_settings ps
set
usr=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
ps.usr=maul.l_id;

rollback;
begin;
insert into actor.usr_setting(usr,name,value)
select
usr::numeric,
name,
value
from
m_staging_schema.patron_settings where usr::numeric in(select id from actor.usr);

commit;


update m_staging_schema.actor_org_unit_setting maous
set
org_unit=maoul.id::text
from
m_staging_schema.actor_org_unit_legacy maoul
where
maous.org_unit=maoul.l_id;

begin;

insert into actor.org_unit_setting(org_unit,name,value)
select
org_unit::numeric,
name,
value
from
m_staging_schema.actor_org_unit_setting maous
where
maous.org_unit||'_'||maous.name not in(select org_unit||'_'||name from actor.org_unit_setting) and
maous.name in(select name from config.org_unit_setting_type)
;

update actor.org_unit_setting aous
set
value=maous.value
from
m_staging_schema.actor_org_unit_setting maous
where
aous.org_unit=maous.org_unit::numeric and
aous.name=maous.name and
aous.value!=maous.value;


commit;

-- Give the experiemental setting at consortium
begin;
insert into actor.org_unit_setting(org_unit,name,value)
values(1,'ui.staff.angular_catalog.enabled','true');

commit;

-- Hold notification history
update m_staging_schema.action_hold_notification_legacy
set
notify_time=l_notify_time::timestamp,
method=l_method::text,
note=l_note::text;

update m_staging_schema.action_hold_notification_legacy mahnl
set
hold=mahrl.id
from
m_staging_schema.action_hold_request_legacy mahrl
where
mahnl.l_hold=mahrl.l_id;

update m_staging_schema.action_hold_notification_legacy mahnl
set
notify_staff=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
mahnl.l_notify_staff=maul.l_id;


begin;
insert into action.hold_notification
select * from m_staging_schema.action_hold_notification
where
hold in(select id from action.hold_request);

commit;



-- Group penalty thresholds
update m_staging_schema.permission_grp_penalty_threshold_legacy
set
threshold=l_threshold::numeric(8,2);

update m_staging_schema.permission_grp_penalty_threshold_legacy mpgptl
set
grp=mpgtl.id
from
m_staging_schema.permission_grp_tree_legacy mpgtl
where
mpgptl.l_grp=mpgtl.l_id;

update m_staging_schema.permission_grp_penalty_threshold_legacy mpgptl
set
org_unit=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mpgptl.l_org_unit=maoul.l_id;

update m_staging_schema.permission_grp_penalty_threshold_legacy mpgptl
set
penalty=maoul.id
from
config.standing_penalty maoul
where
mpgptl.l_penalty::numeric=maoul.id;

update m_staging_schema.permission_grp_penalty_threshold_legacy mpgptl
set
penalty=csp.id
from
config.standing_penalty csp
where
mpgptl.l_penalty='102' and
csp.name='PATRON_EXCEEDS_FINES_FOR_HOLDS' and mpgptl.penalty!=csp.id;


begin;
insert into permission.grp_penalty_threshold
select * from m_staging_schema.permission_grp_penalty_threshold 
where id not in(select id from permission.grp_penalty_threshold);

commit;



-- Buckets
 'container.biblio_record_entry_bucket,',
 'container.biblio_record_entry_bucket_item,',
 'container.biblio_record_entry_bucket_item_note,',
 'container.biblio_record_entry_bucket_note'
 
 -- Bucket
update m_staging_schema.container_biblio_record_entry_bucket_legacy
set
name=l_name::text,
btype=l_btype::text,
description=l_description::text,
pub=l_pub::boolean,
create_time=l_create_time::timestamp;

update m_staging_schema.container_biblio_record_entry_bucket_legacy mcbrebl
set
owner=maul.id
from
m_staging_schema.actor_usr_legacy maul
where
mcbrebl.l_owner=maul.l_id;

update m_staging_schema.container_biblio_record_entry_bucket_legacy mcbrebl
set
owning_lib=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mcbrebl.l_owning_lib=maoul.l_id;

-- bucket item

update m_staging_schema.container_biblio_record_entry_bucket_item_legacy
set
pos=l_pos::integer,
create_time=l_create_time::timestamp;

update m_staging_schema.container_biblio_record_entry_bucket_item_legacy mcbrebil
set
bucket=mcbrebl.id
from
m_staging_schema.container_biblio_record_entry_bucket_legacy mcbrebl
where
mcbrebil.l_bucket=mcbrebl.l_id;

update m_staging_schema.container_biblio_record_entry_bucket_item_legacy mcbrebil
set
target_biblio_record_entry=mcbrebl.id
from
m_staging_schema.biblio_record_entry_legacy mcbrebl
where
mcbrebil.l_target_biblio_record_entry=mcbrebl.l_id;

-- bucket item note
update m_staging_schema.container_biblio_record_entry_bucket_item_note_legacy
set
note=l_note::text;

update m_staging_schema.container_biblio_record_entry_bucket_item_note_legacy mcbrebinl
set
item=mcbrebil.id
from
m_staging_schema.container_biblio_record_entry_bucket_item_legacy mcbrebil
where
mcbrebinl.l_item=mcbrebil.l_id;


-- bucket note
update m_staging_schema.container_biblio_record_entry_bucket_note_legacy
set
note=l_note::text;

update m_staging_schema.container_biblio_record_entry_bucket_note_legacy mcbrebinl
set
bucket=mcbrebil.id
from
m_staging_schema.container_biblio_record_entry_bucket_legacy mcbrebil
where
mcbrebinl.l_bucket=mcbrebil.l_id;

rollback;
begin;

insert into container.biblio_record_entry_bucket
select * from m_staging_schema.container_biblio_record_entry_bucket;

insert into container.biblio_record_entry_bucket_item
select * from m_staging_schema.container_biblio_record_entry_bucket_item where target_biblio_record_entry is not null;

insert into container.biblio_record_entry_bucket_item_note
select * from m_staging_schema.container_biblio_record_entry_bucket_item_note where item in(select id from container.biblio_record_entry_bucket_item);

insert into container.biblio_record_entry_bucket_note
select * from m_staging_schema.container_biblio_record_entry_bucket_note;

commit;




-- Get the z39.50 search stuff
update m_staging_schema.config_z3950_source_legacy
set
name=l_name::text,
label=l_label::text,
host=l_host::text,
port=l_port::integer,
db=l_db::text,
record_format=l_record_format::text,
transmission_format=l_transmission_format::text,
auth=l_auth::boolean,
use_perm=l_use_perm::integer;

update m_staging_schema.config_z3950_attr_legacy
set
source=l_source::text,
name=l_name::text,
label=l_label::text,
code=l_code::integer,
format=l_format::integer,
truncation=l_truncation::integer;

-- Not needed, nothing custom
/* update m_staging_schema.config_z3950_index_field_map_legacy
set
label=l_label::text,
metabib_field=l_metabib_field::integer,
record_attr=l_record_attr::text,
z3950_attr=l_z3950_attr::integer,
z3950_attr_type=l_z3950_attr_type::text; */

update m_staging_schema.config_z3950_attr_legacy mczal
set
id=treal.id
from
config.z3950_attr treal
where
treal.source=mczal.l_source;


begin;

insert into config.z3950_source
select * from
m_staging_schema.config_z3950_source
where name not in(select name from config.z3950_source);

insert into config.z3950_attr
select * from m_staging_schema.config_z3950_attr mcza
where
mcza.id not in(select id from config.z3950_attr);

commit;



-- Copy alerts
update m_staging_schema.config_copy_alert_type_legacy
set
active=l_active::boolean,
name=l_name::text,
state=l_state::config.copy_alert_type_state,
event=l_event::config.copy_alert_type_event,
in_renew=l_in_renew::boolean,
invert_location=l_invert_location::boolean,
at_owning=l_at_owning::boolean,
at_circ=l_at_circ::boolean,
next_status=l_next_status::integer[];

update m_staging_schema.config_copy_alert_type_legacy mccatl
set
scope_org=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mccatl.l_scope_org=maoul.l_id;

update m_staging_schema.config_copy_alert_type_legacy mccatl
set
scope_org=1
where scope_org is null;

update m_staging_schema.config_copy_alert_type_legacy mccatl
set
id=treal.id
from
config.copy_alert_type treal
where
treal.name=mccatl.name;


-- asset copy alert connection map
update m_staging_schema.asset_copy_alert_legacy
set
temp=l_temp::boolean,
create_time=l_create_time::timestamp,
note=l_note::text,
ack_time=l_ack_time::timestamp;

update m_staging_schema.asset_copy_alert_legacy macal
set
copy=macl.id
from
m_staging_schema.asset_copy_legacy macl
where
macal.l_copy=macl.l_id;

update m_staging_schema.asset_copy_alert_legacy macal
set
create_staff=maoul.id
from
m_staging_schema.actor_usr_legacy maoul
where
macal.l_create_staff=maoul.l_id;

update m_staging_schema.asset_copy_alert_legacy macal
set
ack_staff=maoul.id
from
m_staging_schema.actor_usr_legacy maoul
where
macal.l_ack_staff=maoul.l_id;


update m_staging_schema.asset_copy_alert_legacy macal
set
alert_type=mccatl.id
from
m_staging_schema.config_copy_alert_type_legacy mccatl
where
macal.l_alert_type=mccatl.l_id;


-- catch the default create user
update m_staging_schema.asset_copy_alert_legacy macal
set
create_staff=1
where
create_staff is null;

rollback;
begin;
insert into config.copy_alert_type
select * from m_staging_schema.config_copy_alert_type where id not in( select id from config.copy_alert_type);

insert into asset.copy_alert
select * from m_staging_schema.asset_copy_alert;

commit;


-- non cats
update m_staging_schema.config_non_cataloged_type_legacy
set
name=l_name::text,
circ_duration=l_circ_duration::interval,
in_house=l_in_house::boolean;

update m_staging_schema.config_non_cataloged_type_legacy mcnctl
set
owning_lib=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
mcnctl.l_owning_lib=maoul.l_id;

-- non cat circulation
update m_staging_schema.action_non_cataloged_circulation_legacy
set
circ_time=l_circ_time::timestamp;

update m_staging_schema.action_non_cataloged_circulation_legacy manccl
set
patron=maoul.id
from
m_staging_schema.actor_usr_legacy maoul
where
manccl.l_patron=maoul.l_id;

update m_staging_schema.action_non_cataloged_circulation_legacy manccl
set
staff=maoul.id
from
m_staging_schema.actor_usr_legacy maoul
where
manccl.l_staff=maoul.l_id;

update m_staging_schema.action_non_cataloged_circulation_legacy manccl
set
staff=1 where staff is null;

update m_staging_schema.action_non_cataloged_circulation_legacy manccl
set
circ_lib=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
manccl.l_circ_lib=maoul.l_id;

update m_staging_schema.action_non_cataloged_circulation_legacy manccl
set
item_type=mcnctl.id
from
 m_staging_schema.config_non_cataloged_type_legacy mcnctl
where
manccl.l_item_type=mcnctl.l_id;

update m_staging_schema.action_non_cataloged_circulation_legacy manccl
set
item_type=1 where item_type is null;


-- action.non_cat_in_house_use
update m_staging_schema.action_non_cat_in_house_use_legacy
set
use_time=l_use_time::timestamp;

update m_staging_schema.action_non_cat_in_house_use_legacy manccl
set
staff=maoul.id
from
m_staging_schema.actor_usr_legacy maoul
where
manccl.l_staff=maoul.l_id;

update m_staging_schema.action_non_cat_in_house_use_legacy manccl
set
staff=1 where staff is null;

update m_staging_schema.action_non_cat_in_house_use_legacy manccl
set
org_unit=maoul.id
from
m_staging_schema.actor_org_unit_legacy maoul
where
manccl.l_org_unit=maoul.l_id;

update m_staging_schema.action_non_cat_in_house_use_legacy manccl
set
item_type=mcnctl.id
from
 m_staging_schema.config_non_cataloged_type_legacy mcnctl
where
manccl.l_item_type=mcnctl.l_id;

update m_staging_schema.action_non_cat_in_house_use_legacy manccl
set
item_type=1 where item_type is null;


rollback;
begin;
insert into config.non_cataloged_type
select * from  m_staging_schema.config_non_cataloged_type;

insert into action.non_cataloged_circulation
select * from m_staging_schema.action_non_cataloged_circulation;

insert into action.non_cat_in_house_use
select * from m_staging_schema.action_non_cat_in_house_use;


commit;




-- Fix patron/staff settings to match the new org unit numbers

begin;
update actor.usr_setting aus
set value=transl.tnew
from
(
select '"'||id||'"' as tnew,'"'||l_id||'"' as org from m_staging_schema.actor_org_unit_legacy
) as transl
where
aus.value=transl.org;

commit;



-- Insert the circulation history for those who had settings for it

begin;


insert into action.usr_circ_history(usr,xact_start,target_copy,due_date,checkin_time,source_circ)
select
au.id,acirc.xact_start,acirc.target_copy,acirc.due_date,acirc.checkin_time,acirc.id
from
actor.usr au,
action.circulation acirc left join action.usr_circ_history auch on acirc.id=auch.source_circ,
actor.usr_setting aus
where
aus.usr=au.id and
acirc.usr=au.id and
aus.name='history.circ.retention_start' and
acirc.xact_start::date::text >= regexp_replace(aus.value,'"','','g') and
auch.id is null;

commit;



^\s*([^\s]*)\s*([^\s]*).*$

\1=l_\1::\2












-- FINALLY BIBS - Electronic

rollback;
begin;
insert into biblio.record_entry
select * from m_staging_schema.biblio_record_entry
where
id not in(select id from biblio.record_entry);

commit;

932m = 15 hours




-- migration reports




-- Get a report of all of the patrons with notes for close inspection
select usrname,first_given_name,family_name,regexp_replace(string_agg(b.value ,' - ') ,'[\r\n]',' ','g')
 from actor.usr a,
actor.usr_note b
 where 
 home_ou in(select id from actor.org_unit_descendants(101))
 and 
 a.id=b.usr
group by usrname,first_given_name,family_name;


-- Things that are floating
select 
acl.name,
acn.label,
ac.barcode,
aou_circ_lib.name,
aou_owning_lib.name,
string_agg(mtfe.value,' ')
from
asset.copy ac,
actor.org_unit aou_circ_lib,
actor.org_unit aou_owning_lib,
asset.copy_location acl,
asset.call_number acn,
metabib.title_field_entry mtfe
where
mtfe.source=acn.record and
acl.id=ac.location and
acn.id=ac.call_number and
acn.owning_lib=aou_owning_lib.id and
ac.circ_lib=aou_circ_lib.id and
ac.floating is not null and
lower(aou_circ_lib.name)~'ozark'
group by 1,2,3,4,5
order by 1,2,3,4,5


-- Report patron alerts 
select usrname,regexp_replace(alert_message,'[\r\n]',' ','g')
from actor.usr au
where
au.home_ou in(select id from actor.org_unit_descendants(101)) and
alert_message is not null
order by length(alert_message) desc;


-- Report items with parts
select
bmp.record,
bmp.label,
ac.barcode
from
asset.copy ac,
asset.copy_part_map acpm,
biblio.monograph_part bmp
where
bmp.id=acpm.part and
acpm.target_copy=ac.id
order by label;


-- Report items with notes
select
bre.id,
ac.barcode,
acnote.value
from
asset.copy ac,
asset.copy_note acnote,
asset.call_number acn,
biblio.record_entry bre
where
bre.id=acn.record and
acn.id=ac.call_number and
acnote.owning_copy=ac.id
order by 3;

-- Report checkouts
select
au.usrname,
ac.barcode,
acirc.xact_start::date,
acirc.due_date::date,
mmbxs.balance_owed
from
asset.copy ac,
action.circulation acirc,
actor.usr au,
money.materialized_billable_xact_summary mmbxs
where
mmbxs.id=acirc.id and
ac.id=acirc.target_copy and
au.id=acirc.usr and
acirc.xact_finish is null
order by 3 desc,1;

-- Report patrons with bills
select usrname,first_given_name,family_name,mmbxs.balance_owed
 from actor.usr a,
money.materialized_billable_xact_summary mmbxs
 where 
 mmbxs.usr=a.id
 and 
 mmbxs.balance_owed>0
and a.home_ou in(select id from actor.org_unit_descendants(101))
order by 4 desc


-- Report shelf/circ mod combo

select
acl.name,
ac.circ_modifier,
count(*)
from
asset.copy_location acl,
asset.copy ac
where
ac.location=acl.id
group by 1,2
order by 1,2;

-- Report permission group counts

select
pgt.name,
count(*)
from
actor.usr au,
permission.grp_tree pgt
where
au.profile=pgt.id
group by 1
order by 1


-- Totals

select count(*) from actor.usr where home_ou in(select id from actor.org_unit_descendants(101));
select count(*) from asset.copy where circ_lib in(select id from actor.org_unit_descendants(101));
select count(*) from biblio.record_entry where id > 1;



    

