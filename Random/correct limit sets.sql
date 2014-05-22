insert into config.circ_limit_set (name, owning_lib, items_out, description) values('One Items',1,1,'Limit of one item');
insert into config.circ_limit_set (name, owning_lib, items_out, description) values('Two Items',1,2,'Limit of Two items');
insert into config.circ_limit_set (name, owning_lib, items_out, description) values('Three Items',1,3,'Limit of Three items');
insert into config.circ_limit_set (name, owning_lib, items_out, description) values('Four Items',1,4,'Limit of Four items');
insert into config.circ_limit_set (name, owning_lib, items_out, description) values('Five Items',1,5,'Limit of Five items');
insert into config.circ_limit_set (name, owning_lib, items_out, description) values('Six Items',1,6,'Limit of Six items');
insert into config.circ_limit_set (name, owning_lib, items_out, description) values('Seven Items',1,7,'Limit of Seven items');
insert into config.circ_limit_set (name, owning_lib, items_out, description) values('Eight Items',1,8,'Limit of Eight items');
insert into config.circ_limit_set (name, owning_lib, items_out, description) values('Ten Items',1,10,'Limit of Ten items');
insert into config.circ_limit_set (name, owning_lib, items_out, description) values('Twelve Items',1,12,'Limit of Twelve items');
insert into config.circ_limit_set (name, owning_lib, items_out, description) values('Twenty-Five Items',1,25,'Limit of Twenty-Five items');
insert into config.circ_limit_set (name, owning_lib, items_out, description) values('Thirty Items',1,30,'Limit of Thirty items');
insert into config.circ_limit_set (name, owning_lib, items_out, description) values('Fifty Items',1,50,'Limit of Fifty items');
insert into config.circ_limit_set (name, owning_lib, items_out, description) values('Seventy-Five Items',1,75,'Limit of Seventy-Five items');

begin;
-- Remove mapings that map the same matchpoint to a limit set with the same number of items_out
delete from config.circ_matrix_limit_set_map where id in
(
select id from config.circ_matrix_limit_set_map where matchpoint in(
select split_part(b.to,' ',1)::integer from
(
select ccmlsm.matchpoint||' '|| ccls.items_out "to",count(*) from 
config.circ_matrix_limit_set_map ccmlsm,config.circ_matrix_matchpoint ccmm,config.circ_limit_set ccls
where
ccmlsm.limit_set=ccls.id and
ccmlsm.matchpoint=ccmm.id 
group by ccmlsm.matchpoint||' '|| ccls.items_out
having count(*)>1) "b"
) limit 1
);

-- Just poplar bluff

/* update config.circ_matrix_limit_set_map ccmlsm set limit_set=(select id from config.circ_limit_set where items_out=ccls.items_out and id>((select id from config.circ_limit_set where description='Limit of one item')-1))
from
config.circ_limit_set ccls,
config.circ_matrix_matchpoint ccmm
where
ccmlsm.limit_set=ccls.id and
ccmlsm.matchpoint=ccmm.id and
ccmm.org_unit in(2,4,101)
; */


update config.circ_matrix_limit_set_map ccmlsm set limit_set=(select id from config.circ_limit_set where items_out=ccls.items_out and id>((select id from config.circ_limit_set where description='Limit of one item')-1))
from
config.circ_limit_set ccls,
config.circ_matrix_matchpoint ccmm
where
ccmlsm.limit_set=ccls.id and
ccmlsm.matchpoint=ccmm.id;

-- Clean the mess
delete from config.circ_limit_set where id <(select id from config.circ_limit_set where description='Limit of one item');

commit;