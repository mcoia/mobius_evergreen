#!/usr/bin/perl


use Loghandler;
use Mobiusutil;
use Data::Dumper;
use XML::Simple;
use XML::TreeBuilder;
use Getopt::Long;
use DBhandler;


our $schema;
our $mobUtil = new Mobiusutil();
our $log;
our $dbHandler;
our $evergreendbHandler;
our $evergreenhost;
our $evergreenport;
our $evergreenlogin;
our $evergreenpass;
our $evergreendatabase;
our $evergreenlocationcodes;
our $loginvestigationoutput;
our $sample;
our @columns;
our @allRows;

my $xmlconf = "/openils/conf/opensrf.xml";
GetOptions (
"logfile=s" => \$logFile,
"xmlconfig=s" => \$xmlconf,
"schema=s" => \$schema,
"evergreenhost=s" => \$evergreenhost,
"evergreendatabase=s" => \$evergreendatabase,
"evergreenport=s" => \$evergreenport,
"evergreenlogin=s" => \$evergreenlogin,
"evergreenpass=s" => \$evergreenpass,
"evergreenlocationcodes=s" => \$evergreenlocationcodes,
"sample=s" => \$sample
)
or die("Error in command line arguments\nYou can specify
--logfile configfilename (required)
--xmlconfig pathtoevergreenopensrf.xml (default /opensrf/conf/opensrf.xml)
--evergreenhost IP/domain
--evergreendatabase DB name
--evergreenport DB Port
--evergreenlogin DB user
--evergreenpass DB password
--evergreenlocationcodes evergreen location codes regex accepted (comma separated)
--schema (eg. m_slmpl)
--sample (number of rows to fetch eg --sample 100)
\n");

if(! -e $xmlconf)
{
	print "I could not find the xml config file: $xmlconf\nYou can specify the path when executing this script --xmlconfig configfilelocation\n";
	exit 0;
}
if(!$logFile)
{
	print "Please specify a log file\n";
	exit;
}
if(!$schema)
{
	print "Please specify an Evergreen DB schema to dump the data to\n";
	exit;
}

	$log = new Loghandler($logFile);
	$log->truncFile("");
	$log->addLogLine(" ---------------- Script Starting ---------------- ");		

	my %dbconf = %{getDBconnects($xmlconf)};
	$dbHandler = new DBhandler($dbconf{"db"},$dbconf{"dbhost"},$dbconf{"dbuser"},$dbconf{"dbpass"},$dbconf{"port"});
	$evergreendbHandler = new DBhandler($evergreendatabase,$evergreenhost,$evergreenlogin,$evergreenpass,$evergreenport);
	
	
	my @sp = split(',',$evergreenlocationcodes);
	$evergreenlocationcodes='';
	$evergreenlocationcodes.="\$\$$_\$\$," for @sp;
	$evergreenlocationcodes=substr($evergreenlocationcodes,0,-1);
    
    $evergreenlocationcodes = getRemoteEGIDs();
	
  #get circ mods
	my $query = "
		select * from config.circ_modifier where code 
        in(
        select circ_modifier from asset.copy where call_number in(
        select id from asset.call_number where owning_lib in($evergreenlocationcodes)
        )
        union all
        select circ_modifier from config.hold_matrix_matchpoint chmm
        where
        chmm.item_owning_ou in($evergreenlocationcodes) and active
        union all
        select circ_modifier from config.hold_matrix_matchpoint target2
        where
        target2.user_home_ou in($evergreenlocationcodes) and active
        )";
	setupEGTable($query,"circ_modifier");
	
  ## get coded value map
	my $query = "
		select * from config.coded_value_map where value~\$\$Playaway\$\$
	";
	setupEGTable($query,"config_coded_value_map_legacy","config_coded_value_map");
  
    ## get composite_attr_entry_definition
	my $query = "
		select * from config.composite_attr_entry_definition
        where
        coded_value in(select id from config.coded_value_map where value~\$\$Playaway\$\$)
	";
	setupEGTable($query,"config_composite_attr_entry_definition_legacy","config_composite_attr_entry_definition");  
    
    # Get billing types
	my $query = "
		select * from config.billing_type where id in
        (
        select mb.btype from 
        money.billing mb,
        money.billable_xact mbx,
        actor.usr au        
        where 
        mb.xact=mbx.id and
        au.id=mbx.usr and
        au.home_ou in( $evergreenlocationcodes   )
        )
        
	";
	setupEGTable($query,"config_billing_type_legacy","config_billing_type");
    
  #get location/branches
	my $query = "
		select * from actor.org_unit where id in($evergreenlocationcodes)
	";
	setupEGTable($query,"actor_org_unit_legacy","actor_org_unit");
    
   #get location/branches addresses
	my $query = "
		select * from actor.org_address where id in(
        select ill_address from actor.org_unit where id in($evergreenlocationcodes)
        union all
        select holds_address from actor.org_unit where id in($evergreenlocationcodes)
        union all
        select mailing_address from actor.org_unit where id in($evergreenlocationcodes)
        union all
        select billing_address from actor.org_unit where id in($evergreenlocationcodes)
        )
	";
	setupEGTable($query,"actor_org_address_legacy","actor_org_address");    
    
    # actor.org_unit_setting
    my $query = "
		select * from actor.org_unit_setting where org_unit in($evergreenlocationcodes ,1)
	";
	setupEGTable($query,"actor_org_unit_setting");   
    
    # actor.hours_of_operation
    my $query = "
		select * from actor.hours_of_operation where id in($evergreenlocationcodes)
	";
    setupEGTable($query,"actor_hours_of_operation");
	
  
	#get patrons
	my $query = "
		select * from actor.usr where home_ou in ($evergreenlocationcodes) 
	";
	setupEGTable($query,"actor_usr_legacy","actor_usr");
    
    #get patron cards
	my $query = "
		select * from actor.card where usr in(select id from actor.usr where home_ou in ($evergreenlocationcodes)) 
	";
	setupEGTable($query,"actor_card_legacy","actor_card");
	
  ## get actor.passwd
	my $query = "
		select * from actor.passwd where usr in(select id from actor.usr where home_ou in ($evergreenlocationcodes)) 
	";
	setupEGTable($query,"patron_passwd");
  
	#get patron addresses	
	my $query = "
		select * from actor.usr_address where 
		id in
		(
		select mailing_address from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
        or id in
        (
		select billing_address from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
        or usr in
        (
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
	";
	setupEGTable($query,"actor_usr_address_legacy","actor_usr_address");
	
	#get patron note
	my $query = "
		select * from actor.usr_note where usr in
		(
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
	";
	setupEGTable($query,"actor_usr_note_legacy","actor_usr_note");
	
	#get patron messages
	my $query = "
		select * from actor.usr_message where usr in
		(
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
	";
	setupEGTable($query,"actor_usr_message_legacy","actor_usr_message");
    
    #get patron settings
	my $query = "
		select * from actor.usr_setting where usr in
		(
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
	";
	setupEGTable($query,"patron_settings");
    
    #get patron penalty
	my $query = "
		select * from actor.usr_standing_penalty where usr in
		(
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
	";
	setupEGTable($query,"actor_usr_standing_penalty_legacy","actor_usr_standing_penalty");
	
	#get patron checkouts
	my $query = "
		select * from action.circulation where usr in
		(
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
	";
	setupEGTable($query,"action_circulation_legacy","action_circulation");
    
    #get historic circulation counts
	my $query = "
		select ac.id,count(*) as count from 
        action.circulation acirc,
        asset.copy ac
        where 
        acirc.target_copy=ac.id and
        acirc.target_copy in (select id from asset.copy where call_number in(select id from asset.call_number where owning_lib in($evergreenlocationcodes))) and
        acirc.usr in
		(   
            select id from actor.usr where home_ou in (
                select id from actor.org_unit where id not in($evergreenlocationcodes)
            ) 
		)
        group by 1
	";
	setupEGTable($query,"asset_legacy_circ_count");
    
    #get patron in house use
	my $query = "
		select * from action.in_house_use where item in
		(
		select id from asset.copy where call_number in(select id from asset.call_number where owning_lib in($evergreenlocationcodes))
		)
	";
	setupEGTable($query,"action_in_house_use_legacy","action_in_house_use");
    
    
	# get patron grocery
	my $query = "
		select * from money.grocery where usr in
		(
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
	";
	setupEGTable($query,"money_grocery_legacy","money_grocery");
    
    # get patron fines
	my $query = "
		select * from money.billing where xact in(
        select id from money.billable_xact where xact_finish is null and
        usr in
		(
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
        )
	";
	setupEGTable($query,"money_billing_legacy","money_billing");
    
    # get patron account adjustment
	my $query = "
		select * from money.account_adjustment where xact in(
        select id from money.billable_xact where xact_finish is null and
        usr in
		(
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
        )
	";
	setupEGTable($query,"money_account_adjustment_legacy","money_account_adjustment");
    
    # get patron cash payments
	my $query = "
		select * from money.cash_payment where xact in(
        select id from money.billable_xact where xact_finish is null and
        usr in
		(
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
        )
	";
	setupEGTable($query,"money_cash_payment_legacy","money_cash_payment");
    
    # get patron check payments
	my $query = "
		select * from money.check_payment where xact in(
        select id from money.billable_xact where xact_finish is null and
        usr in
		(
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
        )
	";
	setupEGTable($query,"money_check_payment_legacy","money_check_payment");
    
    # get patron forgive payments
	my $query = "
		select * from money.forgive_payment where xact in(
        select id from money.billable_xact where xact_finish is null and
        usr in
		(
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
        )
	";
	setupEGTable($query,"money_forgive_payment_legacy","money_forgive_payment");
    
    
    # get patron stat categories
	my $query = "
		select * from actor.stat_cat where id in(
        select stat_cat from actor.stat_cat_entry_usr_map where target_usr in
        (
		
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
        )
	";
	setupEGTable($query,"actor_stat_cat_legacy","actor_stat_cat");
    
    # get patron stat options
	my $query = "
		select * from actor.stat_cat_entry where stat_cat in(
        select id from actor.stat_cat where id in(
        select stat_cat from actor.stat_cat_entry_usr_map where target_usr in
        (
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
        )
        )
	";
	setupEGTable($query,"actor_stat_cat_entry_legacy","actor_stat_cat_entry");
        
    # get patron stat map
	my $query = "
		select * from actor.stat_cat_entry_usr_map
        where target_usr in
        (
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
	";
	setupEGTable($query,"actor_stat_cat_entry_usr_map_legacy","actor_stat_cat_entry_usr_map");
    
    
	#get patron permission group map
	my $query = "
		select *
        from
        permission.grp_perm_map
	";
	setupEGTable($query,"permission_grp_perm_map_legacy","permission_grp_perm_map");
    
    
    #get patron types
    my $query = "
		select *
        from        
        permission.grp_tree pgt
        
	";
	setupEGTable($query,"permission_grp_tree_legacy","permission_grp_tree");
	
    #get patron work ou map
    my $query = "
		select * from permission.usr_work_ou_map puwom
        where usr
        in
        (
        select au.id
        from
        actor.usr au
        where
        au.home_ou in($evergreenlocationcodes)
        )
	";
	setupEGTable($query,"permission_usr_work_ou_map_legacy","permission_usr_work_ou_map");

    
	
	#get copy location
	my $query = "
    select * from asset.copy_location where 
    id
    in(select location from asset.copy where call_number in(
        select id from asset.call_number where owning_lib in($evergreenlocationcodes)
        )
        )
	";
	setupEGTable($query,"asset_copy_location_legacy","asset_copy_location");
  
	# get holds - clever trick to get the bib records for copy level holds that are potientially out of scope
	my $query = "
        select ahr.*,(select record from asset.call_number where id in(select call_number from asset.copy where id=ahr.id and ahr.hold_type='C')) from 
        action.hold_request ahr,
        actor.usr au        
        where 
        au.id=ahr.usr and
        au.home_ou in($evergreenlocationcodes) and
        ahr.cancel_time is null
        
	";
	setupEGTable($query,"action_hold_request_legacy","action_hold_request");
	
    #get holds notification
	my $query = "
    select * from action.hold_notification where hold in
    (
        select ahr.id from 
        action.hold_request ahr,
        actor.usr au        
        where 
        au.id=ahr.usr and
        au.home_ou in($evergreenlocationcodes) and
        ahr.cancel_time is null
    )
	";
	setupEGTable($query,"action_hold_notification_legacy","action_hold_notification");	
    
       
    # get full Item table
	my $query = "
        select ac.*,acn.record,acn.label from 
        asset.copy ac,
        asset.call_number acn
        where
        acn.id=ac.call_number and
        acn.owning_lib in($evergreenlocationcodes)
        
	";
	setupEGTable($query,"asset_copy_legacy","asset_copy");
    
    #get item notes
	my $query = "
        select acnote.*
        from 
        asset.copy_note acnote,
        asset.copy ac,
        asset.call_number acn
        where
        acnote.owning_copy=ac.id and
        acn.id=ac.call_number and
        acn.owning_lib in($evergreenlocationcodes)
        
	";
	setupEGTable($query,"asset_copy_note_legacy","asset_copy_note");
	
    
    # get item stat categories
	my $query = "
		select distinct ttarget.*        
        from 
        asset.stat_cat ttarget,
        asset.copy ac,
        asset.call_number acn,
        asset.stat_cat_entry_copy_map ascecm
        where
        ascecm.stat_cat=ttarget.id and
        ascecm.owning_copy=ac.id and
        acn.id=ac.call_number and
        acn.owning_lib in($evergreenlocationcodes)
        
	";
	setupEGTable($query,"asset_stat_cat_legacy","asset_stat_cat");
    
    # get item stat options
	my $query = "
		select distinct ttarget.*        
        from 
        asset.stat_cat assc,
        asset.stat_cat_entry ttarget,
        asset.stat_cat_entry_copy_map ascecm,
        asset.copy ac,
        asset.call_number acn
        where
        ascecm.stat_cat=assc.id and
        assc.id=ttarget.stat_cat and
        ascecm.owning_copy=ac.id and
        acn.id=ac.call_number and
        acn.owning_lib in($evergreenlocationcodes)
	";
	setupEGTable($query,"asset_stat_cat_entry_legacy","asset_stat_cat_entry");
        
    # get item stat map
	my $query = "
		select ttarget.*        
        from 
        asset.stat_cat_entry_copy_map ttarget,
        asset.copy ac,
        asset.call_number acn
        where
        ttarget.owning_copy=ac.id and
        acn.id=ac.call_number and
        acn.owning_lib in($evergreenlocationcodes)
	";
	setupEGTable($query,"asset_stat_cat_entry_copy_map_legacy","asset_stat_cat_entry_copy_map");
    
    # get item tags
	my $query = "
		select distinct ttarget.*        
        from 
        asset.copy_tag ttarget,
        asset.copy ac,
        asset.copy_tag_copy_map actcm,
        asset.call_number acn
        where
        ttarget.id=actcm.tag and
        actcm.copy = ac.id and
        acn.id=ac.call_number and
        acn.owning_lib in($evergreenlocationcodes)
	";
	setupEGTable($query,"asset_copy_tag_legacy","asset_copy_tag");
    
    # get item tag map
	my $query = "
		select actcm.*        
        from 
        asset.copy ac,
        asset.copy_tag_copy_map actcm,
        asset.call_number acn
        where
        actcm.copy = ac.id and
        acn.id=ac.call_number and
        acn.owning_lib in($evergreenlocationcodes)
	";
	setupEGTable($query,"asset_copy_tag_copy_map_legacy","asset_copy_tag_copy_map");
     
    
    # get bibs
	my $query = "
		select * from biblio.record_entry where id in
        (
        
       select acn2.record from
        asset.call_number acn2,
        asset.copy ac2,
        action.hold_request ahr2
        where
        ahr2.target=ac2.id and
        acn2.id=ac2.call_number and
        ahr2.hold_type='C' and
        ahr2.usr in(select id from actor.usr where home_ou in ($evergreenlocationcodes))
        
        union all
        
        select record from 
        asset.call_number acn
        where
        acn.owning_lib in($evergreenlocationcodes) and not deleted
        
        union all
        
        select ahr.target from 
        action.hold_request ahr,
        actor.usr au
        where
        au.id=ahr.usr and
        au.home_ou in($evergreenlocationcodes) and
        ahr.cancel_time is null and
        ahr.capture_time is null
        
        union all
        
        select cbrebi.target_biblio_record_entry
        from
        container.biblio_record_entry_bucket_item cbrebi
        where cbrebi.bucket in
        (
		select id from container.biblio_record_entry_bucket
        where owner in (select id from actor.usr where home_ou in ($evergreenlocationcodes) )
        )
        
        ) and not deleted
	";    
	setupEGTable($query,"biblio_record_entry_legacy","biblio_record_entry");


    # get monograph parts
	my $query = "
		select * from biblio.monograph_part bmp
        where
        id in
        (
        select acpm.part
        from 
        biblio.monograph_part bmp2,
        asset.copy_part_map acpm,
        asset.copy ac,
        asset.call_number acn
        where
        acn.id=ac.call_number and
        ac.id=acpm.target_copy and
        bmp2.id=acpm.part and
        acn.owning_lib in($evergreenlocationcodes)
        )
	";
	setupEGTable($query,"biblio_monograph_part_legacy","biblio_monograph_part");
    
    # get monograph part map
	my $query = "
		select * from asset.copy_part_map acpm2
        where
        id in
        (select acpm.id from
        biblio.monograph_part bmp,
        asset.copy_part_map acpm,
        asset.copy ac,
        asset.call_number acn
        where
        acn.id=ac.call_number and
        ac.id=acpm.target_copy and
        bmp.id=acpm.part and
        acn.owning_lib in($evergreenlocationcodes)
        )
	";
	setupEGTable($query,"asset_copy_part_map_legacy","asset_copy_part_map");
    
    
    # get circ rules
	my $query = "
		select * from config.circ_limit_set target
        where
        id in
        (
        select ccls.id from
        config.circ_limit_set ccls,
        config.circ_matrix_matchpoint ccmm,
        config.circ_matrix_limit_set_map ccmlsm
        where
        ccmlsm.matchpoint=ccmm.id and
        ccmlsm.limit_set=ccls.id and
        ccmm.org_unit in($evergreenlocationcodes)
        )
	";
	setupEGTable($query,"config_circ_limit_set_legacy","config_circ_limit_set");
    
    # get circ rules
	my $query = "
		select * from config.circ_limit_set_circ_mod_map target
        where
        limit_set in
        (
        select ccmlsm.limit_set from
        config.circ_matrix_matchpoint ccmm,
        config.circ_matrix_limit_set_map ccmlsm
        where
        ccmlsm.matchpoint=ccmm.id and
        ccmm.org_unit in($evergreenlocationcodes)
        )
	";
	setupEGTable($query,"config_circ_limit_set_circ_mod_map_legacy","config_circ_limit_set_circ_mod_map");
    
    # get circ rules
	my $query = "
		select * from config.circ_matrix_limit_set_map target
        where
        matchpoint in
        (
        select ccmm.id from config.circ_matrix_matchpoint ccmm
        where
        ccmm.org_unit in($evergreenlocationcodes) and active
        )
	";
	setupEGTable($query,"config_circ_matrix_limit_set_map_legacy","config_circ_matrix_limit_set_map");
    
    # get circ rules
	my $query = "
		select * from config.circ_matrix_matchpoint ccmm
        where
        ccmm.org_unit in($evergreenlocationcodes) and active
       
	";
	setupEGTable($query,"config_circ_matrix_matchpoint_legacy","config_circ_matrix_matchpoint");
    
     # get circ rules
	my $query = "
		select * from config.rule_circ_duration target
        where
        id in
        (
        select duration_rule from
        config.circ_matrix_matchpoint ccmm
        where
        ccmm.org_unit in($evergreenlocationcodes)
        )       
	";
	setupEGTable($query,"config_rule_circ_duration_legacy","config_rule_circ_duration");
    
    # get circ rules
	my $query = "
		select * from config.rule_max_fine target
        where
        id in
        (
        select max_fine_rule from
        config.circ_matrix_matchpoint ccmm
        where
        ccmm.org_unit in($evergreenlocationcodes)
        )       
	";
	setupEGTable($query,"config_rule_max_fine_legacy","config_rule_max_fine");
       
    # get circ rules
	my $query = "
		select * from config.rule_recurring_fine target
        where
        id in
        (
        select recurring_fine_rule from
        config.circ_matrix_matchpoint ccmm
        where
        ccmm.org_unit in($evergreenlocationcodes)
        )       
	";
	setupEGTable($query,"config_rule_recurring_fine_legacy","config_rule_recurring_fine");
    
    # get hold rules
	my $query = "
    select distinct * from 
    (
		select * from config.hold_matrix_matchpoint target
        where
        target.item_owning_ou in($evergreenlocationcodes) and active
        union all
        select * from config.hold_matrix_matchpoint target2
        where
        target2.user_home_ou in($evergreenlocationcodes) and active
        ) as a
	";
	setupEGTable($query,"config_hold_matrix_matchpoint_legacy","config_hold_matrix_matchpoint");
    
     # get report templates folders
	my $query = "
		select * from reporter.template_folder
	";
	setupEGTable($query,"reporter_template_folder_legacy","reporter_template_folder");
    
    # get report templates
	my $query = "
		select * from reporter.template
	";
	setupEGTable($query,"reporter_template_legacy","reporter_template");
    
    # get report output folder
	my $query = "
		select * from reporter.output_folder
	";
	setupEGTable($query,"reporter_output_folder_legacy","reporter_output_folder");
    
    # get report output folder
	my $query = "
		select * from reporter.report_folder
	";
	setupEGTable($query,"reporter_report_folder_legacy","reporter_report_folder");
    
     # get workstations
	my $query = "
		select * from actor.workstation where id
        in
        (
        select workstation from action.circulation where        
        usr in
		(
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
        union all
        select checkin_workstation from action.circulation where        
        usr in
		(
		select id from actor.usr where home_ou in ($evergreenlocationcodes) 
		)
        )
        
	";
	setupEGTable($query,"actor_workstation_legacy","actor_workstation");
    
    # get copy status
	my $query = "
		select * from config.copy_status
	";
	setupEGTable($query,"config_copy_status_legacy","config_copy_status");
    
    # get sms carriers
	my $query = "
		select * from config.sms_carrier
	";
	setupEGTable($query,"config_sms_carrier_legacy","config_sms_carrier");
    
    # get action triggers
	my $query = "
		select * from action_trigger.event_definition
        where owner in ($evergreenlocationcodes ,1) and active
	";
	setupEGTable($query,"action_trigger_event_definition_legacy","action_trigger_event_definition");
    
    # get action trigger environment
	my $query = "
    select * from action_trigger.environment where event_def in
    (
		select id from action_trigger.event_definition
        where owner in ($evergreenlocationcodes ,1) and active
    )
	";
	setupEGTable($query,"action_trigger_environment_legacy","action_trigger_environment");
    
   # get action trigger event params
	my $query = "
    select * from action_trigger.event_params where event_def in
    (
		select id from action_trigger.event_definition
        where owner in ($evergreenlocationcodes ,1) and active
    )
	";
	setupEGTable($query,"action_trigger_event_params_legacy","action_trigger_event_params");
    
    ## get container buckets
    my $query = "
		select * from container.biblio_record_entry_bucket
        where owner in (select id from actor.usr where home_ou in ($evergreenlocationcodes) )
	";
	setupEGTable($query,"container_biblio_record_entry_bucket_legacy","container_biblio_record_entry_bucket");
    
     # get container buckets items
    my $query = "
        select * from container.biblio_record_entry_bucket_item
        where bucket in
        (
		select id from container.biblio_record_entry_bucket
        where owner in (select id from actor.usr where home_ou in ($evergreenlocationcodes) )
        )
	";
	setupEGTable($query,"container_biblio_record_entry_bucket_item_legacy","container_biblio_record_entry_bucket_item");
    
     ## get container buckets items notes
    my $query = "
        select * from container.biblio_record_entry_bucket_item_note
        where item in
        (
        select id from container.biblio_record_entry_bucket_item
        where bucket in
        (
		select id from container.biblio_record_entry_bucket
        where owner in (select id from actor.usr where home_ou in ($evergreenlocationcodes) )
        )
        )
	";
	setupEGTable($query,"container_biblio_record_entry_bucket_item_note_legacy","container_biblio_record_entry_bucket_item_note");
    
    ## get container buckets notes
    my $query = "
        select * from container.biblio_record_entry_bucket_note
        where bucket in
        (
		select id from container.biblio_record_entry_bucket
        where owner in (select id from actor.usr where home_ou in ($evergreenlocationcodes) )
        )
	";
	setupEGTable($query,"container_biblio_record_entry_bucket_note_legacy","container_biblio_record_entry_bucket_note");
    
    ## get Permission group penalty definitions
    my $query = "
        select * from permission.grp_penalty_threshold
        where org_unit in ($evergreenlocationcodes)
	";
	setupEGTable($query,"permission_grp_penalty_threshold_legacy","permission_grp_penalty_threshold");
    
    ## get z39.50 stuff
    my $query = "
        select * from config.z3950_source
	";
	setupEGTable($query,"config_z3950_source_legacy","config_z3950_source");
    
    ## get z39.50 stuff
    my $query = "
        select * from config.z3950_index_field_map
	";
	setupEGTable($query,"config_z3950_index_field_map_legacy","config_z3950_index_field_map");
    
    ## get z39.50 stuff
    my $query = "
        select * from config.z3950_attr
	";
	setupEGTable($query,"config_z3950_attr_legacy","config_z3950_attr");
    
    ## get non catalog types
    my $query = "
        select * from config.non_cataloged_type
        where
        owning_lib in($evergreenlocationcodes)
	";
	setupEGTable($query,"config_non_cataloged_type_legacy","config_non_cataloged_type");
    
    ## get copy alert types
    my $query = "
        select * from config.copy_alert_type
        where
        scope_org in($evergreenlocationcodes,1)
	";
	setupEGTable($query,"config_copy_alert_type_legacy","config_copy_alert_type");
    
    ## get copy alerts
    my $query = "
        select * from asset.copy_alert
        where
        copy in( select id from asset.copy where call_number in(select id from asset.call_number where owning_lib in  ($evergreenlocationcodes) ))
	";
	setupEGTable($query,"asset_copy_alert_legacy","asset_copy_alert");    
    
    ## get action.non_cataloged_circulation
    my $query = "
        select * from action.non_cataloged_circulation
        where
        circ_lib in( $evergreenlocationcodes)
	";
	setupEGTable($query,"action_non_cataloged_circulation_legacy","action_non_cataloged_circulation");
    
    ## get action_non_cat_in_house_use
    my $query = "
        select * from action.non_cat_in_house_use
        where
        org_unit in( $evergreenlocationcodes)
    ";
    setupEGTable($query,"action_non_cat_in_house_use_legacy","action_non_cat_in_house_use");
    
    
    
	$log->addLogLine(" ---------------- Script End ---------------- ");
	
	
sub setupEGTable
{
	my $query = @_[0];
	my $tablename = @_[1];
    my $inherited = @_[2] || 0;
    my $append = @_[3] || 0;
    
    my $insertChunkSize = 500;
	
	print "Gathering $tablename....";
	
    my @ret = @{getRemoteEvergreenData($query)};
    
	my @allRows = @{@ret[0]};
	my @cols = @{@ret[1]};
    if($inherited)
    {
        for my $i (0..$#cols)
        {
            @cols[$i] = "l_".@cols[$i]
        }
    }
	print $#allRows." rows\n";
	
	
    if(!$append)
    {
        #drop the table
        my $query = "DROP TABLE IF EXISTS $schema.$tablename";
        $log->addLine($query);
        $dbHandler->update($query);
        
        #create the table
        $query = "CREATE TABLE $schema.$tablename (";
        $query.=$_." TEXT," for @cols;
        $query=substr($query,0,-1).")";
        $query.=" inherits ( $schema.$inherited )" if($inherited);

        $log->addLine($query);
        $dbHandler->update($query);
    }
    
    my @vals = ();
    my $valpos = 1;
    my $totalInserted = 0;
	
	if($#allRows > -1)
	{
		#insert the data
        my $rowcount = 0;
		$query = "INSERT INTO $schema.$tablename (";
		$query.=$_."," for @cols;
		$query=substr($query,0,-1).")\nVALUES\n";
        my $queryTemplate = $query;
		foreach(@allRows)
		{
			$query.="(";
			my @thisrow = @{$_};
			$query.= "\$" . $valpos++ . "," for(@thisrow);
            push @vals, @thisrow;
            # for(@thisrow)
			# {
				# my $value = $_;
				# #add period on trialing $ signs
				# #print "$value -> ";
				# $value =~ s/\$$/\$\./;
                # #add period on head $ signs
                # $value =~ s/^\$/\.\$/;
				# #print "$value\n";
				# $query.='$data$'.$value.'$data$,'
			# }
			$query=substr($query,0,-1)."),\n";
            $rowcount++;
            if($rowcount % $insertChunkSize == 0)
            {
                $totalInserted+=$insertChunkSize;
                $query=substr($query,0,-2)."\n";
                $loginvestigationoutput.="select count(*),$_ from $schema.$tablename group by $_ order by $_\n" for @cols;
                print "Inserted ".$totalInserted." Rows into $schema.$tablename\n";
                $log->addLine($query);
                $dbHandler->updateWithParameters($query, \@vals);
                $query = $queryTemplate;
                $rowcount=0;
                @vals = ();
                $valpos = 1;
            }
		}
        
        if($valpos > 1)
        {
            $query=substr($query,0,-2)."\n";
            $loginvestigationoutput.="select count(*),$_ from $schema.$tablename group by $_ order by $_\n" for @cols;
            print "Inserted ".$#allRows." Rows into $schema.$tablename\n";
            $log->addLine($query);
            $dbHandler->updateWithParameters($query, \@vals);
        }

	}
	else
	{
		print "Empty dataset for $tablename \n";
		$log->addLine("Empty dataset for $tablename");
	}
}

sub getRemoteEvergreenData
{
    my $queryTemplate = @_[0];
    my $offset = 0;
    my @ret = ();
    my $limit = 10000;
    $limit = $sample if $sample;
    $queryTemplate.="\nORDER BY 1\n LIMIT $limit OFFSET !OFFSET!";
    my $loops = 0;
    my @cols;
    my $data = 1;
    my @allRows = ();
    
    while($data)
    {
        my $query = $queryTemplate;
        $query =~ s/!OFFSET!/$offset/g;
        $log->addLine($query);
        my @theseRows = @{$evergreendbHandler->query($query)};
        @cols = @{$evergreendbHandler->getColumnNames()} if !(@cols);
        $data = 0 if($#theseRows < 0 );
        push @allRows, @theseRows if ($#theseRows > -1 );
        $loops++;
        $offset = ($loops * $limit) + 1;
        $data = 0 if $sample;
        undef @theseRows;
    }


    push @ret, [@allRows];
    push @ret, [@cols];
    return \@ret;
}

sub getRemoteEGIDs
{
    my $query = "select id from actor.org_unit where shortname in($evergreenlocationcodes)";
    my $ret = '';
    my @theseRows = @{$evergreendbHandler->query($query)};
    foreach(@theseRows)
    {
        my @row = @{$_};
        my @descendents  = @{$evergreendbHandler->query("select id from actor.org_unit_descendants(".@row[0].")")};
        foreach(@descendents)
        {
            my @d = @{$_};
            $ret.=@d[0].',';
        }
    }
    $ret = substr($ret,0,-1);
    print $ret."\n";
    return $ret;
}

sub calcCheckDigit
{
	my $seed =@_[1];
	$seed = reverse($seed);
	my @chars = split("", $seed);
	my $checkDigit = 0;
	for my $i (0.. $#chars)
	{
		$checkDigit += @chars[$i] * ($i+2);
	}
	$checkDigit =$checkDigit%11;
	if($checkDigit>9)
	{
		$checkDigit='x';
	}
	return $checkDigit;
}

sub getDBconnects
{
	my $openilsfile = @_[0];
	my $xml = new XML::Simple;
	my $data = $xml->XMLin($openilsfile);
	my %conf;
	$conf{"dbhost"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{host};
	$conf{"db"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{db};
	$conf{"dbuser"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{user};
	$conf{"dbpass"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{pw};
	$conf{"port"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{port};
	##print Dumper(\%conf);
	return \%conf;

}

exit;