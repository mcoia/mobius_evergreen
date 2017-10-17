-- a script to set users to inactive per ME Scenic policy
-- designed to run nightly via cron

begin;
update actor.usr au
set
        active = false,
        alert_message = 'automatically set to inactive status via Scenic policy ' || alert_message,
        last_update_time = now()
-- no unfinished circulations and no circulations within the last 3 years
where not exists (
        select 1
                from action.circulation ac
                where ac.usr = au.id
                and (
                        xact_finish is null or (
                                now() - ac.xact_start < '3 years'::interval
                        )
                )
        )
-- no hold requests placed in the last 3 years
and not exists (
        select 1
                from action.hold_request ahr
                where ahr.usr = au.id
                and (now() - request_time) < '3 years'::interval
        )
-- no owed money in either direction and no payment within the last 3 years
and not exists (
        select 1
                from money.materialized_billable_xact_summary mmbxs
                where mmbxs.usr = au.id
                and (
                        balance_owed <> '0.00' or (now() - last_payment_ts) < '3 years'::interval)
        )
-- no activity entries within the last 3 years
and not exists (
        select 1
                from actor.usr_activity aua
                where aua.usr = au.id
                and (now() - event_time) < '3 years'::interval
        )
-- we only care about active users
and au.active
-- we don't care about deleted users
and not au.deleted
-- don't include non-expired users that don't otherwise meet the "inactive" criteria
and expire_date < now()
-- we don't want users that have been created within the last 3 years
and (now() - au.create_date) > '3 years'
-- restrict to patron profiles ('Patrons' = 2)
and profile in (
        select id
                from permission.grp_descendants(2)
        )
-- and patron is within the scenic regional library set of branches
and home_ou in (
        select id
            from actor.org_unit where lower(name)~'scenic'
        )
;
commit;
