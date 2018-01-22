SELECT pg_cancel_backend(pid) from pg_stat_activity 
WHERE (now() - pg_stat_activity.query_start) > interval '5 minutes'
and state='active'
and query~'^SELECT  "mp".amount, "mp".id, "mp".note,';