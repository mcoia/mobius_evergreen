DO $proc$
DECLARE
    auditor_table TEXT;
    delete_count INT:=0;
    total_count INT:=0;
    history_retention TEXT:='6 months';
BEGIN

FOR auditor_table IN

    SELECT 'auditor.'||table_name FROM information_schema.tables 
    WHERE table_schema = 'auditor' and table_type='BASE TABLE'
LOOP

    -- get total
    EXECUTE 'SELECT count(*) AS total_count FROM ' ||  auditor_table || ' as a' INTO total_count;
    -- get total deletions
    EXECUTE 'SELECT count(*) AS delete_count FROM ' ||  auditor_table || ' as a WHERE audit_time < now() - ''' || history_retention || '''::INTERVAL' INTO delete_count;

    RAISE NOTICE 'DELETING % / % Rows from % ',delete_count, total_count, auditor_table;
    
    EXECUTE 'DELETE FROM '|| auditor_table ||' WHERE audit_time < now() - ''' || history_retention || '''::INTERVAL';
    
    -- EXECUTE 'VACUUM FULL ANALYZE '|| auditor_table ||';';
END LOOP;
END
$proc$