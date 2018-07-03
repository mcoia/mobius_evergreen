\connect nc

BEGIN;

ALTER TABLE asset.copy DISABLE TRIGGER audit_asset_copy_update_trigger;

-- Copy the events for the last 14 months
CREATE TEMP TABLE temp_aach ON COMMIT DROP AS
SELECT *
FROM auditor.asset_copy_history
WHERE audit_time > now() - '14 months'::interval;


-- Truncate the tables.
TRUNCATE TABLE  auditor.asset_copy_history;

-- Copy the output back to the event_output table.
INSERT INTO auditor.asset_copy_history
SELECT *
FROM temp_aach;


COMMIT;

ALTER TABLE asset.copy ENABLE TRIGGER audit_asset_copy_update_trigger;