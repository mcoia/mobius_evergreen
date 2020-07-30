
INSERT INTO mymig.monograph_part_conversion_job(status,current_action)
VALUES ('starting','Starting up...');


-- Record the map with the affected copies
select mymig.monograph_part_update_current_job('Create the final map with bre.record and asset.copy.id');
INSERT INTO mymig.monograph_part_conversion_map
(copy,record,acpm,original_label,new_label,job)
SELECT
acpm.target_copy,
bmp.record,
acpm.id,
bmp.label,
mmpc.original_label,
mymig.monograph_part_get_current_job()
FROM
biblio.monograph_part bmp
JOIN asset.copy_part_map acpm ON (bmp.id = acpm.part)
JOIN mymig.monograph_part_conversion_map mmpcm ON (mmpcm.new_label = bmp.label AND acpm.part = bmp.id and mmpcm.copy = acpm.target_copy),
mymig.monograph_part_conversion mmpc
WHERE
NOT bmp.deleted AND
mmpcm.job = 1 and
mmpc.res_query='Vol X,(for rows starting with {digits}th)' and
mmpc.original_label = mmpcm.original_label;



ROLLBACK;
-- now we have all the stuff we need to start making the conversion in production
BEGIN;

-- Create new monograph labels that don't exist yet
select mymig.monograph_part_update_current_job('Inserting biblio.monograph_part new part labels that do not already exist');
INSERT INTO biblio.monograph_part(record,label)
SELECT
DISTINCT mmpcm.record,mmpcm.new_label
FROM
mymig.monograph_part_conversion_map mmpcm
LEFT JOIN biblio.monograph_part bmp ON (bmp.label = mmpcm.new_label AND bmp.record = mmpcm.record )
WHERE
mmpcm.job = mymig.monograph_part_get_current_job() AND
bmp is null;

-- assign the copies to the new part where appropriate
select mymig.monograph_part_update_current_job('Updating copies Inserting biblio.monograph_part new part labels that do not already exist');
UPDATE asset.copy_part_map acpm
SET
part = bmp.id
FROM
biblio.monograph_part bmp,
mymig.monograph_part_conversion_map mmpcm
WHERE
mmpcm.job = mymig.monograph_part_get_current_job() AND
mmpcm.acpm = acpm.id AND
mmpcm.copy = acpm.target_copy AND
mmpcm.record = bmp.record AND
mmpcm.new_label = bmp.label;

-- assign the copies to the new part where appropriate
select mymig.monograph_part_update_current_job('Updating monograph parts deleted=false for the matches');
UPDATE biblio.monograph_part bmp
SET
deleted = false
FROM
asset.copy_part_map acpm,
mymig.monograph_part_conversion_map mmpcm
WHERE
mmpcm.job = mymig.monograph_part_get_current_job() AND
mmpcm.acpm = acpm.id AND
mmpcm.copy = acpm.target_copy AND
mmpcm.record = bmp.record AND
mmpcm.new_label = bmp.label;


-- delete unused labels when we can
select mymig.monograph_part_update_current_job('Deleting old/unused bmp labels where we can');
UPDATE
biblio.monograph_part bmp_outter
SET
deleted = TRUE
FROM
biblio.monograph_part bmp
JOIN mymig.monograph_part_conversion_map mmpcm ON ( bmp.record=mmpcm.record AND mmpcm.original_label = bmp.label AND mmpcm.job = mymig.monograph_part_get_current_job() )
LEFT JOIN asset.copy_part_map acpm ON ( acpm.part=bmp.id )
WHERE
bmp_outter.id=bmp.id AND
acpm.id IS NULL;

select mymig.monograph_part_update_current_job('Committing transaction');

COMMIT;

select mymig.monograph_part_update_current_job('Done',true);
-- 
