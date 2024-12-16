
CREATE TABLE IF NOT EXISTS mymig.manual_bib_merge_job
(
id bigserial NOT NULL,
start_time timestamp with time zone NOT NULL DEFAULT now(),
last_update_time timestamp with time zone NOT NULL DEFAULT now(),
status text default 'processing',    
current_action text,
current_action_num bigint default 0,
CONSTRAINT manual_bib_merge_job_pkey PRIMARY KEY (id)
);

CREATE OR REPLACE FUNCTION mymig.manual_bib_merge_get_current_job() RETURNS BIGINT AS
$func$
DECLARE

 job bigint;
 
BEGIN
    SELECT INTO job MAX(id) FROM mymig.manual_bib_merge_job;
RETURN job;
END;
$func$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION mymig.manual_bib_merge_update_current_job(step_text TEXT, finished boolean = FALSE) RETURNS TEXT AS
$func$
DECLARE
 status_text TEXT := 'processing';
 cjob BIGINT;
BEGIN

SELECT INTO cjob mymig.manual_bib_merge_get_current_job();

    IF finished THEN
    status_text = 'complete';
    END IF;

    UPDATE mymig.manual_bib_merge_job
    SET
    last_update_time = now(),
    status = status_text,
    current_action = step_text,
    current_action_num = current_action_num + 1
    WHERE
    id = cjob;
RETURN step_text;
END;
$func$ LANGUAGE plpgsql;

CREATE TABLE IF NOT EXISTS mymig.manual_bib_merge
(
id bigserial NOT NULL,
lead bigint,
sub bigint,
insert_date TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
done BOOLEAN DEFAULT FALSE,
failed BOOLEAN,
complete_time timestamp,
job bigint,
error_message TEXT,
CONSTRAINT mymig_manual_bib_merge_job FOREIGN KEY (job) REFERENCES mymig.manual_bib_merge_job (id) MATCH SIMPLE
);


CREATE OR REPLACE FUNCTION mymig.manual_bib_merge_run_job(chunksize bigint DEFAULT 100) RETURNS TEXT AS
$func$
DECLARE
 this_id BIGINT;
 cjob BIGINT;
 total_outstanding BIGINT;
 processed_total BIGINT;
 return_string TEXT;
 success_count INT := 0;
 fail_count INT := 0;
 worked BOOLEAN;
BEGIN


-- Eliminate pairs that merge onto deleted bibs
UPDATE mymig.manual_bib_merge mmbm
SET
done = 't',
complete_time = now(),
failed = 't',
error_message = 'lead is deleted'
FROM
biblio.record_entry bre
WHERE
bre.id=mmbm.lead AND
bre.deleted AND
NOT done AND
job IS NULL AND
complete_time IS NULL;

-- Eliminate pairs that merge already deleted bibs
UPDATE mymig.manual_bib_merge mmbm
SET
done = 't',
complete_time = now(),
failed = 't',
error_message = 'sub is already deleted'
FROM
biblio.record_entry bre
WHERE
bre.id=mmbm.sub AND
bre.deleted AND
NOT done AND
job IS NULL AND
complete_time IS NULL;

-- First, see if there is anything to process, if not, we don't make a new job
SELECT count(distinct mmbm_lead.sub)
INTO total_outstanding
FROM
mymig.manual_bib_merge mmbm_lead
LEFT JOIN mymig.manual_bib_merge mmbm_sub ON(NOT mmbm_sub.done AND mmbm_sub.job IS NULL AND mmbm_sub.complete_time IS NULL AND mmbm_sub.sub=mmbm_lead.lead)
WHERE
mmbm_sub.lead IS NULL AND
NOT mmbm_lead.done AND
mmbm_lead.job IS NULL AND
mmbm_lead.complete_time IS NULL;


IF total_outstanding > 0 THEN

    -- Create job
    INSERT INTO mymig.manual_bib_merge_job(status,current_action)
    VALUES ('starting','Starting up...');

    -- Get the assigned job id
    SELECT INTO cjob mymig.manual_bib_merge_get_current_job();

    -- Assign the job number to this batch
    UPDATE mymig.manual_bib_merge
    SET
    job = cjob
    WHERE
    id IN
    (
        SELECT outside_lead.id
        FROM
        mymig.manual_bib_merge outside_lead
        WHERE
        outside_lead.id IN
        (
           SELECT id FROM
           (
                SELECT min(mmbm_lead.id) "id", mmbm_lead.sub
                FROM
                mymig.manual_bib_merge mmbm_lead
                LEFT JOIN mymig.manual_bib_merge mmbm_sub ON(NOT mmbm_sub.done AND mmbm_sub.job IS NULL AND mmbm_sub.complete_time IS NULL AND mmbm_sub.sub=mmbm_lead.lead)
                WHERE
                mmbm_sub.lead IS NULL AND
                NOT mmbm_lead.done AND
                mmbm_lead.job IS NULL AND
                mmbm_lead.complete_time IS NULL
                GROUP BY 2
            ) AS dedupe
        ) AND
        NOT outside_lead.done AND
        outside_lead.job IS NULL AND
        outside_lead.complete_time IS NULL
        ORDER BY outside_lead.id
        LIMIT chunksize
    );

    -- How many are we going to process?
    SELECT count(*) INTO processed_total FROM mymig.manual_bib_merge WHERE NOT done AND job = cjob AND complete_time IS NULL;

    -- Record the job status before we get looping
    PERFORM mymig.manual_bib_merge_update_current_job('targeted '|| processed_total || ' merges');

    -- Loop over each merge
    FOR this_id IN SELECT DISTINCT(id)
        FROM mymig.manual_bib_merge WHERE NOT done AND job = cjob AND complete_time IS NULL ORDER BY id
        LOOP

            -- Let the job know what ID we're on
            PERFORM mymig.manual_bib_merge_update_current_job('processing id: '|| this_id);

            -- Do the actual merge
            PERFORM asset.merge_record_assets(lead, sub) FROM mymig.manual_bib_merge WHERE id = this_id;

            -- See if the sub record is now deleted
            SELECT INTO worked NOT EXISTS(SELECT bre.id
                FROM biblio.record_entry bre
                JOIN mymig.manual_bib_merge mmbm on(mmbm.sub=bre.id AND NOT bre.deleted AND mmbm.id = this_id)
                );

            IF worked
            THEN
                success_count := success_count + 1;
                UPDATE mymig.manual_bib_merge
                SET
                done='t',
                failed='f',
                complete_time = now()
                WHERE
                id = this_id;
            ELSE
                fail_count := fail_count + 1;
                UPDATE mymig.manual_bib_merge
                SET
                done='t',
                failed='t',
                complete_time = now()
                WHERE
                id = this_id;
            END IF;
                
        END LOOP;

    PERFORM mymig.manual_bib_merge_update_current_job('Done '||success_count ||' / '||processed_total,true);
    return_string := 'Total: '||processed_total||' Success: '||success_count||' Failed: '||fail_count;

ELSE
    return_string := 'Nothing to process';
    -- If there are still unfinished rows, then we have an infinite loop, where sub->lead->sub->lead
    UPDATE
    mymig.manual_bib_merge mmbm_lead2
    SET
    done = 't',
    complete_time = now(),
    failed = 't',
    error_message = 'infinite merge loop detected'
    FROM
    mymig.manual_bib_merge mmbm_lead
    LEFT JOIN mymig.manual_bib_merge mmbm_sub ON(NOT mmbm_sub.done AND mmbm_sub.job IS NULL AND mmbm_sub.complete_time IS NULL AND mmbm_sub.sub=mmbm_lead.lead) 
    WHERE
    mmbm_lead.id=mmbm_lead2.id AND
    mmbm_sub.lead IS NOT NULL AND
    NOT mmbm_lead.done AND
    mmbm_lead.job IS NULL AND
    mmbm_lead.complete_time IS NULL;

END IF;

RETURN return_string;

END;
$func$ LANGUAGE plpgsql;


GRANT ALL ON ALL tables IN SCHEMA mymig TO readonlyuser;
GRANT ALL ON mymig.manual_bib_merge_id_seq TO readonlyuser;
