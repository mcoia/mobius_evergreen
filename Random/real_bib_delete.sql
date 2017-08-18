/*
 * real_bib_delete.sql
 * Copyright (c) 2014 Bibliomation, Inc.
 * Copyright (c) 2014 Jason Stephenson <jason@sigio.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */

-- Disable rules and triggers that prevent actual record deletion.
ALTER TABLE biblio.record_entry DISABLE RULE protect_bib_rec_delete;
ALTER TABLE biblio.record_entry DISABLE TRIGGER audit_biblio_record_entry_update_trigger;
ALTER TABLE biblio.record_entry DISABLE TRIGGER bbb_simple_rec_trigger;
ALTER TABLE serial.record_entry DISABLE RULE protect_mfhd_delete;
ALTER TABLE asset.call_number DISABLE RULE protect_cn_delete;
ALTER TABLE action.hold_request DISABLE TRIGGER action_hold_request_aging_tgr;

-- Do the work in a DO procedure.

DO $proc$
DECLARE
    bre_id BIGINT;
BEGIN

    SET LOCAL synchronous_commit TO OFF;

    FOR bre_id IN 
        SELECT bre.id
        FROM biblio.record_entry bre
        LEFT JOIN asset.call_number acn
        ON acn.record = bre.id
        LEFT JOIN asset.copy acp
        ON acp.call_number = acn.id
        LEFT JOIN asset.uri_call_number_map aucnm
        ON aucnm.call_number = acn.id
        LEFT JOIN serial.unit su
        ON su.call_number = acn.id
        LEFT JOIN acq.lineitem ali
        ON ali.eg_bib_id = bre.id
        LEFT JOIN serial.distribution sd
        ON sd.record_entry = bre.id
        LEFT JOIN biblio.monograph_part mp
        ON mp.record = bre.id
        LEFT JOIN vandelay.bib_match bm
        ON bm.eg_record = bre.id
        LEFT JOIN vandelay.queued_bib_record qr
        ON qr.imported_as = bre.id
        WHERE bre.deleted = 't'
        and bre.edit_date < now()-'2 years'::interval
        and bre.id > 0
        GROUP BY bre.id
        HAVING COUNT(acp.id) = 0
        AND COUNT(aucnm.id) = 0
        AND COUNT(su.id) = 0
        AND COUNT(ali.id) = 0
        AND COUNT(sd.id) = 0
        AND COUNT(mp.id) = 0
        AND COUNT(bm.id) = 0
        AND COUNT(qr.id) = 0
        limit 100
    LOOP

        DELETE FROM action.aged_hold_request
        WHERE target = bre_id
        AND hold_type = 'T';

        DELETE FROM action.hold_request
        WHERE target = bre_id
        AND hold_type = 'T';

        DELETE FROM asset.call_number
        WHERE record = bre_id;

        DELETE FROM authority.bib_linking
        WHERE bib = bre_id;

        DELETE FROM booking.resource_type
        WHERE record = bre_id;

        DELETE FROM metabib.author_field_entry
        WHERE source = bre_id;

        DELETE FROM metabib.identifier_field_entry
        WHERE source = bre_id;

        DELETE FROM metabib.keyword_field_entry
        WHERE source = bre_id;

        DELETE FROM metabib.series_field_entry
        WHERE source = bre_id;

        DELETE FROM metabib.subject_field_entry
        WHERE source = bre_id;

        DELETE FROM metabib.title_field_entry
        WHERE source = bre_id;

        DELETE FROM metabib.browse_entry_def_map
        WHERE source = bre_id;

        DELETE FROM metabib.metarecord
        WHERE master_record = bre_id;

        DELETE FROM metabib.real_full_rec
        WHERE record = bre_id;

        IF EXISTS(SELECT * FROM information_schema.tables
                  WHERE table_schema = 'metabib'
                  AND table_name = 'record_attr_vector_list')
        THEN
            DELETE FROM metabib.record_attr_vector_list
            WHERE source = bre_id;
        ELSE
            DELETE FROM metabib.record_attr
            WHERE id = bre_id;
        END IF;

        DELETE FROM serial.record_entry
        WHERE record = bre_id;

        DELETE FROM serial.subscription
        WHERE record_entry = bre_id;

        DELETE FROM container.biblio_record_entry_bucket_item
        WHERE target_biblio_record_entry = bre_id;

        DELETE FROM biblio.record_note
        WHERE record = bre_id;

        DELETE FROM biblio.record_entry
        WHERE id = bre_id;

    END LOOP;

END
$proc$;

-- Vaccuum the affected tables.
VACUUM FULL ANALYZE action.aged_hold_request;
VACUUM FULL ANALYZE action.hold_request;
VACUUM FULL ANALYZE action.hold_copy_map;
VACUUM FULL ANALYZE action.hold_notification;
VACUUM FULL ANALYZE action.hold_request_note;
VACUUM FULL ANALYZE action.hold_transit_copy;
VACUUM FULL ANALYZE asset.call_number;
VACUUM FULL ANALYZE authority.bib_linking;
VACUUM FULL ANALYZE biblio.record_note;
VACUUM FULL ANALYZE biblio.record_entry;
VACUUM FULL ANALYZE booking.resource_type;
VACUUM FULL ANALYZE booking.reservation;
VACUUM FULL ANALYZE booking.resource_attr;
VACUUM FULL ANALYZE booking.resource;
VACUUM FULL ANALYZE container.biblio_record_entry_bucket_item;
VACUUM FULL ANALYZE container.biblio_record_entry_bucket_item_note;
VACUUM FULL ANALYZE metabib.browse_entry_def_map;
VACUUM FULL ANALYZE metabib.author_field_entry;
VACUUM FULL ANALYZE metabib.identifier_field_entry;
VACUUM FULL ANALYZE metabib.keyword_field_entry;
VACUUM FULL ANALYZE metabib.metarecord;
VACUUM FULL ANALYZE metabib.metarecord_source_map;
VACUUM FULL ANALYZE metabib.real_full_rec;

DO $$
BEGIN
    IF EXISTS(SELECT * FROM information_schema.tables
                       WHERE table_schema = 'metabib'
                       AND table_name = 'record_attr_vector_list')
    THEN
        VACUUM FULL ANALYZE metabib.record_attr_vector_list;
    ELSE
        VACUUM FULL ANALYZE metabib.record_attr;
    END IF;
END
$$;

VACUUM FULL ANALYZE metabib.series_field_entry;
VACUUM FULL ANALYZE metabib.subject_field_entry;
VACUUM FULL ANALYZE metabib.title_field_entry;
VACUUM FULL ANALYZE reporter.materialized_simple_record;
VACUUM FULL ANALYZE serial.record_entry;
VACUUM FULL ANALYZE serial.subscription;

-- Reinstate the rules and triggers that we disable earlier.
ALTER TABLE biblio.record_entry ENABLE RULE protect_bib_rec_delete;
ALTER TABLE biblio.record_entry ENABLE TRIGGER audit_biblio_record_entry_update_trigger;
ALTER TABLE biblio.record_entry ENABLE TRIGGER bbb_simple_rec_trigger;
ALTER TABLE serial.record_entry ENABLE RULE protect_mfhd_delete;
ALTER TABLE asset.call_number ENABLE RULE protect_cn_delete;
ALTER TABLE action.hold_request ENABLE TRIGGER action_hold_request_aging_tgr;