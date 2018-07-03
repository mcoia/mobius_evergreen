/* ---------------------------------------------------------------
 * Copyright (C) 2018 CW MARS, INC.
 * Jason Stephenson <jstephenson@cwmars.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */ ---------------------------------------------------------------
BEGIN;

-- First, lock the tables to prevent updates.
LOCK TABLE action_trigger.event, action_trigger.event_output IN EXCLUSIVE MODE;

-- Second, drop the constraints that seem to block us on truncate.
ALTER TABLE action_trigger.event DROP CONSTRAINT event_async_output_fkey;
ALTER TABLE action_trigger.event DROP CONSTRAINT event_error_output_fkey;
ALTER TABLE action_trigger.event DROP CONSTRAINT event_template_output_fkey;

-- Copy the events for 2018.
CREATE TEMP TABLE temp_ate ON COMMIT DROP AS
SELECT *
FROM action_trigger.event
WHERE add_time > '2017-12-31 23:59:59';

-- Copy the output of the above events.
CREATE TEMP TABLE temp_ato ON COMMIT DROP AS
SELECT DISTINCT event_output.*
FROM action_trigger.event_output
JOIN temp_ate
ON event_output.id = temp_ate.template_output
UNION DISTINCT
SELECT DISTINCT event_output.*
FROM action_trigger.event_output
JOIN temp_ate
ON event_output.id = temp_ate.error_output
UNION DISTINCT
SELECT DISTINCT event_output.*
FROM action_trigger.event_output
JOIN temp_ate
ON event_output.id = temp_ate.async_output;

-- Truncate the tables.
TRUNCATE TABLE action_trigger.event;
TRUNCATE TABLE action_trigger.event_output;

-- Copy the output back to the event_output table.
INSERT INTO action_trigger.event_output
SELECT *
FROM temp_ato;

-- Copy the events back the event table.
INSERT INTO action_trigger.event
SELECT *
FROM temp_ate;

COMMIT;

-- Finally, re-add the constraints.  This is done outside the
-- transaction because you can't modify a table with pending updates.
ALTER TABLE action_trigger.event ADD CONSTRAINT event_async_output_fkey FOREIGN KEY (async_output)
      REFERENCES action_trigger.event_output (id); --DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE action_trigger.event ADD CONSTRAINT event_error_output_fkey FOREIGN KEY (error_output)
      REFERENCES action_trigger.event_output (id); --DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE action_trigger.event ADD CONSTRAINT event_template_output_fkey FOREIGN KEY (template_output)
      REFERENCES action_trigger.event_output (id); --DEFERRABLE INITIALLY DEFERRED;

-- The DEFERRABLE INITIALLY DEFERRED are commented out above because I
-- think that may be a useful change to make in the future, but I want
-- to see if the standard delete function still works with the normal
-- triggers.