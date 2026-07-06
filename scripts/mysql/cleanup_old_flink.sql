-- Remove legacy Flink 1.14/1.16 seed data from an existing MySQL database.
-- This is only needed for databases that were initialized before the Flink 2.1-only cleanup.
-- It keeps existing Flink21 / flink2.1.x records intact.
--
-- Recommended usage:
--   1. Back up the lineage database.
--   2. Run this script once.
--   3. Verify bas_plugin has one default flink2.1.x plugin and bas_catalog has one default Flink21_memory catalog.

START TRANSACTION;

CREATE TEMPORARY TABLE tmp_old_plugins AS
SELECT plugin_id
FROM lineage.bas_plugin
WHERE plugin_code IN ('flink1.14.x', 'flink1.16.x')
   OR plugin_name IN ('Flink14', 'Flink16');

CREATE TEMPORARY TABLE tmp_old_catalogs AS
SELECT catalog_id
FROM lineage.bas_catalog
WHERE plugin_id IN (SELECT plugin_id FROM tmp_old_plugins)
   OR catalog_name IN ('Flink14_memory', 'Flink16_memory', 'Flink16_Memory');

CREATE TEMPORARY TABLE tmp_old_tasks AS
SELECT task_id
FROM lineage.bas_task
WHERE catalog_id IN (SELECT catalog_id FROM tmp_old_catalogs);

CREATE TEMPORARY TABLE tmp_old_functions AS
SELECT function_id
FROM lineage.bas_function
WHERE catalog_id IN (SELECT catalog_id FROM tmp_old_catalogs);

UPDATE lineage.rel_task_lineage
SET invalid = 1
WHERE task_id IN (SELECT task_id FROM tmp_old_tasks);

UPDATE lineage.rel_task_sql
SET invalid = 1
WHERE task_id IN (SELECT task_id FROM tmp_old_tasks);

UPDATE lineage.rel_task_function
SET invalid = 1
WHERE task_id IN (SELECT task_id FROM tmp_old_tasks)
   OR function_id IN (SELECT function_id FROM tmp_old_functions);

UPDATE lineage.bas_task
SET invalid = 1
WHERE task_id IN (SELECT task_id FROM tmp_old_tasks);

UPDATE lineage.bas_table
SET invalid = 1
WHERE catalog_id IN (SELECT catalog_id FROM tmp_old_catalogs);

UPDATE lineage.bas_function
SET invalid = 1
WHERE function_id IN (SELECT function_id FROM tmp_old_functions);

UPDATE lineage.bas_catalog
SET invalid = 1
WHERE catalog_id IN (SELECT catalog_id FROM tmp_old_catalogs);

UPDATE lineage.bas_plugin
SET invalid = 1
WHERE plugin_id IN (SELECT plugin_id FROM tmp_old_plugins);

DELETE FROM lineage.rel_task_lineage
WHERE task_id IN (SELECT task_id FROM tmp_old_tasks);

DELETE FROM lineage.rel_task_function
WHERE task_id IN (SELECT task_id FROM tmp_old_tasks)
   OR function_id IN (SELECT function_id FROM tmp_old_functions);

DELETE FROM lineage.rel_task_sql
WHERE task_id IN (SELECT task_id FROM tmp_old_tasks);

DELETE FROM lineage.bas_task
WHERE task_id IN (SELECT task_id FROM tmp_old_tasks);

DELETE FROM lineage.bas_table
WHERE catalog_id IN (SELECT catalog_id FROM tmp_old_catalogs);

DELETE FROM lineage.bas_function
WHERE function_id IN (SELECT function_id FROM tmp_old_functions);

DELETE FROM lineage.bas_catalog
WHERE catalog_id IN (SELECT catalog_id FROM tmp_old_catalogs);

DELETE FROM lineage.bas_plugin
WHERE plugin_id IN (SELECT plugin_id FROM tmp_old_plugins);

UPDATE lineage.bas_plugin
SET default_plugin = CASE WHEN plugin_code = 'flink2.1.x' THEN 1 ELSE 0 END
WHERE invalid = 0;

UPDATE lineage.bas_catalog
SET default_catalog = CASE WHEN catalog_name = 'Flink21_memory' THEN 1 ELSE 0 END
WHERE invalid = 0;

DROP TEMPORARY TABLE IF EXISTS tmp_old_functions;
DROP TEMPORARY TABLE IF EXISTS tmp_old_tasks;
DROP TEMPORARY TABLE IF EXISTS tmp_old_catalogs;
DROP TEMPORARY TABLE IF EXISTS tmp_old_plugins;

COMMIT;
