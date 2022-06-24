/*
   Create Table-1 with eventkey, dt, usercount, skey, sval, platform columns
*/
CREATE TABLE IF NOT EXISTS datasketches_events_${app_id}(eventkey varchar, dt date, usercount theta_sketch, skey text, sval text, p smallint)
CREATE UNIQUE INDEX IF NOT EXISTS  keydt_seg_p_unique ON datasketches_events_${app_id} (eventkey, dt, skey, sval, p) 
SELECT create_distributed_table(''datasketches_events_${app_id}'', 'eventkey')

/*
   Create Table-2 with dt, usercount, platform columns
*/
CREATE TABLE IF NOT EXISTS datasketches_dailyactiveusers_${app_id}(dt date, usercount theta_sketch, p smallint)
CREATE UNIQUE INDEX IF NOT EXISTS  dt_unique ON datasketches_dailyactiveusers_${app_id} (dt)
SELECT create_distributed_table(''datasketches_dailyactiveusers_${app_id}'', 'dt')

/*
   Create Table-3 with eventkey, skey, sval columns so that we handle only the categorical attributes of segment for the datasketches_events table 
*/
CREATE TABLE IF NOT EXISTS datasketches_segmentstats_${app_id}(eventkey varchar, skey text, sval theta_sketch)
CREATE UNIQUE INDEX IF NOT EXISTS  skey_dt_unique ON datasketches_segmentstats_${app_id} (skey, dt)
SELECT create_distributed_table(''datasketches_segmentstats_${app_id}'', 'eventkey')