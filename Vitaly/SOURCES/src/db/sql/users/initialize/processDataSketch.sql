CREATE OR REPLACE PROCEDURE processdatasketch(app_id text)  
LANGUAGE plpgsql  
AS  
$$  
DECLARE query text; sketch_tbl varchar; activeuser_tbl varchar; stat_tbl varchar; tbl_name varchar; row RECORD;
BEGIN 
tbl_name := 'events_' || app_id;
sketch_tbl := 'datasketches_' || tbl_name;
activeuser_tbl := 'datasketches_dailyactiveusers_' || app_id;
stat_tbl := 'datasketches_segmentstats_' || app_id;
query := 'SELECT key, dt, did, segment, p FROM ' || tbl_name;
EXECUTE format('CREATE TABLE IF NOT EXISTS %s(eventkey varchar, dt date, usercount theta_sketch, skey text, sval text, p smallint)', sketch_tbl);
EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS  keydt_seg_p_unique ON %s (eventkey, dt, skey, sval, p) ', sketch_tbl);
EXECUTE format('CREATE TABLE IF NOT EXISTS %s(dt date, usercount theta_sketch, p smallint)', activeuser_tbl);
EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS  dt_unique ON %s (dt)', activeuser_tbl);
EXECUTE format('CREATE TABLE IF NOT EXISTS %s(eventkey varchar, skey text, sval theta_sketch)', stat_tbl);
EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS  skey_dt_unique ON %s (skey, dt)', stat_tbl);
FOR row IN EXECUTE query
LOOP
EXECUTE upsertdatasketchsegment(row.key, CAST(row.dt AS text), row.did, row.segment, row.p, sketch_tbl, activeuser_tbl, stat_tbl);
END LOOP;
END  
$$;