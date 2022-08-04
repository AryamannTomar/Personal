/**
  * DATASKETCHES_EVENTS_APPID TABLE           - EventKey, Date, Usercount(for counting DID's in form of theta_sketches), Segment_Key, Segment_Value, Platform  
  * DATASKETCHES_DAILYACTIVEUSERS_APPID TABLE - Date, Usercount(for counting DID's in form of theta_sketches), Platform 
  * DATASKETCHES_SEGMENTSTATS_APPID TABLE     - Date, EventKey, Segment_Key, Segment_Value, Platform
*/

CREATE TABLE IF NOT EXISTS datasketches_events_${appid:value}(eventkey varchar, dt date, usercount theta_sketch, skey text, sval text, p smallint);
CREATE UNIQUE INDEX IF NOT EXISTS keydt_seg_p_unique_${appid:value} ON datasketches_events_${appid:value} (eventkey, dt, skey, sval, p);
CREATE TABLE IF NOT EXISTS datasketches_dailyactiveusers_${appid:value}(dt date, usercount theta_sketch, p smallint);
CREATE UNIQUE INDEX IF NOT EXISTS dt_unique_${appid:value} ON datasketches_dailyactiveusers_${appid:value} (dt);
CREATE TABLE IF NOT EXISTS datasketches_segmentstats_${appid:value}(dt date, eventkey varchar, skey text, sval theta_sketch);
CREATE UNIQUE INDEX IF NOT EXISTS skey_dt_unique_${appid:value} ON datasketches_segmentstats_${appid:value} (eventkey, dt);
