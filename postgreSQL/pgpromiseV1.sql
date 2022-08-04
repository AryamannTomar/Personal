const pgp = require('pg-promise')({});
const cn = 'postgres://postgres:Password@123@localhost:5432/postgres';
const db = pgp(cn);
module.exports = db;
db.connect().then(() => {
                   console.log('Connection established successfully.');
           }).catch(err => {
                   console.error('Unable to connect to the database:', err);
           });

data =[{ appid: '5fa41afe6fc1e678a8344311',
            did: '832a275b-6f7d-4c81-8c2f-3bf8967a75f9',
            key: 'Login',
            sid: 'a874041b-435f-4c0f-a06f-fad04d87d47e',
            p: 'Android',
            rtime: 1629522744,
            utime: 1629522744,
            segment:
             { UCIC: '2211773100000011998',
                            LoginMethod: 'Password',
                            LoginStatus: 'True',
                            PaymentDueDate: '2021-08-11' },
            context: { who: [Object], what: {}, when: [Object], where: [Object] },
            eventtime: 1631855409,
            dt: '2021-09-17',
            createat: '2021-08-21' },

          { appid: '5fa41afe6fc1e678a8344311',
             did: '832a275b-6f7d-4c81-8c2f-3bf8967a75f9',
             key: 'Session_Start',
             sid: 'a874041b-435f-4c0f-a06f-fad04d87d47e',
             p: 'Android',
             rtime: 1629522744,
             utime: 1629522744,
             segment: {},
             context: { who: [Object], what: {}, when: [Object], where: [Object] },
             eventtime: 1631855389,
             dt: '2021-09-17',
             createat: '2021-08-21' }];

db.task(t => {
	var obj = data
	var keys = Object.keys(obj);
      for(var i=0;i<keys.length;i++){
	   const tbl1_name = 'datasketches_events_' + data[i].appid;
         const tbl2_name = 'datasketches_dailyactiveusers_' + data[i].appid;
         const tbl3_name = 'datasketches_segmentstats_' + data[i].appid;
         const arr = [obj[i].key, obj[i].dt, obj[i].did, obj[i].segment, obj[i].p, tbl1_name, tbl2_name, tbl3_name];
	   t.batch([
            db.func('upsertdatasketchsegment', arr)
         ]).Add} return t.batch; 
       }).then(data => {
          console.log("Successful");
       })
       .catch(err => {
         console.log(err);
	 });

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE datasketches_events_5fa41afe6fc1e678a8344311(eventkey varchar, dt date, usercount theta_sketch, skey text, sval text, p int);
ALTER TABLE datasketches_events_5fa41afe6fc1e678a8344311 ADD constraint keydt_seg_unique UNIQUE(eventkey, dt, skey, sval);

CREATE TABLE datasketches_dailyactiveusers_5fa41afe6fc1e678a8344311(dt date, usercount theta_sketch, p int);
ALTER TABLE datasketches_dailyactiveusers_5fa41afe6fc1e678a8344311 ADD constraint dt_unique UNIQUE(dt);

CREATE TABLE datasketches_segmentstats_5fa41afe6fc1e678a8344311(skey text, sval theta_sketch);

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE processDataSketch(app_id text)  
LANGUAGE plpgsql  
AS  
$$  
DECLARE query text; sketch_tbl varchar; activeuser_tbl varchar; stat_tbl varchar; tbl_name varchar; row RECORD;
BEGIN 
tbl_name := 'events_' || app_id;
sketch_tbl := 'datasketches_' || tbl_name;
activeuser_tbl := 'dailyactiveusers_' || tbl_name;
stat_tbl := 'segmentstats_' || tbl_name;
query := 'SELECT key, dt, did, segment, p FROM ' || tbl_name;
EXECUTE format('CREATE TABLE %s(eventkey varchar, dt date, usercount theta_sketch, skey text, sval text, p int)', sketch_tbl);
EXECUTE format('ALTER TABLE %s ADD constraint keydt_seg_unique UNIQUE(eventkey, dt, skey, sval)', sketch_tbl);
EXECUTE format('CREATE TABLE %s(dt date, usercount theta_sketch, p int)', activeuser_tbl);
EXECUTE format('ALTER TABLE %s ADD constraint dt_unique UNIQUE(dt)', activeuser_tbl);
EXECUTE format('CREATE TABLE %s(skey text, sval theta_sketch)', stat_tbl);   
FOR row IN EXECUTE query
LOOP
EXECUTE upsertdatasketchsegment(row.key, CAST(row.dt AS text), row.did, row.segment, row.p, sketch_tbl, activeuser_tbl, stat_tbl);
END LOOP;
END  
$$;

create function upsertdatasketchsegment(key_input varchar, dt_input text, did_input text, segment jsonb, p_input varchar, sketch_tbl varchar, activeuser_tbl varchar, stat_tbl varchar)
returns void
language plpgsql
as
$$
DECLARE row RECORD; p int; query text; c int;
BEGIN
    IF (p_input = 'Android') THEN
	p = 0;
    ELSEIF (p_input = 'IOS') THEN
	p = 1;
    ELSE
	p = 2;
    END IF;
    
   EXECUTE format('INSERT INTO %s(dt, usercount, p) select ''%s'', theta_sketch_build(''%s''::bytea), ''%s'' 
    ON CONFLICT(dt)
    DO
      UPDATE SET usercount = theta_sketch_union(%s.usercount, (select theta_sketch_build((select ''%s''::bytea)))) where %s.dt = ''%s'';', activeuser_tbl, dt_input, did_input, p, activeuser_tbl, did_input, activeuser_tbl, dt_input);     
     
    FOR row IN (select d.key a, d.value b from jsonb_each_text(segment) d)                          
      LOOP
        query := 'select theta_sketch_distinct(sval) from ' || stat_tbl || ' where skey = ''' || row.a || ''';';
        FOR c IN EXECUTE query
         LOOP
          IF (c<100) THEN
           IF (length(segment::text) = 2) THEN 
	      EXECUTE upsertdatasketchrow(key_input, dt_input, did_input, 'EMPTY', 'EMPTY', p, sketch_tbl);
            EXECUTE format('INSERT INTO %s(skey, sval) select ''EMPTY'', ''EMPTY'' ', stat_tbl);
           ELSEIF (length(segment::text) > 2) THEN
            EXECUTE upsertdatasketchrow(key_input, dt_input, did_input, row.a, row.b, p, sketch_tbl);
            EXECUTE format('INSERT INTO %s(skey, sval) select ''%s'', ''%s'' ', stat_tbl, row.a, row.b);
           END IF; 
          END IF;
         END LOOP;
      END LOOP;
END;
$$;

create function upsertdatasketchrow(key_input varchar, dt_input text, did_input text, skey_input text, sval_input text, p_input int, sketch_tbl varchar)
returns void
language plpgsql
as
$$
BEGIN 
    EXECUTE format('INSERT INTO %s(eventkey, dt, usercount, skey, sval, p) select ''%s'', ''%s'', theta_sketch_build(''%s''::bytea), ''%s'', ''%s'', ''%s'' 
ON CONFLICT(eventkey, dt, skey, sval)
DO
   UPDATE SET usercount = theta_sketch_union(%s.usercount, (select theta_sketch_build((select ''%s''::bytea)))) where %s.eventkey = ''%s'' AND %s.dt = ''%s'' ', sketch_tbl, key_input, dt_input, did_input, skey_input, sval_input, p_input, sketch_tbl, did_input, sketch_tbl, key_input, sketch_tbl, dt_input); 
    END;
$$;