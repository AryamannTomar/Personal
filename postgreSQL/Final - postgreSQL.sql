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
EXECUTE format('CREATE TABLE %s(eventkey varchar NOT NULL, dt date NOT NULL, usercount theta_sketch NOT NULL, skey text, sval text, p smallint NOT NULL)', sketch_tbl);
EXECUTE format('ALTER TABLE %s ADD constraint keydt_seg_unique UNIQUE(eventkey, dt, skey, sval, p)', sketch_tbl);
EXECUTE format('CREATE TABLE %s(dt date NOT NULL, usercount theta_sketch NOT NULL, p smallint NOT NULL)', activeuser_tbl);
EXECUTE format('ALTER TABLE %s ADD constraint dt_unique UNIQUE(dt)', activeuser_tbl);
EXECUTE format('CREATE TABLE %s(dt date NOT NULL, eventkey varchar NOT NULL, skey text, sval theta_sketch)', stat_tbl);   
EXECUTE format('ALTER TABLE %s ADD constraint skey_unique UNIQUE(eventkey, dt)', stat_tbl);
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
DECLARE row1 RECORD; row2 RECORD; p smallint; query text; c int;
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
      UPDATE SET usercount = theta_sketch_union(%s.usercount, (select theta_sketch_build(''%s''::bytea))) where %s.dt = ''%s'';', activeuser_tbl, dt_input, did_input, p, activeuser_tbl, did_input, activeuser_tbl, dt_input);     
      
     IF (length(segment::text) = 2) THEN 
      EXECUTE format('INSERT INTO %s(dt, eventkey, skey, sval) select ''%s'', ''%s'', ''NULL'', theta_sketch_build(''NULL''::bytea) 
ON CONFLICT(eventkey, dt)
DO 
    NOTHING ', stat_tbl, dt_input, key_input); 
    ELSEIF (length(segment::text) > 2) THEN
    	FOR row1 IN (select d.key a, d.value b from jsonb_each_text(segment) d)                          
      LOOP
      EXECUTE format('INSERT INTO %s(dt, eventkey, skey, sval) select ''%s'', ''%s'', ''%s'', theta_sketch_build(''%s''::bytea) 
ON CONFLICT(eventkey, dt)
DO
   UPDATE SET sval = theta_sketch_union(%s.sval, (select theta_sketch_build(''%s''::bytea))) where %s.eventkey = ''%s'' AND %s.skey = ''%s'' ', stat_tbl, dt_input, key_input, row1.a, row1.b, stat_tbl, row1.b, stat_tbl, key_input, stat_tbl, row1.a); 
	END LOOP; 
    END IF;

      IF (length(segment::text) = 2) THEN 
	      EXECUTE upsertdatasketchrow(key_input, dt_input, did_input, 'NULL', 'NULL', p, sketch_tbl);
      ELSEIF (length(segment::text) > 2) THEN
        FOR row2 IN (select d.key a, d.value b from jsonb_each_text(segment) d) 
        LOOP 
         query := 'select theta_sketch_get_estimate(sval) from ' || stat_tbl || ' where skey = ''' || row2.a || ''';';
         FOR c IN EXECUTE query
         LOOP
           IF (c<100) THEN    
            EXECUTE upsertdatasketchrow(key_input, dt_input, did_input, row2.a, row2.b, p, sketch_tbl);
          END IF;
         END LOOP; 
       END LOOP;
      END IF;         
END;
$$;

create function upsertdatasketchrow(key_input varchar, dt_input text, did_input text, skey_input text, sval_input text, p_input smallint, sketch_tbl varchar)
returns void
language plpgsql
as
$$
BEGIN 
    EXECUTE format('INSERT INTO %s(eventkey, dt, usercount, skey, sval, p) select ''%s'', ''%s'', theta_sketch_build(''%s''::bytea), ''%s'', ''%s'', ''%s'' 
ON CONFLICT(eventkey, dt, skey, sval, p)
DO
   UPDATE SET usercount = theta_sketch_union(%s.usercount, (select theta_sketch_build(''%s''::bytea))) where %s.eventkey = ''%s'' AND %s.dt = ''%s'' ', sketch_tbl, key_input, dt_input, did_input, skey_input, sval_input, p_input, sketch_tbl, did_input, sketch_tbl, key_input, sketch_tbl, dt_input); 
    END;
$$;



-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE datasketches_events_5bebe93c25d705690ffbc75811(eventkey varchar, dt date, usercount theta_sketch, skey text, sval text, p smallint);
ALTER TABLE datasketches_events_5bebe93c25d705690ffbc75811 ADD constraint keydt_seg_unique UNIQUE(eventkey, dt, skey, sval, p);

CREATE TABLE datasketches_dailyactiveusers_5bebe93c25d705690ffbc75811(dt date, usercount theta_sketch, p smallint);
ALTER TABLE datasketches_dailyactiveusers_5bebe93c25d705690ffbc75811 ADD constraint dt_unique UNIQUE(dt);

CREATE TABLE datasketches_segmentstats_5bebe93c25d705690ffbc75811(dt date, eventkey varchar, skey text, sval theta_sketch);
ALTER TABLE datasketches_segmentstats_5bebe93c25d705690ffbc75811 ADD constraint skey_unique UNIQUE(eventkey, dt);
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE PROCEDURE processDataSketch(app_id text)  
LANGUAGE plpgsql  
AS  
$$  
DECLARE query text; sketch_tbl varchar; activeuser_tbl varchar; stat_tbl varchar; tbl_name varchar; row1 RECORD; row2 RECORD;
BEGIN 
tbl_name := 'events_' || app_id;
sketch_tbl := 'datasketches_' || tbl_name;
activeuser_tbl := 'dailyactiveusers_' || tbl_name;
stat_tbl := 'segmentstats_' || tbl_name;
query := 'SELECT key, dt, did, segment, p FROM ' || tbl_name;
EXECUTE format('CREATE TABLE %s(eventkey varchar NOT NULL, dt date NOT NULL, usercount theta_sketch NOT NULL, skey text, sval text, p smallint NOT NULL)', sketch_tbl);
EXECUTE format('ALTER TABLE %s ADD constraint keydt_seg_unique UNIQUE(eventkey, dt, skey, sval)', sketch_tbl);
EXECUTE format('CREATE TABLE %s(dt date NOT NULL, usercount theta_sketch NOT NULL, p smallint NOT NULL)', activeuser_tbl);
EXECUTE format('ALTER TABLE %s ADD constraint dt_unique UNIQUE(dt)', activeuser_tbl);
EXECUTE format('CREATE TABLE %s(eventkey varchar NOT NULL, skey text, sval theta_sketch)', stat_tbl);   
EXECUTE format('ALTER TABLE %s ADD constraint skey_unique UNIQUE(skey)', stat_tbl);

FOR row1 IN EXECUTE query
LOOP
EXECUTE statsegment(row1.key, row1.segment, stat_tbl);
END LOOP;

FOR row2 IN EXECUTE query
LOOP
EXECUTE upsertdatasketchsegment(row2.key, CAST(row2.dt AS text), row2.did, row2.segment, row2.p, sketch_tbl, activeuser_tbl, stat_tbl);
END LOOP;
END  
$$;

create function statsegment(key_input varchar, segment jsonb, stat_tbl varchar)
returns void
language plpgsql
as
$$
DECLARE row RECORD;
BEGIN
    IF (length(segment::text) = 2) THEN 
      EXECUTE format('INSERT INTO %s(eventkey, skey, sval) select ''%s'', ''NULL'', theta_sketch_build(''NULL''::bytea) 
ON CONFLICT(skey)
DO 
    NOTHING ', stat_tbl, key_input); 
    ELSEIF (length(segment::text) > 2) THEN
    	FOR row IN (select d.key a, d.value b from jsonb_each_text(segment) d)                          
      LOOP
      EXECUTE format('INSERT INTO %s(eventkey, skey, sval) select ''%s'', ''%s'', theta_sketch_build(''%s''::bytea) 
ON CONFLICT(skey)
DO
   UPDATE SET sval = theta_sketch_union(%s.sval, (select theta_sketch_build(''%s''::bytea))) where %s.eventkey = ''%s'' AND %s.skey = ''%s'' ', stat_tbl, key_input, row.a, row.b, stat_tbl, row.b, stat_tbl, key_input, stat_tbl, row.a); 
	END LOOP; 
    END IF;
END;
$$;


create function upsertdatasketchsegment(key_input varchar, dt_input text, did_input text, segment jsonb, p_input varchar, sketch_tbl varchar, activeuser_tbl varchar, stat_tbl varchar)
returns void
language plpgsql
as
$$
DECLARE row RECORD; p smallint; query text; c int;
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
      UPDATE SET usercount = theta_sketch_union(%s.usercount, (select theta_sketch_build(''%s''::bytea))) where %s.dt = ''%s'';', activeuser_tbl, dt_input, did_input, p, activeuser_tbl, did_input, activeuser_tbl, dt_input);     
    
      IF (length(segment::text) = 2) THEN 
	      EXECUTE upsertdatasketchrow(key_input, dt_input, did_input, 'NULL', 'NULL', p, sketch_tbl);
      ELSEIF (length(segment::text) > 2) THEN
        FOR row IN (select d.key a, d.value b from jsonb_each_text(segment) d) 
        LOOP 
         query := 'select theta_sketch_get_estimate(sval) from ' || stat_tbl || ' where skey = ''' || row.a || ''';';
         FOR c IN EXECUTE query
         LOOP
           IF (c<100) THEN    
            EXECUTE upsertdatasketchrow(key_input, dt_input, did_input, row.a, row.b, p, sketch_tbl);
          END IF;
         END LOOP; 
       END LOOP;
      END IF;         
END;
$$;

create function upsertdatasketchrow(key_input varchar, dt_input text, did_input text, skey_input text, sval_input text, p_input smallint, sketch_tbl varchar)
returns void
language plpgsql
as
$$
BEGIN 
    EXECUTE format('INSERT INTO %s(eventkey, dt, usercount, skey, sval, p) select ''%s'', ''%s'', theta_sketch_build(''%s''::bytea), ''%s'', ''%s'', ''%s'' 
ON CONFLICT(eventkey, dt, skey, sval)
DO
   UPDATE SET usercount = theta_sketch_union(%s.usercount, (select theta_sketch_build(''%s''::bytea))) where %s.eventkey = ''%s'' AND %s.dt = ''%s'' ', sketch_tbl, key_input, dt_input, did_input, skey_input, sval_input, p_input, sketch_tbl, did_input, sketch_tbl, key_input, sketch_tbl, dt_input); 
    END;
$$;