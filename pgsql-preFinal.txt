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
EXECUTE format('CREATE TABLE %s(eventkey varchar, dt date, usercount theta_sketch, skey text, sval text, p int)', sketch_tbl);
EXECUTE format('ALTER TABLE %s ADD constraint keydt_seg_unique UNIQUE(eventkey, dt, skey, sval)', sketch_tbl);
EXECUTE format('CREATE TABLE %s(dt date, usercount theta_sketch, p int)', activeuser_tbl);
EXECUTE format('ALTER TABLE %s ADD constraint dt_unique UNIQUE(dt)', activeuser_tbl);
EXECUTE format('CREATE TABLE %s(skey text, sval theta_sketch)', stat_tbl);   

FOR row1 IN EXECUTE query
LOOP
EXECUTE statsegment(row1.segment, stat_tbl);
END LOOP;

FOR row2 IN EXECUTE query
LOOP
EXECUTE upsertdatasketchsegment(row2.key, CAST(row2.dt AS text), row2.did, row2.segment, row2.p, sketch_tbl, activeuser_tbl, stat_tbl);
END LOOP;
END  
$$;

create function statsegment(segment jsonb, stat_tbl varchar)
returns void
language plpgsql
as
$$
DECLARE row RECORD;
BEGIN
    IF (length(segment::text) = 2) THEN 
       EXECUTE format('INSERT INTO %s(skey, sval) select ''EMPTY'', ''EMPTY'' ', stat_tbl);
    ELSEIF (length(segment::text) > 2) THEN
    	FOR row IN (select d.key a, d.value b from jsonb_each_text(segment) d)                          
      LOOP
       EXECUTE format('INSERT INTO %s(skey, sval) select ''%s'', ''%s'' ', stat_tbl, row.a, row.b);
	END LOOP; 
    END IF;
END;
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
           ELSEIF (length(segment::text) > 2) THEN
            EXECUTE upsertdatasketchrow(key_input, dt_input, did_input, row.a, row.b, p, sketch_tbl);
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
