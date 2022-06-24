CREATE OR REPLACE PROCEDURE processDataSketch(app_id text)  
LANGUAGE plpgsql  
AS  
$$  
DECLARE query text; dest_tbl1_name varchar; dest_tbl2_name varchar; dest_tbl3_name varchar; tbl_name varchar; row RECORD;
BEGIN 
tbl_name := 'events_' || app_id;
dest_tbl1_name := 'datasketches_' || tbl_name;
dest_tbl2_name := 'dailyactiveusers_' || tbl_name;
dest_tbl3_name := 'stats_' || tbl_name;
query := 'SELECT key, dt, did, segment, p FROM ' || tbl_name;
EXECUTE format('CREATE TABLE %s(eventkey varchar, dt date, usercount theta_sketch, skey text, sval text, p int)', dest_tbl1_name);
EXECUTE format('ALTER TABLE %s ADD constraint keydt_seg_unique UNIQUE(eventkey, dt, skey, sval)', dest_tbl1_name);
EXECUTE format('CREATE TABLE %s(dt date, usercount theta_sketch, p int)', dest_tbl2_name);
EXECUTE format('ALTER TABLE %s ADD constraint dt_unique UNIQUE(dt)', dest_tbl2_name);
EXECUTE format('CREATE TABLE %s(skey text, sval theta_sketch)', dest_tbl3_name);   
FOR row IN EXECUTE query
LOOP
EXECUTE upsertDataSketchSegment(row.key, CAST(row.dt AS text), row.did, row.segment, row.p, dest_tbl1_name, dest_tbl2_name, dest_tbl3_name);
END LOOP;
END  
$$;

create function upsertdatasketchsegment(key_input varchar, dt_input text, did_input text, segment jsonb, p_input varchar, dest_tbl1_name_input varchar, dest_tbl2_name_input varchar, dest_tbl3_name_input varchar)
returns void
language plpgsql
as
$$
DECLARE row RECORD; p int;
BEGIN
    IF (p_input = 'Android') THEN
	p = 0;
    ELSEIF (p_input = 'IOS') THEN
	p = 1;
    ELSE
	p = 2;
    END IF;
    
    IF (length(segment::text) = 2) THEN 
       EXECUTE format('INSERT INTO %s(skey, sval) select ''EMPTY'', ''EMPTY'' ', dest_tbl3_name_input);
    ELSEIF (length(segment::text) > 2) THEN
    	FOR row IN (select d.key a, d.value b from jsonb_each_text(segment) d)                          
      LOOP
       EXECUTE format('INSERT INTO %s(skey, sval) select ''%s'', ''%s'' ', dest_tbl3_name_input, row.a, row.b);
	END LOOP; 
    END IF;

    IF (length(segment::text) = 2) THEN 
	EXECUTE upsertdatasketchrow(key_input, dt_input, did_input, 'EMPTY', 'EMPTY', p, dest_tbl3_name_input, dest_tbl1_name_input);
    ELSEIF (length(segment::text) > 2) THEN
    	FOR row IN (select d.key a, d.value b from jsonb_each_text(segment) d)                          
      LOOP
	EXECUTE upsertdatasketchrow(key_input, dt_input, did_input, row.a, row.b, p, dest_tbl3_name_input, dest_tbl1_name_input);
	END LOOP; 
    END IF;

    EXECUTE format('INSERT INTO %s(dt, usercount, p) select ''%s'', theta_sketch_build(''%s''::bytea), ''%s'' 
    ON CONFLICT(dt)
    DO
      UPDATE SET usercount = theta_sketch_union(%s.usercount, (select theta_sketch_build((select ''%s''::bytea)))) where %s.dt = ''%s'';', dest_tbl2_name_input, dt_input, did_input, p, dest_tbl2_name_input, did_input, dest_tbl2_name_input, dt_input);     
    END;
$$;

create function upsertdatasketchrow(key_input varchar, dt_input text, did_input text, skey_input text, sval_input text, p_input int, dest_tbl1_name_input varchar, dest_tbl2_name_input varchar)
returns void
language plpgsql
as
$$
BEGIN 
    EXECUTE format('INSERT INTO %s(eventkey, dt, usercount, skey, sval, p) select ''%s'', ''%s'', theta_sketch_build(''%s''::bytea), ''%s'', ''%s'', ''%s'' 
ON CONFLICT(eventkey, dt, skey, sval)
DO
   UPDATE SET usercount = theta_sketch_union(%s.usercount, (select theta_sketch_build((select ''%s''::bytea)))) where %s.eventkey = ''%s'' AND %s.dt = ''%s'' ', dest_tbl2_name_input, key_input, dt_input, did_input, skey_input, sval_input, p_input, dest_tbl2_name_input, did_input, dest_tbl2_name_input, key_input, dest_tbl2_name_input, dt_input); 
    END;
$$;


CREATE TABLE datasketches_events_5fa41afe6fc1e678a8344311(eventkey varchar, dt date, usercount theta_sketch, skey text, sval text, p int);
ALTER TABLE datasketches_events_5fa41afe6fc1e678a8344311 ADD constraint keydt_seg_unique UNIQUE(eventkey, dt, skey, sval);

CREATE TABLE datasketches_dailyactiveusers_5fa41afe6fc1e678a8344311(dt date, usercount theta_sketch, p int);
ALTER TABLE datasketches_dailyactiveusers_5fa41afe6fc1e678a8344311 ADD constraint dt_unique UNIQUE(dt);

CREATE TABLE stats(skey text, sval text);



======================================================================================================================================================================================



CREATE OR REPLACE PROCEDURE DataSketch(app_id text)  
LANGUAGE plpgsql  
AS  
$$  
DECLARE query text; dest_tbl1_name varchar; dest_tbl2_name varchar; dest_tbl3_name varchar; tbl_name varchar; row RECORD;
BEGIN 
tbl_name := 'events_' || app_id;
dest_tbl1_name := 'datasketches_' || tbl_name;
dest_tbl2_name := 'dailyactiveusers_' || tbl_name;
dest_tbl3_name := 'stats_' || tbl_name;
query := 'SELECT key, dt, did, segment, p FROM ' || tbl_name;
FOR row IN EXECUTE query
LOOP
EXECUTE seg(row.key, CAST(row.dt AS text), row.did, row.segment, row.p, dest_tbl1_name, dest_tbl2_name, dest_tbl3_name);
END LOOP;
END  
$$;

create function seg(key_input varchar, dt_input text, did_input text, segment jsonb, p_input varchar, dest_tbl1_name_input varchar, dest_tbl2_name_input varchar, dest_tbl3_name_input varchar)
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

    FOR row IN (select d.key a, d.value b from jsonb_each_text(segment) d)                          
      LOOP
        query := 'select theta_sketch_distinct(sval) from ' || dest_tbl3_name_input || ' where skey = ''' || row.a || ''';';
        FOR c IN EXECUTE query
         LOOP
          IF (c<100) THEN
           IF (length(segment::text) = 2) THEN 
	      EXECUTE upsertdatasketchrow(key_input, dt_input, did_input, 'EMPTY', 'EMPTY', p, dest_tbl3_name_input, dest_tbl1_name_input);
           ELSEIF (length(segment::text) > 2) THEN
            EXECUTE upsertdatasketchrow(key_input, dt_input, did_input, row.a, row.b, p, dest_tbl3_name_input, dest_tbl1_name_input);
           END IF; 
        END IF;
      END LOOP;
   END LOOP;
END;
$$;


create table stats as select skey, count(*) count from countsegment_events_5bebe93c25d705690ffbc758 group by skey;
select * from stats where count > 100;

select theta_sketch_distinct(sval) from stats_events_5bebe93c25d705690ffbc758 where skey='MAD';

select (select theta_sketch_distinct(sval) from stats_events_5bebe93c25d705690ffbc758 where skey='PaymentDueDate') > 100;

