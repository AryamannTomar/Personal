upsertDataSketchRow FUNCTION - 
create function upsertDataSketchRow(key_input varchar, dt_input date, did_input varchar, skey_input text, sval_input text)
returns void
language plpgsql
as
$$
BEGIN
    INSERT INTO action(eventkey, dt, usercount, skey, sval) select key_input, dt_input, theta_sketch_build(did_input::bytea), skey_input, sval_input
ON CONFLICT(eventkey, dt, skey, sval)
DO
   UPDATE SET usercount = theta_sketch_union(action.usercount, (select theta_sketch_build((select did_input::bytea)))) where action.eventkey = key_input AND action.dt = dt_input;  
   END;
$$;

//upsertDataSketchSegment takes in the row values and calls the upsertDataSketchRow function 
upsertDataSketchSegment FUNCTION - 
create function upsertDataSketchSegment(key_input varchar, dt_input date, did_input varchar, segment jsonb)
returns void
language plpgsql
as
$$
DECLARE row RECORD;
BEGIN
    IF (length(segment::text) = 2) THEN 
	EXECUTE upsertDataSketchRow(key_input, dt_input, did_input, 'EMPTY', 'EMPTY');
    ELSEIF (length(segment::text) > 2) THEN
    	FOR row IN (select d.key a, d.value b from jsonb_each_text(segment) d)                          
      LOOP
	EXECUTE upsertDataSketchRow(key_input, dt_input, did_input, row.a, row.b);
	END LOOP; 
    END IF;
END;
$$;

//processDataSketch function iterates over each row values of the Source Table
processDataSketch(app_id text) PROCEDURE -
CREATE OR REPLACE PROCEDURE processDataSketch(app_id text)  
LANGUAGE plpgsql  
AS  
$$  
DECLARE query text; row RECORD;
BEGIN 
query := 'SELECT key, dt, did, segment FROM ' || 'events_' || app_id; 
FOR row IN EXECUTE query
LOOP
EXECUTE upsertDataSketchSegment(row.key, row.dt, row.did, row.segment);
END LOOP;
END  
$$;


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

create function upsertDataSketchRow(key_input varchar, dt_input text, did_input text, skey_input text, sval_input text, dest_tbl_name_input varchar)
returns void
language plpgsql
as
$$
BEGIN
    EXECUTE format('INSERT INTO %s(eventkey, dt, usercount, skey, sval) select ''%s'', ''%s'', theta_sketch_build(''%s''::bytea), ''%s'', ''%s'' 
ON CONFLICT(eventkey, dt, skey, sval)
DO
   UPDATE SET usercount = theta_sketch_union(%s.usercount, (select theta_sketch_build((select ''%s''::bytea)))) where %s.eventkey = ''%s'' AND %s.dt = ''%s'';', dest_tbl_name_input, key_input, dt_input, did_input, skey_input, sval_input, dest_tbl_name_input, did_input, dest_tbl_name_input, key_input, dest_tbl_name_input, dt_input);   
   END;
$$;

create function upsertDataSketchSegment(key_input varchar, dt_input text, did_input text, segment jsonb, dest_tbl_name_input varchar)
returns void
language plpgsql
as
$$
DECLARE row RECORD;
BEGIN
    IF (length(segment::text) = 2) THEN 
	EXECUTE upsertDataSketchRow(key_input, dt_input, did_input, 'EMPTY', 'EMPTY', dest_tbl_name_input);
    ELSEIF (length(segment::text) > 2) THEN
    	FOR row IN (select d.key a, d.value b from jsonb_each_text(segment) d)                          
      LOOP
	EXECUTE upsertDataSketchRow(key_input, dt_input, did_input, row.a, row.b, dest_tbl_name_input);
	END LOOP; 
    END IF;
END;
$$;

CREATE OR REPLACE PROCEDURE processDataSketch(app_id text)  
LANGUAGE plpgsql  
AS  
$$  
DECLARE query text; dest_tbl_name varchar; tbl_name varchar; row RECORD;
BEGIN 
tbl_name := 'events_' || app_id;
dest_tbl_name := 'datasketches_' || tbl_name;
query := 'SELECT key, dt, did, segment FROM ' || tbl_name;
EXECUTE format('CREATE TABLE %s(eventkey varchar, dt date, usercount theta_sketch, skey text, sval text)', dest_tbl_name);
EXECUTE format('ALTER TABLE %s ADD constraint keydt_seg_unique UNIQUE(eventkey, dt, skey, sval)', dest_tbl_name);   
FOR row IN EXECUTE query
LOOP
EXECUTE upsertDataSketchSegment(row.key, CAST(row.dt AS text), row.did, row.segment, dest_tbl_name);
END LOOP;
END  
$$;