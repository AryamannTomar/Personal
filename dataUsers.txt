create function upsertRow(dt_input text, did_input text, p_input varchar, dest_tbl_name_input varchar)
returns void
language plpgsql
as
$$
DECLARE p int;
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
   UPDATE SET usercount = theta_sketch_union(%s.usercount, (select theta_sketch_build((select ''%s''::bytea)))) where %s.dt = ''%s'';', dest_tbl_name_input, dt_input, did_input, p, dest_tbl_name_input, did_input, dest_tbl_name_input, dt_input);   
   END;
$$;


CREATE OR REPLACE PROCEDURE processUsers(app_id text)  
LANGUAGE plpgsql  
AS  
$$  
DECLARE query text; dest_tbl_name varchar; tbl_name varchar; row RECORD;
BEGIN 
tbl_name := 'events_' || app_id;
dest_tbl_name := 'dataUsers_' || tbl_name;
query := 'SELECT dt, did, p FROM ' || tbl_name;
EXECUTE format('CREATE TABLE %s(dt date, usercount theta_sketch, p varchar)', dest_tbl_name);
EXECUTE format('ALTER TABLE %s ADD constraint usercount_unique UNIQUE(dt)', dest_tbl_name);   
FOR row IN EXECUTE query
LOOP
EXECUTE upsertRow(CAST(row.dt AS text), row.did, row.p, dest_tbl_name);
END LOOP;
END  
$$;


