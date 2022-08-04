CREATE OR REPLACE FUNCTION upsertdatasketchrow(key_input varchar, dt_input text, did_input text, skey_input text, sval_input text, p_input smallint, sketch_tbl varchar)
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