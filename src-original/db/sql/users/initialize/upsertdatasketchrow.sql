/*
   Create Function UpsertDataSketchRow for upserting in datasketch_events table. It updates the already existing key, dt, skey and sval pair with theta_sketch_union(new + existing)
*/
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

SELECT create_distributed_function(
  'upsertdatasketchrow(varchar, text, text, text, text, smallint, varchar)', 'key_input',
);