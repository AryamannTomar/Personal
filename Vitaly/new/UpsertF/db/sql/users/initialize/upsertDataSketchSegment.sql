CREATE OR REPLACE FUNCTION upsertdatasketchsegment(key_input varchar, dt_input text, did_input text, segment jsonb, p_input varchar, sketch_tbl varchar, activeuser_tbl varchar, stat_tbl varchar)
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
