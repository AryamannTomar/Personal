select jsonb_object_keys(segment) from events2;
select * from jsonb_each_text((select segment from events2 LIMIT 1));
select json_each(segment::json) from events2;

insert into action SELECT d.key, d.value FROM events2 JOIN jsonb_each_text(events2.segment) d ON true;

create table action(key varchar, dt date, skey text, sval text);
insert into action(skey, sval, key, dt) SELECT d.key, d.value, events2.key, dt FROM events2 JOIN jsonb_each_text(events2.segment) d ON true;

alter table action add constraint key_dt_unique UNIQUE(eventkey, dt);
alter table action add constraint keydt_seg_unique UNIQUE(eventkey, dt, skey, sval);

select * from events2 where segment::text = '{}'::text; 
select * from events2 where (select count(*) from jsonb_each(segment)) = 0;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
query1 := ''select key, dt, did, segment from events2'';
For row IN EXECUTE query1
LOOP
EXECUTE upsert(row.key, row.dt, row.did, (row.segment)::json->''LoginMethod'', (row.segment)::json->''UCIC'');
END LOOP;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
alter table action add constraint keydt_unique UNIQUE(eventkey, dt);
alter table action add constraint keydt_seg_unique UNIQUE(eventkey, dt, skey, sval);

select key, dt, did from events2 where (select count(*) from jsonb_each(segment)) = 0;
select * from events2 where segment::text = '{}'::text; 
select * from events2 where length(segment::text) = 2 ;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create function upsert(key_input varchar, dt_input date, did_input varchar, skey_input text, sval_input text)
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

create function upsertseg(key_input varchar, dt_input date, did_input varchar, skey_input text, sval_input text)
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

create or replace procedure row_op()
LANGUAGE 'plpgsql' as '
DECLARE query1 text; query2 text; row RECORD; row2 RECORD;
BEGIN 
query1 := ''SELECT events2.key, dt, did, d.key a, d.value b FROM events2, jsonb_each_text(events2.segment) d'';
query2 := ''select key, dt, did from events2 where (select count* from jsonb_each(segment)) = 0'';
For row2 IN EXECUTE query2
LOOP
EXECUTE upsert(row2.key, row2.dt, row2.did, ''EMPTY'', ''EMPTY'');
END LOOP;
For row IN EXECUTE query1
LOOP
EXECUTE upsertseg(row.key, row.dt, row.did, row.a, row.b);
END LOOP;
END';

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
create function upsert(key_input varchar, dt_input date, did_input varchar, skey_input text, sval_input text)
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

create or replace procedure row_op()
LANGUAGE 'plpgsql' as '
DECLARE query text; row RECORD; row1 RECORD;
BEGIN
query := ''SELECT key, dt, did, segment FROM events2''; 
FOR row IN EXECUTE query
LOOP
IF (length(row.segment::text) = 2) THEN 
EXECUTE upsert(row.key, row.dt, row.did, ''EMPTY'', ''EMPTY'');
END IF;
FOR row1 IN (select d.key a, d.value b from jsonb_each_text(row.segment) d)                          
LOOP
EXECUTE upsert(row.key, row.dt, row.did, row1.a, row1.b);
END LOOP;
END LOOP;
END';
____________________________________________________________________________________________________________________________________________________________________________________________






