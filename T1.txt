				CREATING A NEW TABLE FROM AN EXISTING TABLE 
-------------------------------------------------------------------------------------------------------------------------------------------------------------

create table events1 as (select * from events_5bebexxxxxxxx);
ALTER TABLE events1 ADD COLUMN theta_sketches theta_sketch; 
update events1 set theta_sketches = theta_sketch(did);

-------------------------------------------------------------------------------------------------------------------------------------------------------------

PROBLEM STATEMENT -> Rewrite the query to 

--insert if the key is being inserted for first time for that date 
--else update the row by doing a union operation on the theta sketch value of existing usercount column with the theta sketch value of events did column

xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
create table events(key varchar, dt date,num integer);
insert into events values ('App_Launch', '2021-09-01', 34), ('App_Launch', '2021-09-01', 12), ('Session_End', '2021-09-01', 20), ('Session_Start', '2021-10-01', 20);

create table eventscpy (key varchar, dt date,num integer);
insert into eventscpy(key, dt, num) select distinct eventkey, dt, sum(num) from events group by dt, eventkey;
xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

insert into action(eventkey, dt, usercount) select distinct key, dt, theta_sketch_union(theta_sketches) from events1 group by dt, key;

-----------------------------------------------------------------------------------------------------------------------------------------------------------------

	did -> sha256(did::bytea) -> theta_sketch(#hash_values) -> theta_sketch_union(theta_values)

----------------------------------------------------------------------------------------------------------------------------------------------------------------

create table test( did varchar, sketch theta_sketch);
insert into test(did) select did from events;
insert into test(sketch) select theta_sketch_build(sha256(did::bytea)) from test;
select theta_sketch_get_estimate(theta_sketch_union(sketch)) from test;

create table test as (select theta_sketch_build(sha256(did::bytea)) from events);
select theta_sketch_get_estimate(theta_sketch_union(sketch)) from test;

create table hi as (select theta_sketch_get_estimate(theta_sketch_union(sketch)) from events);

-----------------------------------------------------------------------------------------------------------------------------------------------------------------

select theta_sketch_union(theta_sketch_build(sha256('6e52d01eAfcebA4332A8af0A868fbc192bbf')), theta_sketch_build(sha256('8fb90519A8f32A4835A8520A2ae068811175')));

update action3 set sketch =  sha256(usercount::bytea);
select theta_sketch_union(theta_sketch_build((select did from events where id = 1)), theta_sketch_build((select sketch from action where id=3)));

alter table datasketches add constraint key_dt_unique UNIQUE(eventkey, dt);
update events set usercount = (select theta_sketch_union(theta_sketch_build((select usercount from events where id = 2)), theta_sketch_build((select usercount from events where id=5)))) where id=2; 

-----------------------------------------------------------------------------------------------------------------------------------------------------------------
create table action1(eventkey varchar, dt date, usercount theta_sketch);
alter table action add constraint key_dt_unique UNIQUE(eventkey, dt);
alter table action add constraint keydt_seg_unique UNIQUE(eventkey, dt, skey, sval);

create function upsert(key_input varchar, dt_input date, did_input varchar)
returns void
language plpgsql
as
$$
BEGIN
    INSERT INTO action1(eventkey, dt, usercount) select key_input, dt_input, theta_sketch_build(did_input::bytea)
ON CONFLICT(eventkey, dt)
DO
   UPDATE SET usercount = theta_sketch_union(action1.usercount, (select theta_sketch_build((select did_input::bytea)))) where action1.eventkey = key_input AND action1.dt = dt_input;  
   END;
$$;


create or replace procedure row_op()
LANGUAGE 'plpgsql' as '
DECLARE query1 text; row RECORD;
BEGIN 
query1 := ''select key, dt, did from events2'';
For row IN EXECUTE query1
LOOP
EXECUTE upsert(row.key, row.dt, row.did);
END LOOP;
END';

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
