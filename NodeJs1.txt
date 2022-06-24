let app_id = "5bebe93c25d705690ffbc75811";
const {Client} = require('pg')
const client = new Client ({
                        host: "localhost",
                        user: "postgres",
                        port: 5432,
                        password: "Password@123",
                        database: "postgres"
        })

client.connect();

let AppEventsQuery1 = `create or replace function upsertDataSketchRow(key_input varchar, dt_input text, did_input text, skey_input text, sval_input text, dest_tbl_name_input varchar)
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
                          $$;`;

let AppEventsQuery2 = `create or replace function upsertDataSketchSegment(key_input varchar, dt_input text, did_input text, segment jsonb, dest_tbl_name_input varchar)
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
        $$;`;

let AppEventsQuery3 = `CREATE OR REPLACE PROCEDURE processDataSketch(app_id text)
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
        $$;`;


client.query(AppEventsQuery1, (err, res)=>{
        if(!err){
        	console.log("upsertDataSketchRow() Function created Successfully!");
        } else {
        	console.log(err.message);
        }
})

client.query(AppEventsQuery2, (err, res)=>{
        if(!err){
        	console.log("upsertDataSketchSegment() Function created Successfully!");
        } else {
        	console.log(err.message);
        }
})

client.query(AppEventsQuery3, (err, res)=>{
        if(!err){
        	console.log("processDataSketch() Procedure created Successfully!");
        } else {
        	console.log(err.message);
        }
})

client.query(`call processDataSketch('`+app_id+`');`, (err, res)=>{
        if(!err){
        	console.log("datasketches_events_"+app_id+" Table created Successfully");
        } else {
            console.log(err.message);
        }
})

client.end;