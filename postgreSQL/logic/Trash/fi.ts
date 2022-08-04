const pgp = require('pg-promise')({});
const cn = 'postgres://postgres:Password@123@localhost:5432/postgres';
const db = pgp(cn);
module.exports = db;
db.connect().then(() => {
                          console.log('Connection established successfully.');
                        }).catch(err => {
                          console.log('Unable to connect to the database:', err);
});

db.tx(async t => {
            let sketch_tbl = 'datasketches_events_5bebe93c25d705690ffbc75811';
            let activeuser_tbl = 'datasketches_dailyactiveusers_5bebe93c25d705690ffbc75811';
            let stat_tbl = 'datasketches_segmentstats_5bebe93c25d705690ffbc75811';
            await t.none(`CREATE TABLE ${sketch_tbl}(eventkey varchar NOT NULL, dt date NOT NULL, usercount theta_sketch NOT NULL, skey text, sval text, p smallint NOT NULL)`);
            await t.none(`ALTER TABLE ${sketch_tbl} ADD constraint keydt_seg_unique UNIQUE(eventkey, dt, skey, sval)`);
            await t.none(`CREATE TABLE ${activeuser_tbl}(dt date NOT NULL, usercount theta_sketch NOT NULL, p smallint NOT NULL)`);
            await t.none(`ALTER TABLE ${activeuser_tbl} ADD constraint dt_unique UNIQUE(dt)`);
            await t.none(`CREATE TABLE ${stat_tbl}(eventkey varchar NOT NULL, skey text, sval theta_sketch)`);   
            await t.none(`ALTER TABLE ${stat_tbl} ADD constraint skey_unique UNIQUE(skey)`);
    
            await t.none(`DECLARE test_cursor CURSOR WITH HOLD FOR select key, dt, did, segment, p FROM events_5bebe93c25d705690ffbc75811;`);
            let sum = 0;
            while(1){
                    let res = await t.any("FETCH FORWARD 1000 FROM test_cursor")
                    var keys = Object.keys(res);
                    if(res.length == 0)
                        break
                    for(var i=0;i<keys.length;i++){
                        await t.many(`EXECUTE upsertdatasketchsegment(${res[i].key}, CAST(${res[i].dt} AS text), ${res[i].did}, ${res[i].segment}, ${res[i].p}, ${sketch_tbl}, ${activeuser_tbl}, ${stat_tbl})`);
                        sum++
                    }
            }
            await t.none("CLOSE test_cursor");
            console.log('doneee');
            console.log(sum);
})
    .then(data => {
            console.log("D.O.N.E.");
    })
    .catch(error => {
            console.log("NOTOK", error);
});