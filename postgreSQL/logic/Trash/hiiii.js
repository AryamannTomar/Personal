const pgp = require('pg-promise')({});
const cn = 'postgres://postgres:Password@123@localhost:5432/postgres';
const db = pgp(cn);
module.exports = db;
db.connect().then(() => {
                          console.log('Connection established successfully.');
                        }).catch(err => {
                          console.log('Unable to connect to the database:', err);
});

db.task(async t => {
            let sketch_tbl = 'datasketches_events_5bebe93c25d705690ffbc75811';
            let activeuser_tbl = 'datasketches_dailyactiveusers_5bebe93c25d705690ffbc75811';
            let stat_tbl = 'datasketches_segmentstats_5bebe93c25d705690ffbc75811';
            await t.none(`DECLARE test_cursor CURSOR WITH HOLD FOR select key, dt, did, segment, p FROM events_5bebe93c25d705690ffbc758`);
            let count = 0;
            

            while(1){
                let res = await t.any("FETCH FORWARD 1000 FROM test_cursor");
                if(res.length == 0){ break; }
                var keys = Object.keys(res);
                for(var i=0;i<keys.length;i++){
                        const arr = [res[i].key, (res[i].dt).toISOString().substring(0,10), res[i].did, res[i].segment, res[i].p, sketch_tbl, activeuser_tbl, stat_tbl];
                        t.batch([
                            db.func('upsertdatasketchsegment', arr)
                         ]).Add} 
                count++
                console.log('in loop:', count)
                }
            await t.none("CLOSE test_cursor");
            console.log('doneee');
            return t.batch; 
        })
    .then(data => {
            console.log("D.O.N.E.");
    })
    .catch(error => {
                    console.log("NOTOK", error);
});