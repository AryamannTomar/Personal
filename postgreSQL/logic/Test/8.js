const pgp = require('pg-promise')({});
const { performance } = require('perf_hooks');
class DB {
    constructor() {
        this.db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');
    }
    connect() {
        this.db.connect().then(() => {
            console.log('Connection established successfully.');
        }).catch((err) => {
            console.log('Unable to connect to the database:', err);
        });
    }
    async upsertData(tbl_id) {
            this.db.task(async (t) => { 
            let events_tbl = 'events_' + tbl_id;
            let sketch_tbl = 'datasketches_events1_' + tbl_id;
            let activeuser_tbl = 'datasketches_dailyactiveusers1_' + tbl_id;
            let stat_tbl = 'datasketches_segmentstats1_' + tbl_id;
            let count = 0;
            let j =0;
            var startTime2 = performance.now()
            while (1) {
                let res = await t.any(`select key, dt, did, segment, p from ${events_tbl} LIMIT 1000 OFFSET ${j}`);
                var startTime = performance.now()
                console.log("res.length = ", res.length);
                if (res.length == 0) { break; }
                await this.db.task((p) => {
                    for (var i = 0; i < res.length; i++) {
                        const arr = [res[i].key, res[i].dt, res[i].did, res[i].segment, res[i].p, sketch_tbl, activeuser_tbl, stat_tbl];
                        p.batch([
                           this.db.func('upsertdatasketchsegment' , arr) 
                        ]).Add
                    } return p.batch;
                })
                
                count++;
                var endTime = performance.now()
                console.log(`TOTAL TIME - ${(endTime - startTime)/1000} -- Seconds`);
                console.log('in loop:', count);
                j+=1000;
            }
            var endTime2 = performance.now()
            console.log(`TOTAL TIME - ${(endTime2 - startTime2)/1000} -- Seconds`);
        })
            .then((data) => {
                console.log("Successful");
                pgp.end();
            })
            .catch((error) => {
                console.log("NOT-OK", error);
            });
    }
}
module.exports = DB;
var db = new DB();
db.connect();
db.upsertData('5bebe93c25d705690ffbc75811');

