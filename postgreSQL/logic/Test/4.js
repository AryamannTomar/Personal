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
            await this.db.task(async (t) => { 
            let events_tbl = 'events_' + tbl_id;
            let sketch_tbl = 'datasketches_events_' + tbl_id;
            let activeuser_tbl = 'datasketches_dailyactiveusers_' + tbl_id;
            let stat_tbl = 'datasketches_segmentstats_' + tbl_id;
            await t.none(`DECLARE test_cursor CURSOR WITH HOLD FOR select key, dt, did, segment, p FROM ${events_tbl}`);
            let count = 0;
            let i =0;
            while (1) {
                let res = await t.any(`select key, dt, did, segment, p from ${events_tbl} LIMIT 1000 OFFSET ${i}`);
                var startTime = performance.now()
                if (res.length == 0) { break; }
                await this.db.task((p) => {
                    for (var i = 0; i < res.length; i++) {
                        const arr = [res[i].key, res[i].dt, res[i].did, res[i].segment, res[i].p, sketch_tbl, activeuser_tbl, stat_tbl];
                        p.batch([
                             this.db.any(`INSERT INTO ${activeuser_tbl}(dt, p) select $1, $2 
                             ON CONFLICT(dt)
                             DO
                               UPDATE SET dt = $3  where ${activeuser_tbl}.dt = $3`, [res[i].dt, res[i].p, res[i].dt]) 
                        ]).Add
                    } return p.batch;
                })
                var endTime = performance.now()
                count++;
                console.log(`${(endTime - startTime)/1000} -- Seconds`);
                console.log('in loop:', count);
                i+=1000;
            }
            await t.none("CLOSE test_cursor");
            console.log('Completed');
        })
            .then((data) => {
                console.log("Successful");
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

