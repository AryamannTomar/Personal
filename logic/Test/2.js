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
            await this.db.task( async (t) => { 
            let events_tbl = 'events_' + tbl_id;
            let sketch_tbl = 'datasketches_events_' + tbl_id;
            let activeuser_tbl = 'datasketches_dailyactiveusers_' + tbl_id;
            let stat_tbl = 'datasketches_segmentstats_' + tbl_id;
            await t.none(`DECLARE test_cursor CURSOR WITH HOLD FOR select key, dt, did, segment, p FROM ${events_tbl}`);
            let count = 0;
            var startTimeT = performance.now()
            while (1) {
                let res = await t.any("FETCH FORWARD 10000 FROM test_cursor");
                var startTime = performance.now()
                if (res.length == 0) { break; }
                await this.db.task((p) => {
                    for (var i = 0; i < res.length; i++) {
                        const arr = [res[i].key, res[i].dt, res[i].did, res[i].segment, res[i].p, sketch_tbl, activeuser_tbl, stat_tbl];
                        p.batch([
                             this.db.func('upsertdatasketchsegment', arr) 
                        ]).Add
                    } return p.batch;
                })
                var endTime = performance.now()
                console.log(`${(endTime - startTime)/(1000)} -- Seconds`);    
                count++;
                console.log('in loop:', count);
            }
            var endTimeT = performance.now()
            console.log(`${(endTimeT - startTimeT)/(60*1000)} -- Minutes`); 
            

            await t.none("CLOSE test_cursor");
            console.log('Completed');
        })
    }
}
module.exports = DB;
var db = new DB();
db.connect();
db.upsertData('5bebe93c25d705690ffbc75811');

