const pgp = require('pg-promise')({});
const cn = 'postgres://postgres:Password@123@localhost:5432/postgres';
const db = pgp(cn);
module.exports = db;
db.connect().then(() => {
    console.log('Connection established successfully.');
}).catch((err: any) => {
    console.log('Unable to connect to the database:', err);
});

async function upsertData(tbl_id:String) {
    await db.tx(async (t: any) => {
        let events_tbl = 'events_'+ tbl_id;
        let sketch_tbl = 'datasketches_events_'+ tbl_id;
        let activeuser_tbl = 'datasketches_dailyactiveusers_'+ tbl_id;
        let stat_tbl = 'datasketches_segmentstats_'+ tbl_id;
        await t.none(`DECLARE test_cursor CURSOR WITH HOLD FOR select key, dt, did, segment, p FROM ${events_tbl}`);
        let count = 0;

        while (1) {
            let res = await t.any("FETCH FORWARD 1000 FROM test_cursor");
            if (res.length == 0) { break; }
            await db.task((p: any) => {
                for (var i = 0; i < res.length; i++) {
                    const arr = [res[i].key, res[i].dt, res[i].did, res[i].segment, res[i].p, sketch_tbl, activeuser_tbl, stat_tbl];
                    p.batch([

                        db.func('upsertdatasketchsegment', arr)

                    ]).Add

                } return p.batch;

            })
            count++;
            console.log('in loop:', count);
        }

        await t.none("CLOSE test_cursor");
        console.log('Completed');
    })
        .then((data: any) => {
            console.log("Successful");
        })
        .catch((error: any) => {
            console.log("NOT-OK", error);
        });
}

upsertData('5bebe93c25d705690ffbc75811');
