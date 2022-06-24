const pgp = require('pg-promise')({});
const cn = 'postgres://postgres:Password@123@localhost:5432/postgres';
const db = pgp(cn);
module.exports = db;
db.connect().then(() => {
    console.log('Connection established successfully.');
}).catch(err => {
    console.log('Unable to connect to the database:', err);
});

async function getResult() {
    await db.tx(async t => {
        let sketch_tbl = 'datasketches_events_5bebe93c25d705690ffbc75811';
        let activeuser_tbl = 'datasketches_dailyactiveusers_5bebe93c25d705690ffbc75811';
        let stat_tbl = 'datasketches_segmentstats_5bebe93c25d705690ffbc75811';
        await t.none(`DECLARE test_cursor CURSOR WITH HOLD FOR select key, dt, did, segment, p FROM events_5bebe93c25d705690ffbc75811`);
        let count = 0;

        while (1) {
            let res = await t.any("FETCH FORWARD 1000 FROM test_cursor");
            if (res.length == 0) { break; }
            await db.task(p => {
                for (var i = 0; i < res.length; i++) {
                    const arr = [res[i].key, res[i].dt, res[i].did, res[i].segment, res[i].p, sketch_tbl, activeuser_tbl, stat_tbl];
                    p.batch([
                        db.func('upsertdatasketchsegment', arr)
                    ]).Add
                } return p.batch
            })

            count++
            console.log('in loop:', count)
        }

        await t.none("CLOSE test_cursor");
        console.log('Completed');
    })
        .then(data => {
            console.log("Successful");
        })
        .catch(error => {
            console.log("NOT-OK", error);
        });
}

getResult();