import {db} from '../../db';
const { performance } = require('perf_hooks');
async function upsertData(tbl_id) {
    await db.task(async (t) => {
        let events_tbl = 'events_' + tbl_id;
        let sketch_tbl = 'datasketches_events_' + tbl_id;
        let activeuser_tbl = 'datasketches_dailyactiveusers_' + tbl_id;
        let stat_tbl = 'datasketches_segmentstats_' + tbl_id;
        await t.users.declareCursor({events_tbl: events_tbl});
        let count = 0;
        while (1) {
            let res = await t.users.fetchCursor({count: 1000});
            var startTime = performance.now()
            if (res.length == 0) { break; }
            await db.tx((p) => {
                for (var i = 0; i < res.length; i++) {
                    const arr = [res[i].key, res[i].dt, res[i].did, res[i].segment, res[i].p, sketch_tbl, activeuser_tbl, stat_tbl];
                    p.batch([
                        db.func('upsertdatasketchsegment', arr)
                    ]).Add
                } return p.batch;
            })
            var endTime = performance.now()
            count++;
            console.log(`${(endTime - startTime) / 1000} -- Seconds`);
            console.log('in loop:', count);
        }
        await t.users.closeCursor();
        console.log('Completed');
    })
}
upsertData('5bebe93c25d705690ffbc75811');
