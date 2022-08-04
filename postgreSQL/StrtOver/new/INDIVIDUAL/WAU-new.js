const pgp = require('pg-promise')({});
const db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');
var moment = require('moment');
async function wauCount(st_dt, end_dt, app_id, p = 3) {
    var start = moment(st_dt),
        end = moment(end_dt),
        day = 0;

    var result = [];
    var current = start.clone();
    while (current.day(7 + day).isBefore(end)) {
        result.push(current.clone());
    }

    var arr = result.map(m => m.format().substring(0, 10));
    var sunarr = [];

    sunarr.push(st_dt);
    for (const key of arr) {
        let dt = new Date(key);
        const dateCopy = new Date(dt.getTime());
        dateCopy.setDate(dateCopy.getDate() + 1);
        sunarr.push(dt.toISOString().substring(0, 10));
        sunarr.push((dateCopy).toISOString().substring(0, 10));
    }
    sunarr.push(end_dt);

    let sum = 0;
    var weekarraynull_final = [];
    await db.task(async (p) => {
        for (let i = 0; i < sunarr.length; i = i + 2) {
            var weekarr_pre = [];
            weekarr_pre.push(sunarr[i]);
            p.batch([
                weekarr_pre.push((await db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${sunarr[i]}' AND '${sunarr[i + 1]}' `))[0].theta_sketch_get_estimate)
            ]).Add
            weekarraynull_final.push(weekarr_pre);
        } return p.batch;
    });
    var weekarr_final = [];
    for (let i = 0;i < weekarraynull_final.length; i++){
        if(weekarraynull_final[i][1]!= null){
            weekarr_final.push(weekarraynull_final[i]);
            sum += weekarraynull_final[i][1];
        }
    }
    var wau_res = {
        "WAU": Math.round((sum / (weekarr_final.length + 1)) * 10) / 10
    };
    console.log(wau_res);
}
wauCount('2021-09-02', '2022-09-29', '5bebe93c25d705690ffbc75811');