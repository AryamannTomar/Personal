const pgp = require('pg-promise')({});
const db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');
var moment = require('moment');
async function getWAU(st_dt, end_dt, app_id, p = 3) {
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
    /* console.log('sunarr =',sunarr); */
    let sum = 0;
    var weekarr_final = [];
    for (let i = 0; i < sunarr.length; i = i + 2) {
        await db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${sunarr[i]}' AND '${sunarr[i + 1]}' `).then((res) => {
            var object = {};
            if (res[0].theta_sketch_get_estimate != null) {
                object[sunarr[i]] = res[0].theta_sketch_get_estimate;
                weekarr_final.push(object);
            }
            sum += res[0].theta_sketch_get_estimate;
        }).catch((err) => {
            console.log(err);
        })
    }
    var wau_res = {
        "WAU": Math.round((sum/(weekarr_final.length+1))*10) / 10
    };
    return wau_res;
}

let myresult = getWAU('2021-09-01', '2021-09-27', '5bebe93c25d705690ffbc75811');
myresult.then((result) => {
    console.log(result);
});