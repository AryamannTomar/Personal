const pgp = require('pg-promise')({});
const db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');
var moment = require('moment');
async function mauCount(st_dt, end_dt, app_id, p = 3) {
    let stdt = moment(st_dt);
    let endt = moment(end_dt);
    const montharr = [];
    let st_mnth = parseInt((stdt).toISOString().substring(5, 7));
    let end_mnth = parseInt((endt).toISOString().substring(5, 7));
    montharr.push(stdt.toISOString().substring(0, 10));
    while (st_mnth < end_mnth) {
        montharr.push(new Date(stdt.getFullYear(), stdt.getMonth() + 1, 0).toISOString().substring(0, 10));
        stdt.setMonth(stdt.getMonth() + 1);
        st_mnth++;
        montharr.push(new Date(stdt.getFullYear(), stdt.getMonth(), 1).toISOString().substring(0, 10));
    }
    montharr.push(endt.toISOString().substring(0, 10));

    let sum = 0;
    let montharr_final = [];
    for (let i = 0; i < montharr.length; i = i + 2) {
        var res = await db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${montharr[i]}' AND '${montharr[i + 1]}' `);
        var object = {};
        if (res[0].theta_sketch_get_estimate != null) {
            object[montharr[i]] = res[0].theta_sketch_get_estimate;
            montharr_final.push(object);
        }
        sum += res[0].theta_sketch_get_estimate;
    }
    var mau_res = {
        "MAU": Math.round((sum / montharr_final.length) * 10) / 10
    };
    console.log(mau_res);
}
mauCount('2021-09-02', '2022-09-29', '5bebe93c25d705690ffbc75811');