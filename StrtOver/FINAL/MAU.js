const pgp = require('pg-promise')({});
const db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');
var moment = require('moment');
async function getMAU(st_dt, end_dt, app_id, p = 3) {
    let dt1 = new Date(st_dt);
    let dt2 = new Date(end_dt);
    const montharr = [];
    let st_mnth = parseInt((dt1).toISOString().substring(5, 7));
    let end_mnth = parseInt((dt2).toISOString().substring(5, 7));
    montharr.push(dt1.toISOString().substring(0, 10));
    while (st_mnth < end_mnth) {
        montharr.push(new Date(dt1.getFullYear(), dt1.getMonth() + 1, 0).toISOString().substring(0, 10));
        dt1.setMonth(dt1.getMonth() + 1);
        st_mnth++;
        montharr.push(new Date(dt1.getFullYear(), dt1.getMonth(), 1).toISOString().substring(0, 10));
    }
    montharr.push(dt2.toISOString().substring(0, 10));

    let sum = 0;
    let montharr_final=[];
    for (let i = 0; i < montharr.length; i = i + 2) {
        await db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${montharr[i]}' AND '${montharr[i + 1]}' `).then((res) => {
            var object = {};
            if (res[0].theta_sketch_get_estimate != null) {
                object[montharr[i]] = res[0].theta_sketch_get_estimate;
                montharr_final.push(object);
            }
            sum += res[0].theta_sketch_get_estimate;
        }).catch((err) => {
            console.log(err);
        })
    }
    var mau_res = {
        "MAU": Math.round((sum/montharr_final.length)*10) / 10
    };
    return mau_res;
}


let myresult = getMAU('2021-09-01', '2021-09-27', '5bebe93c25d705690ffbc75811');
myresult.then((result) => {
    console.log(result);
});