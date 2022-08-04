const pgp = require('pg-promise')({});
var moment = require('moment');
var db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');

async function dauCount(st_dt, end_dt, app_id) {
    var array = [];
    let stdt = new Date(st_dt);
    let endt = new Date(end_dt)
    for (stdt; stdt <= endt; stdt.setDate(stdt.getDate() + 1)) {
        array.push((new Date(stdt)).toISOString().substring(0, 10));
    }
    var arraynull_final = [];
    await db.task(async (t) => {
        for (let i = 0; i < array.length; i++) {
            var array_prefinal = [];
            array_prefinal.push(array[i]);
            t.batch([
                array_prefinal.push((await db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${array[i]}' AND '${array[i]}' `))[0].theta_sketch_get_estimate)
            ]).Add
            arraynull_final.push(array_prefinal);
        } return t.batch;
    });
    console.log(arraynull_final);
}
dauCount('2021-09-02', '2021-09-27', '5bebe93c25d705690ffbc75811')
