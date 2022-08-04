const pgp = require('pg-promise')({});
const db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');

async function getDAU(st_dt, end_dt, app_id, p = 3) {
    await db.query(`select dt, theta_sketch_get_estimate(usercount) from datasketches_events_${app_id} where dt BETWEEN '${st_dt}' AND '${end_dt}' `).then((res) => {
        let obj = res;
        keys = Object.keys(obj);
        const dayarr = [];
        let sum = 0;
        for (var i = 0; i < keys.length; i++) {
            const dayobj = [];
            var flag = 0;
            sum += obj[i].theta_sketch_get_estimate;
            let item = (obj[i].dt).toISOString().substring(5, 10);

            for (let j = 0; j < dayarr.length; j++) {
                if ((dayarr[j].indexOf(item)) != -1) {
                    flag = 1;
                    dayarr[j][1] += obj[i].theta_sketch_get_estimate;
                    break;
                }
            }

            if (flag == 0) {
                dayobj.push(item);
                dayobj.push(obj[i].theta_sketch_get_estimate);
                dayarr.push(dayobj);
            }
        }
        var dau_res = {
            "DAU": Math.round(sum / dayarr.length * 10) / 10,
            "daywisedau": dayarr
        };
        console.log(dau_res);
    }).catch((err) => {
        console.log(err);
    })
}

getDAU('2021-09-02', '2021-09-25', '5bebe93c25d705690ffbc75811');