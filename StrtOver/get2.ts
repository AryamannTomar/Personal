const pgp = require('pg-promise')({});
const db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');
var moment = require('moment');

type getDAU = {
    DAU: number;
    daywisedau: Array<any>;
};
async function getDAU(st_dt: String, end_dt: String, app_id: String, p = 3): Promise<any> {
    return await db.query(`select dt, theta_sketch_get_estimate(usercount) from datasketches_events_${app_id} where dt BETWEEN '${st_dt}' AND '${end_dt}' `).then((res: any) => {
        let obj = res;
        var keys = Object.keys(obj);
        const dayarr: Array<any> = [];
        let sum = 0;
        for (var i = 0; i < keys.length; i++) {
            const dayobj: Array<any> = [];
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
        return dau_res;
    }).catch((err: any) => {
        console.log(err);
    })
}


type getWAU = {
    WAU: number;
};
async function getWAU(st_dt: String, end_dt: String, app_id: String, p = 3): Promise<any> {
    var start = moment(st_dt),
        end = moment(end_dt),
        day = 0;
    var result = [];
    var current = start.clone();
    while (current.day(7 + day).isBefore(end)) {
        result.push(current.clone());
    }
    var arr = result.map(m => m.format().substring(0, 10));
    var sunarr: any = [];
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
    var weekarr_final = [];
    for (let i = 0; i < sunarr.length; i = i + 2) {
        await db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${sunarr[i]}' AND '${sunarr[i + 1]}' `).then((res) => {
            var object: any = {};
            if (res[0].theta_sketch_get_estimate != null) {
                object[sunarr[i]] = res[0].theta_sketch_get_estimate;
                weekarr_final.push(object);
            }
            sum += res[0].theta_sketch_get_estimate;
        }).catch((err: any) => {
            console.log(err);
        })
    }
    var wau_res = {
        "WAU": Math.round((sum / (weekarr_final.length + 1)) * 10) / 10
    };
    return wau_res;
}


type getMAU = {
    WAU: number;
};
async function getMAU(st_dt: String, end_dt: String, app_id: String, p = 3): Promise<any> {
    let dt1 = moment(st_dt);
    let dt2 = moment(end_dt);
    const montharr: any = [];
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
    let montharr_final = [];
    for (let i = 0; i < montharr.length; i = i + 2) {
        await db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${montharr[i]}' AND '${montharr[i + 1]}' `).then((res) => {
            var object: any = {};
            if (res[0].theta_sketch_get_estimate != null) {
                object[montharr[i]] = res[0].theta_sketch_get_estimate;
                montharr_final.push(object);
            }
            sum += res[0].theta_sketch_get_estimate;
        }).catch((err: any) => {
            console.log(err);
        })
    }
    var mau_res = {
        "MAU": Math.round((sum / montharr_final.length) * 10) / 10
    };
    return mau_res;
}

async function getUserCount(st_dt: String, end_dt: String, app_id: String, p = 3) {
    let daures = await getDAU(st_dt, end_dt, app_id, p = 3);
    let maures = await getMAU(st_dt, end_dt, app_id, p = 3);
    let waures = await getWAU(st_dt, end_dt, app_id, p = 3);
    let res = {
        "DAU": daures.DAU,
        "WAU": waures.WAU,
        "MAU": maures.MAU,
        "daywisedau": daures.daywisedau
    };
    return res;
}

let myresult = getUserCount('2021-09-02', '2021-09-27', '5bebe93c25d705690ffbc75811');
myresult.then(function (result) {
    console.log(result);
});