const pgp = require('pg-promise')({});
const db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');
var moment = require('moment');

async function dauCount(st_dt: String, end_dt: String, app_id: String, p: Number): Promise<any> {
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
        var dau_res: any = {
            "DAU": Math.round(sum / dayarr.length * 10) / 10,
            "daywisedau": dayarr
        };
        return dau_res;
    }).catch((err: any) => {
        console.log(err);
    })
}

async function wauCount(st_dt: String, end_dt: String, app_id: String, p: Number): Promise<any> {
    var start = moment(st_dt),
        end = moment(end_dt),
        day = 0;
    var result: any = [];
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
    var weekarraynull_final: Array<any> = [];
    await db.task(async (p: any) => {
        for (let i = 0; i < sunarr.length; i = i + 2) {
            var weekarr_pre: Array<any> = [];
            weekarr_pre.push(sunarr[i]);
            p.batch([
                weekarr_pre.push((await db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${sunarr[i]}' AND '${sunarr[i + 1]}' `))[0].theta_sketch_get_estimate)
            ]).Add
            weekarraynull_final.push(weekarr_pre);
        } return p.batch;
    });
    var weekarr_final: Array<any> = [];
    for (let i = 0; i < weekarraynull_final.length; i++) {
        if (weekarraynull_final[i][1] != null) {
            weekarr_final.push(weekarraynull_final[i]);
            sum += weekarraynull_final[i][1];
        }
    }
    var wau_res = {
        "WAU": Math.round((sum / (weekarr_final.length + 1)) * 10) / 10
    };
    return wau_res;
}

async function mauCount(st_dt: String, end_dt: String, app_id: String, p: Number): Promise<any> {
    let stdt = moment(st_dt);
    let endt = moment(end_dt);
    const montharr: Array<any> = [];
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
    var montharraynull_final: Array<any> = [];
    await db.task(async (p: any) => {
        for (let i = 0; i < montharr.length; i = i + 2) {
            var montharr_pre: Array<any> = [];
            montharr_pre.push(montharr[i]);
            p.batch([
                montharr_pre.push((await db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${montharr[i]}' AND '${montharr[i + 1]}' `))[0].theta_sketch_get_estimate)
            ]).Add
            montharraynull_final.push(montharr_pre);
        } return p.batch;
    });
    var montharr_final: Array<any> = [];
    for (let i = 0; i < montharraynull_final.length; i++) {
        if (montharraynull_final[i][1] != null) {
            montharr_final.push(montharraynull_final[i]);
            sum += montharraynull_final[i][1];
        }
    }
    var mau_res = {
        "MAU": Math.round((sum / montharr_final.length) * 10) / 10
    };
    return mau_res;
}

async function userCount(st_dt: String, end_dt: String, app_id: String, p = 3): Promise<any> {
    let daures = await dauCount(st_dt, end_dt, app_id, p = 3);
    let maures = await mauCount(st_dt, end_dt, app_id, p = 3);
    let waures = await wauCount(st_dt, end_dt, app_id, p = 3);
    let res = {
        "DAU": daures.DAU,
        "WAU": waures.WAU,
        "MAU": maures.MAU,
        "daywisedau": daures.daywisedau
    };
    return res;
}

let myresult = userCount('2021-09-02', '2021-09-27', '5bebe93c25d705690ffbc75811');
myresult.then((result:any) => {
    console.log(result);
});