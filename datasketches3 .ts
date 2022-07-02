const pgp = require('pg-promise')({});
const db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');
var moment = require('moment');

async function timeUnits(st_dt: String, end_dt: String, app_id: String, timeUnit: String, p: Number) {
    if (p == 3) {
        if (timeUnit == 'M' || timeUnit == 'W') {
            return await db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${st_dt}' AND '${end_dt}' `);
        } else if (timeUnit == 'D') {
            return await db.query(`select dt, theta_sketch_get_estimate(usercount) from datasketches_events_${app_id} where dt BETWEEN '${st_dt}' AND '${end_dt}' `);
        }
    }else{
        if (timeUnit == 'M' || timeUnit == 'W') {
            return await db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${st_dt}' AND '${end_dt}' AND p='${p}' `);
        } else if (timeUnit == 'D') {
            
            return await db.query(`select dt, theta_sketch_get_estimate(usercount) from datasketches_dailyactiveusers_${app_id} where dt BETWEEN '${st_dt}' AND '${end_dt}' AND p='${p}' `);
        }
    }
}

async function getResult(arr: Array<any>, app_id: String, timeUnit: String, p: Number) {
    let sum = 0;
    var arraynull_final: Array<any> = [];
    await db.task(async (t: any) => {
        for (let i = 0; i < arr.length; i = i + 2) {
            var arr_pre: Array<any> = [];
            arr_pre.push(arr[i]);
            t.batch([
                arr_pre.push((await timeUnits(arr[i], arr[i + 1], app_id, 'W', p))[0].theta_sketch_get_estimate)
            ]).Add
            arraynull_final.push(arr_pre);
        } return t.batch;
    });
    var arr_final: Array<any> = [];
    for (let i = 0; i < arraynull_final.length; i++) {
        if (arraynull_final[i][1] != null) {
            arr_final.push(arraynull_final[i]);
            sum += arraynull_final[i][1];
        }
    }
    if (timeUnit == 'M') {
        return Math.round((sum / arr_final.length) * 10) / 10;
    }
    else if (timeUnit == 'W') {
        return Math.round((sum / (arr_final.length + 1)) * 10) / 10;
    }
}

async function dauCount(st_dt: String, end_dt: String, app_id: String, p: Number): Promise<any> {
    let obj = await timeUnits(st_dt, end_dt, app_id, 'D', p);
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
}

async function wauCount(st_dt: String, end_dt: String, app_id: String, p:Number): Promise<any> {
    var start = moment(st_dt),
        end = moment(end_dt),
        day = 0;
    var result: Array<any> = [];
    var current = start.clone();
    while (current.day(7 + day).isBefore(end)) {
        result.push(current.clone());
    }
    var arr = result.map(m => m.format().substring(0, 10));
    var sunarr: Array<any> = [];
    sunarr.push(st_dt);
    for (const key of arr) {
        let dt = new Date(key);
        const dateCopy = new Date(dt.getTime());
        dateCopy.setDate(dateCopy.getDate() + 1);
        sunarr.push(dt.toISOString().substring(0, 10));
        sunarr.push((dateCopy).toISOString().substring(0, 10));
    }
    sunarr.push(end_dt);
    var wau_res = {
        "WAU": await getResult(sunarr, app_id, 'W', p)
    };
    return wau_res;
}

async function mauCount(st_dt: String, end_dt: String, app_id: String, p:Number): Promise<any> {
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
    var mau_res = {
        "MAU": await getResult(montharr, app_id, 'M', p)
    };
    return mau_res;
}

async function userCount(st_dt: String, end_dt: String, app_id: String, p = 3): Promise<any> {
    let daures = await dauCount(st_dt, end_dt, app_id, p);
    let maures = await mauCount(st_dt, end_dt, app_id, p);
    let waures = await wauCount(st_dt, end_dt, app_id, p);
    let res = {
        "DAU": daures.DAU,
        "WAU": waures.WAU,
        "MAU": maures.MAU,
        "daywisedau": daures.daywisedau
    };
    return res;
}

module.exports = userCount;