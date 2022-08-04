const pgp = require('pg-promise')({});
var moment = require('moment');
var db= pgp('postgres://postgres:Password@123@localhost:5432/postgres');

async function fetch(st_dt: String, end_dt: String, app_id: String, timeUnit: String, p: Number): Promise<any> {
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

async function timeUnits(st_dt: String, end_dt: String, app_id: String, timeUnit: String, p: Number): Promise<any>{
    if (timeUnit == 'D') {
        let obj = await fetch(st_dt, end_dt, app_id, 'D', p);
        var keys = Object.keys(obj);
        const dayarray: Array<any> = [];
        let sum = 0;
        for (var i = 0; i < keys.length; i++) {
            const dayobj: Array<any> = [];
            var flag = 0;
            sum += obj[i].theta_sketch_get_estimate;
            let item = (obj[i].dt).toISOString().substring(5, 10);
            for (let j = 0; j < dayarray.length; j++) {
                if ((dayarray[j].indexOf(item)) != -1) {
                    flag = 1;
                    dayarray[j][1] += obj[i].theta_sketch_get_estimate;
                    break;
                }
            }
            if (flag == 0) {
                dayobj.push(item);
                dayobj.push(obj[i].theta_sketch_get_estimate);
                dayarray.push(dayobj);
            }
        }
        var dau_res = {
            "DAU": Math.round(sum / dayarray.length * 10) / 10,
            "daywisedau": dayarray
        };
        return dau_res;
    }

    else if (timeUnit == 'W') {
        var start = moment(st_dt),
            end = moment(end_dt),
            day = 0;
        var result: Array<any> = [];
        var current = start.clone();
        while (current.day(7 + day).isBefore(end)) {
            result.push(current.clone());
        }
        var arr = result.map(m => m.format().substring(0, 10));
        var sundayarray: Array<any> = [];
        sundayarray.push(st_dt);
        for (const key of arr) {
            let dt = new Date(key);
            const dateCopy = new Date(dt.getTime());
            dateCopy.setDate(dateCopy.getDate() + 1);
            sundayarray.push(dt.toISOString().substring(0, 10));
            sundayarray.push((dateCopy).toISOString().substring(0, 10));
        }
        sundayarray.push(end_dt);
        return await getResult(sundayarray, app_id, 'W', p);
    }
    
    else if (timeUnit == 'M') {
        let stdt = moment(st_dt);
        let endt = moment(end_dt);
        const montharray: Array<any> = [];
        let start_month = parseInt((stdt).toISOString().substring(5, 7));
        let end_month = parseInt((endt).toISOString().substring(5, 7));
        montharray.push(stdt.toISOString().substring(0, 10));
        while (start_month < end_month) {
            montharray.push(new Date(stdt.getFullYear(), stdt.getMonth() + 1, 0).toISOString().substring(0, 10));
            stdt.setMonth(stdt.getMonth() + 1);
            start_month++;
            montharray.push(new Date(stdt.getFullYear(), stdt.getMonth(), 1).toISOString().substring(0, 10));
        }
        montharray.push(endt.toISOString().substring(0, 10));
        return await getResult(montharray, app_id, 'M', p);
    }
}

async function getResult(array: Array<any>, app_id: String, timeUnit: String, p: Number): Promise<any>{
    let sum = 0;
    var arraynull_final: Array<any> = [];
    await db.task(async (t:any) => {
        for (let i = 0; i < array.length; i = i + 2) {
            var array_prefinal: Array<any> = [];
            array_prefinal.push(array[i]);
            t.batch([
                array_prefinal.push((await fetch(array[i], array[i + 1], app_id, 'W', p))[0].theta_sketch_get_estimate)
            ]).Add
            arraynull_final.push(array_prefinal);
        } return t.batch;
    });
    var array_final: Array<any> = [];
    for (let i = 0; i < arraynull_final.length; i++) {
        if (arraynull_final[i][1] != null) {
            array_final.push(arraynull_final[i]);
            sum += arraynull_final[i][1];
        }
    }
    if (timeUnit == 'M') {
        return Math.round((sum / array_final.length) * 10) / 10;
    }
    else if (timeUnit == 'W') {
        return Math.round((sum / (array_final.length + 1)) * 10) / 10;
    }
}

async function dauCount(st_dt: String, end_dt: String, app_id: String, p: Number): Promise<any>{
    return await timeUnits(st_dt, end_dt, app_id, 'D', p);
}

async function wauCount(st_dt: String, end_dt: String, app_id: String, p: Number): Promise<any>{
    var wau_res = {
        "WAU": await timeUnits(st_dt, end_dt, app_id, 'W', p)
    };
    return wau_res;
}

async function mauCount(st_dt: String, end_dt: String, app_id: String, p: Number): Promise<any>{
    var mau_res = {
        "MAU": await timeUnits(st_dt, end_dt, app_id, 'M', p)
    };
    return mau_res;
}

async function userCount(st_dt: String, end_dt: String, app_id: String, p = 3): Promise<any>{
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

let res = userCount('2021-09-02', '2021-09-27', '5bebe93c25d705690ffbc75811');
res.then((result) => {
    console.log(result);
});