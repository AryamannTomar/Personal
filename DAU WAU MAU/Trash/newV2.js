const pgp = require('pg-promise')({});
var moment = require('moment');
var db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');


function timeUnits(st_dt, end_dt, timeUnit) {
    if (timeUnit == 'D') {
        var dayarray = [];
        let stdt = new Date(st_dt);
        let endt = new Date(end_dt)
        for (stdt; stdt <= endt; stdt.setDate(stdt.getDate() + 1)) {
            dayarray.push((new Date(stdt)).toISOString().substring(0, 10));
        }
        return dayarray;
    }

    else if (timeUnit == 'W') {
        var start = moment(st_dt),
            end = moment(end_dt),
            day = 0;
        var result = [];
        var current = start.clone();
        while (current.day(7 + day).isBefore(end)) {
            result.push(current.clone());
        }
        var array_prefinal = result.map(m => m.format().substring(0, 10));
        var array = [];
        if (new Date(st_dt).getDay() == 6) {
            array.push(st_dt);
        }
        for (let i of array_prefinal) {
            array.push(i);
        }
        var sundayarray = [];
        sundayarray.push(st_dt);
        for (const key of array) {
            let dt = new Date(key);
            const dateCopy = new Date(dt.getTime());
            dateCopy.setDate(dateCopy.getDate() + 1);
            sundayarray.push(dt.toISOString().substring(0, 10));
            sundayarray.push((dateCopy).toISOString().substring(0, 10));
        }
        sundayarray.push(end_dt);
        return sundayarray;
    }

    else if (timeUnit == 'M') {
        let stdt = new Date(st_dt);
        let endt = new Date(end_dt);
        const montharray = [];
        let start_month = parseInt((stdt).toISOString().substring(5, 7));
        let end_month = parseInt((endt).toISOString().substring(5, 7));
        montharray.push(stdt.toISOString().substring(0, 10));
        if ((stdt).toISOString().substring(8, 10) == '01') {
            stdt.setDate(stdt.getDate() + 1);
        }
        while (start_month < end_month) {
            montharray.push(new Date(stdt.getFullYear(), stdt.getMonth() + 1, 0).toISOString().substring(0, 10));
            stdt.setMonth(stdt.getMonth() + 1);
            start_month++;
            montharray.push(new Date(stdt.getFullYear(), stdt.getMonth(), 1).toISOString().substring(0, 10));
        }
        montharray.push(endt.toISOString().substring(0, 10));
        return montharray;
    }
}

async function getResult(st_dt, end_dt, app_id, timeUnit, p) {
    if (timeUnit == 'daywisedau') {
        let obj = await db.query(`select dt, theta_sketch_get_estimate(usercount) from datasketches_events_${app_id} where dt BETWEEN '${st_dt}' AND '${end_dt}' `)
        var keys = Object.keys(obj);
        const dayarray = [];
        let sum = 0;
        for (var i = 0; i < keys.length; i++) {
            const dayobj = [];
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

        let sorted_dayarray = dayarray.sort((a, b) => {
            return parseInt(a[0].substring(3,)) - parseInt(b[0].substring(3,))
        })
        return sorted_dayarray;
    }
    else {
        let array = timeUnits(st_dt, end_dt, timeUnit);
        let sum = 0;
        var arraynull_final = [];

        if (timeUnit == 'W' || timeUnit == 'M') {
            await db.task(async (t) => {
                for (let i = 0; i < array.length; i = i + 2) {
                    var array_temp = [];
                    array_temp.push(array[i]);
                    t.batch([
                        array_temp.push((await db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${array[i]}' AND '${array[i + 1]}' `))[0].theta_sketch_get_estimate)
                    ]).Add
                    arraynull_final.push(array_temp);
                } return t.batch;
            });
            var array_final = [];
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
        else if (timeUnit == 'D') {
            await db.task(async (t) => {
                for (let i = 0; i < array.length; i++) {
                    var array_temp = [];
                    array_temp.push(array[i]);
                    t.batch([
                        array_temp.push((await db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${array[i]}' AND '${array[i]}' `))[0].theta_sketch_get_estimate)
                    ]).Add
                    arraynull_final.push(array_temp);
                } return t.batch;
            });
            var array_final = [];
            for (let i = 0; i < arraynull_final.length; i++) {
                if (arraynull_final[i][1] != null) {
                    array_final.push(arraynull_final[i]);
                    sum += arraynull_final[i][1];
                }
            }
            return Math.round((sum / array_final.length) * 10) / 10;
        }
    }
}

async function dauCount(st_dt, end_dt, app_id, p) {
    var dau_res = {
        "daywisedau": await getResult(st_dt, end_dt, app_id, 'daywisedau', p),
        "DAU": await getResult(st_dt, end_dt, app_id, 'D', p)
    };
    return dau_res;
}

async function wauCount(st_dt, end_dt, app_id, p) {
    var wau_res = {
        "WAU": await getResult(st_dt, end_dt, app_id, 'W', p)
    };
    return wau_res;
}

async function mauCount(st_dt, end_dt, app_id, p) {
    var mau_res = {
        "MAU": await getResult(st_dt, end_dt, app_id, 'M', p)
    };
    return mau_res;
}
async function userCount(st_dt, end_dt, app_id, p = 3) {
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