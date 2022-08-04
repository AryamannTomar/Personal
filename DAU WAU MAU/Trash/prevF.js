const pgp = require('pg-promise')({});
var moment = require('moment');
const db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');
function timeUnits(startDate, endDate, timeUnit) {
    let startdate = new Date(moment(startDate));
    let enddate = new Date(moment(endDate));

    if (timeUnit == 'D') {
        var dayarray = [];
        for (startdate; startdate <= enddate; startdate.setDate(startdate.getDate() + 1)) {
            dayarray.push(startdate.toUTCString().substring(5, 16));
            dayarray.push(startdate.toUTCString().substring(5, 16));
        }
        return dayarray;
    }

    else if (timeUnit == 'W') {
        var start = moment(startDate),
            end = moment(endDate),
            day = 0;
        var result = [];
        var current = start.clone();
        while (current.day(7 + day).isBefore(end)) {
            result.push(current.clone());
        }
        var array_prefinal = result.map(m => m.format().substring(0, 10));
        var array = [];
        if (startdate.getDay() == 0) {
            array.push(startdate.toUTCString().substring(5, 16));
        }
        for (let i of array_prefinal) {
            array.push(i);
        }
        var sundayarray = [];
        sundayarray.push(startdate.toUTCString().substring(5, 16));
        for (const key of array) {
            let dt = new Date(key);
            const dateCopy = new Date(dt.getTime());
            dateCopy.setDate(dateCopy.getDate() + 1);
            sundayarray.push(dt.toUTCString().substring(5, 16));
            sundayarray.push((dateCopy).toUTCString().substring(5, 16));
        }
        sundayarray.push(enddate.toUTCString().substring(5, 16));
        return sundayarray;
    }

    else if (timeUnit == 'M') {
        const montharray = [];
        let startMonth = parseInt((startdate).toISOString().substring(5, 7));
        let endMonth = parseInt((enddate).toISOString().substring(5, 7));

        let startYear = parseInt(startdate.toUTCString().substring(12, 16));
        let endYear = parseInt(enddate.toUTCString().substring(12, 16));

        montharray.push(startdate.toUTCString().substring(5, 16));
        if ((startdate).toUTCString().substring(5, 7) == '01') {
            startdate.setDate(startdate.getDate() + 1);
        }

        while (startdate < enddate) {
            if (startMonth == endMonth && startYear == endYear) {
                break;
            }
            if (startMonth < endMonth || startYear <= endYear) {
                montharray.push(new Date(startdate.getFullYear(), startdate.getMonth() + 1, 0).toUTCString().substring(5, 16));
                startdate.setMonth(startdate.getMonth() + 1);
                startMonth++;
                montharray.push(new Date(startdate.getFullYear(), startdate.getMonth(), 1).toUTCString().substring(5, 16));
                if (startMonth == 12 && startYear != endYear) {
                    montharray.push(new Date(startdate.getFullYear(), startdate.getMonth() + 1, 0).toUTCString().substring(5, 16));
                    startdate.setMonth(startdate.getMonth() + 1);
                    montharray.push(new Date(startdate.getFullYear(), startdate.getMonth(), 1).toUTCString().substring(5, 16));
                    startMonth = parseInt((startdate).toISOString().substring(5, 7));
                    startYear = parseInt(startdate.toISOString().substring(0, 4));
                }
            }
        }
        montharray.push(enddate.toUTCString().substring(5, 16));
        return montharray;
    }
}

async function getResult(startDate, endDate, appid, timeUnit, p) {
    if (timeUnit == 'daywisedau') {
        if (p == 3) {
            obj = await db.query(`select dt, theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_dailyactiveusers_${appid} where dt BETWEEN '${startDate}' AND '${endDate}' GROUP BY dt ORDER BY dt`);
        } else {
            obj = await db.query(`select dt, theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_dailyactiveusers_${appid} where dt BETWEEN '${startDate}' AND '${endDate}' AND p = '${p}' GROUP BY dt ORDER BY dt`);
        }
        var keys = Object.keys(obj);
        const dayarray = [];
        for (var i = 0; i < keys.length; i++) {
            const dayobj = [];
            let item = (obj[i].dt).toISOString().substring(0, 10)
            dayobj.push(item);
            dayobj.push(obj[i].theta_sketch_get_estimate);
            dayarray.push(dayobj);
        }
        return dayarray;
    }
    else {
        let array = timeUnits(startDate, endDate, timeUnit);
        let sum = 0;
        var arrayNull = [];
        await db.task(async (t) => {
            for (let i = 0; i < array.length; i = i + 2) {
                var arrayTemp = [];
                arrayTemp.push(array[i]);
                if (p == 3) {
                    t.batch([
                        arrayTemp.push((await db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${appid} where dt BETWEEN '${array[i]}' AND '${array[i + 1]}' `))[0].theta_sketch_get_estimate)
                    ]).Add
                } else {
                    t.batch([
                        arrayTemp.push((await db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${appid} where dt BETWEEN '${array[i]}' AND '${array[i + 1]}' AND p='${p}' `))[0].theta_sketch_get_estimate)
                    ]).Add
                }
                arrayNull.push(arrayTemp);
            } return t.batch;
        });
        var arrayFinal = [];
        for (let i = 0; i < arrayNull.length; i++) {
            if (arrayNull[i][1] != null) {
                arrayFinal.push(arrayNull[i]);
                sum += arrayNull[i][1];
            }
        }
        return Math.round((sum / arrayFinal.length) * 10) / 10;
    }
}

async function userCount(startDate, endDate, appid, p = 3) {
    let res = {
        "DAU": await getResult(startDate, endDate, appid, 'D', p),
        "WAU": await getResult(startDate, endDate, appid, 'W', p),
        "MAU": await getResult(startDate, endDate, appid, 'M', p),
        "daywisedau": await getResult(startDate, endDate, appid, 'daywisedau', p)
    };
    return res;
}

console.log(userCount('2021-09-01', '2021-09-30', '5bebe93c25d705690ffbc75811'));