import { db } from '../../db';
var moment = require('moment');

function timeUnits(startDate: String, endDate: String, timeUnit: String): any {
    let startdate = new Date(moment(startDate));
    let enddate = new Date(moment(endDate));

    if (timeUnit == 'D') {
        var dayarray: Array<any> = [];
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
        var result: Array<any> = [];
        var current = start.clone();
        while (current.day(7 + day).isBefore(end)) {
            result.push(current.clone());
        }
        var array_prefinal = result.map(m => m.format().substring(0, 10));
        var array: Array<any> = [];
        if (startdate.getDay() == 0) {
            array.push(startdate.toUTCString().substring(5, 16));
        }
        for (let i of array_prefinal) {
            array.push(i);
        }
        var sundayarray: Array<any> = [];
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
        const montharray: Array<any> = [];
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

async function getResult(startDate: String, endDate: String, appid: String, timeUnit: String, p: Number): Promise<any> {
    let obj: any;
    if (timeUnit == 'daywisedau') {
        obj = await db.users.dau({appid: appid, startDate: startDate, endDate:endDate});
        var keys = Object.keys(obj);
        const dayarray: any = [];
        for (var i = 0; i < keys.length; i++) {
            const dayobj: any = [];
            let item = (obj[i].dt).toISOString().substring(0, 10)
            dayobj.push(item);
            dayobj.push(obj[i].theta_sketch_get_estimate);
            dayarray.push(dayobj);
        }
        return dayarray;
    }
    else {
        let array: Array<any> = timeUnits(startDate, endDate, timeUnit)!;
        let sum = 0;
        var arrayNull: Array<any> = [];
        await db.task(async (t: any) => {
            for (let i = 0; i < array.length; i = i + 2) {
                var arrayTemp: Array<any> = [];
                arrayTemp.push(array[i]);
                    t.batch([
                        arrayTemp.push((await db.users.wau({appid: appid, startDate: array[i], endDate: array[i+1]}))[0].theta_sketch_get_estimate)
                    ]).Add
                arrayNull.push(arrayTemp);
            } return t.batch;
        });
        var arrayFinal: Array<any> = [];
        for (let i = 0; i < arrayNull.length; i++) {
            if (arrayNull[i][1] != null) {
                arrayFinal.push(arrayNull[i]);
                sum += arrayNull[i][1];
            }
        }
        return Math.round((sum / arrayFinal.length) * 10) / 10;
    }
}

async function userCount(startDate: String, endDate: String, appid: String, p = 3): Promise<any> {
    let res = {
        "DAU": await getResult(startDate, endDate, appid, 'D', p),
        "WAU": await getResult(startDate, endDate, appid, 'W', p),
        "MAU": await getResult(startDate, endDate, appid, 'M', p),
        "daywisedau": await getResult(startDate, endDate, appid, 'daywisedau', p)
    };
    return res;
}

let result = userCount('2021-09-02', '2021-09-27', '5bebe93c25d705690ffbc75811');
result.then((result) => {
    console.log(result);
});