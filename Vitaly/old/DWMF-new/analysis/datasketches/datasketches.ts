import { db } from '../../db';
import { JsonSerializer, throwError } from 'typescript-json-serializer';
import { ActiveUserCount } from '../../models/users/activeusercount';
const defaultSerializer = new JsonSerializer();
async function getResult(startDate: String, endDate: String, appid: String, timeUnit: String, p: Number) {
    let obj: any;
    if (timeUnit == 'daywisedau') {
        if (p == 3) {
            obj = await db.users.getdaywisedau({appid: appid, startDate: startDate, endDate: endDate})
        } else {
            obj = await db.users.getdaywisedaup({appid: appid, startDate: startDate, endDate: endDate, p: p});
        }
        var keys = Object.keys(obj);
        const dayarray: Array<any> = [];
        for (var i = 0; i < keys.length; i++) {
            const dayobj: Array<any> = [];
            let item = (obj[i].dt).toISOString().substring(0, 10)
            dayobj.push(item);
            dayobj.push(obj[i].theta_sketch_get_estimate);
            dayarray.push(dayobj);
        }
        return dayarray;
    } else {
        if (p == 3) {
            obj = await db.users.getunit({appid: appid, timeUnit: timeUnit, startDate: startDate, endDate: endDate})
        } else {
            obj = await db.users.getunitp({appid: appid, timeUnit: timeUnit, startDate: startDate, endDate: endDate, p: p});
        }
        return Math.round((obj.sum / obj.count) * 100) / 100;
    }
}

async function userCount(startDate: String, endDate: String, appid: String, p = 3) {
    await db.users.initializefetchfunc();
    let res = {
        "DAU": await getResult(startDate, endDate, appid, 'day', p),
        "WAU": await getResult(startDate, endDate, appid, 'week', p),
        "MAU": await getResult(startDate, endDate, appid, 'month', p),
        "daywisedau": await getResult(startDate, endDate, appid, 'daywisedau', p)
    };
    let activeuserinstance = new ActiveUserCount(res.DAU, res.WAU, res.MAU, res.daywisedau);
    let data = defaultSerializer.serialize(activeuserinstance);
    return data;
}

let result = userCount('2021-09-01', '2021-09-30', '5bebe93c25d705690ffbc75811');
result.then((result) => {
    console.log(result);
});
