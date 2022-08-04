import { db } from '../../db';
class DataSketches {
    async getResult(startDate: String, endDate: String, appid: String, timeUnit: String, p: Number) {
        let obj: any;
        if (timeUnit == 'daywisedau') {
            if (p == 3) {
                obj = await db.users.getDayWiseDau({appid: appid, startDate: startDate, endDate: endDate})
            } else {
                obj = await db.users.getDayWiseDauP({appid: appid, startDate: startDate, endDate: endDate, p: p});
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
                obj = await db.users.getCount({appid: appid, timeUnit: timeUnit, startDate: startDate, endDate: endDate})
            } else {
                obj = await db.users.getCountP({appid: appid, timeUnit: timeUnit, startDate: startDate, endDate: endDate, p: p});
            }
            return Math.round((obj.sum / obj.count) * 100) / 100;
        }
    }

    async userCount(startDate: String, endDate: String, appid: String, p = 3) {
        let res = {
            "DAU": await this.getResult(startDate, endDate, appid, 'day', p),
            "WAU": await this.getResult(startDate, endDate, appid, 'week', p),
            "MAU": await this.getResult(startDate, endDate, appid, 'month', p),
            "daywisedau": await this.getResult(startDate, endDate, appid, 'daywisedau', p)
        };
        return res;
    }
}

var ds = new DataSketches();
let result = ds.userCount('2021-09-01', '2021-09-29', '5bebe93c25d705690ffbc75811');
result.then((result) => {
  console.log(result);
});