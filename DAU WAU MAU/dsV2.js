const pgp = require('pg-promise')({});
var moment = require('moment');

class DataSketches {

    db;
    constructor() {
        this.db = pgp('postgres://postgres:Password@123@localhost:5432/postgres')
    }

    #timeUnits(start_date, end_date, timeUnit) {
        let startdate = new Date(moment(start_date));
        let enddate = new Date(moment(end_date));

        if (timeUnit == 'D') {
            var dayarray = [];
            for (startdate; startdate <= enddate; startdate.setDate(startdate.getDate() + 1)) {
                dayarray.push(startdate.toUTCString().substring(5, 16));
                dayarray.push(startdate.toUTCString().substring(5, 16));
            }
            return dayarray;
        }

        else if (timeUnit == 'W') {
            var start = moment(start_date),
                end = moment(end_date),
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
            let start_month = parseInt((startdate).toISOString().substring(5, 7));
            let end_month = parseInt((enddate).toISOString().substring(5, 7));

            let start_year = parseInt(startdate.toUTCString().substring(12, 16));
            let end_year = parseInt(enddate.toUTCString().substring(12, 16));

            montharray.push(startdate.toUTCString().substring(5, 16));
            if ((startdate).toUTCString().substring(5, 7) == '01') {
                startdate.setDate(startdate.getDate() + 1);
            }

            while (startdate < enddate) {
                if (start_month == end_month && start_year == end_year) {
                    break;
                }
                if (start_month < end_month || start_year <= end_year) {
                    montharray.push(new Date(startdate.getFullYear(), startdate.getMonth() + 1, 0).toUTCString().substring(5, 16));
                    startdate.setMonth(startdate.getMonth() + 1);
                    start_month++;
                    montharray.push(new Date(startdate.getFullYear(), startdate.getMonth(), 1).toUTCString().substring(5, 16));
                    if (start_month == 12 && start_year != end_year) {
                        montharray.push(new Date(startdate.getFullYear(), startdate.getMonth() + 1, 0).toUTCString().substring(5, 16));
                        startdate.setMonth(startdate.getMonth() + 1);
                        montharray.push(new Date(startdate.getFullYear(), startdate.getMonth(), 1).toUTCString().substring(5, 16));
                        start_month = parseInt((startdate).toISOString().substring(5, 7));
                        start_year = parseInt(startdate.toISOString().substring(0, 4));
                    }
                }
            }
            montharray.push(enddate.toUTCString().substring(5, 16));
            return montharray;
        }
    }

    async #getResult(start_date, end_date, app_id, timeUnit, p) {
        let obj;
        if (timeUnit == 'daywisedau') {
            if (p == 3) {
                obj = await this.db.query(`select dt, theta_sketch_get_estimate(usercount) from datasketches_dailyactiveusers_${app_id} where dt BETWEEN '${start_date}' AND '${end_date}' ORDER BY dt`);
            } else {
                obj = await this.db.query(`select dt, theta_sketch_get_estimate(usercount) from datasketches_dailyactiveusers_${app_id} where dt BETWEEN '${start_date}' AND '${end_date}' AND p = '${p}' ORDER BY dt`);
            }
            var keys = Object.keys(obj);
            const dayarray = [];
            for (var i = 0; i < keys.length; i++) {
                const dayobj = [];
                var flag = 0;
                let item = (obj[i].dt).toISOString().substring(5, 10)
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
            return dayarray;
        }
        else {
            let array = this.#timeUnits(start_date, end_date, timeUnit);
            let sum = 0;
            var arraynull_final = [];
            await this.db.task(async (t) => {
                for (let i = 0; i < array.length; i = i + 2) {
                    var array_temp = [];
                    array_temp.push(array[i]);
                    if (p == 3) {
                        t.batch([
                            array_temp.push((await this.db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${array[i]}' AND '${array[i + 1]}' `))[0].theta_sketch_get_estimate)
                        ]).Add
                    } else {
                        t.batch([
                            array_temp.push((await this.db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${array[i]}' AND '${array[i + 1]}' AND p='${p}' `))[0].theta_sketch_get_estimate)
                        ]).Add
                    }
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

    async userCount(start_date, end_date, app_id, p = 3) {
        let res = {
            "DAU": await this.#getResult(start_date, end_date, app_id, 'D', p),
            "WAU": await this.#getResult(start_date, end_date, app_id, 'W', p),
            "MAU": await this.#getResult(start_date, end_date, app_id, 'M', p),
            "daywisedau": await this.#getResult(start_date, end_date, app_id, 'daywisedau', p)
        };
        return res;
    }

    async upsertTableData(tbl_id) {
        await this.db.task(async (t) => {
            let events_tbl = 'events_' + tbl_id;
            let sketch_tbl = 'datasketches_events_' + tbl_id;
            let activeuser_tbl = 'datasketches_dailyactiveusers_' + tbl_id;
            let stat_tbl = 'datasketches_segmentstats_' + tbl_id;
            this.db.users.declareCursor(events_tbl);
            while (1) {
                let res = await this.db.users.fetchCursor();
                if (res.length == 0) { break; }
                var count = 0;
                await this.db.task((p) => {
                    for (var i = 0; i < res.length; i++) {
                        const arr = [res[i].key, res[i].dt, res[i].did, res[i].segment, res[i].p, sketch_tbl, activeuser_tbl, stat_tbl];
                        p.batch([
                            this.db.func('upsertdatasketchsegment', arr)
                        ]).Add
                        count += 10000;
                    } return p.batch;
                }).then(async (events) => {
                    await this.db.users.closeCursor();
                    console.log(`${count} Rows upserted Successfully!`);
                    console.log('Completed');
                })
                    .catch(async (error) => {
                        await this.db.users.closeCursor();
                        console.log('Error => ', error);
                    });
            }
        });
    }

    async upsertBatchData(data) {
        var keys;
        await this.db.task(async (t) => {
            var obj = data;
            keys = Object.keys(obj);
            for (var i = 0; i < keys.length; i++) {
                const sketch_tbl = 'datasketches_events_' + data[i].appid;
                const activeuser_tbl = 'datasketches_dailyactiveusers_' + data[i].appid;
                const stat_tbl = 'datasketches_segmentstats_' + data[i].appid;
                const arr = [obj[i].key, obj[i].dt, obj[i].did, obj[i].segment, obj[i].p, sketch_tbl, activeuser_tbl, stat_tbl];
                t.batch([
                    this.db.func('upsertdatasketchsegment', arr)
                ]).Add
            }
            return t.batch;
        }).then((res) => {
            console.log("Successful");
            console.log(`${keys.length} Rows upserted Successfully!`);
        })
            .catch((err) => {
                console.log("Error => ", err);
            });
    }
}

var ds = new DataSketches();
let result = ds.userCount('2021-09-02', '2021-09-27', '5bebe93c25d705690ffbc75811');
result.then((result) => {
    console.log(result);
});