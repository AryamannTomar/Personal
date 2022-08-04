const pgp = require('pg-promise')({});
var moment = require('moment');

class DataSketches {

    db;
    constructor() {
        this.db = pgp('postgres://postgres:Password@123@localhost:5432/postgres')
    }

    /** 6 Functions(FOR FETCHING O/P) - timeUnits(start_date, end_date, app_id, timeUnit)   - returns array of days/ weeks /months
    *                                   getResult(array, app_id, timeUnit)                  - returns WAU, MAU
    *                                   dauCount(start_date, end_date, appid, p)            - returns DAU and daywisedau
    *                                   wauCount(start_date, end_date, appid, p)            - returns WAU
    *                                   mauCount(start_date, end_date, appid, p)            - returns MAU
    *                                   userCount(start_date, end_date, appid, p[optional]) - returns DAU, WAU, MAU and daywisedau result
    *  We get the dau count, mau count, daywisedau count from getDAU() and wau count, Sundays falling in-between the start_date and end_date from getWAU() 
    */

    #timeUnits(start_date: String, end_date: String, timeUnit: String): any {
        let startdate = new Date(moment(start_date));
        let enddate = new Date(moment(end_date));

        if (timeUnit == 'D') {
            var dayarray: Array<any> = [];
            for (startdate; startdate <= enddate; startdate.setDate(startdate.getDate() + 1)) {
                dayarray.push(startdate.toUTCString().substring(5, 16));
            }
            return dayarray;
        }

        else if (timeUnit == 'W') {
            var start = moment(start_date),
                end = moment(end_date),
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

    async #getResult(start_date: String, end_date: String, app_id: String, timeUnit: String, p: Number): Promise<any> {
        let obj: any;
        if (timeUnit == 'daywisedau') {
            if (p == 3) {
                obj = await this.db.query(`select dt, theta_sketch_get_estimate(usercount) from datasketches_dailyactiveusers_${app_id} where dt BETWEEN '${start_date}' AND '${end_date}' `);
            } else {
                obj = await this.db.query(`select dt, theta_sketch_get_estimate(usercount) from datasketches_dailyactiveusers_${app_id} where dt BETWEEN '${start_date}' AND '${end_date}' AND p = '${p}' `);
            }
            var keys = Object.keys(obj);
            const dayarray: Array<any> = [];
            for (var i = 0; i < keys.length; i++) {
                const dayobj: Array<any> = [];
                var flag = 0;
                let item = (obj[i].dt).toUTCString().substring(5, 16);
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
                let m= new Date(a[0]).toISOString().substring(5, 10);
                let n= new Date(b[0]).toISOString().substring(5, 10);
                return parseInt(m.substring(3,)) - parseInt(n.substring(3,))
            })
            
            return sorted_dayarray;
        }
        else {
            let array: Array<any> = this.#timeUnits(start_date, end_date, timeUnit)!;
            let sum = 0;
            var arraynull_final: Array<any> = [];

            if (timeUnit == 'W' || timeUnit == 'M') {
                if (p == 3) {
                    await this.db.task(async (t: any) => {
                        for (let i = 0; i < array.length; i = i + 2) {
                            var array_temp: Array<any> = [];
                            array_temp.push(array[i]);
                            t.batch([
                                array_temp.push((await this.db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${array[i]}' AND '${array[i + 1]}' `))[0].theta_sketch_get_estimate)
                            ]).Add
                            arraynull_final.push(array_temp);
                        } return t.batch;
                    });
                }
                else {
                    await this.db.task(async (t: any) => {
                        for (let i = 0; i < array.length; i = i + 2) {
                            var array_temp: Array<any> = [];
                            array_temp.push(array[i]);
                            t.batch([
                                array_temp.push((await this.db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${array[i]}' AND '${array[i + 1]}' AND p='${p}' `))[0].theta_sketch_get_estimate)
                            ]).Add
                            arraynull_final.push(array_temp);
                        } return t.batch;
                    });
                }
                var array_final: Array<any> = [];
                for (let i = 0; i < arraynull_final.length; i++) {
                    if (arraynull_final[i][1] != null) {
                        array_final.push(arraynull_final[i]);
                        sum += arraynull_final[i][1];
                    }
                }
                return Math.round((sum / array_final.length) * 10) / 10;
            }
            else if (timeUnit == 'D') {
                if (p == 3) {
                    await this.db.task(async (t: any) => {
                        for (let i = 0; i < array.length; i++) {
                            var array_temp: Array<any> = [];
                            array_temp.push(array[i]);
                            t.batch([
                                array_temp.push((await this.db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${array[i]}' AND '${array[i]}' `))[0].theta_sketch_get_estimate)
                            ]).Add
                            arraynull_final.push(array_temp);
                        } return t.batch;
                    });
                } else {
                    await this.db.task(async (t: any) => {
                        for (let i = 0; i < array.length; i++) {
                            var array_temp: Array<any> = [];
                            array_temp.push(array[i]);
                            t.batch([
                                array_temp.push((await this.db.query(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_${app_id} where dt BETWEEN '${array[i]}' AND '${array[i]}' AND p='${p}' `))[0].theta_sketch_get_estimate)
                            ]).Add
                            arraynull_final.push(array_temp);
                        } return t.batch;
                    });
                }
                var array_final: Array<any> = [];
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

    async userCount(start_date: String, end_date: String, app_id: String, p = 3): Promise<any> {
        let res = {
            "DAU": await this.#getResult(start_date, end_date, app_id, 'D', p),
            "WAU": await this.#getResult(start_date, end_date, app_id, 'W', p),
            "MAU": await this.#getResult(start_date, end_date, app_id, 'M', p),
            "daywisedau": await this.#getResult(start_date, end_date, app_id, 'daywisedau', p)
        };
        return res;
    }

    /** 
    * 1 Function(FOR UPSERTING VIA CURSORS) - upsertData(tbl_id); Takes Raw input from the table events_{tbl_id} and then upserts it data into 3 desired tables in postgreSQL
    * We Fetch in a batch of 1000 from the test_cursor and then upsert the data into the desired tables by calling the function UpsertDataSketchSegment
    */

    async upsertTableData(tbl_id: String) {
        await this.db.task(async (t: any) => {
            let events_tbl = 'events_' + tbl_id;
            let sketch_tbl = 'datasketches_events_' + tbl_id;
            let activeuser_tbl = 'datasketches_dailyactiveusers_' + tbl_id;
            let stat_tbl = 'datasketches_segmentstats_' + tbl_id;
            this.db.users.declareCursor(events_tbl);
            while (1) {
                let res = await this.db.users.fetchCursor();
                if (res.length == 0) { break; }
                var count = 0;
                await this.db.task((p: any) => {
                    for (var i = 0; i < res.length; i++) {
                        const arr = [res[i].key, res[i].dt, res[i].did, res[i].segment, res[i].p, sketch_tbl, activeuser_tbl, stat_tbl];
                        p.batch([
                            this.db.func('upsertdatasketchsegment', arr)
                        ]).Add
                        count += 10000;
                    } return p.batch;
                }).then(async (events: any) => {
                    await this.db.users.closeCursor();
                    console.log(`${count} Rows upserted Successfully!`);
                    console.log('Completed');
                })
                    .catch(async (error: any) => {
                        await this.db.users.closeCursor();
                        console.log('Error => ', error);
                    });
            }
        });
    }
    /** 
    * 1 Function(FOR UPSERTING A BATCH) - This function takes an events array and does an upsert using Batch 
    */
    async upsertBatchData(data: any) {
        var keys;
        await this.db.task(async (t: any) => {
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
        }).then((res: any) => {
            console.log("Successful");
            console.log(`${keys.length} Rows upserted Successfully!`);
        })
            .catch((err: any) => {
                console.log("Error => ", err);
            });
    }
}

module.exports = DataSketches;