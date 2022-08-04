import { JsonSerializer, throwError } from 'typescript-json-serializer';
import { ActiveUserCount } from '../../models/users/activeusercount';
const defaultSerializer = new JsonSerializer();
var moment = require('moment');
import { db } from '../../db';
import { users } from '../../db/sql/index';
const logger = require('../../utils/logger');
const log = logger.createLogger();

/** 7 Functions inside Class - DataSketches 
    *   -> timeUnits (startDate, endDate, timeUnit)             - returns array of pairs consisting of days/ weeks /months
    *   -> getResult (startDate, endDate, appid, timeUnit, p)  - returns DAU , WAU , MAU , daywisedau - Calls timeUnits function
    *   -> userCount (startDate, endDate, appid, p[optional])   - returns a ModelObject - Calls getResult function
    *   -> upsertTableData (tblid)                             - upsert using Cursors
    *   -> upsertBatchData (data)                               - upsert using db.batch
    *   -> upsertCohort (obj, timeUnit)                         - upsert CohortData
    *   -> Cohort (firstEvent, secondEvent)                     - performs Cohort Analysis - Calls upsertCount 
    * 
*/

class DataSketches {

    /**
    * timeUnits (@startDate , @endDate , @timeUnit ) 
    * -> Based on the timeUnit character, this function returns an array consisting of required dates  
    */

    private timeUnits(startDate: String, endDate: String, timeUnit: String): any {
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

    /**
    * getResult (@startDate , @endDate , @appid , @timeUnit , @p ) 
    * -> All the Queries for computing DAU, WAU and MAU are called from this function,
    * -> This basically return the final answer based on the timeUnit  
    */
    private async getResult(startDate: String, endDate: String, appid: String, timeUnit: String, p: Number): Promise<any> {
        let obj: any;
        if (timeUnit == 'daywisedau') {
            if (p == 3) {
                obj = await db.users.dayWiseDau(appid, startDate, endDate);
            } else {
                obj = await db.users.dayWiseDauWithp(appid, startDate, endDate, p);
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
            let array: Array<any> = this.timeUnits(startDate, endDate, timeUnit)!;
            let sum = 0;
            var arrayNull: Array<any> = [];
            await db.task(async (t: any) => {
                for (let i = 0; i < array.length; i = i + 2) {
                    var arrayTemp: Array<any> = [];
                    arrayTemp.push(array[i]);
                    if (p == 3) {
                        t.batch([
                            arrayTemp.push((await db.users.count(appid, array[i], array[i + 1]))[0].theta_sketch_get_estimate)
                        ]).Add
                    } else {
                        t.batch([
                            arrayTemp.push((await db.users.countWithp(appid, array[i], array[i + 1], p)[0].theta_sketch_get_estimate))
                        ]).Add
                    }
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

    /**
    * userCount ( @startDate , @endDate , @appid , @p [optional]) 
    * -> Calls the getResult Function to get the final result
    * -> TimeUnits are taken such that DAU - 'D', WAU - 'W', MAU - 'M', daywisedau - 'daywisedau'   
    */
    async userCount(startDate: String, endDate: String, appid: String, p = 3): Promise<any> {
        let res = {
            "DAU": await this.getResult(startDate, endDate, appid, 'D', p),
            "WAU": await this.getResult(startDate, endDate, appid, 'W', p),
            "MAU": await this.getResult(startDate, endDate, appid, 'M', p),
            "daywisedau": await this.getResult(startDate, endDate, appid, 'daywisedau', p)
        };

        let activeuserinstance = new ActiveUserCount(res.DAU, res.WAU, res.MAU, res.daywisedau);
        let data = defaultSerializer.serialize(activeuserinstance);
        return data;
    }

    /** 
    * upsertTableData (@tblid or appid) 
    * -> It Upserts data Via Cursors  
    * -> It takes RawInput from the events_(appid) Table and then upserts the data in @postgreSQL Tables
    * -> We Fetch 10000 Rows from the source Table through datasketch_cursor and then upsert the data in the 3 postregreSQL tables 
    */

    async upsertTableData(tblid: String) {
        await db.task(async (t: any) => {
            let events_tbl = 'events_' + tblid;
            let sketch_tbl = 'datasketches_events_' + tblid;
            let activeuser_tbl = 'datasketches_dailyactiveusers_' + tblid;
            let stat_tbl = 'datasketches_segmentstats_' + tblid;
            db.users.declareCursor(events_tbl);
            while (1) {
                let res = await db.users.fetchCursor();
                if (res.length == 0) { break; }
                var count = 0;
                await db.task((p: any) => {
                    for (var i = 0; i < res.length; i++) {
                        const arr = [res[i].key, res[i].dt, res[i].did, res[i].segment, res[i].p, sketch_tbl, activeuser_tbl, stat_tbl];
                        p.batch([
                            db.func('upsertdatasketchsegment', arr)
                        ]).Add
                        count += 10000;
                    } return p.batch;
                }).then(async (events: any) => {
                    await db.users.closeCursor();
                    log.info(`${count} Rows upserted Successfully!`);
                })
                    .catch(async (error: any) => {
                        await db.users.closeCursor();
                        log.info('Error => ', error);
                    });
            }
        });
    }
    /** 
    * upsertBatchData (@data - Array consisting of event objects)
    * -> Upserts Batch Data in postgreSQL Tables
    */
    async upsertBatchData(data: any) {
        var keys;
        await db.task(async (t: any) => {
            var obj = data;
            keys = Object.keys(obj);
            for (var i = 0; i < keys.length; i++) {
                const sketch_tbl = 'datasketches_events_' + data[i].appid;
                const activeuser_tbl = 'datasketches_dailyactiveusers_' + data[i].appid;
                const stat_tbl = 'datasketches_segmentstats_' + data[i].appid;
                const arr = [obj[i].key, obj[i].dt, obj[i].did, obj[i].segment, obj[i].p, sketch_tbl, activeuser_tbl, stat_tbl];
                t.batch([
                    db.func('upsertdatasketchsegment', arr)
                ]).Add
            }
            return t.batch;
        }).then((res: any) => {
            log.info(`${keys.length} Rows upserted Successfully!`);
        })
            .catch((err: any) => {
                log.info("Error => ", err);
            });
    }

    /**
    * upsertCohort (@obj , @timeUnit ) 
    * -> upserts CohortData  
    */
    private async upsertCohort(obj: any, timeUnit: String): Promise<any> {
        await db.task(async (t:any) => {
            for (var i = 0; i < Object.keys(obj).length; i++) {
                await t.batch([
                    db.users.upsertCohort(timeUnit, parseInt(obj[i].initialdate.split(" ")[1]), parseInt(obj[i].nextdate.split(" ")[1]), parseInt(obj[i].initialdate.split(" ")[0]), obj[i].initialdatecount, obj[i].nextdatecount)
                ]).Add
            }
            return t.batch;
        })
    }
    
    /**
    * Cohort (@firstEvent , @secondEvent ) 
    * -> performs Cohort Analysis  
    */
    async Cohort(firstEvent: String, secondEvent: String): Promise<any>{
    var dquery = db.users.dquery(firstEvent, secondEvent); 
    var mquery = db.users.mquery(firstEvent, secondEvent);
    var wquery = db.users.wquery(firstEvent, secondEvent);
 
    await this.upsertCohort(dquery, 'D');
    await this.upsertCohort(wquery, 'W');
    await this.upsertCohort(mquery, 'M');
    return "CohortData Upserted Sucessfully!";
}}
 
module.exports = DataSketches;