import { db } from '../../db';
import { JsonSerializer, throwError } from 'typescript-json-serializer';
import { ActiveUserCount } from '../../models/users/activeusercount';
const defaultSerializer = new JsonSerializer();
const { performance } = require('perf_hooks');

/** 
    * 5 Functions in Class DataSketches (@Constructor (@appid )  
    *   -> appid - is a string used to name 3 tables [datasketches_events_appid,
    *                                              datasketches_dailyactiveusers_appid
    *                                              datasketches_segmentstats_appid]  in postgreSQL 
    *            - is also used to name the cohort analysis table [cohort_guid_appid]
    * 
    *   @UPSERT_THETA_SKETCHES_INSIDE_postgreSQL
    *   -> upsertData      (@tblid )                                           - upsert using Cursors
    *   -> upsertBatchData (@data )                                            - upsert using db.Batch
    * 
    *   @DAU_WAU_MAU_ANALYSIS
    *   -> userCount       (@startDate , @endDate , @appid , @p [optional] )   - returns a ModelObject - Calls getResult function
    * 
    *   @COHORT_ANALYSIS
    *   -> upsertCohort    (@obj , @timeUnit )                                 - upsert CohortData
    *   -> Cohort          (@firstEvent , @secondEvent )                       - performs Cohort Analysis - Calls upsertCount 
*/

export class DataSketches {
    appid: String;
    constructor(appid: String) {
        this.appid = appid;
        db.users.initializeUpsertDataSketchRow();
        db.users.initializeUpsertDataSketchSegment();
        db.users.initializeTables({ appid: this.appid });
    }

    /**
     * 
     * datasketches_events_appid           - is used to store @eventkey , @date ,@usercount (stores theta_sketch instead of did) ,@segment_keys ,@segment_values , @platform (device on which event occured)
     * datasketches_dailyactiveusers_appid - is used to store @date ,@usercount (stores theta_sketch instead of did) ,@platform (device on which event occured)
     * datasketches_segmentstats_appid     - is used to store @eventkey , @date ,@segment_keys ,@segment_values - For removing @Categorical_Attributes from datasketches_events_appid table
    */
    async upsertData() {
        await db.task(async (t: any) => {
            let events_tbl = 'events_' + this.appid;
            let sketch_tbl = 'datasketches_events_' + this.appid;
            let activeuser_tbl = 'datasketches_dailyactiveusers_' + this.appid;
            let stat_tbl = 'datasketches_segmentstats_' + this.appid;
            await t.users.declareCursor({ events_tbl: events_tbl });
            let count = 0;
            var startTime = performance.now();
            while (1) {
                var startTimeBatch = performance.now();
                let res = await t.users.fetchCursor({ count: 1000 });
                if (res.length == 0) { break; }
                await db.task((p: any) => {
                    for (var i = 0; i < res.length; i++) {
                        const arr = [res[i].key, res[i].dt, res[i].did, res[i].segment, res[i].p, sketch_tbl, activeuser_tbl, stat_tbl];
                        p.batch([
                            db.func('upsertdatasketchsegment', arr)
                        ]).Add
                    } return p.batch;
                })
                count++;
                var endTimeBatch = performance.now();
                console.log('in loop:', count);
                console.log(`${(endTimeBatch - startTimeBatch) / (1000)} -- Seconds`);
            }
            await t.users.closeCursor();
            var endTime = performance.now();
            console.log(`${(endTime - startTime) / (60 * 1000)} -- Minutes`);
            console.log('Completed');
        })
    }

    /**
     * 
     * @param data - data is an array consisting of multiple event objects which are then upserted in postgreSQL
     */
    async upsertBatchData(data: any) {
        var keys: any;
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
            console.log(`${keys.length} Rows upserted Successfully!`);
        }).catch((err: any) => {
            console.log("Error => ", err);
        });
    }

    /**
     * 
     * @param startDate   - is the initial Date from which the analysis has to be performed
     * @param endDate     - is the final Date till which the analysis has to be performed
     * @param p[optional] - platform is an optional parameter here which can be given as 0-Android, 1-IOS, 2-Web
     * @returns           - DAU, WAU, MAU and daywisedau array
     */
    async userCount(startDate: String, endDate: String, p = 3) {
        interface res {
            "DAU": any,
            "WAU": any,
            "MAU": any,
            "daywisedau": any
        };
        let res: res = await db.users.getActiveUserCount({ appid: this.appid, startDate: startDate, endDate: endDate, p: p });
        let activeuserinstance = new ActiveUserCount(res.DAU, res.WAU, res.MAU, res.daywisedau);
        let data = defaultSerializer.serialize(activeuserinstance);
        return data;
    }

    /**
     * 
     * @param obj          - is an object consisting of raw data - is simplified and the upserted in the cohort_firstEvent_secondEvent_appid table
     * @param timeUnit     - timeUnit can be [daywisedau, day, week, month] for which the data has to be fetched from postgreSQL
     * @param guid         - A Unique guid used for name the Cohort analysis Table
     * @param appid        - is a string unique to the 3 tables inside postgreSQL - is used to name the cohort analysis table
     */
    private async upsertCohort(obj: any, timeUnit: String, guid: String, appid: String): Promise<any> {
        await db.task(async (t: any) => {
            for (var i = 0; i < Object.keys(obj).length; i++) {
                await t.batch([
                    db.users.upsertCohort({ guid: guid, appid: appid, timeUnit: timeUnit, year: parseInt(obj[i].initialdateformat.split(" ")[0]), initialdate: obj[i].initialdate, nextdate: obj[i].nextdate, initialdateformat: parseInt(obj[i].initialdateformat.split(" ")[1]), nextdateformat: parseInt(obj[i].nextdateformat.split(" ")[1]), platform: obj[i].platform, initialdatecount: parseInt(obj[i].initialdatecount), nextdatecount: parseInt(obj[i].nextdatecount) })
                ]).Add
            }
            return t.batch;
        })
    }

    /**
     * 
     * @param firstEvent   - is the initial Event for which the Cohort analysis has to be performed
     * @param secondEvent  - is the second Event for which the Cohort analysis has to be performed
     * @returns            - String = "CohortData upserted successfully in cohort_guid_appid Table"
     */
    async Cohort(firstEvent: String, secondEvent: String): Promise<any> {
        function createGuid() {
            return 'xxxxxxxxxxxx4xxxyxxxxxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
                var r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
                return v.toString(16);
            });
        }
        var guid = createGuid();
        await db.users.createCohortTable({ guid: guid, appid: this.appid });
        await db.users.dailyCohort({ firstEvent: firstEvent, secondEvent: secondEvent, appid: this.appid }).then((obj: any) => { this.upsertCohort(obj, 'D', guid, this.appid) });
        await db.users.weeklyCohort({ firstEvent: firstEvent, secondEvent: secondEvent, appid: this.appid }).then((obj: any) => { this.upsertCohort(obj, 'W', guid, this.appid) });
        await db.users.monthlyCohort({ firstEvent: firstEvent, secondEvent: secondEvent, appid: this.appid }).then((obj: any) => { this.upsertCohort(obj, 'M', guid, this.appid) });
        return `CohortData upserted successfully in cohort_${guid}_${this.appid} Table`;
    }
}
var ds= new DataSketches('5bebe93c25d705690ffbc75811');
ds.upsertData()