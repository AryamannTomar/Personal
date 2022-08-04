import { IDatabase, IMain } from 'pg-promise';
import { IResult } from 'pg-promise/typescript/pg-subset';
import { users as sql } from '../sql';

export class UsersRepository {
    constructor(private db: IDatabase<any>, private pgp: IMain) {
    }

    // Create upsertdatasketchrow function;
    async initializeUpsertDataSketchRow(): Promise<null> {
        return this.db.none(sql.initializeUpsertDataSketchRow);
    }

    // Create upsertdatasketchsegment function;
    async initializeUpsertDataSketchSegment(): Promise<null> {
        return this.db.none(sql.initializeUpsertDataSketchSegment);
    }

    // Create processdatasketch procedure;
    async initializeProcessDataSketch(): Promise<null> {
        return this.db.none(sql.initializeProcessDataSketch);
    }

    // Creates required tables taking parameter: app_id as the ending table_name;
    async initializeTables(values: { appid: String }): Promise<null> {
        return this.db.none(sql.initializeTables, {
            appid: values.appid
        });
    }

    // Declare datasketch_cursor;
    async declareCursor(values: { events_tbl: String }): Promise<null> {
        return this.db.none(sql.declareCursor, {
            events_tbl: values.events_tbl
        });
    }

    // Fetch from datasketch_cursor;
    async fetchCursor(values: { count: Number }): Promise<any> {
        return this.db.any(sql.fetchCursor, {
            count: +values.count
        });
    }

    // Close datasketch_cursor;
    async closeCursor(): Promise<null> {
        return this.db.none(sql.closeCursor);
    }

    //get DauWauMauCount using [getCount, getCountP, getDayWiseDau, getDayWiseDauP].sql files
    async getActiveUserCount(values: { appid: String, startDate: String, endDate: String, p: Number }): Promise<any> {
        let timeArray: Array<String> = ['day', 'week', 'month', 'daywisedau']
        let queries: Array<any> = [];
        let array: Array<any> = [];
        let daywisedau: Array<any> = [];
        if (values.p == 3) {
            array.push(await this.db.task(async (t: any) => {
                for (let i = 0; i < timeArray.length - 1; i++) {
                    queries.push(t.one(sql.getCount, {
                        timeUnit: timeArray[i],
                        appid: values.appid,
                        startDate: values.startDate,
                        endDate: values.endDate
                    }))
                }
                queries.push(t.any(sql.getDayWiseDau, {
                    appid: values.appid,
                    startDate: values.startDate,
                    endDate: values.endDate
                }));
                return t.batch(queries);
            }))
        } else {
            array.push(await this.db.task(async (t: any) => {
                for (let i = 0; i < timeArray.length - 1; i++) {
                    queries.push(t.one(sql.getCountP, {
                        timeUnit: timeArray[i],
                        appid: values.appid,
                        startDate: values.startDate,
                        endDate: values.endDate,
                        p: +values.p
                    }))
                }
                queries.push(t.any(sql.getDayWiseDauP, {
                    appid: values.appid,
                    startDate: values.startDate,
                    endDate: values.endDate,
                    p: +values.p
                }));
                return t.batch(queries);
            }))
        }
        for (let i of array[0][3]) {
            daywisedau.push([i.concat.split(' ')[0], parseInt(i.concat.split(' ')[1])]);
        };
        let obj = {
            "DAU": Math.round((array[0][0].sum / array[0][0].count) * 100) / 100,
            "WAU": Math.round((array[0][1].sum / array[0][1].count) * 100) / 100,
            "MAU": Math.round((array[0][2].sum / array[0][2].count) * 100) / 100,
            "daywisedau": daywisedau
        };
        return obj;
    }

    // Create Cohort Table
    async createCohortTable(values: { guid: String, appid: String }): Promise<any> {
        return this.db.none(sql.createCohortTable, {
            guid: values.guid,
            appid: values.appid
        });
    }

    // Upsert Cohort Values in Cohort Table;
    async upsertCohort(values: { guid: String, appid: String, timeUnit: String, year: Number, initialdate: String, nextdate: String, initialdateformat: Number, nextdateformat: Number, platform: Number, initialdatecount: Number, nextdatecount: Number }): Promise<null> {
        return this.db.none(sql.upsertCohort, {
            guid: values.guid,
            appid: values.appid,
            timeUnit: values.timeUnit,
            year: +values.year,
            initialdate: values.initialdate,
            nextdate: values.nextdate,
            initialdateformat: +values.initialdateformat,
            nextdateformat: +values.nextdateformat,
            platform: +values.platform,
            initialdatecount: +values.initialdatecount,
            nextdatecount: +values.nextdatecount
        });
    }

    // Fetch daywise cohort analysis query;
    async dailyCohort(values: { firstEvent: String, secondEvent: String, appid: String }): Promise<any> {
        return this.db.any(sql.dailyCohort, {
            firstEvent: values.firstEvent,
            secondEvent: values.secondEvent,
            appid: values.appid
        });
    }

    // Fetch weekwise cohort analysis query;
    async weeklyCohort(values: { firstEvent: String, secondEvent: String, appid: String }): Promise<any> {
        return this.db.any(sql.weeklyCohort, {
            firstEvent: values.firstEvent,
            secondEvent: values.secondEvent,
            appid: values.appid
        });
    }

    // Fetch monthwise cohort analysis query;
    async monthlyCohort(values: { firstEvent: String, secondEvent: String, appid: String }): Promise<any> {
        return this.db.any(sql.monthlyCohort, {
            firstEvent: values.firstEvent,
            secondEvent: values.secondEvent,
            appid: values.appid
        });
    }
}
