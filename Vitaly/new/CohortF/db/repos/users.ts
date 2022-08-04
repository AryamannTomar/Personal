import {IDatabase, IMain} from 'pg-promise';
import {IResult} from 'pg-promise/typescript/pg-subset';
import {users as sql} from '../sql';

export class UsersRepository {
    constructor(private db: IDatabase<any>, private pgp: IMain) {
    }
    // Create Cohort Table
    async createCohortTable(values: {guid:String, appid:String}): Promise<any> {
        return this.db.none(sql.createCohortTable, {
            guid: values.guid,
            appid: values.appid
        });
    }

    // Upsert Cohort Values in Cohort Table;
    async upsertCohort(values: {guid:String, appid:String, timeUnit:String, year:Number, initialdate:String, nextdate:String, initialdateformat:Number, nextdateformat:Number, platform: Number, initialdatecount:Number, nextdatecount:Number}): Promise<null> {
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
    async dailyCohort(values: {firstEvent:String, secondEvent:String, appid: String}): Promise<any> {
        return this.db.any(sql.dailyCohort, {
            firstEvent: values.firstEvent,
            secondEvent: values.secondEvent,
            appid: values.appid
        });
    }

    // Fetch weekwise cohort analysis query;
    async weeklyCohort(values: {firstEvent:String, secondEvent:String, appid: String}): Promise<any> {
        return this.db.any(sql.weeklyCohort, {
            firstEvent: values.firstEvent,
            secondEvent: values.secondEvent,
            appid: values.appid
        });
    }

    // Fetch monthwise cohort analysis query;
    async monthlyCohort(values: {firstEvent:String, secondEvent:String, appid: String}): Promise<any> {
        return this.db.any(sql.monthlyCohort, {
            firstEvent: values.firstEvent,
            secondEvent: values.secondEvent,
            appid: values.appid
        });
    }
}