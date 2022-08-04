import {IDatabase, IMain} from 'pg-promise';
import {IResult} from 'pg-promise/typescript/pg-subset';
import {users as sql} from '../sql';

export class UsersRepository {
    constructor(private db: IDatabase<any>, private pgp: IMain) {
    }
    
    async cohortCreateTable(values: {firstEvent:String, secondEvent:String, appid:String}): Promise<null> {
        return this.db.none(sql.cohortCreateTable, {
            firstEvent: values.firstEvent,
            secondEvent: values.secondEvent,
            appid: values.appid
        });
    }

    async upsertCohort(values: {firstEvent:String, secondEvent:String, appid:String, timeUnit:string, initialdate:Number, nextdate:Number, year:Number, initialdatecount:Number, nextdatecount:Number}): Promise<null> {
        return this.db.none(sql.upsertCohort, {
            firstEvent: values.firstEvent, 
            secondEvent:values.secondEvent, 
            appid:values.appid,
 	        timeUnit: values.timeUnit,
            initialdate: values.initialdate,
            nextdate: values.nextdate,
            year: values.year,
            initialdatecount: values.initialdatecount,
            nextdatecount: values.nextdatecount
        });
    }

    async dquery(values: {firstEvent:String, secondEvent:String}): Promise<null> {
        return this.db.any(sql.dquery, {
            firstEvent: values.firstEvent,
            secondEvent: values.secondEvent
        });
    }

    async wquery(values: {firstEvent:String, secondEvent:String}): Promise<null> {
        return this.db.any(sql.wquery, {
            firstEvent: values.firstEvent,
            secondEvent: values.secondEvent
        });
    }

    async mquery(values: {firstEvent:String, secondEvent:String}): Promise<null> {
        return this.db.any(sql.mquery, {
            firstEvent: values.firstEvent,
            secondEvent: values.secondEvent
        });
    }
}
