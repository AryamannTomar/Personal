import {IDatabase, IMain} from 'pg-promise';
import {IResult} from 'pg-promise/typescript/pg-subset';
import {users as sql} from '../sql';

export class UsersRepository {
    constructor(private db: IDatabase<any>, private pgp: IMain) {
    }
    async initializefetchfunc(): Promise<null> {
        return this.db.none(sql.initializefetchfunc);
    }
    async getdaywisedau(values: {appid:String, startDate:String, endDate:String}): Promise<any> {
        return this.db.any(sql.getdaywisedau, {
            appid:values.appid,
            startDate:values.startDate,
            endDate:values.endDate
        });
    }
    async getdaywisedaup(values: {appid:String, startDate:String, endDate:String, p:Number}): Promise<any> {
        return this.db.any(sql.getdaywisedau, {
            appid:values.appid,
            startDate:values.startDate,
            endDate:values.endDate,
            p:+values.p,
        });
    }
    async getunit(values: {appid:String, timeUnit:String, startDate:String, endDate:String}): Promise<any> {
        return this.db.one(sql.getunit, {
            appid:values.appid,
            timeUnit:values.timeUnit,
            startDate:values.startDate,
            endDate:values.endDate
        });
    }
    async getunitp(values: {appid:String, timeUnit:String, startDate:String, endDate:String, p:Number}): Promise<any> {
        return this.db.one(sql.getunitp, {
            appid:values.appid,
            timeUnit:values.timeUnit,
            startDate:values.startDate,
            endDate:values.endDate,
            p:+values.p
        });
    }
  
}
