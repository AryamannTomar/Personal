import {IDatabase, IMain} from 'pg-promise';
import {IResult} from 'pg-promise/typescript/pg-subset';
import {users as sql} from '../sql';

export class UsersRepository {
    constructor(private db: IDatabase<any>, private pgp: IMain) {
    }

    // Fetch daywisedauCount Without value of platform;
    async getDayWiseDau(values: {appid:String, startDate:String, endDate:String}): Promise<any> {
        return this.db.any(sql.getDayWiseDau, {
            appid:values.appid,
            startDate:values.startDate,
            endDate:values.endDate
        });
    }

    // Fetch daywisedauCount With value of platform;
    async getDayWiseDauP(values: {appid:String, startDate:String, endDate:String, p:Number}): Promise<any> {
        return this.db.any(sql.getDayWiseDauP, {
            appid:values.appid,
            startDate:values.startDate,
            endDate:values.endDate,
            p:+values.p,
        });
    }

    // Fetch [Dau, WAU, MAU] Count Without value of platform;
    async getCount(values: {appid:String, timeUnit:String, startDate:String, endDate:String}): Promise<any> {
        return this.db.one(sql.getCount, {
            appid:values.appid,
            timeUnit:values.timeUnit,
            startDate:values.startDate,
            endDate:values.endDate
        });
    }

    // Fetch [Dau, WAU, MAU] Count With value of platform;
    async getCountP(values: {appid:String, timeUnit:String, startDate:String, endDate:String, p:Number}): Promise<any> {
        return this.db.one(sql.getCountP, {
            appid:values.appid,
            timeUnit:values.timeUnit,
            startDate:values.startDate,
            endDate:values.endDate,
            p:+values.p
        });
    }
}