import {IDatabase, IMain} from 'pg-promise';
import {IResult} from 'pg-promise/typescript/pg-subset';
import {users as sql} from '../sql';

export class UsersRepository {
    constructor(private db: IDatabase<any>, private pgp: IMain) {
    }
    async dau(values: {appid:String, startDate:String, endDate:String}): Promise<null> {
        return this.db.any(sql.dau, {
            appid:values.appid,
            startDate:values.startDate,
            endDate:values.endDate
        });
    }

    async wau(values: {appid:String, startDate:String, endDate:String}): Promise<null> {
        return this.db.any(sql.wau, {
            appid:values.appid,
            startDate:values.startDate,
            endDate:values.endDate
        });
    }
  
}
