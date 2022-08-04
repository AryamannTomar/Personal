import {IDatabase, IMain} from 'pg-promise';
import {IResult} from 'pg-promise/typescript/pg-subset';
import {users as sql} from '../sql';

export class UsersRepository {
    constructor(private db: IDatabase<any>, private pgp: IMain) {
    }
    async createTable(values: {appid: String}): Promise<null> {
        return this.db.none(sql.createTable, {
            appid: values.appid
        });
    }
}