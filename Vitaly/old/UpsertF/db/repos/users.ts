import {IDatabase, IMain} from 'pg-promise';
import {IResult} from 'pg-promise/typescript/pg-subset';
import {users as sql} from '../sql';

export class UsersRepository {
    constructor(private db: IDatabase<any>, private pgp: IMain) {
    }
    
    async declareCursor(values: {events_tbl: string}): Promise<null> {
        return this.db.none(sql.declareCursor, {
            events_tbl: values.events_tbl
        });
    }

    async fetchCursor(values: {count: Number}): Promise<null> {
        return this.db.any(sql.fetchCursor, {
            count: +values.count
        });
    }

    async closeCursor(): Promise<null> {
        return this.db.none(sql.closeCursor);
    }
}