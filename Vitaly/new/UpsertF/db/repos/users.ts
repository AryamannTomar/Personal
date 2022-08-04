import {IDatabase, IMain} from 'pg-promise';
import {IResult} from 'pg-promise/typescript/pg-subset';
import {users as sql} from '../sql';

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
    async initializeTables(values: {appid: String}): Promise<null> {
        return this.db.none(sql.initializeTables, {
            appid: values.appid
        });
    }
    
    // Declare datasketch_cursor;
    async declareCursor(values: {events_tbl: String}): Promise<null> {
        return this.db.none(sql.declareCursor, {
            events_tbl: values.events_tbl
        });
    }

    // Fetch from datasketch_cursor;
    async fetchCursor(values: {count: Number}): Promise<any> {
        return this.db.any(sql.fetchCursor, {
            count: +values.count
        });
    }

    // Close datasketch_cursor;
    async closeCursor(): Promise<null> {
        return this.db.none(sql.closeCursor);
    }
}