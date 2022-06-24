import {IDatabase, IMain} from 'pg-promise';
import {IResult} from 'pg-promise/typescript/pg-subset';
import {users as sql} from '../sql';

export class UsersRepository {
  
    constructor(private db: IDatabase<any>, private pgp: IMain) {
    }
    
    // Create upsertdatasketchsegment function;
    async createUpsertDataSketchSegment(): Promise<any> {
        return this.db.any(sql.createsegment);
    }

    // Create upsertdatasketchrow function;
    async createUpsertDataSketchRow(): Promise<any> {
        return this.db.any(sql.createrow);
    }

    // Create processdatasketch procedure;
    async createProcessDataSketch(): Promise<any> {
        return this.db.any(sql.createprocess);
    }

    // Creates required tables taking parameter: app_id as the ending table_name;
    async createTables(values: {app_id:String}): Promise<null> {
        return this.db.none(sql.createtables, {
            app_id:values.app_id
        });
    }
    
    // Call upsertdatasketchsegment function;
    async upsertDataSketchSegment(values: {key_input:String, dt_input:string, did_input:string, segment:JSON, p_input:string, sketch_tbl:string, activeuser_tbl:string, stat_tbl:string}): Promise<null> {
        return this.db.none(sql.segment, {
            key_input:values.key_input,
            dt_input:values.dt_input,
            did_input:values.did_input,
            segment:values.segment,
            p_input:values.p_input,
            sketch_tbl:values.sketch_tbl,
            activeuser_tbl:values.activeuser_tbl,
            stat_tbl:values.stat_tbl
        });
    }

    // Call upsertdatasketchrow function;
    async upsertDataSketchRow(values: {key_input:string, dt_input:string, did_input:string, skey_input:string, sval_input:string, p_input:number, sketch_tbl:string}): Promise<null> {
        return this.db.none(sql.row, {
            key_input:values.key_input,
            dt_input:values.dt_input,
            did_input:values.did_input,
            skey_input:values.skey_input,
            sval_input:values.sval_input,
            p_input:values.p_input,
            sketch_tbl:values.sketch_tbl
        });
    }

    // Call processdatasketch procedure;
    async processDataSketch(values: {app_id:String}): Promise<null> {
        return this.db.none(sql.process, {
            app_id:values.app_id
        });
    }

    // Declare cursor;
    async declareCursor(values: {events_tbl:String}): Promise<null> {
        return this.db.none(sql.declarecursor, {
            events_tbl:values.events_tbl
        });
    }

    // Fetch from cursor;
    async fetchCursor(): Promise<any> {
        return this.db.any(sql.fetchcursor);
    }

    // Close cursor;
    async closeCursor(): Promise<null> {
        return this.db.none(sql.closecursor);
    }

    // Fetch dauCount Without value of platform;
    async dauCount(values: {app_id:String, st_dt: String, end_dt: String}): Promise<any> {
        return this.db.none(sql.daucount, {
            app_id:values.app_id,
            st_dt:values.st_dt,
            end_dt:values.end_dt
        });
    }

    // Fetch wauCount Without value of platform;
    async wauCount(values: {app_id:String, st_dt: String, end_dt: String}): Promise<any> {
        return this.db.none(sql.waucount, {
            app_id:values.app_id,
            st_dt:values.st_dt,
            end_dt:values.end_dt
        });
    }

        // Fetch dauCountp With value of platform;
        async dauCountp(values: {app_id:String, st_dt: String, end_dt: String, p:Number}): Promise<any> {
            return this.db.none(sql.daucountp, {
                app_id:values.app_id,
                st_dt:values.st_dt,
                end_dt:values.end_dt,
                p:values.p
            });
        }
    
        // Fetch wauCountp With value of platform;
        async wauCountp(values: {app_id:String, st_dt: String, end_dt: String, p:Number }): Promise<any> {
            return this.db.none(sql.waucountp, {
                app_id:values.app_id,
                st_dt:values.st_dt,
                end_dt:values.end_dt,
                p:values.p
            });
        }
}