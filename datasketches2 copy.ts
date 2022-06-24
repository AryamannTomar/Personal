import { JsonSerializer, throwError } from 'typescript-json-serializer';
import { ActiveUserCount } from './src-original/models/users/activeusercount';
import { DAU, WAU, MAU } from './src-original/models/users/types';
const defaultSerializer = new JsonSerializer();
var moment = require('moment');
import { db } from './src-original/db';
const logger = require('../../utils/logger');
const log = logger.createLogger();

/* 
    3 Functions(FOR FETCHING O/P) - getDAU(st_dt, end_dt, keylen[Number of keys in the fetched response from database], fetched response from database); - returns DAU, MAU and daywisedau
                                    getWAU(st_dt, end_dt, keylen, fetched response from database); -  returns WAU and weekwise_usercount
                                    getUserCount(st_dt, end_dt, appid, p[optional]); -  returns the desired result
                                    We get the dau count, mau count, daywisedau count from getDAU() and wau count, Sundays falling in-between the start_date and end_date from getWAU() 
*/

async function dauCount(st_dt: String, end_dt: String, app_id: String, p: Number): Promise<any> {
    var obj;
    if (p == 3) { obj = await db.users.dauCount(app_id, st_dt, end_dt) }
    else { obj = await db.users.dauCountp(app_id, st_dt, end_dt, p) }
    var keys = Object.keys(obj);
    const dayarr: Array<any> = [];
    let sum = 0;
    for (var i = 0; i < keys.length; i++) {
        const dayobj: Array<any> = [];
        var flag = 0;
        sum += obj[i].theta_sketch_get_estimate;
        let item = (obj[i].dt).toISOString().substring(5, 10);

        for (let j = 0; j < dayarr.length; j++) {
            if ((dayarr[j].indexOf(item)) != -1) {
                flag = 1;
                dayarr[j][1] += obj[i].theta_sketch_get_estimate;
                break;
            }
        }
        if (flag == 0) {
            dayobj.push(item);
            dayobj.push(obj[i].theta_sketch_get_estimate);
            dayarr.push(dayobj);
        }
    }
    var dau_res: any = {
        "DAU": Math.round(sum / dayarr.length * 10) / 10,
        "daywisedau": dayarr
    };
    return dau_res;
}

async function wauCount(st_dt: String, end_dt: String, app_id: String, p: Number): Promise<any> {
    var start = moment(st_dt),
        end = moment(end_dt),
        day = 0;
    var result: any = [];
    var current = start.clone();
    while (current.day(7 + day).isBefore(end)) {
        result.push(current.clone());
    }
    var arr = result.map(m => m.format().substring(0, 10));
    var sunarr: Array<any> = [];
    sunarr.push(st_dt);
    for (const key of arr) {
        let dt = new Date(key);
        const dateCopy = new Date(dt.getTime());
        dateCopy.setDate(dateCopy.getDate() + 1);
        sunarr.push(dt.toISOString().substring(0, 10));
        sunarr.push((dateCopy).toISOString().substring(0, 10));
    }
    sunarr.push(end_dt);
    let sum = 0;
    var weekarraynull_final: Array<any> = [];
    if (p == 3) {
        await db.task(async (p: any) => {
            for (let i = 0; i < sunarr.length; i = i + 2) {
                var weekarr_pre: Array<any> = [];
                weekarr_pre.push(sunarr[i]);
                p.batch([
                    weekarr_pre.push((await db.users.wauCount(app_id, sunarr[i], sunarr[i + 1]))[0].theta_sketch_get_estimate)
                ]).Add
                weekarraynull_final.push(weekarr_pre);
            } return p.batch;
        })
    }
    else {
        await db.task(async (p: any) => {
            for (let i = 0; i < sunarr.length; i = i + 2) {
                var weekarr_pre: Array<any> = [];
                weekarr_pre.push(sunarr[i]);
                p.batch([
                    weekarr_pre.push((await db.users.wauCountp(app_id, sunarr[i], sunarr[i + 1], p))[0].theta_sketch_get_estimate)
                ]).Add
                weekarraynull_final.push(weekarr_pre);
            } return p.batch;
        })
    }
    var weekarr_final: Array<any> = [];
    for (let i = 0; i < weekarraynull_final.length; i++) {
        if (weekarraynull_final[i][1] != null) {
            weekarr_final.push(weekarraynull_final[i]);
            sum += weekarraynull_final[i][1];
        }
    }
    var wau_res = {
        "WAU": Math.round((sum / (weekarr_final.length + 1)) * 10) / 10
    };
    return wau_res;
}

async function mauCount(st_dt: String, end_dt: String, app_id: String, p: Number): Promise<any> {
    let stdt = moment(st_dt);
    let endt = moment(end_dt);
    const montharr: Array<any> = [];
    let st_mnth = parseInt((stdt).toISOString().substring(5, 7));
    let end_mnth = parseInt((endt).toISOString().substring(5, 7));
    montharr.push(stdt.toISOString().substring(0, 10));
    while (st_mnth < end_mnth) {
        montharr.push(new Date(stdt.getFullYear(), stdt.getMonth() + 1, 0).toISOString().substring(0, 10));
        stdt.setMonth(stdt.getMonth() + 1);
        st_mnth++;
        montharr.push(new Date(stdt.getFullYear(), stdt.getMonth(), 1).toISOString().substring(0, 10));
    }
    montharr.push(endt.toISOString().substring(0, 10));
    let sum = 0;
    var montharraynull_final: Array<any> = [];

    if (p == 3) {
        await db.task(async (p: any) => {
            for (let i = 0; i < montharr.length; i = i + 2) {
                var montharr_pre: Array<any> = [];
                montharr_pre.push(montharr[i]);
                p.batch([
                    montharr_pre.push((await db.users.wauCount(app_id, montharr[i], montharr[i + 1]))[0].theta_sketch_get_estimate)
                ]).Add
                montharraynull_final.push(montharr_pre);
            } return p.batch;
        })
    }
    else {
        await db.task(async (p: any) => {
            for (let i = 0; i < montharr.length; i = i + 2) {
                var montharr_pre: Array<any> = [];
                montharr_pre.push(montharr[i]);
                p.batch([
                    montharr_pre.push((await db.users.wauCountp(app_id, montharr[i], montharr[i + 1], p))[0].theta_sketch_get_estimate)
                ]).Add
                montharraynull_final.push(montharr_pre);
            } return p.batch;
        })
    }

    var montharr_final: Array<any> = [];
    for (let i = 0; i < montharraynull_final.length; i++) {
        if (montharraynull_final[i][1] != null) {
            montharr_final.push(montharraynull_final[i]);
            sum += montharraynull_final[i][1];
        }
    }
    var mau_res = {
        "MAU": Math.round((sum / montharr_final.length) * 10) / 10
    };
    return mau_res;
}

async function userCount(st_dt: String, end_dt: String, app_id: String, p = 3) {
    let daures = await dauCount(st_dt, end_dt, app_id, p);
    let maures = await mauCount(st_dt, end_dt, app_id, p);
    let waures = await wauCount(st_dt, end_dt, app_id, p);
    let res = {
        "DAU": daures.DAU,
        "WAU": waures.WAU,
        "MAU": maures.MAU,
        "daywisedau": daures.daywisedau
    };

    let activeuserinstance = new ActiveUserCount(res.DAU, res.WAU, res.MAU, res.daywisedau);
    var data: any = defaultSerializer.serialize(activeuserinstance);
    return data;
}

/* 
    1 Function(FOR UPSERTING VIA CURSORS) - upsertData(tbl_id); Takes Raw input from the table events_{tbl_id} and then upserts it data into 3 desired tables in postgreSQL
                                            We Fetch in a batch of 1000 from the test_cursor and then upsert the data into the desired tables by calling the function UpsertDataSketchSegment
*/
async function upsertDataCursor(tbl_id: String): Promise<any> {
    await db.task(async (t: any) => {
        let events_tbl = 'events_' + tbl_id;
        let sketch_tbl = 'datasketches_events_' + tbl_id;
        let activeuser_tbl = 'datasketches_dailyactiveusers_' + tbl_id;
        let stat_tbl = 'datasketches_segmentstats_' + tbl_id;
        db.users.declareCursor(events_tbl);
        let count = 0;
        while (1) {
            let res = await db.users.fetchCursor();
            if (res.length == 0) { break; }
            await db.task((p: any) => {
                for (var i = 0; i < res.length; i++) {
                    const arr = [res[i].key, res[i].dt, res[i].did, res[i].segment, res[i].p, sketch_tbl, activeuser_tbl, stat_tbl];
                    p.batch([
                        db.func('upsertdatasketchsegment', arr)
                    ]).Add
                } return p.batch;
            })
            count++;
            log.info('in loop:', count);
        }
        await db.users.closeCursor();
        log.info('Completed');
    });
}

/*
    1 Function(FOR UPSERTING A BATCH) - This function takes an events array and does an upsert using batch 
*/
async function upsertDataBatch(data: any): Promise<any> {
    await db.task(async (t: any) => {
        var obj = data;
        var keys = Object.keys(obj);
        for (var i = 0; i < keys.length; i++) {
            const sketch_tbl = 'datasketches_events_' + data[i].appid;
            const activeuser_tbl = 'datasketches_dailyactiveusers_' + data[i].appid;
            const stat_tbl = 'datasketches_segmentstats_' + data[i].appid;
            const arr = [obj[i].key, obj[i].dt, obj[i].did, obj[i].segment, obj[i].p, sketch_tbl, activeuser_tbl, stat_tbl];
            t.batch([
                db.func('upsertdatasketchsegment', arr)
            ]).Add
        } return t.batch;
    }).then((res: any) => {
        log.info("Successful");
    })
        .catch((err: any) => {
            log.info(err);
        });
}