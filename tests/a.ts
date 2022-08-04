const pgp = require('pg-promise')({});
const db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');
import { DataSketches } from '../../src/analytics/datasketches/datasketches';
var ds = new DataSketches('123');

async function events() {
    function interval(min, max) {
        return Math.floor(Math.random() * (max - min + 1) + min)
    }
    await db.task(t => {
        var platform = ['IOS', 'Android'];
        var key = ['Login', 'Session_Start', 'install'];
        var segment = ['{}', '{"UCIC": "1834365900000000917", "LoginMethod": "MPIN", "LoginStatus": "True", "PaymentDueDate": "2021-09-02"}', '{"ViewAccountDetails": "True"}', '{"OpenTrigger": "PushNotification", "TriggerValue": "6128978fa1ab2401f22a6e8c"}'];
        const queries = [
            t.none(`create table if not exists events_123(key varchar, did varchar, dt date, segment jsonb, p varchar)`)
        ];
        for (let i = 0; i < 100; i++) {
            queries.push(t.none(`insert into events_123 values ('${key[interval(0, 2)]}', '1${i}f205-9305-4e28-b1${interval(0, 9)}6-0fbe6a408cac', '${new Date(`2021-${interval(1, 12)}-${interval(1, 30)}`).toISOString().substring(0, 10)}', '${segment[interval(0, 3)]}', '${platform[interval(0, 1)]}')`));
        }
        return t.batch(queries);
    }).then(res => {
        console.log(`Created Table events_123 -  ${res.length}`);
    });

    let upsert = await ds.upsertData();
    console.log(upsert);
    let res = await ds.userCount('2020-01-01', '2021-05-01');
    console.log(res);
    let cohort = await ds.Cohort('Login', 'Session_Start');
    console.log(cohort);
    var guid = cohort.substring(43, 75);
    console.log(guid);
    await db.task(p => {
        const dropqueries = [
            p.none(`drop table events_123`),
            p.none(`drop table datasketches_events_123`),
            p.none(`drop table datasketches_dailyactiveusers_123`),
        ];
        return p.batch(dropqueries);
    })
}
events();