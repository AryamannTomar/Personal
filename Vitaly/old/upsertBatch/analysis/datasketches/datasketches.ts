const pgp = require('pg-promise')({});
const cn = 'postgres://postgres:Password@123@localhost:5432/postgres';
const db = pgp(cn);
db.connect().then(() => {
    console.log('Connection established successfully.');
}).catch(err => {
    console.error('Unable to connect to the database:', err);
});

var data: Array<any> = [{
    appid: '5bebe93c25d705690ffbc75811',
    did: '832a275b-6f7d-4c81-8c2f-3bf8967a75f9',
    key: 'Login',
    sid: 'a874041b-435f-4c0f-a06f-fad04d87d47e',
    p: 'IOS',
    rtime: 1629522744,
    utime: 1629522744,
    segment:
    {
        UCIC: '2211773100000011998',
        LoginMethod: 'Password',
        LoginStatus: 'True',
        PaymentDueDate: '2021-08-11'
    },
    context: { who: [Object], what: {}, when: [Object], where: [Object] },
    eventtime: 1631855409,
    dt: '2021-09-17',
    createat: '2021-08-21'
},
{
    appid: '5bebe93c25d705690ffbc75811',
    did: '832a275b-6f7d-4c81-8c2f-3bf8967a75f9',
    key: 'Session_Start',
    sid: 'a874041b-435f-4c0f-a06f-fad04d87d47e',
    p: 'Android',
    rtime: 1629522744,
    utime: 1629522744,
    segment: {},
    context: { who: [Object], what: {}, when: [Object], where: [Object] },
    eventtime: 1631855389,
    dt: '2021-09-17',
    createat: '2021-08-21'
}];

db.task(t => {
    var obj = data;
    var keys = Object.keys(obj);
    for (var i = 0; i < keys.length; i++) {
        const tbl1_name = 'datasketches_events_' + data[i].appid;
        const tbl2_name = 'datasketches_dailyactiveusers_' + data[i].appid;
        const tbl3_name = 'datasketches_segmentstats_' + data[i].appid;
        const arr = [obj[i].key, obj[i].dt, obj[i].did, obj[i].segment, obj[i].p, tbl1_name, tbl2_name, tbl3_name];
        t.batch([
            db.func('upsertdatasketchsegment', arr)
        ]).Add
    } return t.batch;
}).then(data => {
    console.log("Batch Data Upseerted Successfully!");
})
    .catch(err => {
        console.log("Error => ", err);
 });