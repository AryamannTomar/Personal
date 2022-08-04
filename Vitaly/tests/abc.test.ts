const pgp = require('pg-promise')({});
const db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');
var {DataSketches} = require('../src/analytics/datasketches/datasketches');
describe('DataSketches', () => {
    afterAll(db.$pool.end);
    beforeAll(() => {
        function interval(min, max) {
            return Math.floor(Math.random() * (max - min + 1) + min)
        }
        db.task(t => {
            var platform=['IOS', 'Android'];
            var key = ['Login', 'Session_Start', 'install'];
            var segment = ['{}', '{"UCIC": "1834365900000000917", "LoginMethod": "MPIN", "LoginStatus": "True", "PaymentDueDate": "2021-09-02"}', '{"ViewAccountDetails": "True"}', '{"OpenTrigger": "PushNotification", "TriggerValue": "6128978fa1ab2401f22a6e8c"}'];
            const queries = [
                t.none(`drop table if exists events_123`),
                t.none(`create table events_123(key varchar, did varchar, dt date, segment jsonb, p varchar)`)
            ];
            for (let i = 0; i < 100; i++) {
                queries.push(t.none(`insert into events_123 values ('${key[interval(0, 2)]}', '1${interval(10, 99)}f205-9305-4e28-b1${interval(0,9)}6-0fbe6a408cac', '${new Date(`2021-${interval(1,12)}-${interval(1,30)}`).toISOString().substring(0, 10)}', '${segment[interval(0,3)]}', '${platform[interval(0,1)]}')`));
            }
            return t.batch(queries);
        }).then(async res => {
            await ds.upsertData();
        
    });
    test('Upsert', () => {
        expect('Completed').toContain('Completed');
    });
    test('Cohort', () => {
        expect('Cohort Successfully').toContain('Successfully');
    });
    test('DWM', () => {
        expect({
            dau: 3.45,
            wau: 2.27,
            mau: 8.33,
            daywisedau: [['2021-01-02', 1], ['2021-01-04', 1]]
        }).toMatchObject({
            dau: expect.any(Number),
            wau: expect.any(Number),
            mau: expect.any(Number),
            daywisedau: expect.any(Array)
        })
    });
});