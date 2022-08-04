import { db } from '../../db';
class DataSketches {
    private async upsertCohort(obj: any, timeUnit: String, guid: String, appid: String): Promise<any> {
        await db.task(async (t: any) => {
            for (var i = 0; i < Object.keys(obj).length; i++) {
                await t.batch([
                    db.users.upsertCohort({ guid: guid, appid: appid, timeUnit: timeUnit, initialdate: new Date(obj[i].initialdate).toISOString().substring(0, 10), nextdate: new Date(obj[i].nextdate).toISOString().substring(0, 10), initialdateformat: obj[i].initialdateformat, nextdateformat: obj[i].nextdateformat, platform: obj[i].platform, initialdatecount: parseInt(obj[i].initialdatecount), nextdatecount: parseInt(obj[i].nextdatecount) })
                ]).Add
            }
            return t.batch;
        })
    }

    async Cohort(firstEvent: String, secondEvent: String, startDate: String, endDate: String, appid: String, p=3): Promise<any> {
        function createGuid() {
            return 'xxxxxxxxxxxx4xxxyxxxxxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
                var r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
                return v.toString(16);
            });
        }
        var guid = createGuid();
        await db.users.createCohortTable({ guid: guid, appid: appid });
        if(p==3){
            await db.users.dailyCohort({ firstEvent: firstEvent, secondEvent: secondEvent, startDate: startDate, endDate: endDate, appid: appid }).then((obj: any) => { this.upsertCohort(obj, 'D', guid, appid) });
            await db.users.weeklyCohort({ firstEvent: firstEvent, secondEvent: secondEvent, startDate: startDate, endDate: endDate, appid: appid }).then((obj: any) => { this.upsertCohort(obj, 'W', guid, appid) });
            await db.users.monthlyCohort({ firstEvent: firstEvent, secondEvent: secondEvent, startDate: startDate, endDate: endDate, appid: appid }).then((obj: any) => { this.upsertCohort(obj, 'M', guid, appid) });
        }else{
            await db.users.dailyCohortP({ firstEvent: firstEvent, secondEvent: secondEvent, startDate: startDate, endDate: endDate, appid: appid, platform: p }).then((obj: any) => { this.upsertCohort(obj, 'D', guid, appid) });
            await db.users.weeklyCohortP({ firstEvent: firstEvent, secondEvent: secondEvent, startDate: startDate, endDate: endDate, appid: appid, platform: p }).then((obj: any) => { this.upsertCohort(obj, 'W', guid, appid) });
            await db.users.monthlyCohortP({ firstEvent: firstEvent, secondEvent: secondEvent, startDate: startDate, endDate: endDate, appid: appid, platform: p }).then((obj: any) => { this.upsertCohort(obj, 'M', guid, appid) });
        }
        return `CohortData upserted successfully in cohort_${guid}_${appid} Table`;
    }
}

var ds = new DataSketches();
let result2 = ds.Cohort('Login', 'Session_Start', '2021-08-01', '2021-10-01', '5bebe93c25d705690ffbc75811', 0);
result2.then((result) => {
    console.log(result);
});