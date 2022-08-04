import { db } from '../../db';
class DataSketches {
    private async upsertCohort(obj: any, timeUnit: String, guid: String, appid: String): Promise<any> {
        await db.task(async (t: any) => {
            for (var i = 0; i < Object.keys(obj).length; i++) {
                await t.batch([
                    db.users.upsertCohort({ guid: guid, appid: appid, timeUnit: timeUnit, year: parseInt(obj[i].initialdateformat.split(" ")[0]), initialdate: new Date(obj[i].initialdate).toISOString().substring(0, 10), nextdate: new Date(obj[i].nextdate).toISOString().substring(0, 10), initialdateformat: parseInt(obj[i].initialdateformat.split(" ")[1]), nextdateformat: parseInt(obj[i].nextdateformat.split(" ")[1]), platform: obj[i].platform, initialdatecount: parseInt(obj[i].initialdatecount), nextdatecount: parseInt(obj[i].nextdatecount) })
                ]).Add
            }
            return t.batch;
        })
    }

    async Cohort(firstEvent: String, secondEvent: String, appid: String): Promise<any> {
        function createGuid() {
            return 'xxxxxxxxxxxx4xxxyxxxxxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
                var r = Math.random() * 16 | 0, v = c === 'x' ? r : (r & 0x3 | 0x8);
                return v.toString(16);
            });
        }

        var guid = createGuid();
        await db.users.createCohortTable({ guid: guid, appid: appid });
        await db.users.dailyCohort({ firstEvent: firstEvent, secondEvent: secondEvent, appid: appid }).then((obj: any) => { this.upsertCohort(obj, 'D', guid, appid) });
        await db.users.weeklyCohort({ firstEvent: firstEvent, secondEvent: secondEvent, appid: appid }).then((obj: any) => { this.upsertCohort(obj, 'W', guid, appid) });
        await db.users.monthlyCohort({ firstEvent: firstEvent, secondEvent: secondEvent, appid: appid }).then((obj: any) => { this.upsertCohort(obj, 'M', guid, appid) });
        return `CohortData upserted successfully in cohort_${guid}_${appid} Table`;
    }
}

var ds = new DataSketches();
let result2 = ds.Cohort('Login', 'Session_Start', '5bebe93c25d705690ffbc75811');;
result2.then((result) => {
    console.log(result);
});