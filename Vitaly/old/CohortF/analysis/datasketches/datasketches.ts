import { db } from '../../db';
async function upsertCohort(obj: any, timeUnit: String, firstEvent: String, secondEvent: String, appid: String): Promise<any> {
    await db.task(async (t: any) => {
        for (var i = 0; i < Object.keys(obj).length; i++) {
            await t.batch([
                db.users.upsertCohort({firstEvent: firstEvent, secondEvent: secondEvent, appid: appid, timeUnit: timeUnit, initialdate: parseInt(obj[i].initialdate.split(" ")[1]), nextdate: parseInt(obj[i].nextdate.split(" ")[1]), year: parseInt(obj[i].initialdate.split(" ")[0]), initialdatecount: parseInt(obj[i].initialdatecount), nextdatecount: parseInt(obj[i].nextdatecount) })
            ]).Add
        }
        return t.batch;
    })
}

async function Cohort(firstEvent: String, secondEvent: String, appid: String): Promise<any> {
    await db.users.cohortCreateTable({firstEvent: firstEvent, secondEvent: secondEvent, appid: appid });
    await db.users.dquery({ firstEvent: firstEvent, secondEvent: secondEvent }).then((obj: any) => { upsertCohort(obj, 'D', firstEvent, secondEvent, appid) });
    await db.users.wquery({ firstEvent: firstEvent, secondEvent: secondEvent }).then((obj: any) => { upsertCohort(obj, 'W', firstEvent, secondEvent, appid) });
    await db.users.mquery({ firstEvent: firstEvent, secondEvent: secondEvent }).then((obj: any) => { upsertCohort(obj, 'M', firstEvent, secondEvent, appid) });
    return "CohortData Upserted Sucessfully!";
}

let result = Cohort('Login', 'Session_Start', '5bebe93c25d705690ffbc75811');
result.then((result) => {
    console.log(result);
});
