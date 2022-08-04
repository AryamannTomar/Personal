const pgp = require('pg-promise')({});
const db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');

async function upsertCohort(obj: any, timeUnit: String): Promise<any> {
    await db.task(async (t: any) => {
        for (var i = 0; i < Object.keys(obj).length; i++) {
            await t.batch([
                db.query(`insert into cohort values ('${timeUnit}', ${parseInt(obj[i].initialdate.split(" ")[1])}, ${parseInt(obj[i].nextdate.split(" ")[1])}, ${parseInt(obj[i].initialdate.split(" ")[0])}, ${obj[i].initialdatecount}, ${obj[i].nextdatecount})`)
            ]).Add
        }
        return t.batch;
    })
}

async function Cohort(firstEvent: String, secondEvent: String): Promise<any> {
    var dquery = `SELECT
            CONCAT(to_char(a.dt, 'yyyy'), ' ', extract(doy from a.dt)) initialdate,
            CONCAT(to_char(b.dt, 'yyyy'), ' ', extract(doy from b.dt)) nextdate,
            theta_sketch_get_estimate(theta_sketch_union(a.usercount)) initialdatecount,
            theta_sketch_get_estimate(theta_sketch_intersection(theta_sketch_union(a.usercount), theta_sketch_union(b.usercount))) nextdatecount
            FROM datasketches_events_5bebe93c25d705690ffbc75811 a
            JOIN datasketches_events_5bebe93c25d705690ffbc75811 b ON a.p=b.p
            WHERE a.eventkey = '${firstEvent}' AND b.eventkey = '${secondEvent}' AND a.dt <= b.dt
            GROUP BY initialdate, nextdate
            ORDER BY initialdate ASC`;

    var mquery = `SELECT
            a.date initialdate,
            b.date nextdate,
            sum(theta_sketch_get_estimate(a.usercount)) initialdatecount,
            sum(theta_sketch_get_estimate(theta_sketch_intersection(a.usercount, b.usercount))) nextdatecount
            from (SELECT
            to_char(dt, 'yyyy mm') AS date,
            p,
            theta_sketch_union(usercount) as usercount
            FROM datasketches_events_5bebe93c25d705690ffbc75811 m
            WHERE m.eventkey='${firstEvent}'
            GROUP BY date, p
            ORDER BY date) a
            Join (SELECT
            to_char(dt, 'yyyy mm') AS date,
            p,
            theta_sketch_union(usercount) as usercount
            FROM datasketches_events_5bebe93c25d705690ffbc75811 n
            WHERE n.eventkey='${secondEvent}'
            GROUP BY date, p
            ORDER BY date) b on a.p = b.p
            WHERE RIGHT(a.date, 2) <= RIGHT(b.date, 2)
            GROUP BY initialdate, nextdate
            ORDER BY initialdate ASC`;

    var wquery = `SELECT
            a.date initialdate,
            b.date nextdate,
            sum(theta_sketch_get_estimate(a.usercount)) initialdatecount,
            sum(theta_sketch_get_estimate(theta_sketch_intersection(a.usercount, b.usercount))) nextdatecount
            from (SELECT
            CONCAT(to_char(dt, 'yyyy'), ' ', DATE_PART('week', dt)) AS date,
            p,
            theta_sketch_union(usercount) as usercount
            FROM datasketches_events_5bebe93c25d705690ffbc75811 m
            WHERE m.eventkey='${firstEvent}'
            GROUP BY date, p
            ORDER BY date) a
            Join (SELECT
            CONCAT(to_char(dt, 'yyyy'), ' ', DATE_PART('week', dt)) AS date,
            p,
            theta_sketch_union(usercount) as usercount
            FROM datasketches_events_5bebe93c25d705690ffbc75811 n
            WHERE n.eventkey='${secondEvent}'
            GROUP BY date, p
            ORDER BY date) b on a.p = b.p
            WHERE RIGHT(a.date, 2) <= RIGHT(b.date, 2)
            GROUP BY initialdate, nextdate
            ORDER BY initialdate ASC`;

    await db.query(dquery).then((obj: any) => { upsertCohort(obj, 'D') });
    await db.query(wquery).then((obj: any) => { upsertCohort(obj, 'W') });
    await db.query(mquery).then((obj: any) => { upsertCohort(obj, 'M') });
    return "CohortData Upserted Sucessfully!";
}

let result = Cohort('Login', 'Session_Start');
result.then((result) => {
    console.log(result);
});