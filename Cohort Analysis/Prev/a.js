const pgp = require('pg-promise')({});
const db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');

async function upsert(obj, timeUnit) {
    await db.task(async (t) => {
        for (var i = 0; i < obj.length; i++) {
            if (timeUnit == 'D') {
                await t.batch([
                    db.query(`insert into cohort values ('D', ${getUnits(obj[i]['First Event Date'], 'D')}, ${getUnits(obj[i]['Second Event Date'], 'D')}, ${obj[0]['First Event Date'].substring(0, 4)}, ${obj[i].firstcount}, ${obj[i].secondcount})`)
                ]).Add
            }
            else {
                await t.batch([
                    db.query(`insert into cohort values ('${timeUnit}', ${obj[i]['First Event Date']}, ${obj[i]['Second Event Date']}, 2021, ${obj[i].firstcount}, ${obj[i].secondcount})`)
                ]).Add
            }
        }
        return t.batch;
    })
}

function getUnits(dt, timeUnit) {
    dt = new Date(dt);
    dt.setDate(dt.getDate() + 1);
    if (timeUnit == 'D') {
        return (Date.UTC(dt.getFullYear(), dt.getMonth(), dt.getDate()) - Date.UTC(dt.getFullYear(), 0, 0)) / (24 * 60 * 60 * 1000);
    }
    else if (timeUnit == 'W') {
        return Math.ceil(Math.floor((dt - new Date(dt.getFullYear(), 0, 1)) / (24 * 60 * 60 * 1000)) / 7);
    }
    else if (timeUnit == 'M')
        if ((parseInt(dt.toISOString().substring(8, 10))) == 1) {
            return dt.getMonth() + 2;
        } else {
            return dt.getMonth() + 1;
        }
}

function getResult(obj, timeUnit) {
    var keys = Object.keys(obj);
    var arr = [];
    var fin = [];
    for (var i = 0; i < keys.length; i++) {
        if (arr.indexOf(`${getUnits(obj[i]['First Event Date'], timeUnit)}-${getUnits(obj[i]['Second Event Date'], timeUnit)}`) == -1) {
            arr.push(`${getUnits(obj[i]['First Event Date'], timeUnit)}-${getUnits(obj[i]['Second Event Date'], timeUnit)}`);
        }
    }

    for (var i = 0; i < arr.length; i++) {
        var d = {};
        d['First Event Date'] = arr[i].split("-")[0];
        d['Second Event Date'] = arr[i].split("-")[1];
        d['firstcount'] = 0;
        d['secondcount'] = 0;
        fin.push(d);
    }

    for (var i = 0; i < keys.length; i++) {
        for (var j = 0; j < fin.length; j++) {
            if (((getUnits(obj[i]['First Event Date'], timeUnit)) == fin[j]['First Event Date']) && ((getUnits(obj[i]['Second Event Date'], timeUnit)) == fin[j]['Second Event Date'])) {
                fin[j].firstcount += obj[i].firstcount;
                fin[j].secondcount += obj[i].secondcount;
            }
        }
    }
    return fin;
}

query = `SELECT
to_char(a.dt, 'yyyy-mm-dd') AS "First Event Date",
to_char(b.dt, 'yyyy-mm-dd') AS "Second Event Date",
theta_sketch_get_estimate(theta_sketch_union(a.usercount)) AS "firstcount",
theta_sketch_get_estimate(theta_sketch_intersection(theta_sketch_union(a.usercount), theta_sketch_union(b.usercount))) AS "secondcount"
FROM datasketches_events_5bebe93c25d705690ffbc75811 a
JOIN datasketches_events_5bebe93c25d705690ffbc75811 b ON a.p=b.p
WHERE a.eventkey = 'Login' AND b.eventkey = 'Session_Start' AND a.dt <= b.dt
GROUP BY a.dt, b.dt
ORDER BY a.dt ASC`;

db.query(query).then((obj) => {
    upsert(obj, 'D');
    upsert(getResult(obj, 'W'), 'W');
    console.log(getResult(obj, 'W'));
    upsert(getResult(obj, 'M'), 'M');
}).then((res) => {
    console.log('Success');
}).catch((err) => {
    console.log('Error => ', err);
});