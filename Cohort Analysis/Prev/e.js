// create table cohort(Type char, initialdate integer, nextdate integer, year integer, nextdatecount float, initialdatecount float);
// insert into cohort values ('D', 1, 2, 2001, 24, 36)

const pgp = require('pg-promise')({});
const db = pgp('postgres://postgres:Password@123@localhost:5432/postgres');

obj = [{ 'First Event Date': '37', 'Second Event Date': '37', firstcount: 22, secondcount: 6 },
{ 'First Event Date': '38', 'Second Event Date': '38', firstcount: 25, secondcount: 10 }]

db.task(async (t) => {
    for (var i = 0; i < obj.length; i++) {
        await t.batch([
            db.query(`insert into cohort values ('D', ${obj[i]['First Event Date']}, ${obj[i]['Second Event Date']}, 2021, ${obj[i].firstcount}, ${obj[i].secondcount})`)
        ]).Add
    }
    return t.batch;
}).then((res) => {
    console.log('Success');
}).catch((err) => {
    console.log('Error => ', err);
});