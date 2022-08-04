var moment = require('moment');
let start_date='2021-09-01';
let end_date='2021-09-15';
var start = moment(start_date),
    end = moment(end_date),
    day = 0;
var result = [];
var current = start.clone();
while (current.day(7 + day).isBefore(end)) {
    result.push(current.clone());
}
var array_prefinal = result.map(m => m.format().substring(0, 10));
var array = [];
if (new Date(moment(start_date)).getDay() == 0) {
    array.push(new Date(moment(start_date)).toUTCString().substring(5, 16));
}
for (let i of array_prefinal) {
    array.push(i);
}
var sundayarray = [];
sundayarray.push(new Date(moment(start_date)).toUTCString().substring(5, 16));
for (const key of array) {
    let dt = new Date(key);
    const dateCopy = new Date(dt.getTime());
    dateCopy.setDate(dateCopy.getDate() + 1);
    sundayarray.push(dt.toUTCString().substring(5, 16));
    sundayarray.push((dateCopy).toUTCString().substring(5, 16));
}
sundayarray.push(new Date(moment(end_date)).toUTCString().substring(5, 16));
console.log(sundayarray);