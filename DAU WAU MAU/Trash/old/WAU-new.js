var moment = require('moment');
async function wauCount(st_dt, end_dt, app_id, p = 3) {
    var start = moment(st_dt),
        end = moment(end_dt),
        day = 0;

    var result = [];
    var current = start.clone();
    while (current.day(7 + day).isBefore(end)) {
        result.push(current.clone());
    }
    var arr_pre = [];
    var arr_pre = result.map(m => m.format().substring(0, 10));
    var arr=[];
    if(new Date(st_dt).getDay() == 6){
        arr.push(st_dt);
    }
    for(let i of arr_pre){
        arr.push(i);
    }
    console.log(arr);
    var sunarr = [];
    sunarr.push(st_dt);
    for (const key of arr) {
        let dt = new Date(key);
        const dateCopy = new Date(dt.getTime());
        dateCopy.setDate(dateCopy.getDate() + 1);
        sunarr.push(dt.toISOString().substring(0, 10));
        sunarr.push((dateCopy).toISOString().substring(0, 10));
    }
    sunarr.push(end_dt);
    console.log(sunarr);
}
wauCount('2021-09-05', '2021-09-29', '5bebe93c25d705690ffbc75811');