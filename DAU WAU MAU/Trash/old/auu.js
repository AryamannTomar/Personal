var moment = require('moment');

function dauCount(st_dt, end_dt, app_id) {
    var arr=[];
    let stdt = new Date(st_dt);
    let endt = new Date(end_dt)
    for(stdt; stdt<=endt; stdt.setDate(stdt.getDate()+1)){ 
        arr.push((new Date(stdt)).toISOString().substring(0, 10));
    }
    console.log(arr);
}

function wauCount(st_dt, end_dt, app_id, p = 3) {
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

async function mauCount(st_dt, end_dt, app_id, p = 3) {
    let dt1 = new Date(st_dt);
    let dt2 = new Date(end_dt);
    const montharr= [];
    let st_mnth = parseInt((dt1).toISOString().substring(5, 7));
    let end_mnth = parseInt((dt2).toISOString().substring(5, 7));
    montharr.push(dt1.toISOString().substring(0, 10));
    if((dt1).toISOString().substring(8, 10)=='01'){
        dt1.setDate(dt1.getDate() + 1);
     }
    while (st_mnth < end_mnth) {
        montharr.push(new Date(dt1.getFullYear(), dt1.getMonth() + 1, 0).toISOString().substring(0, 10));
        dt1.setMonth(dt1.getMonth() + 1);
        st_mnth++;
        montharr.push(new Date(dt1.getFullYear(), dt1.getMonth(), 1).toISOString().substring(0, 10));
    }
    montharr.push(dt2.toISOString().substring(0, 10));
    console.log(montharr);
}


dauCount('2021-09-05', '2021-10-28', '5bebe93c25d705690ffbc75811');
wauCount('2021-09-05', '2021-10-28', '5bebe93c25d705690ffbc75811');
mauCount('2021-09-05', '2021-10-28', '5bebe93c25d705690ffbc75811');