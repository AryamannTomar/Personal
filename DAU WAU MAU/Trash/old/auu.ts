var moment = require('moment');

function dauCount(st_dt: String, end_dt: String, app_id: String) {
    var arr: Array<any>=[];
    let stdt = new Date(st_dt);
    let endt = new Date(end_dt)
    for(stdt; stdt<=endt; stdt.setDate(stdt.getDate()+1)){ 
        arr.push((new Date(stdt)).toISOString().substring(0, 10));
    }
    console.log(arr);
}

function wauCount(st_dt: String, end_dt: String, app_id: String, p = 3){
    var start = moment(st_dt),
        end = moment(end_dt),
        day = 0;
    var result = [];
    var current = start.clone();
    while (current.day(7 + day).isBefore(end)) {
        result.push(current.clone());
    }
    var arr = result.map(m => m.format().substring(0, 10));
    var sundayarray: Array<any> = [];
    sundayarray.push(st_dt);
    for (const key of arr) {
        let dt = new Date(key);
        const dateCopy = new Date(dt.getTime());
        dateCopy.setDate(dateCopy.getDate() + 1);
        sundayarray.push(dt.toISOString().substring(0, 10));
        sundayarray.push((dateCopy).toISOString().substring(0, 10));
    }
    sundayarray.push(end_dt);
    console.log(sundayarray);
}

async function mauCount(st_dt: String, end_dt: String, app_id: String, p = 3) {
    let dt1 = new Date(st_dt);
    let dt2 = new Date(end_dt);
    const montharr: Array<any>= [];
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


dauCount('2021-09-02', '2021-12-27', '5bebe93c25d705690ffbc75811');
wauCount('2021-09-02', '2021-12-27', '5bebe93c25d705690ffbc75811');
mauCount('2021-09-02', '2021-11-30', '5bebe93c25d705690ffbc75811');