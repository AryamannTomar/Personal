import { JsonSerializer, throwError } from 'typescript-json-serializer';
import { ActiveUserCount } from './models/activeusercount';
const defaultSerializer = new JsonSerializer();
const pgp = require('pg-promise')({});
const cn = 'postgres://postgres:Password@123@localhost:5432/postgres';
const db = pgp(cn);
var moment = require('moment');
module.exports = db; 

db.connect().then(() => {
                 console.log('Connected');
             }).catch((error:any) => {
                 console.error('Unable to connect to the database:', error);
});

type getDAU = {
  DAU: number;
  MAU: number;
  daywisedau: Array<any>;
};
function getDAU(st_dt:String, end_dt:String, keylen:Number, obj:Array<any>):getDAU{
    const dayarr:any = [];
    var sum = 0;
    var stdt1 = moment(st_dt).format('YYYY-MM-DD'); 
    var endt1 = moment(end_dt).format('YYYY-MM-DD'); 
    var stdt = new Date(stdt1);
    var endt = new Date(endt1);
    let months = (endt.getMonth() - stdt.getMonth() + 12 * (endt.getFullYear() - stdt.getFullYear()) + 1);
    // let weeks = Math.abs(Math.round((endt.getTime() - stdt.getTime()) / (60 * 60 * 24 * 7 * 1000))) + 1;
    for(var i=0;i<keylen;i++){              
        const dayobj=[];    
        sum = sum + obj[i].theta_sketch_get_estimate;
        let item = (obj[i].dt).toISOString().substring(5,7) +'_'+(obj[i].dt).toISOString().substring(8,10);
        if(dayarr.indexOf(item) === -1) {
            dayobj.push(item);
            dayobj.push(obj[i].theta_sketch_get_estimate);
            dayarr.push(dayobj);
        }
    }        
    var dau_res:any = {
    "DAU" : sum/dayarr.length,
    "MAU" : sum/months,
    "daywisedau" : dayarr};
    return dau_res;
}

type getWAU = {
  WAU: number;
  Sundays: any;
};
function getWAU(st_dt:String, end_dt:String, keylen:Number, obj:Array<any>):getWAU{
    var start = moment(st_dt), end = moment(end_dt), day = 0;                    
    var result = [];
    var data = [];
    var current = start.clone();
    while (current.day(7 + day).isBefore(end)) {
    result.push(current.clone());
    }
    var arr = result.map(m => m.format().substring(0,10));
    var sun:any = [];
    for (const key of arr) {
        var object:any = {};
        object[key] = 0;
        sun.push(object);
    }
    for(var i=0;i<keylen;i++){                    
        const object:any = {
        "dt" : (obj[i].dt).toISOString().substring(0,10),
        "val": obj[i].theta_sketch_get_estimate};
        data.push(object);
    }
    sun.push({end: 0});
    for(var i=0;i<data.length;i++){
        var dt = new Date(data[i].dt);
        for(var j=0;j<sun.length-1;j++){
            var last_sun_dt = new Date(Object.keys(sun[sun.length-2])[0]);
            var sun_dt = new Date(Object.keys(sun[j])[0]);
            if(dt.getTime() > last_sun_dt.getTime()){
               sun[sun.length-1].end+=data[i].val;
               break;  
            }
            else if(dt.getTime() <= sun_dt.getTime()){
               sun[j][Object.keys(sun[j])[0]]+=data[i].val;
               break;
            }else{
                continue;
            }
    }}
    let weeksum:any = 0;
    for(var i=0;i<sun.length;i++){
        weeksum += Object.values(sun[i])[0];
    }
    var wau_res:any  = {
        "WAU" : weeksum/sun.length,
        "Sundays" : sun };
    return wau_res;
}


function getUserCount(st_dt:String, end_dt:String, app_id:String, p=3){
    if(p==3){
    db.task((t:any) => {
      return t.many(`select dt, theta_sketch_get_estimate(usercount) from dailyactiveusers_events_`+ app_id +` where dt BETWEEN $1 AND $2`, [st_dt, end_dt])
                 }).then((response:any) => {
        var obj:any = response;
	  var keys = Object.keys(obj);
        let daures = getDAU(st_dt, end_dt, keys.length, obj);
        let waures = getWAU(st_dt, end_dt, keys.length, obj);

        let res:any = {
        "DAU" : daures.DAU,
        "WAU" : waures.WAU,
        "MAU" : daures.MAU,
        "daywisedau" : daures.daywisedau
        };
	  let activeuserinstance = new ActiveUserCount(res.DAU, res.WAU, res.MAU, res.daywisedau);
	  const data = defaultSerializer.serialize(activeuserinstance);
	  console.log(data);

                })
                .catch((error:any) => {
                    console.log("Error Occured: " + error.message);
                });
        }
}

getUserCount('2021-09-11', '2021-09-29', '5bebe93c25d705690ffbc758');
