const pgp = require('pg-promise')({});
const cn = 'postgres://postgres:Password@123@localhost:5432/postgres';
const db = pgp(cn);
var moment = require('moment');
module.exports = db;
db.connect().then(() => {
                 console.log('Connected');
             }).catch(err => {
                 console.error('Unable to connect to the database:', err);
             });


function getUserCount(st_dt, end_dt, app_id, p=3){
            if(p==3){
                 db.task(t => {
                   return t.many(`select dt, theta_sketch_get_estimate(usercount) from datasketches_events_`+ app_id +` where dt BETWEEN $1 AND $2`, [st_dt, end_dt])
                 }).then(res => {
            var obj = res;
            var keys = Object.keys(obj);
			let stdt = new Date(st_dt);
			let endt = new Date(end_dt);
			var start = moment(st_dt), 
            end = moment(end_dt), 
            day = 0;                    
            var result = [];
            var data = [];
            var current = start.clone();
            while (current.day(7 + day).isBefore(end)) {
            result.push(current.clone());
            }
            var arr = result.map(m => m.format().substring(0,10));
            var sun = [];
            for (const key of arr) {
                const obj = {}; 
                obj[key] = 0;
                sun.push(obj);
            }
            for(var i=0;i<keys.length;i++){                    
                const object = {}; 
                object['dt'] = (obj[i].dt).toISOString().substring(0,10);
                object['val'] = obj[i].theta_sketch_get_estimate;
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
            console.log(sun);
            let weeksum= 0;
            for(var i=0;i<sun.length;i++){
                weeksum += Object.values(sun[i])[0];
            }
            console.log("weeksum: ", weeksum);
            console.log("WAU: ", weeksum/sun.length);
        
        })
                   .catch(err => {
                    console.log("Error Occured: " + err.message);
                   });
                   }
}

getUserCount('2021-09-11', '2021-09-29', '5bebe93c25d705690ffbc758');