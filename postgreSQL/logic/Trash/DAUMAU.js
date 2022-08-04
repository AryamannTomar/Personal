const pgp = require('pg-promise')({});
const cn = 'postgres://postgres:Password@123@localhost:5432/postgres';
const db = pgp(cn);
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
                   var sum = 0;
                   const dayarr = [];
			 let stdt = new Date(st_dt);
			 let endt = new Date(end_dt);
			 let months = (endt.getMonth() -
                      stdt.getMonth() +
                      12 * (endt.getFullYear() - stdt.getFullYear()) + 1);
			 let weeks = Math.abs(Math.round((endt.getTime() - stdt.getTime()) / (60 * 60 * 24 * 7 * 1000))) + 1;

                 for(var i=0;i<keys.length;i++){                    
                      console.log(obj[i]);
                      sum = sum + obj[i].theta_sketch_get_estimate;
                      let item = (obj[i].dt).toISOString().substring(0,10);
                      if(dayarr.indexOf(item) === -1) {
    					dayarr.push(item);
                      }
  			}	               
			console.log("arr: ", dayarr);
			console.log("weeks: ", weeks);
        		console.log("months: ", months);
                  console.log("UU: ", sum);	
			
})
                   .catch(err => {
                    console.log("Error Occured: " + err.message);
                   });
                   }
}

getUserCount('2021-09-27', '2021-09-29', '5bebe93c25d705690ffbc758');