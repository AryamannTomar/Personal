db.any(`select dt, theta_sketch_get_estimate(usercount) from dailyactiveusers_events_5bebe93c25d705690ffbc758`)
                .then(res => {
                        var obj = res
                        var keys = Object.keys(obj);
                        for(var i=0;i<keys.length;i++){
                            console.log((obj[i].dt).toISOString().substring(0,10)," - ", obj[i].theta_sketch_get_estimate);
                        }
                        }).catch(err => {console.log("Error Occured: " + err.message);
                        });

select theta_sketch_get_estimate(usercount) from dailyactiveusers_events_5bebe93c25d705690ffbc758 where dt = '2021-09-01'
select theta_sketch_get_estimate(theta_sketch_union((usercount)) from dailyactiveusers_events_5bebe93c25d705690ffbc758;
select dt, theta_sketch_get_estimate(usercount), p from dailyactiveusers_events_5bebe93c25d705690ffbc758 order by dt DESC;

select * from datasketches_events_5bebe93c25d705690ffbc758 where sval BETWEEN '2021-01-01' AND '2021-10-01' and eventkey ='TransactionHistory' and skey != 'ViewTransactionHistory';

let st_dt = '2021-08-16';
let end_dt = '2021-08-20';
let app_id = '5bebe93c25d705690ffbc758';

const pgp = require('pg-promise')({});
const cn = 'postgres://postgres:Password@123@localhost:5432/postgres';
const db = pgp(cn);
module.exports = db;
db.connect().then(() => {
                           console.log('Connection established successfully.');
                                }).catch(err => {
                           console.error('Unable to connect to the database:', err);
});

function getUserCount(st_dt, end_dt, app_id){
                       db.any(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_`+ app_id +` where sval BETWEEN $1 AND $2`, [st_dt, end_dt])
                                .then(res => {
                                          console.log("theta_sketch_get_estimate = ", res[0].theta_sketch_get_estimate) ;
                                 }).catch(err => {
                                          console.log("Error Occured: " + err.message);
                                 });
}

getUserCount('2021-08-16', '2021-08-20', '5bebe93c25d705690ffbc758');

============================================================================================================================================================================================
const pgp = require('pg-promise')({});
const cn = 'postgres://postgres:Password@123@localhost:5432/postgres';
const db = pgp(cn);
module.exports = db;
db.connect().then(() => {
                 console.log('Connection established successfully.');
             }).catch(err => {
                 console.error('Unable to connect to the database:', err);
             });

function getUserCount(st_dt, end_dt, app_id, p=3){
    if(p==3){
     db.task(t => {
      return t.many(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_`+ app_id +` where sval BETWEEN $1 AND $2`, [st_dt, end_dt])
    }).then(res => {
        console.log("theta_sketch_get_estimate = ", res[0].theta_sketch_get_estimate) ;
    })
    .catch(err => {
        console.log("Error Occured: " + err.message);
    });}
    
   else{
     db.task(t => {
      return t.many(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_`+ app_id +` where sval BETWEEN $1 AND $2 and p=$3`, [st_dt, end_dt, p])
   }).then(res => {
      console.log("theta_sketch_get_estimate = ", res[0].theta_sketch_get_estimate) ;
   })
   .catch(err => {
      console.log("Error Occured: " + err.message);
   });}
}

getUserCount('2021-08-16', '2021-08-20', '5bebe93c25d705690ffbc758');


function getUserCount(st_dt, end_dt, app_id, p=3){
    if(p==3){
       db.any(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_`+ app_id +` where sval BETWEEN $1 AND $2`, [st_dt, end_dt])
       .then(res => {
          console.log("theta_sketch_get_estimate = ", res[0].theta_sketch_get_estimate) ;
       }).catch(err => {
          console.log("Error Occured: " + err.message);
       });}
   else{
       db.any(`select theta_sketch_get_estimate(theta_sketch_union(usercount)) from datasketches_events_`+ app_id +` where sval BETWEEN $1 AND $2 and p=$3`, [st_dt, end_dt, p])
       .then(res => {
         console.log("theta_sketch_get_estimate = ", res[0].theta_sketch_get_estimate) ;
       }).catch(err => {
         console.log("Error Occured: " + err.message);
       });}
}

============================================================================================================================================================================================
const pgp = require('pg-promise')({});
const cn = 'postgres://postgres:Password@123@localhost:5432/postgres';
const db = pgp(cn);
module.exports = db;
db.connect().then(() => {
                 console.log('Connection established successfully.');
             }).catch(err => {
                 console.error('Unable to connect to the database:', err);
});

function getUserCount(st_dt, end_dt, app_id, p=3){
    if(p==3){
     db.task(t => {
      return t.many(`select dt, theta_sketch_get_estimate(usercount) from datasketches_events_`+ app_id +` where sval BETWEEN $1 AND $2`, [st_dt, end_dt])
    }).then(res => {
        var obj = res;
	  var keys = Object.keys(obj);
        for(var i=0;i<keys.length;i++){
           console.log(obj[i]);
    }})
    .catch(err => {
        console.log("Error Occured: " + err.message);
    });
    }
    
   else{
     db.task(t => {
      return t.many(`select dt, theta_sketch_get_estimate(usercount) from datasketches_events_`+ app_id +` where sval BETWEEN $1 AND $2 and p=$3`, [st_dt, end_dt, p])
   }).then(res => {
      var obj = res;
	var keys = Object.keys(obj);
      for(var i=0;i<keys.length;i++){
           console.log(obj[i]);
   }})
   .catch(err => {
      console.log("Error Occured: " + err.message);
   });
   }
}

getUserCount('2021-08-16', '2021-08-20', '5bebe93c25d705690ffbc758');

function getUserCount(st_dt, end_dt, app_id, p=3){
    if(p==3){
     db.task(t => {
      return t.many(`select dt, theta_sketch_get_estimate(usercount) from datasketches_events_`+ app_id +` where sval BETWEEN $1 AND $2`, [st_dt, end_dt])
    }).then(res => {
        var obj = res;
	  var keys = Object.keys(obj);
        for(var i=0;i<keys.length;i++){
           console.log((obj[i].dt).toISOString().substring(0,10), " - ", obj[i].theta_sketch_get_estimate);
    }})
    .catch(err => {
        console.log("Error Occured: " + err.message);
    });
    }
    
   else{
     db.task(t => {
      return t.many(`select dt, theta_sketch_get_estimate(usercount) from datasketches_events_`+ app_id +` where sval BETWEEN $1 AND $2 and p=$3`, [st_dt, end_dt, p])
   }).then(res => {
      var obj = res;
	var keys = Object.keys(obj);
      for(var i=0;i<keys.length;i++){
           console.log((obj[i].dt).toISOString().substring(0,10), " - ", obj[i].theta_sketch_get_estimate);
   }})
   .catch(err => {
      console.log("Error Occured: " + err.message);
   });
   }
}

getUserCount('2021-08-16', '2021-08-20', '5bebe93c25d705690ffbc758');




