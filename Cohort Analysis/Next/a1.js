// create table cohort(type, initialdate, nextdate, year, nextdatecount, initialdatecount);

dobj=[{initialdate: '2021 248', nextdate: '2021 251', initialdatecount: 4, nextdatecount: 0},
      {initialdate: '2021 248', nextdate: '2021 252', initialdatecount: 4, nextdatecount: 0},
      {initialdate: '2021 248', nextdate: '2021 254', initialdatecount: 4, nextdatecount: 0}];

wobj=[{initialdate: '2021 38', nextdate: '2021 40', initialdatecount: 1, nextdatecount: 0},
      {initialdate: '2021 39', nextdate: '2021 39', initialdatecount: 6, nextdatecount: 6},
      {initialdate: '2021 39', nextdate: '2021 40', initialdatecount: 1, nextdatecount: 1}];

mobj=[{initialdate: '2021 09', nextdate: '2021 09', initialdatecount: 12787.567233909414, nextdatecount: 11671.133946376798},
      {initialdate: '2021 09', nextdate: '2021 10', initialdatecount: 12787.567233909414, nextdatecount: 0},
      {initialdate: '2021 10', nextdate: '2021 10', initialdatecount: 2, nextdatecount: 2}];

var arr=[]
function getResult(obj, timeUnit){
    var keys = Object.keys(obj);
    for (var i=0; i<keys.length; i++) {
        let d = {};
        d['type'] = timeUnit;
        d['initialdate'] = parseInt(obj[i].initialdate.split(" ")[1]);
        d['nextdate'] = parseInt(obj[i].nextdate.split(" ")[1]);
        d['year']=parseInt(obj[i].initialdate.split(" ")[0]);
        d['initialdatecount'] = obj[i].initialdatecount;
        d['nextdatecount'] = obj[i].nextdatecount;  
        arr.push(d);
    };
}
getResult(dobj, 'D');
getResult(wobj, 'W');
getResult(mobj, 'M');

console.log(arr);