function getUnits(dt) {
  dt = new Date(dt);
  var D = (Date.UTC(dt.getFullYear(), dt.getMonth(), dt.getDate()) - Date.UTC(dt.getFullYear(), 0, 0)) / (24 * 60 * 60 * 1000);
  var Jan1 = new Date(dt.getFullYear(), 0, 1);
  var numberOfDays = Math.floor((dt - Jan1) / (24 * 60 * 60 * 1000));
  var W = Math.ceil((dt.getDay() + 1 + numberOfDays) / 7);
  if ((parseInt(dt.toISOString().substring(8, 10))) == 1) {
    var M = dt.getMonth() + 2;
  } else {
    var M = dt.getMonth() + 1;
  }
  return { 'D': D, 'W': W, 'M': M };
}

function increment(dt, timeUnits) {
  dt1 = new Date(dt);
  if (timeUnits == 'D') {
    dt1.setDate(dt1.getDate() + 1);
  } else if (timeUnits == 'W') {
    dt1.setDate(dt1.getDate() + 1 * 7);
  } else if (timeUnits == 'M') {
    if ((parseInt(dt1.toISOString().substring(8, 10))) == 1) {
      dt1.setMonth(dt1.getMonth() + 2, 0);
    } else {
      dt1.setMonth(dt1.getMonth() + 1);
    }
  }
  return dt1.toISOString().substring(0, 10);
}

function months(dt) {
  dt1 = new Date(dt);
  let array = [];
  if ((parseInt(dt1.toISOString().substring(8, 10))) == 1) {
    dt1.setDate(dt1.getDate() + 1);
  }
  array.push(new Date(dt1.getFullYear(), dt1.getMonth(), 1).toISOString().substring(0, 10));
  array.push(new Date(dt1.getFullYear(), dt1.getMonth() + 1, 0).toISOString().substring(0, 10));
  return array;
}

function weeks(dt) {
  dt1 = new Date(dt);
  let array = [];
  var first = dt1.getDate() - dt1.getDay();
  var last = first + 6; 
  array.push(new Date(dt1.setDate(first)).toISOString().substring(0,10));
  array.push(new Date(dt1.setDate(last)).toISOString().substring(0,10));
  return array;
}

function fn(dt, timeUnit){
  array = [];
  if(timeUnit=='D'){
    array.push(dt);
    array.push(dt);
    dt1 = increment(dt, 'D');
    array.push(dt1);
    array.push(dt1);
  }
  else if(timeUnit=='W'){
    array.push(weeks(dt));
    dt1 = increment(dt, 'M');
    array.push(weeks(dt1));
  }else if(timeUnit=='M'){
    array.push(months(dt));
    dt1 = increment(dt, 'M');
    array.push(months(dt1));
  }
  return array;
}

console.log(getUnits('2021-09-06'));
console.log(getUnits('2021-09-14'));