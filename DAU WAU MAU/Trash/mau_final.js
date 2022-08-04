let startdate = new Date('2021-09-01');
let enddate = new Date('2021-10-27');


const montharray = [];
let start_month = parseInt((startdate).toISOString().substring(5, 7));
let end_month = parseInt((enddate).toISOString().substring(5, 7));

let start_year = parseInt(startdate.toISOString().substring(0, 4));
let end_year = parseInt(enddate.toISOString().substring(0, 4));

montharray.push(startdate.toUTCString().substring(5, 16));
if ((startdate).toUTCString().substring(5, 7) == '01') {
    startdate.setDate(startdate.getDate() + 1);
}

while (startdate<enddate) {
    if(start_month==end_month && start_year == end_year){
        break;
    }
    if(start_month<end_month || start_year <= end_year){
        montharray.push(new Date(startdate.getFullYear(), startdate.getMonth() + 1, 0).toUTCString().substring(5, 16));
        startdate.setMonth(startdate.getMonth() + 1);
        start_month++;
        montharray.push(new Date(startdate.getFullYear(), startdate.getMonth(), 1).toUTCString().substring(5, 16));
        if (start_month == '12' && start_year != end_year) {
            montharray.push(new Date(startdate.getFullYear(), startdate.getMonth() + 1, 0).toUTCString().substring(5, 16));
            startdate.setMonth(startdate.getMonth() + 1);
            montharray.push(new Date(startdate.getFullYear(), startdate.getMonth(), 1).toUTCString().substring(5, 16));
            start_month = parseInt((startdate).toISOString().substring(5, 7));
            start_year = parseInt(startdate.toISOString().substring(0, 4));
        }
    }
}
montharray.push(enddate.toUTCString().substring(5, 16));
console.log(montharray);