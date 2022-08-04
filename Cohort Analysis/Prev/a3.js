function getUnits(dt, timeUnit) {
    dt = new Date(dt);
    dt.setDate(dt.getDate() + 1);
    if (timeUnit == 'D') {
        return (Date.UTC(dt.getFullYear(), dt.getMonth(), dt.getDate()) - Date.UTC(dt.getFullYear(), 0, 0)) / (24 * 60 * 60 * 1000);
    }
    else if (timeUnit == 'W') {
        return Math.ceil(Math.floor((dt - new Date(dt.getFullYear(), 0, 1)) / (24 * 60 * 60 * 1000)) / 7);
    }
    else if (timeUnit == 'M')
        if ((parseInt(dt.toISOString().substring(8, 10))) == 1) {
            return dt.getMonth() + 2;
        } else {
            return dt.getMonth() + 1;
        }
}

obj = [
    { 'First Event Date': '2021-09-01', 'Second Event Date': '2021-09-01', firstcount: 128, secondcount: 1 },
    { 'First Event Date': '2021-09-01', 'Second Event Date': '2021-09-02', firstcount: 1, secondcount: 3 },
    { 'First Event Date': '2021-09-01', 'Second Event Date': '2021-09-05', firstcount: 1, secondcount: 0 },
    { 'First Event Date': '2021-09-01', 'Second Event Date': '2021-09-11', firstcount: 1, secondcount: 0 },
    { 'First Event Date': '2021-09-01', 'Second Event Date': '2021-09-12', firstcount: 1, secondcount: 0 },

    { 'First Event Date': '2021-09-02', 'Second Event Date': '2021-09-02', firstcount: 4, secondcount: 4 },
    { 'First Event Date': '2021-09-02', 'Second Event Date': '2021-09-05', firstcount: 2, secondcount: 1 },

    { 'First Event Date': '2021-09-05', 'Second Event Date': '2021-09-05', firstcount: 4, secondcount: 4 },
    { 'First Event Date': '2021-09-06', 'Second Event Date': '2021-09-06', firstcount: 4, secondcount: 4 }
];

function getResult(timeUnit) {
    var keys = Object.keys(obj);
    var arr = [];
    var fin = [];
    for (var i = 0; i < keys.length; i++) {
        if (arr.indexOf(`${getUnits(obj[i]['First Event Date'], timeUnit)}-${getUnits(obj[i]['Second Event Date'], timeUnit)}`) == -1) {
            arr.push(`${getUnits(obj[i]['First Event Date'], timeUnit)}-${getUnits(obj[i]['Second Event Date'], timeUnit)}`);
        }
    }

    for (var i = 0; i < arr.length; i++) {
        var d = {};
        d['First Event Date'] = arr[i].split("-")[0];
        d['Second Event Date'] = arr[i].split("-")[1];
        d['firstcount'] = 0;
        d['secondcount'] = 0;
        fin.push(d);
    }

    for (var i = 0; i < keys.length; i++) {
        for (var j = 0; j < fin.length; j++) {
            if (((getUnits(obj[i]['First Event Date'], timeUnit)) == fin[j]['First Event Date']) && ((getUnits(obj[i]['Second Event Date'], timeUnit)) == fin[j]['Second Event Date'])) {
                fin[j].firstcount += obj[i].firstcount;
                fin[j].secondcount += obj[i].secondcount;
            }
        }
    }
    return fin;
}

console.log(getResult('M'));