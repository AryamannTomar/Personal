obj = [                                             
    {'a': 35, 'b': 35, f: 2, s: 1},
    {'a': 35, 'b': 35, f: 3, s: 4},
    {'a': 35, 'b': 35, f: 8, s: 5},
    {'a': 35, 'b': 36, f: 1, s: 0 },
    {'a': 35, 'b': 37, f: 1, s: 0 },
    
    {'a': 36, 'b': 36, f: 4, s: 4 },
    {'a': 36, 'b': 36, f: 3, s: 2 },
    {'a': 36, 'b': 36, f: 5, s: 1 },
    {'a': 36, 'b': 37, f: 2, s: 1 },
        
    {'a': 37, 'b': 37, f: 5, s: 5}
];

var keys = Object.keys(obj);
var arr=[];
var fin=[];
for (var i = 0; i < keys.length; i++) {
    if(arr.indexOf(`${obj[i]['a']}-${obj[i]['b']}`) == -1 ){
      arr.push(`${obj[i]['a']}-${obj[i]['b']}`);
    }
}

for (var i = 0; i < arr.length; i++) {
    var d={};
    d['a']=arr[i].split("-")[0];
    d['b']=arr[i].split("-")[1];
    d['f']=0;
    d['s']=0;
    fin.push(d);
}

var objkeys = Object.keys(obj);
for (var i = 0; i < objkeys.length; i++) {    
    for(var j = 0; j < fin.length; j++){
        if((obj[i]['a'] == fin[j]['a']) && (obj[i]['b'] == fin[j]['b'])){
            fin[j].f+=obj[i].f;
            fin[j].s+=obj[i].s;
        }
    }
}
console.log(fin);