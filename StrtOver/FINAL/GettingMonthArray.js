let dt1 = new Date('2021-08-04');
let dt2 = new Date('2021-09-04');
const montharr = [];
let st_mnth = parseInt((dt1).toISOString().substring(5, 7));
let end_mnth = parseInt((dt2).toISOString().substring(5, 7));
montharr.push(dt1.toISOString().substring(0, 10));
while(st_mnth<end_mnth){
    montharr.push(new Date(dt1.getFullYear(), dt1.getMonth()+1, 0).toISOString().substring(0, 10));
    dt1.setMonth(dt1.getMonth()+1);
    st_mnth++;
    montharr.push(new Date(dt1.getFullYear(), dt1.getMonth(), 1).toISOString().substring(0, 10));
}
montharr.push(dt2.toISOString().substring(0, 10));
// console.log(montharr);
for(let i=0; i<montharr.length; i=i+2){
    console.log(montharr[i], " ", montharr[i+1])
}