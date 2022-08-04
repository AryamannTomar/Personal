import {QueryFile, IQueryFileOptions} from 'pg-promise';
const {join: joinPath} = require('path');

export const users = {
    dau: sql('users/fetch/dau.sql'),
    //dauP: sql('users/fetch/dauP.sql'),
    wau: sql('users/fetch/wau.sql'),
    //wauP: sql('users/fetch/wauP.sql')
};

function sql(file: string): QueryFile {
    const fullPath: string = joinPath(__dirname, file);
    const options: IQueryFileOptions = {
        minify: true
    };
    const qf: QueryFile = new QueryFile(fullPath, options);
    if (qf.error) {
        console.log('error => ', qf.error);
    }
    return qf;
}
