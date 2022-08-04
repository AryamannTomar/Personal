import {QueryFile, IQueryFileOptions} from 'pg-promise';
const {join: joinPath} = require('path');

export const users = {
    createTable: sql('users/initialize/createTable.sql')
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
