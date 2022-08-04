import {QueryFile, IQueryFileOptions} from 'pg-promise';
const {join: joinPath} = require('path');

export const users = {
    declareCursor: sql('users/upsert/declarecursor.sql'),
    fetchCursor: sql('users/upsert/fetchcursor.sql'),
    closeCursor: sql('users/upsert/closecursor.sql')
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
