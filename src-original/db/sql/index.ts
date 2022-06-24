import {QueryFile, IQueryFileOptions} from 'pg-promise';
const {join: joinPath} = require('path');

export const users = {
    createsegment: sql('users/initialize/upsertdatasketchsegment.sql'),
    createrow: sql('users/initialize/upsertdatasketchrow.sql'),
    createprocess: sql('users/initialize/processdatasketch.sql'),
    process: sql('users/call/process.sql'),
    segment: sql('users/call/upsertsegment.sql'),
    row: sql('users/call/upsertrow.sql'),
    createtables: sql('users/initialize/createtables.sql'),
    declarecursor: sql('users/cursors/declarecursor.sql'),
    fetchcursor: sql('users/cursors/fetchcursor.sql'),
    closecursor: sql('users/cursors/closecursor.sql'),
    daucount: sql('users/sql-queries/daucount.sql'),
    daucountp: sql('users/sql-queries/daucountp.sql'),
    waucount: sql('users/sql-queries/waucount.sql'),
    waucountp: sql('users/sql-queries/waucountp.sql')
};

///////////////////////////////////////////////
// Helper for linking to external query files;
function sql(file: string): QueryFile {
    const fullPath: string = joinPath(__dirname, file); // generating full path;
    const options: IQueryFileOptions = {
        // minifying the SQL is always advised;
        // see also option 'compress' in the API;
        minify: true
    };
    const qf: QueryFile = new QueryFile(fullPath, options);
    if (qf.error) {
        // Something is wrong with our query file :(
        // Testing all files through queries can be cumbersome,
        // so we also report it here, while loading the module:
        console.error(qf.error);
    }
    return qf;
}
