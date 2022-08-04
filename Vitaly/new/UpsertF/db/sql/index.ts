import {QueryFile, IQueryFileOptions} from 'pg-promise';
const {join: joinPath} = require('path');

export const users = {
    initializeTables                 : sql('users/initialize/createTables.sql'),
    initializeUpsertDataSketchRow    : sql('users/initialize/upsertDataSketchRow.sql'),
    initializeUpsertDataSketchSegment: sql('users/initialize/upsertDataSketchSegment.sql'),
    initializeProcessDataSketch      : sql('users/initialize/processDataSketch.sql'),

    closeCursor                      : sql('users/upsert/closeCursor.sql'),
    declareCursor                    : sql('users/upsert/declareCursor.sql'),
    fetchCursor                      : sql('users/upsert/fetchCursor.sql')

};

///////////////////////////////////////////////
// Helper for linking to external query files;
function sql(file: string): QueryFile {
    const fullPath: string = joinPath(__dirname, file);  // generating full path;
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
        console.log('error => ', qf.error);
    }
    return qf;
}
