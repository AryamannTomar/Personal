import {QueryFile, IQueryFileOptions} from 'pg-promise';
const {join: joinPath} = require('path');

export const users = {
    getCount                         : sql('users/dauwaumau/getCount.sql'),
    getCountP                        : sql('users/dauwaumau/getCountP.sql'),
    getDayWiseDau                    : sql('users/dauwaumau/getDayWiseDau.sql'),
    getDayWiseDauP                   : sql('users/dauwaumau/getDayWiseDauP.sql')
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
