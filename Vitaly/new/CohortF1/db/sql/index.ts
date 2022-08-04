import {QueryFile, IQueryFileOptions} from 'pg-promise';
const {join: joinPath} = require('path');

export const users = {
    createCohortTable                : sql('users/cohort/createCohortTable.sql'),
    upsertCohort                     : sql('users/cohort/upsertCohort.sql'),
    dailyCohort                      : sql('users/cohort/dailyCohort.sql'),
    weeklyCohort                     : sql('users/cohort/weeklyCohort.sql'),
    monthlyCohort                    : sql('users/cohort/monthlyCohort.sql'),
    dailyCohortP                     : sql('users/cohort/dailyCohortP.sql'),
    weeklyCohortP                    : sql('users/cohort/weeklyCohortP.sql'),
    monthlyCohortP                   : sql('users/cohort/monthlyCohortP.sql')

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
