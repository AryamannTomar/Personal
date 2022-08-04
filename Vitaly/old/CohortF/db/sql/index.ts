import {QueryFile, IQueryFileOptions} from 'pg-promise';
const {join: joinPath} = require('path');

export const users = {
    upsertCohort: sql('users/cohort/upsertcohort.sql'),
    dquery: sql('users/cohort/dquery.sql'),
    wquery: sql('users/cohort/wquery.sql'),
    mquery: sql('users/cohort/mquery.sql'),
    cohortCreateTable: sql('users/cohort/createtable.sql')
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
