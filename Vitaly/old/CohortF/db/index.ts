import * as promise from 'bluebird';
import * as dbConfig from '../config/db-config.json';
import * as pgPromise from 'pg-promise';
import {IInitOptions, IDatabase, IMain} from 'pg-promise';
import {IExtensions, UsersRepository} from './repos';
type ExtendedProtocol = IDatabase<IExtensions> & IExtensions;
const initOptions: IInitOptions<IExtensions> = {
    promiseLib: promise,
    extend(obj: ExtendedProtocol, dc: any) {
        obj.users = new UsersRepository(obj, pgp);
    }
};
const pgp: IMain = pgPromise(initOptions);
const db: ExtendedProtocol = pgp(dbConfig);
export {db, pgp};