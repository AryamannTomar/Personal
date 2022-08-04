require('dotenv').config({path: '../.env'})
var config = {
    // PG config with object Connection
    postgreSQL:{
            user: 'postgres',
            host: 'localhost',
            database: 'postgres',
            password: '',
            port: 9700,
            idle: 30000,
            acquire: 300000,
            max : 10,
            min : 0
        }
};
module.exports = config;