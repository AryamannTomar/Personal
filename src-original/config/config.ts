var config = {
    // PG config with object Connection
    postgreSQL:{
        productionCitus:{
            user: process.env.citusUserProd,
            host: process.env.citusHostProd,
            database: process.env.citusDatabaseProd,
            password: process.env.citusPasswordProd,
            port: process.env.citusPortProd,
            idle: 30000,
            acquire:  300000,
            max : 10,
            min : 0
        }
    }
};
module.exports = config;
