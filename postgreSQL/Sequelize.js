const Sequelize = require('sequelize');
const path = 'postgres://postgres:Password@123@localhost:5432/postgres';
const sequelize = new Sequelize(path);
let app_id = '5bebe93c25d705690ffbc75811';

sequelize.authenticate().then(() => {
          console.log('Connection established successfully.');
}).catch(err => {
          console.error('Unable to connect to the database:', err);
}).finally(() => {
          sequelize.close();
});

sequelize.query("call processdatasketch('"+app_id+"');",
              {
                type: Sequelize.QueryTypes.SELECT
}).then(() => {
        console.log('CREATED');}).catch(err => {
                          console.error('Unable to connect to the database:', err);
        }).finally(() => {
                          sequelize.close();
});

============================================================================================================================================================================================

const RunSequelQuery = function(app_id){
                const Sequelize = require('sequelize');
        const path = 'postgres://postgres:Password@123@localhost:5432/postgres';
        const sequelize = new Sequelize(path);

        sequelize.authenticate().then(() => {
                         console.log('Connection established successfully.');
        }).catch(err => {
                         console.error('Unable to connect to the database:', err);
        }).finally(() => {
                         sequelize.close();
        });

        sequelize.query("call processdatasketch($1);",
                                      {bind:[app_id],
                                       type: Sequelize.QueryTypes.SELECT
                                       }).then(() => {
                                             console.log('CREATED');}).catch(err => {
                                             console.error('Unable to connect to the database:', err);
                                        }).finally(() => {
                                             sequelize.close();
                                        });
};

RunSequelQuery('5bebe93c25d705690ffbc75811');


============================================================================================================================================================================================
const Sequelize = require('sequelize');
const path = 'postgres://postgres:Password@123@localhost:5432/postgres';
const sequelize = new Sequelize(path);
const sequelize1 = new Sequelize(path);
const sequelize2 = new Sequelize(path);

sequelize.authenticate().then(() => {
         console.log('xxxxx--- Connection established successfully ---xxxxx');
}).catch(err => {
         console.error('Unable to connect to the database:', err);
}).finally(() => {
         sequelize.close();
});

sequelize.query("CREATE TABLE datasketches_events_5bebe93c25d705690ffbc75811(eventkey varchar, dt date, usercount theta_sketch, skey text, sval text); ALTER TABLE datasketches_events_5bebe93c25d705690ffbc75811 ADD constraint keydt_seg_unique UNIQUE(eventkey, dt, skey, sval);").then(()=> {console.log("xxxxx--- Datasketch Table Created and Altered ---xxxxx")}).catch((e) => {console.log("Error while creating Datasketch Table => ", e)}).finally(() => {sequelize.close();});

const UpsertDataSketchEvents = function(num_limit){
			sequelize1.query("select * from events_5bebe93c25d705690ffbc75811 LIMIT $1;",
                              { bind: [num_limit],
                                type: Sequelize.QueryTypes.SELECT
                              }).then((result) => {
					  var obj = result;
                                var keys = Object.keys(obj);
		for(var i=0;i<keys.length;i++){UpsertDataSketchSegment(obj[i].key, obj[i].dt, obj[i].did, obj[i].segment, 'datasketches_events_5bebe93c25d705690ffbc75811');}
                              }).catch((error) => {
                                console.log("Error During insertion dataSketch Table => ", error)
                              }).finally(() => {
                                sequelize.close();
                              });
}

const UpsertDataSketchSegment = function(key_input, dt_input, did_input, segment, dest_tbl_name_input){
			sequelize2.query("select upsertDataSketchSegment($1, $2, $3, $4, $5);", {bind: [key_input, dt_input, did_input, segment, dest_tbl_name_input],
                                type: Sequelize.QueryTypes.SELECT
                              }).then((result) => {
					  console.log("DataSketchesSegments() Function Executed Successfully!")
                              }).catch((error) => {
                                console.log(error)
                              }).finally(() => {
                                sequelize.close();
                              });
}

UpsertDataSketchEvents('50');

============================================================================================================================================================================================