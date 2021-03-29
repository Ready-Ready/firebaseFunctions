const functions = require("firebase-functions");
const sf = require('jsforce');

module.exports = {
    query: async(strQuery) => {
        return new Promise(async (resolve, reject) => {

            const connSF = new sf.Connection({
                loginUrl: functions.config().sfdc.loginurl
                , version: functions.config().sfdc.version
            });

            try{
                var sfConn = await connSF.login(functions.config().sfdc.user, functions.config().sfdc.password + functions.config().sfdc.token);
            } catch(err){
                //return callback('Could not connect to SF', {responses: {results: `badJob for ${body.sfdc.user}`}});
                console.log('error logging into Salesforce');
                functions.logger.error('error logging into Salesforce');
                functions.logger.error(err);
                //return res.status(401).send('Error when logging into Salesforce');
                return reject('Error when logging into Salesforce');
                console.log(err);
            }
        
            /*var queryResults = await connSF.query(strQuery, (err, result) => {
                if(err) {
                    console.log('error in sfdc query:');
                    return reject('Error when running query in Salesforce');
                    //console.log(err);
                }
                connSF.logout((err) => {
                    if(err) {
                        functions.logger.error("error when logging out:");
                        functions.logger.error(err);
                    }
                    return result;
                });
            });*/       

            try {
                var queryResults = await connSF.query(strQuery);
                connSF.logout((err) => {
                    if(err) {
                        functions.logger.error("error when logging out:");
                        functions.logger.error(err);
                    }
                    //return result;      
                });          

            } catch(err) {
                console.log('error in sfdc query:');
                return reject('Error when running query in Salesforce');
            }
            
            return resolve(queryResults.records);

        });
    }
}