const axios = require('axios');
const functions = require("firebase-functions");
const querystring = require("querystring");

module.exports = {
    postMessageThread: async(admin, fromPerson, toPerson, toProgram, message) => {
        return new Promise(async (resolve, reject) => {
            const headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                Authorization: `Basic ${functions.config().tibco.authtoken}`
            }            
            const postData = querystring.stringify({
                grant_type: "client_credentials",
                scope: ""
            });
            try{
                var authResp = await axios.post(`${functions.config().tibco.baseurl}/auth`, postData, {"headers": headers});
            } catch (err) {
                return reject(err);
            }
            const headersData = {
                Authorization: `Bearer ${authResp.data.access_token}`
            }
            //Gather extra data needed from Firebase
            try{
                var toPersonResp = await admin.firestore().collection('persons').doc(toPerson).get();
                var toPersonData = toPersonResp.data();
            } catch(err) {
                return reject(`Failed getting TO data from Firestore: ${err}`);
            }
            
            try {
                const dataURL = `${functions.config().tibco.baseurl}/messages/`;
                functions.logger.log("calling post message at");
                functions.logger.log(dataURL);
                var dataResp = await axios.post(
                    dataURL,
                    {
                        "to": {
                            "globalId": toPerson,
                            "email": toPersonData.email,
                            "programId": toProgram
                        },
                        "openParagraph": message,
                        "messageType": "thread",
                        "fromInfo": {
                            "globalId": fromPerson
                        },
                        "deliveryMethod": [
                            "push"
                        ]                        
                    },
                    {headers: headersData}
                );
            } catch(err) {
                return reject(`Failed on post the thread message to Mashery API: ${err}`);
            }

            return resolve(dataResp.data);            
        })
    },
    getDataBundle: async(person, affiliatedProgram, sharedProgram, dataBundle) => {
        return new Promise(async (resolve, reject) => {
            const headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                Authorization: `Basic ${functions.config().tibco.authtoken}`
            }
            const postData = querystring.stringify({
                grant_type: "client_credentials",
                scope: ""
            });
            try{
                var authResp = await axios.post(`${functions.config().tibco.baseurl}/auth`, postData, {"headers": headers});
            } catch (err) {
                return reject(err);
            }
            console.log('auth response:');
            console.log(authResp.data); //authResp.data.access_token
            const headersData = {
                Authorization: `Bearer ${authResp.data.access_token}`
            }
            try {
                const dataURL = `${functions.config().tibco.baseurl}/persons/${person}/affiliatedPrograms/${affiliatedProgram}/sharedPrograms/${sharedProgram}/dataBundles/${dataBundle}`;
                console.log('calling get data at:');
                console.log(dataURL);
                var dataResp = await axios.get(
                    dataURL,
                    {headers: headersData}
                );
            } catch(err) {
                return reject(`Failed on Tibco GET dataBundle request: ${err}`);
            }
            console.log('response of bundle data:');
            console.log(dataResp.data);

            return resolve(dataResp.data);
        })
    }
}