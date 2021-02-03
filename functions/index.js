const functions = require("firebase-functions");
const admin = require("firebase-admin");
const sf = require('jsforce');
admin.initializeApp();

// // Create and Deploy Your First Cloud Functions
// // https://firebase.google.com/docs/functions/write-firebase-functions
//
// exports.helloWorld = functions.https.onRequest((request, response) => {
//   functions.logger.info("Hello logs!", {structuredData: true});
//   response.send("Hello from Firebase!");
// });

// Take the text parameter passed to this HTTP endpoint and insert it into 
// Firestore under the path /messages/:documentId/original
exports.addMessage = functions.https.onRequest(async (req, res) => {
    // Grab the text parameter.
    const original = req.query.text;
    // Push the new message into Firestore using the Firebase Admin SDK.
    const writeResult = await admin.firestore().collection('messages').add({original: original});
    // Send back a message that we've successfully written the message
    res.json({result: `Message with ID: ${writeResult.id} added.`});
});

// Listens for new messages added to /messages/:documentId/original and creates an
// uppercase version of the message to /messages/:documentId/uppercase
exports.makeUppercase = functions.firestore.document('/messages/{documentId}')
    .onCreate((snap, context) => {
      // Grab the current value of what was written to Firestore.
      const original = snap.data().original;

      // Access the parameter `{documentId}` with `context.params`
      functions.logger.log('Uppercasing', context.params.documentId, original);
      
      const uppercase = original.toUpperCase();
      
      // You must return a Promise when performing asynchronous tasks inside a Functions such as
      // writing to Firestore.
      // Setting an 'uppercase' field in Firestore document returns a Promise.
      return snap.ref.set({uppercase}, {merge: true});
    });

exports.refreshPrograms = functions.https.onRequest(async (req, res) => {
    const connSF = new sf.Connection({
        loginUrl: functions.config().sfdc.loginurl
        , version: functions.config().sfdc.version
    });

    try{
        var sfConn = await connSF.login(functions.config().sfdc.user, functions.config().sfdc.password + functions.config().sfdc.token);
    } catch(err){
        //return callback('Could not connect to SF', {responses: {results: `badJob for ${body.sfdc.user}`}});
        console.log('error logging into Salesforce');
        return res.status(401).send('Error when logging into Salesforce');
        //console.log(err);
    }

    var programs = await connSF.query(`select id, name, programID__c, program_description__c
            , Organization_Name__r.name 
            , (select name, referral_processes__c, referral_contact_name__c, referral_email__c, referral_phone_number__c
                , website__c, referral_form__c
                from locations__r limit 1)
            from program__c`
        , (err, result) => {
        if(err) {
            console.log('error in sfdc query:');
            console.log(err);
            return null;
        }
        return result;
    });

    if(programs){
        programs.records.forEach(prog => {
            const objProg = {
                "name": prog.Name,
                "externalId": prog.Id,
                "id":  prog.ProgramID__c,
                "organizationName": prog.Organization_Name__r.Name,
                "description": prog.Program_Description__c
            }
    
            if(prog.Locations__r) {
                objProg.referral = {
                    "applicationLink": prog.Locations__r.records[0].Referral_Form__c,
                    "contact": prog.Locations__r.records[0].Referral_Contact_Name__c,
                    "email": prog.Locations__r.records[0].Referral_Email__c,
                    "phone": prog.Locations__r.records[0].Referral_Phone_Number__c,
                    "process": prog.Locations__r.records[0].Referral_Processes__c,
                    "website":  prog.Locations__r.records[0].Website__c
                }
            }
    
            admin.firestore().collection('programs').add(objProg);
        });

        /*
        console.log('count of programs:');
        console.log(programs.records.length);
        console.log('first record:');
        console.log(programs.records[0]);
        console.log('first records location:');
        console.log(programs.records[0].Locations__r.records);
        */
    
        res.json({result: `${programs.records.length} programs successfully refreshed.`});
    } else {
        return res.status(500).send('Error in Salesforce query');
    }

});