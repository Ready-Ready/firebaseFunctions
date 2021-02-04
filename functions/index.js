const functions = require("firebase-functions");
const admin = require("firebase-admin");
const salesforce = require('./controllers/salesforce');
const fsHelper = require('./controllers/firestore');
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

    const sfQuery = `select id, name, programID__c, program_description__c
        , Organization_Name__r.name 
        , (select name, referral_processes__c, referral_contact_name__c, referral_email__c, referral_phone_number__c
            , website__c, referral_form__c
            from locations__r limit 1)
        from program__c`;

    try {
        programs = await salesforce.query(sfQuery);
    } catch(err) {
        return res.status(500).send(err);
    }

    if(programs){
        try {
            results = await fsHelper.createMany(admin, programs, 'programs');
        } catch(err) {
            return res.status(500).send(err);
        }   

        res.json({result: `${programs.length} programs successfully refreshed.`});

    } else {
        return res.status(500).send('No programs returned');
    }

});