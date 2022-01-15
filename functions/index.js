const cors = require("cors")({origin: true});
const functions = require("firebase-functions");
const admin = require("firebase-admin");
const salesforce = require('./controllers/salesforce');
const fsHelper = require('./controllers/firestore');
const tibco = require('./controllers/tibco');
const { user } = require("firebase-functions/lib/providers/auth");
admin.initializeApp();

// // Create and Deploy Your First Cloud Functions
// // https://firebase.google.com/docs/functions/write-firebase-functions
//
// exports.helloWorld = functions.https.onRequest((request, response) => {
//   functions.logger.info("Hello logs!", {structuredData: true});
//   response.send("Hello from Firebase!");
// });

exports.createUserSeeker = functions.auth.user().onCreate( async(user) => {
    functions.logger.log(`The user created:`);
    functions.logger.log(user);
    var aryName = [];
    var firstName = null;
    var lastName = null;
    if(user.displayName){
        aryName = user.displayName.split(" ");
        firstName = aryName[0]?aryName[0]:null;
        lastName = aryName[1]?aryName[1]:null;
    }

    const doc = {
        "createdByUser": user.uid,
        "devices": [],
        "favorite_programs": [],
        "firstName": firstName,
        "lastName": lastName
    }
    try {
        const insSeeker = await admin.firestore().collection("userSeekers").doc(user.uid).set(doc);
        //email-password provider does not have displayName onCreate (it is updated after by FirebaseUI),
        //so we must go back and get it to update it on the userSeeker record
        if(!user.displayName){
            const userRecord = await admin.auth().getUser(user.uid);
            if(userRecord.displayName){
                aryName = userRecord.displayName.split(" ");
                firstName = aryName[0]?aryName[0]:null;
                lastName = aryName[1]?aryName[1]:null;

                const updSeeker = await admin.firestore().collection("userSeekers").doc(user.uid).update({"firstName": firstName, "lastName": lastName});
                functions.logger.log('The seeker record was updated for display name on a second pass');
            }            
        }
    } catch(err) {
        functions.logger.error(`Error creating userSeeker for ${user.displayName}`);
        functions.logger.log(err);
    }
    
});

// Take the req "body" and post it to the messages of the userSeeker with the "to" email
// and include a call to action to view the "toProgram" details listing
// will post to Firestore under the path /userSeekers/messages/:documentId
exports.addMessage = functions.https.onRequest(async (req, res) => {

    functions.logger.log('got req:');
    functions.logger.log(req.body);
    const message = {
        body: req.body.body,
        status: 'unread',
        createdAt: new Date(),
        toProgram:  req.body.toProgram
    };

    try{
        const userRecord = await admin.auth().getUserByEmail(req.body.to);

        var userSeekerRef = await admin.firestore().collection('userSeekers').where('createdByUser', '==', userRecord.uid).get();
        userSeekerRef.forEach(async (user) => {

            const updExistingDoc = await admin.firestore().collection("userSeekers").doc(user.id).collection("messages").add(message);

            //notify user of a new message
            if(user.data().devices) {
                user.data().devices.forEach(device => {

                    if(device.length > 0) {
                        var registrationToken = device;
    
                        var message = {
                        notification: {
                            title: 'New Message',
                            body: 'You have a new message in your GRG inbox.'
                        },
                        token: registrationToken
                        };
        
                        // Send a message to the device corresponding to the provided
                        // registration token.
                        admin.messaging().send(message)
                        .then((response) => {
                            // Response is a message ID string.
                            console.log('Successfully sent message:', response);
                        })
                        .catch((error) => {
                            console.log('Error sending message:', error);
                            functions.logger.log('Error sending message:', error);
                        });
                    } else {
                        functions.logger.log('could not send notification, device was empty');
                    }

                })    
            }
                
            res.json({result: `Success:  message inserted`});
        })

    } catch(err) {
        res.json({result: `Failure: ${err}`});
    }

    /*
    // Grab the text parameter.
    const original = req.query.text;
    // Push the new message into Firestore using the Firebase Admin SDK.
    const writeResult = await admin.firestore().collection('messages').add({original: original});
    // Send back a message that we've successfully written the message
    res.json({result: `Message with ID: ${writeResult.id} added.`});
    */
});

exports.setProgram = functions.https.onRequest(async (req, res) => {
    try {
        results = await fsHelper.createOneProgram(admin, req.body);
            //console.log(`result was: ${results}`);
            functions.logger.log("Firestore refresh finised", {"resultCount": results});
            res.json({result: `${results} program successfully refreshed.`});
    } catch(err) {
        return res.status(500).send(err);
    }
});

exports.refreshPrograms = functions.https.onRequest(async (req, res) => {

    const sfQuery = `select id, name, programID__c, Brief_Program_Desc__c
        , Organization_Name__r.name 
        , (select name, referral_processes__c, referral_contact_name__c, referral_email__c, referral_phone_number__c
            , website__c, referral_form__c
            from locations__r limit 1)
        from program__c`;

    try {
        programs = await salesforce.query(sfQuery);
        functions.logger.log("Program query executed in Salesforce", {"programCount": programs.length});
    } catch(err) {
        return res.status(500).send(err);
    }

    if(programs){
        functions.logger.log("About to call FS createMany function", {});
        try {
            results = await fsHelper.createMany(admin, programs, 'programs');
            //console.log(`result was: ${results}`);
            functions.logger.log("Firestore refresh finised", {"resultCount": results});
            res.json({result: `${results} programs successfully refreshed.`});
        } catch(err) {
            return res.status(500).send(err);
        }   

    } else {
        return res.status(500).send('No programs returned');
    }

});

exports.getDataBundle = functions.https.onCall(async (data, context) => {
    //cors(data, context, async () => {
        functions.logger.log("getDataBundle called from:", {...context.auth.token});
        try {
            var bundleData = await tibco.getDataBundle(context.auth.uid, data.affiliateProgram, data.sharedProgram, data.dataBundle);
        } catch(err) {
            return {failure: err}
        }
        
        //return {message: `success from getDataBundle for ${context.auth.token.email}`}
        return bundleData;
   //});
});