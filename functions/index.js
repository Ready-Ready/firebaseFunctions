const cors = require("cors")({origin: true});
const functions = require("firebase-functions");
//using second gen function for deleteProgram
const {onRequest} = require("firebase-functions/v2/https");
const {onDocumentUpdated} = require("firebase-functions/v2/firestore");
const admin = require("firebase-admin");
const salesforce = require('./controllers/salesforce');
const fsHelper = require('./controllers/firestore');
const tibco = require('./controllers/tibco');
//const { user } = require("firebase-functions/lib/providers/auth");
admin.initializeApp();

// // Create and Deploy Your First Cloud Functions
// // https://firebase.google.com/docs/functions/write-firebase-functions
//
// exports.helloWorld = functions.https.onRequest((request, response) => {
//   functions.logger.info("Hello logs!", {structuredData: true});
//   response.send("Hello from Firebase!");
// });

//GEN 1 function - 9/23/24 - replicates the current affiliatedPrograms document to the history sub-collection before it is
//updated so that we have an audit history.  Implemented as part of the Supervisors on Care Team project.
//exports.createAffiliatedProgram = onDocumentUpdated("persons/{personId}/affiliatedPrograms/{apId}", async (event) => {
exports.updateAffiliatedProgram = functions.firestore
  .document('persons/{personId}/affiliatedPrograms/{apId}')
  .onUpdate(async (change, context) => {
  //const oldValue = event.data.before.data();
  const oldValue = change.before.data();
  const newHistory = await admin.firestore().collection("persons")
          .doc(context.params.personId).collection("affiliatedPrograms")
          .doc(context.params.apId).collection("versions")
          .doc(new Date().toISOString())
          .set(oldValue);
});

exports.denormCareTeam = functions.firestore
    .document('persons/{personId}/affiliatedPrograms/{apId}')
    .onWrite(async (change, context) => {
      functions.logger.log(`Got into aP write function`);
      var careTeamMembers = [];
      functions.logger.log(`finding aps for personId: ${context.params.personId}`);
      var userAPs = await admin.firestore().collection('persons').doc(context.params.personId).collection('affiliatedPrograms').get();
      userAPs.forEach(async (ap) => {
        functions.logger.log(`got ap for: ${ap.data().programName}`);
        if(ap.data().careTeamMembers){
          ap.data().careTeamMembers.forEach(ctm => {
            if(ctm.idsGuid !== ''){
              careTeamMembers.push({
                ...ctm,
                programId: ap.id,
                programName: ap.data().programName
              })
            }
          })
        }  
      });
      functions.logger.log(`found care team member count: ${careTeamMembers.length}`);
      careTeamMembers.map(async (careTeamMember) => {
        const existAP = await admin.firestore().collection("persons")
          .doc(context.params.personId).collection("relatedPersons")
          .doc(careTeamMember.idsGuid).get();
        if(existAP.exists){
          const updPerson = await admin.firestore().collection("persons")
          .doc(context.params.personId).collection("relatedPersons")
          .doc(careTeamMember.idsGuid)
          .update({
            id: careTeamMember.idsGuid,
            person: {
              firstName: careTeamMember.firstName,
              lastName: careTeamMember.lastName,
              email: careTeamMember.email 
            },
            program: {
              id: careTeamMember.programId,
              name: careTeamMember.programName
            },
            relationship: 'care team'
          });
        } else {
          const updPerson = await admin.firestore().collection("persons")
          .doc(context.params.personId).collection("relatedPersons")
          .doc(careTeamMember.idsGuid)
          .set({
            id: careTeamMember.idsGuid,
            person: {
              firstName: careTeamMember.firstName,
              lastName: careTeamMember.lastName,
              email: careTeamMember.email 
            },
            program: {
              id: careTeamMember.programId,
              name: careTeamMember.programName
            },
            relationship: 'care team'
          });          
        }
      });
      // Delete the care team members that no longer are associated
      var existCTs = await admin.firestore().collection('persons').doc(context.params.personId).collection('relatedPersons').get();
      existCTs.forEach(ct => {
        if(ct.data().relationship === 'care team'){
          if(careTeamMembers.filter(a => {return a.idsGuid === ct.id}).length == 0){
            admin.firestore().collection('persons').doc(context.params.personId).collection('relatedPersons').doc(ct.id).delete();
          }
        }
      })

    });
exports.denormCareTeamClients = functions.firestore
    .document('persons/{personId}/affiliatedPrograms/{apId}')
    .onWrite(async (change, context) => {
      
        const formatHTMLDate = (inDate) => {
            return `${inDate.getFullYear()}-${"0".repeat(2-(inDate.getMonth()+1).toString().length)}${inDate.getMonth()+1}-${"0".repeat(2-(inDate.getDate()+1).toString().length)}${inDate.getDate()}`
        }      

        const personId = context.params.personId;
        const apId = context.params.apId;

        functions.logger.log(`Triggered on affiliatedProgram write for personId: ${personId}, apId: ${apId}`);

        const beforeData = change.before.exists ? change.before.data() : null;
        const afterData = change.after.exists ? change.after.data() : null;

        // If the document is deleted, handle cleanup
        if (!afterData) {
            functions.logger.log(`Document deleted for personId: ${personId}, apId: ${apId}`);
            const careTeamMembers = beforeData?.careTeamMembers || [];
            for (const careTeamMember of careTeamMembers) {
                if (careTeamMember.idsGuid) {
                    const careTeamDocRef = admin.firestore().collection('persons').doc(careTeamMember.idsGuid);
                    const careTeamDoc = await careTeamDocRef.get();

                    if (careTeamDoc.exists) {
                        const updatedClients = (careTeamDoc.data().clients || []).filter(client => client.id !== personId);
                        await careTeamDocRef.update({ clients: updatedClients });
                        functions.logger.log(`Removed client ${personId} from care team member ${careTeamMember.idsGuid}`);
                    }
                }
            }
            return;
        }

        // Validate careTeamMembers
        if (!afterData.careTeamMembers) {
            functions.logger.log(`No careTeamMembers array found for personId: ${personId}, apId: ${apId}`);
            return;
        }

        // Fetch the full person document for PII fields
        const personDocRef = admin.firestore().collection('persons').doc(personId);
        const personDoc = await personDocRef.get();

        const personData = personDoc.data();

        const careTeamMembersBefore = beforeData?.careTeamMembers || [];
        const careTeamMembersAfter = afterData.careTeamMembers;

        // Determine added, removed, and potentially updated care team members
        const addedCareTeamMembers = careTeamMembersAfter.filter(
            afterMember => !careTeamMembersBefore.some(beforeMember => beforeMember.idsGuid === afterMember.idsGuid)
        );

        const removedCareTeamMembers = careTeamMembersBefore.filter(
            beforeMember => !careTeamMembersAfter.some(afterMember => afterMember.idsGuid === beforeMember.idsGuid)
        );

        const potentiallyUpdatedCareTeamMembers = careTeamMembersAfter.filter(afterMember => 
            careTeamMembersBefore.some(beforeMember => beforeMember.idsGuid === afterMember.idsGuid)
        );

        functions.logger.log(`Added care team members: ${addedCareTeamMembers.map(m => m.idsGuid)}`);
        functions.logger.log(`Removed care team members: ${removedCareTeamMembers.map(m => m.idsGuid)}`);
        functions.logger.log(`Potentially updated care team members: ${potentiallyUpdatedCareTeamMembers.map(m => m.idsGuid)}`);

        // Handle added members
        for (const careTeamMember of addedCareTeamMembers) {
            if (careTeamMember.idsGuid) {
                const careTeamDocRef = admin.firestore().collection('persons').doc(careTeamMember.idsGuid);
                const careTeamDoc = await careTeamDocRef.get();

                if (careTeamDoc.exists) {
                    const careTeamData = careTeamDoc.data();
                    const clientsArray = careTeamData.clients || [];

                    const clientData = {
                        id: personId,
                        firstName: personData.firstName || null,
                        lastName: personData.lastName || null,
                        mobilePhone: personData.mobilePhone || null,
                        email: personData.email || null,
                        //dob: personData.dob || null,
                        streetAddress: personData.streetAddress || null,
                        clientType : personData.type || null
                    };

                    if(personData.dateOfBirth){
                        if(typeof(personData.dateOfBirth) != 'string'){
                          clientData.htmlFormattedDOB = formatHTMLDate(personData.dateOfBirth.toDate());
                        } else {
                          clientData.htmlFormattedDOB = null;
                        }
                    } else {
                      clientData.htmlFormattedDOB = null;
                    }                          

                    clientsArray.push(clientData);
                    await careTeamDocRef.update({ clients: clientsArray });
                    functions.logger.log(`Added client ${personId} to care team member ${careTeamMember.idsGuid}`);
                }
            }
        }

        // Handle removed members
        for (const careTeamMember of removedCareTeamMembers) {
            if (careTeamMember.idsGuid) {
                const careTeamDocRef = admin.firestore().collection('persons').doc(careTeamMember.idsGuid);
                const careTeamDoc = await careTeamDocRef.get();

                if (careTeamDoc.exists) {
                    const updatedClients = (careTeamDoc.data().clients || []).filter(client => client.id !== personId);
                    try{
                        const updateResult = await careTeamDocRef.update({ clients: updatedClients });
                        functions.logger.log(`Removed client ${personId} from care team member ${careTeamMember.idsGuid}`);
                    } catch(err) {
                        functions.logger.error(`FAILED to remove client ${personId} from care team member ${careTeamMember.idsGuid}`);
                        functions.logger.error(err);
                    }
                }
            }
        }

        // Handle updated members
        for (const careTeamMember of potentiallyUpdatedCareTeamMembers) {
            if (careTeamMember.idsGuid) {
                const careTeamDocRef = admin.firestore().collection('persons').doc(careTeamMember.idsGuid);
                const careTeamDoc = await careTeamDocRef.get();

                if (careTeamDoc.exists) {
                    const careTeamData = careTeamDoc.data();
                    const clientsArray = careTeamData.clients || [];

                    const clientData = {
                      id: personId,
                      firstName: personData.firstName || null,
                      lastName: personData.lastName || null,
                      mobilePhone: personData.mobilePhone || null,
                      email: personData.email || null,
                      //dob: personData.dob || null,
                      streetAddress: personData.streetAddress || null,
                      clientType : personData.type || null
                    };

                    if(personData.dateOfBirth){
                        if(typeof(personData.dateOfBirth) != 'string'){
                          clientData.htmlFormattedDOB = formatHTMLDate(personData.dateOfBirth.toDate());
                        } else {
                          clientData.htmlFormattedDOB = null;
                        }
                    } else {
                      clientData.htmlFormattedDOB = null;
                    }                   

                    const existingClientIndex = clientsArray.findIndex(client => client.id === personId);

                    if (existingClientIndex !== -1) {
                        const existingClient = clientsArray[existingClientIndex];
                        let updated = false;

                        if (existingClient.firstName !== clientData.firstName) {
                            existingClient.firstName = clientData.firstName;
                            updated = true;
                        }
                        if (existingClient.lastName !== clientData.lastName) {
                            existingClient.lastName = clientData.lastName;
                            updated = true;
                        }
                        if (existingClient.mobilePhone !== clientData.mobilePhone) {
                            existingClient.mobilePhone = clientData.mobilePhone;
                            updated = true;
                        }
                        if (existingClient.email !== clientData.email) {
                            existingClient.email = clientData.email;
                            updated = true;
                        }
                        if (existingClient.dob !== clientData.dob) {
                            existingClient.dob = clientData.dob;
                            updated = true;
                        }
                        if (existingClient.streetAddress !== clientData.streetAddress) {
                            existingClient.streetAddress = clientData.streetAddress;
                            updated = true;
                        }

                        if (updated) {
                            clientsArray[existingClientIndex] = existingClient;
                            functions.logger.log(`Updated client ${personId} for care team member ${careTeamMember.idsGuid}`);
                        } else {
                            functions.logger.log(`No changes detected for client ${personId} in care team member ${careTeamMember.idsGuid}`);
                        }
                    } else {
                        clientsArray.push(clientData);
                        functions.logger.log(`Added new client ${personId} to care team member ${careTeamMember.idsGuid}`);
                    }

                    await careTeamDocRef.update({ clients: clientsArray });
                }
            }
        }

        functions.logger.log('Finished processing care team members.');
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

exports.getPrograms = functions.https.onRequest(async (req, res) => {
  try {
    results = await fsHelper.getPrograms(admin);
    functions.logger.log(`Firestore getPrograms finished`, {"resultCount": results.length});
    res.json({"records": results, "recordCount": results.length});
  } catch(err) {
    return res.status(500).send(err);
  }
});

//exports.deleteProgram = functions.https.onRequest(async (req, res) => {
exports.deleteProgram = onRequest({cors: true}, async (req, res) => {
  if (req.method !== 'DELETE') {
    return res.status(405).send('This endpoint only accepts DELETE requests');
  }

  try {
    functions.logger.log(`delete program called with id: ${req.query.id}`);
    //console.dir(req);
    results = await fsHelper.deleteOneProgram(admin, req.query.id);
    functions.logger.log(`Firestore delete program finished`, {"resultCount": results});
    res.json({result: `${results} program successfully deleted.`});
  } catch(err) {
    return res.status(500).send(err);
  }
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

exports.setProgramForm = functions.https.onRequest(async (req, res) => {
  try {
      results = await fsHelper.createOneProgramForm(admin, req);
          //console.log(`result was: ${results}`);
          functions.logger.log("Firestore create program form finised", {"resultCount": results});
          res.json({result: `${results} program form successfully created.`});
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

/*DELETED DURING MOVE TO BIG QUERY, LOOK IN THE SVELTE CODE NOW FOR THIS FUNCTION SOURCE
exports.getDataBundle = functions.https.onCall(async (data, context) => {

    functions.logger.log("getDataBundle called");
    try {
        var bundleData = await tibco.getDataBundle(context.auth.uid, data.affiliateProgram, data.sharedProgram, data.dataBundle);
    } catch(err) {
        return {failure: err}
    }
    
    //return {message: `success from getDataBundle for ${context.auth.token.email}`}
    return bundleData;

});
*/

exports.postMessageThread = functions.https.onCall(async (data, context) => {
    functions.logger.log("postMessageThread called");
    try {
        var postMessage = await tibco.postMessageThread(admin, context.auth.uid, data.sendTo, data.toProgram, data.message);
    } catch(err) {
        functions.logger.error(`failed with err: `, err);
        return {failure: err}
    }

    return postMessage;
});
