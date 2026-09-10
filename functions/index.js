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
            //if(ctm.idsGuid !== ''){
            if(ctm.idsGuid){
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
/*
denormCareTeamClients - the purpose of this trigger is to take the careTeam array on each clients affiliatedPrograms document and update
each care team's persons record to reflect the current care team.  This allows a denormalized copy of each care team's client list on their
own persons record to help performance of the CPA and avoid scanning the entire persons collection to find who they can see.

This function can generate a lot of actions (adds, updates, removals) and must complete in the Cloud Run Functions set limit of 240 seconds
Therefore, we are batching up the Firestore update actions as an array of Promises and then executing them all with 1 network round trip.
Since we submit the array of promises with a Promises.allSettled method, we can evaluate which of the promises were successful and which
failed.  We can then log the appropriate details of failures to the logs for follow-up and fixing.
*/
exports.denormCareTeamClients = functions.firestore
    .document('persons/{personId}/affiliatedPrograms/{apId}')
    .onWrite(async (change, context) => {
        const { personId, apId } = context.params;
        functions.logger.log(`Triggered for personId: ${personId}, apId: ${apId}`);

        const beforeData = change.before.exists ? change.before.data() : null;
        const afterData = change.after.exists ? change.after.data() : null;

        // Safely log stringified arrays for logging (accounting for undefined arrays)
        const beforeTeamLogged = beforeData?.careTeamMembers ? beforeData.careTeamMembers.map(m => m.idsGuid).join(', ') : '';
        const afterTeamLogged = afterData?.careTeamMembers ? afterData.careTeamMembers.map(m => m.idsGuid).join(', ') : '';
        functions.logger.log(`Clients array before update: ${beforeTeamLogged}`);
        functions.logger.log(`Clients array after update: ${afterTeamLogged}`);

        // Helper to format Date objects to YYYY-MM-DD
        const formatHTMLDate = (inDate) => {
            const yyyy = inDate.getFullYear();
            const mm = String(inDate.getMonth() + 1).padStart(2, '0');
            const dd = String(inDate.getDate()).padStart(2, '0');
            return `${yyyy}-${mm}-${dd}`;
        };

        const db = admin.firestore();
        const writePromises = [];

        // Helper to push individual operations into our parallel execution queue which eventually will be submitted with
        // Promises.allSettled.  This allows for consistent success/failure output so we can evaluate the results after
        // the batch runs and understand what action/document failed.
        const queueParallelUpdate = (ref, updatedArray, action) => {
            const promise = ref.update({ clients: updatedArray })
                .then(() => ({
                    status: 'success',
                    path: ref.path,
                    action
                }))
                .catch((err) => ({
                    status: 'failed',
                    path: ref.path,
                    action,
                    error: err.message
                }));
            
            writePromises.push(promise);
        };

        // =======================================================
        // NESTED HELPER FUNCTION - this function reviews all the results of promises batch and properly formatts the reporting into the
        // error logs.
        // =======================================================
        const logDetailedResults = (results) => {
            let successCount = 0;
            const failures = [];
            // functions.logger.log(`final results to log:`);
            // console.dir(results);

            results.forEach((result) => {
                if (result.status === 'fulfilled') {
                    const val = result.value;
                    if (val.status === 'success') {
                        successCount++;
                    } else {
                        failures.push(val);
                    }
                } else if (result.status === 'rejected') {
                    failures.push({
                        path: 'Unknown Document Reference',
                        action: 'unknown',
                        error: result.reason?.message || 'Rejected promise'
                    });
                }
            });

            functions.logger.log(`✅ Successfully completed ${successCount} parallel care team updates.`);

            if (failures.length > 0) {
                functions.logger.error(`❌ Failed to update ${failures.length} care team documents!`);
                failures.forEach(fail => {
                    functions.logger.error(`[${fail.action.toUpperCase()}] failed on document path: "${fail.path}" | Error: ${fail.error}`);
                });
            }
        };        

        // ==========================================
        // SCENARIO 1: DOCUMENT DELETION (Cleanup) - when the entire client AP document has been deleted
        // ==========================================
        if (!afterData) {
            functions.logger.log(`Document deleted for personId: ${personId}`);
            const careTeamMembers = beforeData?.careTeamMembers || [];
            const validMemberIds = careTeamMembers.map(m => m.idsGuid).filter(Boolean);

            if (validMemberIds.length === 0) return;

            // Fetch all care team docs in parallel
            const refs = validMemberIds.map(id => db.collection('persons').doc(id));
            const snapshots = await db.getAll(...refs);

            snapshots.forEach((docSnap) => {
                if (docSnap.exists) {
                    const originalClients = docSnap.data().clients || [];
                    const updatedClients = originalClients.filter(client => client.id !== personId);
                    
                    if (originalClients.length !== updatedClients.length) {
                        functions.logger.log(`Queueing removal of client ${personId} from care team ${docSnap.id}`);
                        queueParallelUpdate(docSnap.ref, updatedClients, 'remove');
                    }
                }
            });

            if (writePromises.length > 0) {
                const results = await Promise.allSettled(writePromises);
                logDetailedResults(results);
            }
            //If this is only a AP doc deletion, their is nothing else to do, so exit the function
            return;
        }

        // ==========================================
        // SCENARIO 2: CREATE / UPDATE
        // ==========================================
        const careTeamMembersBefore = beforeData?.careTeamMembers || [];
        const careTeamMembersAfter = afterData.careTeamMembers || [];

        if (careTeamMembersAfter.length === 0 && careTeamMembersBefore.length === 0) {
            functions.logger.log(`No careTeamMembers to process.`);
            return;
        }

        // Find added, removed, and sustained (potentially updated) members
        // INFO: using the filter(Boolean) is a highly efficient JavaScript shorthand used to extract a list of IDs and instantly
        // clean up any missing, null, or undefined values.  Since the default is a function that takes in each of the idsGuid values, 
        // any null, empty or undefinied idsGuid will evaluate to FALSE and be removed with the filter
        const addedIds = careTeamMembersAfter
            .filter(afterM => !careTeamMembersBefore.some(beforeM => beforeM.idsGuid === afterM.idsGuid))
            .map(m => m.idsGuid).filter(Boolean);

        functions.logger.log(`Number of care team to add ${personId} to their clients array: ${addedIds.length}`);
        if (addedIds.length > 0) functions.logger.log(addedIds.join(', '));

        const removedIds = careTeamMembersBefore
            .filter(beforeM => !careTeamMembersAfter.some(afterM => afterM.idsGuid === beforeM.idsGuid))
            .map(m => m.idsGuid).filter(Boolean);

        functions.logger.log(`Number of care team to REMOVE ${personId} from their clients array: ${removedIds.length}`);
        if (removedIds.length > 0) functions.logger.log(removedIds.join(', '));

        const sustainedIds = careTeamMembersAfter
            .filter(afterM => careTeamMembersBefore.some(beforeM => beforeM.idsGuid === afterM.idsGuid))
            .map(m => m.idsGuid).filter(Boolean);

        functions.logger.log(`Number of care team to SUSTAIN ${personId} on their clients array: ${sustainedIds.length}`);
        if (sustainedIds.length > 0) functions.logger.log(sustainedIds.join(', '));

        // Gather unique IDs we actually need to read from the DB
        const uniqueIdsToRead = [...new Set([...addedIds, ...removedIds, ...sustainedIds])];
        if (uniqueIdsToRead.length === 0) return;

        // Fetch client parent PII and care team PII documents in parallel so we can reference them later when 
        // denormalizing these attributes onto the Care Team's persons doc clients arreay
        const personDocRef = db.collection('persons').doc(personId);
        const targetRefs = uniqueIdsToRead.map(id => db.collection('persons').doc(id));

        const [personDoc, ...careTeamSnapshots] = await db.getAll(personDocRef, ...targetRefs);
        const personData = personDoc.exists ? personDoc.data() : {};

        // Prepare the standardized client entry payload
        const clientData = {
            id: personId,
            firstName: personData.firstName || null,
            lastName: personData.lastName || null,
            mobilePhone: personData.mobilePhone || null,
            email: personData.email || null,
            streetAddress: personData.streetAddress || null,
            clientType: personData.type || null,
            htmlFormattedDOB: null
        };

        if (personData.dateOfBirth && typeof personData.dateOfBirth !== 'string') {
            clientData.htmlFormattedDOB = formatHTMLDate(personData.dateOfBirth.toDate());
        }

        // Map Snapshots for easy lookup by document ID
        const careTeamMap = new Map(careTeamSnapshots.map(snap => [snap.id, snap]));

        // A. Process Removed Members
        removedIds.forEach(id => {
            const snap = careTeamMap.get(id);
            if (snap && snap.exists) {
                const originalClients = snap.data().clients || [];
                const filtered = originalClients.filter(c => c.id !== personId);
                if (originalClients.length !== filtered.length) {
                    functions.logger.log(`Queueing REMOVE of client ${personId} from care team ${id}`);
                    queueParallelUpdate(snap.ref, filtered, 'remove');
                }
            }
        });

        // B. Process Added Members
        addedIds.forEach(id => {
            const snap = careTeamMap.get(id);
            if (snap && snap.exists) {
                const originalClients = snap.data().clients || [];
                // Prevent duplicate entries by removing the client we are processing it and re-adding with fresh PII data
                const filtered = originalClients.filter(c => c.id !== personId);
                filtered.push(clientData);
                functions.logger.log(`Queueing ADD of client ${personId} to care team ${id}`);
                //functions.logger.log(`Care Team being updated:  ${snap.ref}`);
                //functions.logger.log(`Filtered Data being updated:`);
                //console.dir(filtered);
                queueParallelUpdate(snap.ref, filtered, 'add');
            }
        });

        // C. Process Sustained (Potentially Updated) Members
        sustainedIds.forEach(id => {
            const snap = careTeamMap.get(id);
            if (snap && snap.exists) {
                const originalClients = snap.data().clients || [];
                const existingIndex = originalClients.findIndex(c => c.id === personId);

                if (existingIndex !== -1) {
                    const existingClient = originalClients[existingIndex];
                    let changed = false;

                    // Deep-compare properties that matter to skip wasteful writes
                    const fieldsToCheck = ['firstName', 'lastName', 'mobilePhone', 'email', 'streetAddress', 'clientType', 'htmlFormattedDOB'];
                    for (const key of fieldsToCheck) {
                        if (existingClient[key] !== clientData[key]) {
                            existingClient[key] = clientData[key];
                            changed = true;
                        }
                    }

                    if (changed) {
                        originalClients[existingIndex] = existingClient;
                        functions.logger.log(`Queueing UPDATE of client ${personId} on care team ${id}`);
                        queueParallelUpdate(snap.ref, originalClients, 'update');
                    }
                } else {
                    // Fallback: If they were sustained but somehow missing from the array, append them
                    originalClients.push(clientData);
                    functions.logger.log(`Queueing ADD (fallback) of client ${personId} to care team ${id}`);
                    queueParallelUpdate(snap.ref, originalClients, 'add');
                }
            }
        });

        // Execute all updates concurrently and process results
        if (writePromises.length > 0) {
            functions.logger.log(`Executing ${writePromises.length} parallel updates via Promise.allSettled...`);
            const results = await Promise.allSettled(writePromises);
            logDetailedResults(results);
        } else {
            functions.logger.log('No change in data detected. Skipped writing to database.');
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
/*
*** MPA (6/24/26): No longer used, this was originally for the POC demos
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
*/
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
/*
*** MPA (6/24/26): No longer used, this was originally for the POC demos for client/care team messaging
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
*/
