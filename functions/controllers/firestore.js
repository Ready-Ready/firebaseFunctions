const functions = require("firebase-functions");
const { error } = require("firebase-functions/lib/logger");

const checkExists = async(admin, doc, collection, externalId) => {
    return new Promise(async (resolve, reject) => {
        try{
            var collectionRef = await admin.firestore().collection(collection);
            var result = await collectionRef.where(externalId, "==", doc.Id).get();
            resolve(result);
        } catch(err) {
            console.log('Error in checkExists function');
            console.log(err);
            reject('Error determine if doc exists in Firestore');
        }
    });
}

module.exports = {
    createMany: async(admin, docs, collection) => {
        return new Promise(async (resolve, reject) => {

            var aryPromises = [];


            functions.logger.log("In the FS createMany function", {"docCount": docs.length});
            docs.forEach(prog => {
                aryPromises.push(
                    checkExists(admin, prog, collection, 'externalId')
                    .then(result => {
                        if(!result.empty){
                            result.forEach(function(doc) {
                                try{
                                    doc.ref.delete();
                                    //functions.logger.log("Existing program deleted", {"name": prog.name});
                                } catch(err) {
                                    error("Existing program FAILED delete", {"name": prog.name});
                                }
                            });
                        }

                        const objProg = {
                            "name": prog.Name,
                            "externalId": prog.Id,
                            "id":  prog.ProgramID__c,
                            "organizationName": prog.Organization_Name__r.Name,
                            "description": prog.Brief_Program_Desc__c
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
                    
                        admin.firestore().collection(collection).add(objProg);                       
                    })
                    .catch(err => {
                        error("Checking if doc exists FAILED", {"name": prog.name});
                    })
                );
            });

            functions.logger.log("Starting to run promises", {"promiseCount": aryPromises.length});
            Promise.allSettled(aryPromises)
            .then(async (results) => {
                functions.logger.log("Finished running promises", {"resultCount": results.length});
                resolve(results.length);
            })
            .catch(err => {
                console.log('error inserting to Firestore');
                error("Running promises", {"error": err});
                //return res.status(401).send('Error when logging into Salesforce');
                reject('Error when inserting to Firestore');
            });

        });
    }
}