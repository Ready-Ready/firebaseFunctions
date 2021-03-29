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

const setProgram = async(admin, prog, collection) => {
    return new Promise(async (resolve, reject) => {
        const objProg = {
            "name": prog.Name,
            "externalId": prog.Id,
            "id":  prog.ProgramID__c,
            "description": prog.Brief_Program_Desc__c
        };

        if(prog.Organization_Name__r){
            objProg.organizationName = prog.Organization_Name__r.Name;
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
        try {
            const result = await admin.firestore().collection(collection).doc(prog.Id).set(objProg, {merge: true});
            resolve('Successfully set Program');
        } catch(err) {
            functions.logger.error('Error in Program Set function');
            reject(err);
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
                    setProgram(admin, prog, collection)
                    .then((result)=>{

                    })
                    .catch(err => {
                        functions.logger.error('error in setProgram:');
                        functions.logger.error(err);
                    })
                );
            });

            functions.logger.log("Starting to run promises", {"promiseCount": aryPromises.length});
            //Promise.allSettled(aryPromises)
            Promise.all(aryPromises)
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