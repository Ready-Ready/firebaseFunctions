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

        try {
            const result = await admin.firestore().collection(collection).doc(prog.id).set(prog, {merge: true});
            resolve('Successfully set Program');
        } catch(err) {
            functions.logger.error('Error in Program Set function');
            reject(err);
        }        
    });
}

module.exports = {
    createOneProgram: async(admin, doc) => {
        return new Promise(async (resolve, reject) => {
            setProgram(admin, doc, 'programs')
            .then((result)=>{
                functions.logger.log("Finished running createOneProgram", {"resultCount": 1});
                resolve(1);
            })
            .catch(err => {
                functions.logger.error('error in createOneProgram:');
                functions.logger.error(err);
                reject('Error when inserting program to Firestore');
            })            
        });
    },
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