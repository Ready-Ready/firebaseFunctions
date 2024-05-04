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

const deleteDoc = async(admin, prog, collection) => {
    return new Promise(async (resolve, reject) =>{
        try {
            const result = await admin.firestore().collection(collection).doc(prog).delete();
            resolve(`Successfully deleted document from ${collection}`)
        } catch(err) {
            functions.logger.error('Error in deleteDoc function');
            reject(err);
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

const setForm = async(admin, doc, collection, id) => {
    return new Promise(async (resolve, reject) => {

        try {
            const result = await admin.firestore().collection(collection).doc(id).set(doc, {merge: true});
            resolve('Successfully set Program Form');
        } catch(err) {
            functions.logger.error('Error in Program Form Set function');
            reject(err);
        }        
    });
}

module.exports = {
    deleteOneProgram: async(admin, prog) => {
        return new Promise(async (resolve, reject) => {
            /*
            deleteDoc(admin, prog, 'programs')
            .then((result)=> {
                functions.logger.log("Finished running deleteOneProgram", {"resultCount": 1});
                resolve(1);
            })
            .catch(err => {
                functions.logger.error('error in deleteOneProgram:');
                functions.logger.error(err);
                reject('Error when deleting program in Firestore');
            })*/
            var curProgram = await admin.firestore().collection("programs").doc(prog).get();
            functions.logger.info(`found ${curProgram.ref.path} program to delete `);
            const bulkWriter = admin.firestore().bulkWriter();
            bulkWriter
              .onWriteError((error) => {
                if (
                  error.failedAttempts < MAX_RETRY_ATTEMPTS
                ) {
                  return true;
                } else {
                  console.log('Failed delete at document: ', error.documentRef.path);
                  return false;
                }
              });
              
            await admin.firestore().recursiveDelete(curProgram.ref.path, bulkWriter);
            //await curProgram.ref.path.recursiveDelete;

            functions.logger.log("Finished running deleteOneProgram", {"resultCount": 1});
            resolve(1);      
        });
    },
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
    createOneProgramForm: async(admin, req) => {
        return new Promise(async (resolve, reject) => {
            setForm(admin, req.body, `programs/${req.query.program}/forms`, req.query.id)
            .then((result)=>{
                functions.logger.log("Finished running createOneProgramForm", {"resultCount": 1});
                resolve(1);
            })
            .catch(err => {
                functions.logger.error('error in createOneProgramForm:');
                functions.logger.error(err);
                reject('Error when inserting program form to Firestore');
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
    },
    getPrograms: async(admin) => {
        return new Promise(async (resolve, reject) => {
            try{
                var collectionRef = await admin.firestore().collection("programs");
                var result = await collectionRef.get();
                var resultSend = [];
                for (const doc of result.docs){
                    resultSend.push(
                        {
                            active: doc.data().active,
                            recordTypeDeveloperName: doc.data().recordTypeDeveloperName,
                            name: doc.data().name,
                            AF_Master_Id__c: doc.data().AF_Master_Id__c,
                            id: doc.data().id,
                            status: doc.data().status
                        }
                    )
                }
                resolve(resultSend);
            } catch(err) {
                console.log('Error in getProgram function');
                console.log(err);
                reject(`Error from getPrograms Firestore function.  Error Name: ${err.name}; Error Message: ${err.message}`);
            }
        });
    }
}