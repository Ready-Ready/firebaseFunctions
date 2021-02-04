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

            try {
                docs.forEach(async (prog, idx) => {
                    try {
                        var result = await checkExists(admin, prog, collection, 'externalId');
                    } catch(err) {
                        reject('Error when checking if doc exists in Firestore');
                    }
                    
                    if(!result.empty){
                        result.forEach(function(doc) {
                            doc.ref.delete();
                          });
                    }

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
                
                    admin.firestore().collection(collection).add(objProg);

                });            

                resolve(docs.length);

            } catch(err) {
                console.log('error inserting to Firestore');
                //return res.status(401).send('Error when logging into Salesforce');
                reject('Error when inserting to Firestore');
                console.log(err);                
            }
                    

        });
    }
}