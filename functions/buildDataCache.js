const functions = require('@google-cloud/functions-framework');
const admin = require("firebase-admin");
const { BigQuery } = require('@google-cloud/bigquery');
var https = require('https');
admin.initializeApp();

// Register a CloudEvent callback with the Functions Framework that will
// be executed when the Pub/Sub trigger topic receives a message.
functions.cloudEvent('updateDataBundle', async (cloudEvent) => {
 // The Pub/Sub message is passed as the CloudEvent's data payload.
    const base64name = cloudEvent.data.message.data;

    const name = base64name
    ? Buffer.from(base64name, 'base64').toString()
    : 'World';

    console.log(`Hello, ${name}!`);

    const envVariables = await admin.firestore().collection("environmentVariables").orderBy('highWatermarkBigQueryAccount', 'desc').limit(1).get();

    var env;
    var envData;
    envVariables.forEach(doc => {
        envData = doc.data();
        env = doc;
    });

    const date = envData.highWatermarkBigQueryAccount.toDate();

    await env.ref.update({highWatermarkBigQueryAccount : admin.firestore.Timestamp.fromDate(new Date())});

    const bigquery = new BigQuery();

    /** ACCOUNT QUERY **/
    const accQuery = `SELECT *
    FROM \`${process.env.gcpProject}.RR_DataWarehouse.Account_latest\`
    WHERE rowLoadDate >= @watermarkDate`;
    
    const  accOptions = {
        query: accQuery,
        params : {watermarkDate: date.toISOString().replace('Z', '')},
    };

    const [accounts] = await bigquery.query(accOptions);
     
    if(accounts.length){

        const postBody = "grant_type=client_credentials"

        const encoded64Auth = btoa(process.env.ClientId + ':' + process.env.ClientSecret);

        var options = {
            host: process.env.HOST,
            path: `/v2.0/auth`,
            headers: {'Authorization': 'Basic ' + encoded64Auth, 'Content-Type': 'application/x-www-form-urlencoded', 'Content-Length': Buffer.byteLength(postBody)},
            method: 'POST'
        };

        var tokenResponse = await doRequest(options, postBody);
        var accessToken;

        if(tokenResponse.access_token){
             accessToken = tokenResponse.access_token;
        } else {
            return 'Error getting access token'
        }

        for(const account of accounts){
            console.log('Data Bundles Function reached here')

            var personId = account.IDS_Global_ID__c;
            var affiliateProgram = account.ProgramId;

            if(personId == '' || personId == null){
                console.log('No person id')
                continue;
            }

            if(affiliateProgram == '' || affiliateProgram == null){
                console.log('No affiliate program id')
                continue;
            }

            var options = {
                host: process.env.HOST,
                path: `/v2.0/persons/${personId}/affiliatedPrograms/${affiliateProgram}/sharedPrograms/${process.env.RootsToReadyId}/dataBundles`,
                headers: {'Authorization': 'Bearer ' + accessToken, 'X-Mashery-Oauth-Client-Id' : process.env.ClientId, 'Content-Type': 'application/json'},
                method: 'GET'
            };

            console.log(`personId ${personId} + aId ${affiliateProgram} + sharedProgram ${process.env.RootsToReadyId}`);

            var dataBundles = await doRequest(options, '');

            if(dataBundles.dataBundleTypes){
                for(const db of dataBundles.dataBundleTypes){
                    console.log('inside data bundles ' + db.name)
                    var dataBundleResult = await buildDataBundle(personId, affiliateProgram, db, bigquery);
                }
            }
        }   
    }

    console.log('end of Data Bundles Function')
    console.log('Success')
    
});


function doRequest(options, postBody) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      res.setEncoding('utf8');
      let responseBody = '';
      var result;

      res.on('data', (chunk) => {
        responseBody += chunk;
      });

      res.on('end', () => {
        try {
            result = JSON.parse(responseBody);
            resolve(result);
        } catch (e) {
             resolve('Request Error');
             console.log('Request Error ' + responseBody);
        }
      });
    });

    req.on('error', (err) => {
      reject(err);
    });

    if(postBody != ''){
        req.write(postBody)
    }
    req.end();
  });
}

async function buildDataBundle(personId, affiliateProgramId, dataBundleType, bigquery) {
        return new Promise(async (resolve, reject) => {

            var result = {};
            try{

                console.log(dataBundleType.name)
                if(dataBundleType.name == 'CN Touchpoints (full)' || dataBundleType.name == 'Touchpoints > Consent + Interactions' || 
                    dataBundleType.name == 'Community Navigation Touchpoints' || dataBundleType.name == 'Community Navigation Touchpoints (limited)' ||
                    dataBundleType.name == 'Family Connects Touchpoints' || dataBundleType.name == 'Family Connects Touchpoints (limited)'){

                    console.log('Beginning of Data Bundle build')
                    
                    /** ACCOUNT QUERY **/
                    const accQuery = `SELECT *
                    FROM \`${process.env.gcpProject}.RR_DataWarehouse.Account_latest\`
                    WHERE IDS_Global_ID__c = @guid`;

                    const  accOptions = {
                        query: accQuery,
                        params: {guid: personId},
                    };

                    const [account] = await bigquery.query(accOptions);

                    result.totalSize = 1;
                    result.records = account;
                    result.records[0].attributes = {type : 'Account'}

                    const caseQuery = `SELECT *
                    FROM \`${process.env.gcpProject}.RR_DataWarehouse.Case_latest\`
                    WHERE Client = @clientId 
                    ORDER BY rowLoadDate DESC`;

                    const  caseOptions = {
                        query: caseQuery,
                        params: {clientId: result.records[0].ID},
                    };

                    const [cases] = await bigquery.query(caseOptions);

                    result.records[0].account_case = {};
                    result.records[0].account_case.totalSize = cases.length;
                    result.records[0].account_case.records = cases;
                    

                    /** PROGRAM QUERY **/
                    const accProgramQuery = `SELECT Name
                    FROM \`${process.env.gcpProject}.RR_DataWarehouse.Program__c\`
                    WHERE ID = @ProgramId`;

                    const  accProgramQueryOptions = {
                        query: accProgramQuery,
                        params: {ProgramId: result.records[0].ProgramId},
                    };

                    const [accPrograms] = await bigquery.query(accProgramQueryOptions);

                    if(accPrograms.length){
                        result.records[0].ProgramId = accPrograms[0]?.Name;
                    }

                    for(const currCase of result.records[0].account_case.records){

                        currCase.attributes = {type : 'Case'};
                        
                        /** CASE TO USER QUERY **/
                        if(currCase.OwnerId){
                            var caseUserQuery = `SELECT FirstName, LastName
                            FROM \`${process.env.gcpProject}.RR_DataWarehouse.User_latest\`
                            WHERE ID = @UserId`;

                            var  caseUserOptions = {
                                query: caseUserQuery,
                                params: {UserId: currCase?.OwnerId},
                            };

                            var [caseUsers] = await bigquery.query(caseUserOptions);

                            if(caseUsers.length){
                                currCase.OwnerId = caseUsers[0].FirstName + ' ' + caseUsers[0].LastName;
                            }
                        }

                        /** CASE TOUCHPOINT SITE QUERY **/
                        if(currCase.Touchpoint_Site){
                            const caseTouchpointSiteQuery = `SELECT Site
                            FROM \`${process.env.gcpProject}.RR_DataWarehouse.nav_Touchpoint_Site__c_latest\`
                            WHERE ID = @TouchpointSite`;

                            const caseTouchpointSiteOptions = {
                                query: caseTouchpointSiteQuery,
                                params: {TouchpointSite: currCase?.Touchpoint_Site},
                            };

                            const [caseTouchpointSite] = await bigquery.query(caseTouchpointSiteOptions);

                            if(caseTouchpointSite.length){
                                currCase.Touchpoint_Site = caseTouchpointSite[0]?.Site;
                            }
                        }

                        if(currCase.Consent){
                            /** CASE CONSENT QUERY **/
                            const caseConsentQuery = `SELECT RecordType,Consent_Date
                            FROM \`${process.env.gcpProject}.RR_DataWarehouse.nav_Consent__c\`
                            WHERE ID = @id`;

                            const  caseConsentOptions = {
                                query: caseConsentQuery,
                                params: {id: currCase?.Consent},
                            };

                            const [caseConsent] = await bigquery.query(caseConsentOptions);

                            if(caseConsent.length){
                                currCase.Consent = caseConsent[0].RecordType + ' ' + caseConsent[0]?.Consent_Date?.value;
                            }

                        }

                        if(currCase.ProgramId){
                            /** PROGRAM QUERY **/
                            const caseProgramQuery = `SELECT Name
                            FROM \`${process.env.gcpProject}.RR_DataWarehouse.Program__c\`
                            WHERE ID = @ProgramId`;

                            const  caseProgramQueryOptions = {
                                query: caseProgramQuery,
                                params: {ProgramId: currCase?.ProgramId},
                            };

                            const [casePrograms] = await bigquery.query(caseProgramQueryOptions);

                            if(casePrograms.length){
                                currCase.ProgramId = casePrograms[0].Name;
                            }
                        }


                        /** CASE TEAM MEMBER QUERY **/
                        const teamMemberQuery = `SELECT *
                        FROM \`${process.env.gcpProject}.RR_DataWarehouse.CaseTeamMember_latest\`
                        WHERE ParentId = @ParentId`;

                        const  teamMemberOptions = {
                            query: teamMemberQuery,
                            params: {ParentId: currCase.ID},
                        };

                        const [teamMembers] = await bigquery.query(teamMemberOptions);

                        currCase.case_teamMember = {};
                        currCase.case_teamMember.totalSize = teamMembers.length;
                        currCase.case_teamMember.records = teamMembers;
                        
                        for(const member of currCase.case_teamMember.records){

                            member.attributes = {type : 'CaseTeamMember'};
                            /** USER QUERY **/
                            var userQuery = `SELECT *
                            FROM \`${process.env.gcpProject}.RR_DataWarehouse.User_latest\`
                            WHERE ID = @MemberId`;

                            var  userOptions = {
                                query: userQuery,
                                params: {MemberId: member.MemberId},
                            };

                            var [users] = await bigquery.query(userOptions);

                            member.teamMember_user = {};
                            member.teamMember_user.totalSize = users.length;
                            member.teamMember_user.records = users;

                            for(const user of member.teamMember_user.records){
                            
                                /** PROGRAM QUERY **/
                                if(user.ProgramId){
                                    const userProgramQuery = `SELECT Name
                                    FROM \`${process.env.gcpProject}.RR_DataWarehouse.Program__c\`
                                    WHERE ID = @ProgramId`;

                                    const  userProgramQueryOptions = {
                                        query: userProgramQuery,
                                        params: {ProgramId: user.ProgramId},
                                    };

                                    const [userPrograms] = await bigquery.query(userProgramQueryOptions);

                                    if(userPrograms.length){
                                        user.ProgramId = userPrograms[0]?.Name;
                                    }
                                }

                                user.attributes = {type : 'User'}
                                user.Name = user.FirstName + ' ' + user.LastName;
                            }
                            
                            if(member.teamMember_user?.records){
                                member.MemberId = member.teamMember_user.records[0].FirstName + ' ' + member.teamMember_user.records[0].LastName; 
                            }
                             
                        }

                        /** ASSESSMENT QUERY **/
                        var assessmentQuery = `SELECT *
                        FROM \`${process.env.gcpProject}.RR_DataWarehouse.nav_Assessment__c_latest\`
                        WHERE Touchpoint = @CaseId`;

                        var  assessmentOptions = {
                            query: assessmentQuery,
                            params: {CaseId: currCase.ID},
                        };

                        var [assessments] = await bigquery.query(assessmentOptions);

                        currCase.case_assessment = {};
                        currCase.case_assessment.totalSize = assessments.length;
                        currCase.case_assessment.records = assessments;

                        for(const assessment of currCase.case_assessment.records){

                            assessment.attributes = {type : 'nav_Assessment__c'};

                            /** ASSESSMENT TO INTERACTION QUERY **/
                            if(assessment.Interaction){
                                const assessmentInteractionQuery = `SELECT RecordType
                                FROM \`${process.env.gcpProject}.RR_DataWarehouse.nav_Interaction__c_latest\`
                                WHERE ID = @InteractionId`;

                                const  assessmentInteractionOptions = {
                                    query: assessmentInteractionQuery,
                                    params: {InteractionId: assessment.Interaction},
                                };

                                const [assessmentInteractions] = await bigquery.query(assessmentInteractionOptions);

                                if(assessmentInteractions.length){
                                    assessment.Interaction = assessmentInteractions[0]?.RecordType;
                                }
                            }

                            /** MATRIX QUERY **/
                            var matrixQuery = `SELECT *
                            FROM \`${process.env.gcpProject}.RR_DataWarehouse.nav_Matrix_Factor__c_latest\`
                            WHERE Matrix = @assessmentId`;

                            var  matrixOptions = {
                                query: matrixQuery,
                                params: {assessmentId: assessment.ID},
                            };

                            var [matrix] = await bigquery.query(matrixOptions);
                            
                            assessment.assessment_matrix = {};
                            assessment.assessment_matrix.totalSize = matrix.length;
                            assessment.assessment_matrix.records = matrix;
                            
                            for(const mx of assessment.assessment_matrix.records){
                                mx.attributes = {type : 'nav_Matrix_Factor__c'};
                            }

                        }


                        /** INTERACTIONS QUERY **/
                        const interactionQuery = `SELECT *
                        FROM \`${process.env.gcpProject}.RR_DataWarehouse.nav_Interaction__c_latest\`
                        WHERE Touchpoint__c = @CaseId`;

                        const  interactionOptions = {
                            query: interactionQuery,
                            params: {CaseId: currCase.ID},
                        };

                        const [interactions] = await bigquery.query(interactionOptions);

                        currCase.case_interactions = {};
                        currCase.case_interactions.totalSize = interactions.length;
                        currCase.case_interactions.records = interactions;

                        for(const interaction of currCase.case_interactions.records){
                            interaction.attributes = {type : 'nav_Interaction__c'}
                        }
                    
                        /** RESOURCES QUERY **/
                        const resourcesQuery = `SELECT *
                        FROM \`${process.env.gcpProject}.RR_DataWarehouse.nav_Resource__c_latest\`
                        WHERE Touchpoint = @CaseId AND Referral_Status != @ShareClient AND Referral_Status != @Preparing ORDER BY Date_of_Referral, Referral_Status`;

                        const  resourcesOptions = {
                            query: resourcesQuery,
                            params: {CaseId: currCase.ID, ShareClient : 'Made/Ready to Share with Client', Preparing: 'Preparing to Make'},
                        };

                        const [resources] = await bigquery.query(resourcesOptions);

                        currCase.case_resources = {};
                        currCase.case_resources.totalSize = resources.length;
                        currCase.case_resources.records = resources;

                        for(const resource of currCase.case_resources.records){
                            resource.attributes = {type : 'nav_Resource__c'}

                            /** PROGRAM QUERY **/
                            if(resource.Program){
                                const resourceProgramQuery = `SELECT Name
                                FROM \`${process.env.gcpProject}.RR_DataWarehouse.Program__c\`
                                WHERE ID = @ProgramId`;

                                const resourceProgramOptions = {
                                    query: resourceProgramQuery,
                                    params: {ProgramId: resource.Program},
                                };

                                const [resourceProgram] = await bigquery.query(resourceProgramOptions);

                                if(resourceProgram.length){
                                    currCase.Program = resourceProgram[0].Name;
                                }
                            }


                            /** RESOURCE TO INTERACTION QUERY **/
                            if(resource.Interaction){
                                var resourceInteractionQuery = `SELECT RecordType
                                FROM \`${process.env.gcpProject}.RR_DataWarehouse.nav_Interaction__c_latest\`
                                WHERE ID = @InteractionId`;

                                var resourceInteractionOptions = {
                                    query: resourceInteractionQuery,
                                    params: {InteractionId: resource.Interaction},
                                };

                                var [resourceInteraction] = await bigquery.query(resourceInteractionOptions);

                                if(resourceInteraction.length){
                                    resource.Interaction = resourceInteraction[0].RecordType;
                                }
                            }


                            /** RESOURCE TO SERVICE TYPE QUERY **/
                            if(resource.Service_Type){
                                var resourceServiceTypeQuery = `SELECT Name
                                FROM \`${process.env.gcpProject}.RR_DataWarehouse.Service_Type__c\`
                                WHERE ID = @ServiceTypeId`;

                                var resourceServiceTypeOptions = {
                                    query: resourceServiceTypeQuery,
                                    params: {ServiceTypeId: resource.Service_Type},
                                };

                                var [resourceServiceType] = await bigquery.query(resourceServiceTypeOptions);

                                if(resourceServiceType.length){
                                    resource.Service_Type = resourceServiceType[0].Name;
                                }
                            }


                            /** RESOURCE TO LOCATION QUERY **/
                            if(resource.Location){
                                var resourceLocation = `SELECT Street_Address, City
                                FROM \`${process.env.gcpProject}.RR_DataWarehouse.Location__c\`
                                WHERE ID = @LocationId`;

                                var resourceLocationOptions = {
                                    query: resourceLocation,
                                    params: {LocationId: resource.Location},
                                };

                                var [resourceLocation] = await bigquery.query(resourceLocationOptions);

                                if(resourceLocation.length){
                                    resource.Location = resourceLocation[0].Street_Address + ', ' + resourceLocation[0].City;
                                }
                            }


                        }
                    

                        /** PERSON NEED QUERY **/
                        const personNeedQuery = `SELECT *
                        FROM \`${process.env.gcpProject}.RR_DataWarehouse.nav_Person_Need__c_latest\`
                        WHERE Touchpoint = @CaseId ORDER BY Need_Status DESC`;

                        const personNeedOptions = {
                            query: personNeedQuery,
                            params: {CaseId: currCase.ID},
                        };

                        const [personNeed] = await bigquery.query(personNeedOptions);

                        currCase.case_personNeed = {};
                        currCase.case_personNeed.totalSize = personNeed.length;
                        currCase.case_personNeed.records = personNeed;

                        for(const personNeed of currCase.case_personNeed.records){

                            personNeed.attributes = {type : 'nav_Person_Need__c'}
                            
                            /** PERSON NEED TO INTERACTION QUERY **/
                            if(personNeed.Interaction ){
                                var personNeedInteractionQuery = `SELECT RecordType
                                FROM \`${process.env.gcpProject}.RR_DataWarehouse.nav_Interaction__c_latest\`
                                WHERE ID = @InteractionId`;

                                var personNeedInteractionOptions = {
                                    query: personNeedInteractionQuery,
                                    params: {InteractionId: personNeed.Interaction},
                                };

                                var [personNeedInteraction] = await bigquery.query(personNeedInteractionOptions);

                                if(personNeedInteraction.length){
                                    personNeed.Interaction = personNeedInteraction[0].RecordType;
                                }
                            }

                            
                            /** PERSON NEED TO SERVICE TYPE QUERY **/
                            if(personNeed.Need){
                                var personNeedServiceTypeQuery = `SELECT Name
                                FROM \`${process.env.gcpProject}.RR_DataWarehouse.Service_Type__c\`
                                WHERE ID = @NeedId`;

                                var personNeedServiceOptions = {
                                    query: personNeedServiceTypeQuery,
                                    params: {NeedId: personNeed.Need},
                                };

                                var [personNeedServiceType] = await bigquery.query(personNeedServiceOptions);

                                if(personNeedServiceType.length){
                                    personNeed.Need = personNeedServiceType[0].Name;
                                }
                            }


                            /** NEED RESOURCE QUERY **/
                            var needResourceQuery = `SELECT *
                            FROM \`${process.env.gcpProject}.RR_DataWarehouse.nav_Need_Resource__c_latest\`
                            WHERE Person_Need = @PersonNeedId`;

                            var needResourceOptions = {
                                query: needResourceQuery,
                                params: {PersonNeedId: personNeed.ID},
                            };

                            var [needResource] = await bigquery.query(needResourceOptions);

                            personNeed.personNeed_needResource  = {};
                            personNeed.personNeed_needResource.totalSize = needResource.length;
                            personNeed.personNeed_needResource.records = needResource;

                            for(const needResource of  personNeed.personNeed_needResource.records){
                                needResource.attributes = {type : 'nav_Need_Resource__c'}
                                needResource.Person_Need = personNeed.Need;

                                /** NEED RESOURCE TO RESOURCE QUERY **/
                                if(needResource.Resource ){
                                    var nrToResourceQuery = `SELECT RecordType
                                    FROM \`${process.env.gcpProject}.RR_DataWarehouse.nav_Resource__c_latest\`
                                    WHERE ID = @ResourceId AND Referral_Status != @ShareClient AND Referral_Status != @Preparing ORDER BY Date_of_Referral, Referral_Status`;

                                    var nrToResourceOptions = {
                                        query: nrToResourceQuery,
                                        params: {ResourceId: needResource.Resource, ShareClient: 'Made/Ready to Share with Client', Preparing: 'Preparing to Make'},
                                    };

                                    var [nrToResource] = await bigquery.query(nrToResourceOptions);
                                    
                                    if(nrToResource.length){
                                        needResource.Resource = nrToResource[0].RecordType;
                                    }
                                }

                            }

                        }

                        /** TOUCHPOINT CHILD QUERY **/
                        const touchpointChildQuery = `SELECT *
                        FROM \`${process.env.gcpProject}.RR_DataWarehouse.nav_Touchpoint_Child__c_latest\`
                        WHERE Touchpoint = @CaseId`;

                        const touchpointChildOptions = {
                            query: touchpointChildQuery,
                            params: {CaseId: currCase.ID},
                        };

                        const [touchpointChild] = await bigquery.query(touchpointChildOptions);

                        currCase.case_touchpointChild  = {};
                        currCase.case_touchpointChild.totalSize = touchpointChild.length;
                        currCase.case_touchpointChild.records = touchpointChild;

                        for(const tc of currCase.case_touchpointChild.records){
                            tc.attributes = {type : 'nav_Touchpoint_Child__c'}
                        }
                    

                        /** TOUCHPOINT SITE QUERY **/
                        const touchpointSiteQuery = `SELECT *
                        FROM \`${process.env.gcpProject}.RR_DataWarehouse.nav_Touchpoint_Site__c_latest\`
                        WHERE Touchpoint = @CaseId`;

                        const touchpointSiteOptions = {
                            query: touchpointSiteQuery,
                            params: {CaseId: currCase.ID},
                        };

                        const [touchpointSite] = await bigquery.query(touchpointSiteOptions);

                        currCase.case_touchpointSite  = {};
                        currCase.case_touchpointSite.totalSize = touchpointSite.length;
                        currCase.case_touchpointSite.records = touchpointSite;

                        for(const ts of currCase.case_touchpointSite.records){
                            ts.attributes = {type : 'nav_Touchpoint_Site__c'}
                        }
                        

                    }
                    
                }

                if(Object.keys(result).length){
                    const cacheData = {
                        'PersonId': personId,
                        'AffiliateProgramId': affiliateProgramId,
                        'DataBundleId': dataBundleType.dataBundleId,
                        'BundleCache' : JSON.stringify(result),
                        'rowLoadDate' : new Date().toISOString().replace('Z', '')
                    };

                    const job = await bigquery
                        .dataset('RR_DataWarehouse')
                        .table('DataBundleCache')
                        .insert(cacheData)
                        .then((data) => {
                            const apiResponse = data;
                            console.log(`apiResponse:: ${apiResponse}`);
                        })
                        .catch((err) => { console.log(`err: ${err}`); });

                    console.log('Data Cache Created');
                }


            } catch(err) {
                console.error(`Error running query:`);
                console.error(err);        
                return reject(`Error running query`);
            }


            if(result) {
                return resolve(result);
            } else {
                return reject(`No results returned`);
            }
        
        });
}