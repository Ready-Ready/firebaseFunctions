# firebaseFunctions

Deploying specific functions:
- make sure you are pointed to correct environment using:  firebase use [env] (development, staging, default)
- firebase deploy --only functions:[function-name]

To test functions locally:
- firebase emulators:start --only functions
