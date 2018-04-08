const fs = require("fs");
const admin = require("firebase-admin");

const logger = require("./logger");
const serviceAccount = require("./service-account-key.json");

const filePath = process.argv[2];
const collection = process.argv[3] || "words.basic";

if (!filePath || !fs.statSync(filePath)) {
  return logger.error("File not found");
}

if (!collection) {
  logger.log("Using 'words.basic' collection");
}

const dataFileContent = JSON.parse(fs.readFileSync(filePath));

if (!Array.isArray(dataFileContent)) {
  return logger.error("Invalid JSON file format. Root element must be Array");
}

logger.log(`File "${filePath}" has ${dataFileContent.length} docs`);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://langlearn-ef6ef.firebaseio.com"
});

const store = admin.firestore();

(async () => {
  const docsForRemove = [];
  const docsForUpdate = [];

  const dataSnapshot = await store.collection(collection).get();

  logger.log(`Found ${dataSnapshot.size} docs in "${collection}" collection`);

  dataSnapshot.forEach(doc => {
    const docData = doc.data();
    const docFromFileIndex = dataFileContent.findIndex(
      i => i.name == docData.name
    );
    if (~docFromFileIndex) {
      docsForUpdate.push({
        id: doc.id,
        data: dataFileContent[docFromFileIndex]
      });
      dataFileContent.splice(docFromFileIndex, 1);
    } else {
      docsForRemove.push(doc.id);
    }
  });

  dataBatchUpdate({
    dataForUpdate: docsForUpdate,
    dataForRemove: docsForRemove,
    dataForCreate: dataFileContent
  });
})();

const actionTypes = {
  CREATE: "set",
  UPDATE: "update",
  DELETE: "delete"
};

async function dataBatchUpdate(
  { dataForUpdate, dataForRemove, dataForCreate },
  collectionName = collection
) {
  const actions = [];

  logger.warn(`| ${dataForCreate.length} docs will be created`);
  dataForCreate.forEach(data => {
    actions.push({ type: actionTypes.CREATE, data });
  });

  logger.warn(`| ${dataForUpdate.length} docs will be updated`);
  dataForUpdate.forEach(data => {
    actions.push({ type: actionTypes.UPDATE, data });
  });

  logger.warn(`| ${dataForRemove.length} docs will be deleted`);
  dataForRemove.forEach(docId => {
    actions.push({ type: actionTypes.DELETE, data: docId });
  });

  logger.log("Group sync started");
  let index = 1;
  while (actions.length) {
    await batchUpdate(actions.splice(0, 500), index);
    index++;
  }
  logger.log("Group sync finished");
}

async function batchUpdate(data, group, collectionName = collection) {
  if (data.length > 500) {
    return logger.error("Cannot write more than 500 entities in a single call");
  }

  logger.success(`| Group ${group} sync started: ${data.length} items`);

  const batch = store.batch();

  data.forEach(action => {
    callBatchAction(action.type, action.data, batch);
  });

  return batch
    .commit()
    .then(() => {
      logger.success(`| Group ${group} sync finished`);
    })
    .catch(e => {
      logger.error("\x1b[31m", `| Group ${group} sync failed`);
      logger.error(e);
    });
}

function callBatchAction(type, docData, batch, collectionName = collection) {
  switch (type) {
    case actionTypes.CREATE:
      return batch[actionTypes.CREATE](
        store.collection(collectionName).doc(),
        docData
      );
    case actionTypes.UPDATE:
      return batch[actionTypes.UPDATE](
        store.collection(collectionName).doc(docData.id),
        docData.data
      );
    case actionTypes.DELETE:
      return batch[actionTypes.DELETE](
        store.collection(collectionName).doc(docData)
      );
  }
}
