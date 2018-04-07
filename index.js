const fs = require("fs");
const admin = require("firebase-admin");
const serviceAccount = require("./service-account-key.json");

const filePath = process.argv[2];
const collection = process.argv[3] || "words.basic";

if (!filePath || !fs.statSync(filePath)) {
  return console.error("File not found");
}

if (!collection) {
  console.log("Using 'words.basic' collection");
}

const dataFileContent = JSON.parse(fs.readFileSync(filePath));

if (!Array.isArray(dataFileContent)) {
  return console.error("Invalid JSON file format. Root element must be Array");
}

console.log(`File "${filePath}" has ${dataFileContent.length} docs`);

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: "https://langlearn-ef6ef.firebaseio.com"
});

const store = admin.firestore();

(async () => {
  const docsForRemove = [];
  const docsForUpdate = [];

  const dataSnapshot = await store.collection(collection).get();

  console.log(`Found ${dataSnapshot.size} docs in "${collection}" collection`);

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

  batchUpdate({
    dataForUpdate: docsForUpdate,
    dataForRemove: docsForRemove,
    dataForCreate: dataFileContent
  });
})();

async function batchUpdate(
  { dataForUpdate, dataForRemove, dataForCreate },
  collectionName = collection
) {
  console.log(`${dataForCreate.length} docs will be created`);
  console.log(`${dataForUpdate.length} docs will be updated`);
  console.log(`${dataForRemove.length} docs will be deleted`);

  const batch = store.batch();

  console.log(`Firebase batch sync started`);

  dataForCreate.forEach(docData => {
    batch.set(store.collection(collectionName).doc(), docData);
  });

  dataForUpdate.forEach(doc => {
    batch.update(store.collection(collectionName).doc(doc.id), doc.data);
  });

  dataForRemove.forEach(docId => {
    batch.delete(store.collection(collectionName).doc(docId));
  });

  return batch
    .commit()
    .then(() => {
      console.log("Firebase batch sync finished");
    })
    .catch(() => {
      console.warn("Firebase batch sync failed");
    });
}
