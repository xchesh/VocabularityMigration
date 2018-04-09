const fs = require("fs");
const stitch = require("mongodb-stitch");
const { serviceAccount } = require("./service-account-key.js");

const logger = require("./logger");

const filePath = process.argv[2];
const collection = process.argv[3] || "basic";
const db = process.argv[4] || "words";
const param = process.argv[5] || "name";

if (!filePath || !fs.statSync(filePath)) {
  return logger.error("File not found");
}

if (!process.argv[3]) {
  logger.log("Using 'basic' collection");
}

const dataFileContent = JSON.parse(fs.readFileSync(filePath));

if (!Array.isArray(dataFileContent)) {
  return logger.error("Invalid JSON file format. Root element must be Array");
}

logger.log(`File "${filePath}" has ${dataFileContent.length} docs`);

const isDataEqual = (oldData, newData) => {
  for (let p in newData) {
    if (newData[p] !== oldData[p]) {
      return false;
    }
  }
  return true;
};

(async () => {
  const client = await stitch.StitchClientFactory.create("vocabularity-pqhdn");
  const store = client.service("mongodb", "mongodb-atlas").db(db);

  await client.authenticate("apiKey", serviceAccount.key).catch(logger.error);

  const docsForRemove = [];
  const docsForUpdate = [];

  const docsFromDB = await store
    .collection(collection)
    .find()
    .execute();

  logger.log(`Found ${docsFromDB.length} docs in "${collection}" collection`);

  docsFromDB.forEach(doc => {
    const docFromFileIndex = dataFileContent.findIndex(
      i => i[param] == doc[param]
    );
    if (~docFromFileIndex) {
      if (!isDataEqual(doc, dataFileContent[docFromFileIndex])) {
        docsForUpdate.push({
          _id: doc._id,
          data: dataFileContent[docFromFileIndex]
        });
      }
      dataFileContent.splice(docFromFileIndex, 1);
    } else {
      docsForRemove.push(doc._id);
    }
  });

  return dataBatchUpdate(
    {
      dataForUpdate: docsForUpdate,
      dataForRemove: docsForRemove,
      dataForCreate: dataFileContent
    },
    store
  );
})();

async function dataBatchUpdate(
  { dataForUpdate, dataForRemove, dataForCreate },
  store,
  collectionName = collection
) {
  logger.log("Sync started");

  const currentCollection = store.collection(collection);

  logger.warn(`| ${dataForCreate.length} docs will be created`);
  logger.warn(`| ${dataForRemove.length} docs will be deleted`);
  logger.warn(`| ${dataForUpdate.length} docs will be updated`);

  if (dataForCreate.length > 0) {
    logger.log("Inserting has been started");
    await currentCollection.insertMany(dataForCreate);
    logger.log("Inserting has finished");
  }

  if (dataForRemove.length > 0) {
    logger.log("Removing has been started");
    await currentCollection.deleteMany({ _id: { $in: dataForRemove } });
    logger.log("Removing has finished");
  }

  if (dataForUpdate.length > 0) {
    logger.log("Updating has been started");
    for (let indx = 0; indx < dataForUpdate.length; indx++) {
      await currentCollection.updateOne(
        { _id: dataForUpdate[indx]._id },
        { $set: dataForUpdate[indx].data }
      );
    }
    logger.log("Updating has finished");
  }

  if (dataForCreate.length + dataForRemove.length + dataForUpdate.length < 1) {
    logger.log("------------------------");
    logger.log("   Already up to date");
    logger.log("------------------------");
  }

  logger.log("Sync successful finished");
}
