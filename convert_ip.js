const { MongoClient } = require('mongodb');
const ipaddr = require('ipaddr.js');
const fs = require('fs').promises;

const url = 'mongodb://localhost:57017';
const dbName = 'ton';
const sourceCollection = 'countries';
const targetCollectionIPv4 = 'countries_ipv4';
const targetCollectionIPv6 = 'countries_ipv6';
const batchSize = 10000;
const checkpointFile = 'conversion_checkpoint.json';

function ipToNumber(ipAddress) {
  const addr = ipaddr.parse(ipAddress);
  if (addr.kind() === 'ipv4') {
    return addr.toByteArray().reduce((acc, byte) => acc * 256 + byte, 0);
  } else {
    return BigInt(`0x${addr.toNormalizedString().replace(/:/g, '')}`).toString();
  }
}

async function convertAndInsert(db, sourceData) {
  const targetIPv4 = db.collection(targetCollectionIPv4);
  const targetIPv6 = db.collection(targetCollectionIPv6);

  const convertedDataIPv4 = [];
  const convertedDataIPv6 = [];

  sourceData.forEach(item => {
    const isIPv4 = ipaddr.parse(item.start_ip).kind() === 'ipv4';
    
    if (isIPv4) {
      convertedDataIPv4.push({
        ...item,
        start_ip: ipToNumber(item.start_ip),
        end_ip: ipToNumber(item.end_ip)
      });
    } else {
      convertedDataIPv6.push({
        ...item,
        start_ip: ipToNumber(item.start_ip),
        end_ip: ipToNumber(item.end_ip)
      });
    }
  });

  if (convertedDataIPv4.length > 0) {
    await targetIPv4.insertMany(convertedDataIPv4);
    console.log(`Inserted ${convertedDataIPv4.length} IPv4 records`);
  }

  if (convertedDataIPv6.length > 0) {
    await targetIPv6.insertMany(convertedDataIPv6);
    console.log(`Inserted ${convertedDataIPv6.length} IPv6 records`);
  }
}

async function getCheckpoint() {
  try {
    const data = await fs.readFile(checkpointFile, 'utf8');
    return JSON.parse(data);
  } catch (error) {
    return { lastProcessedId: null, count: 0 };
  }
}

async function saveCheckpoint(lastProcessedId, count) {
  await fs.writeFile(checkpointFile, JSON.stringify({ lastProcessedId, count }));
}

async function main() {
  const client = new MongoClient(url);
  try {
    await client.connect();
    console.log('Connected to MongoDB');

    const db = client.db(dbName);
    const source = db.collection(sourceCollection);
    const targetIPv4 = db.collection(targetCollectionIPv4);
    const targetIPv6 = db.collection(targetCollectionIPv6);

    let { lastProcessedId, count } = await getCheckpoint();
    console.log(`Resuming from checkpoint: ${lastProcessedId}, processed: ${count}`);

    let query = {};
    if (lastProcessedId) {
      query._id = { $gt: lastProcessedId };
    }

    const totalCount = await source.countDocuments(query);
    console.log(`Total records to process: ${totalCount}`);

    while (true) {
      const batch = await source.find(query).limit(batchSize).toArray();
      if (batch.length === 0) break;

      await convertAndInsert(db, batch);
      
      lastProcessedId = batch[batch.length - 1]._id;
      count += batch.length;
      await saveCheckpoint(lastProcessedId, count);

      console.log(`Processed ${count} out of ${totalCount} records`);
      query._id = { $gt: lastProcessedId };
    }

    // Create optimized indexes on the new collections
    await targetIPv4.createIndex({ start_ip: 1, end_ip: 1 });
    await targetIPv6.createIndex({ start_ip: 1, end_ip: 1 });
    console.log('Created optimized indexes on the new collections');

    console.log('Conversion completed successfully');
  } finally {
    await client.close();
    console.log('Disconnected from MongoDB');
  }
}

main().catch(console.error);