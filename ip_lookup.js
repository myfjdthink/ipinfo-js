const { MongoClient } = require('mongodb');
const ipaddr = require('ipaddr.js');

const url = 'mongodb://localhost:57017';
const dbName = 'ton';
const collectionNameIPv4 = 'countries_ipv4';
const collectionNameIPv6 = 'countries_ipv6';

let collectionIPv4, collectionIPv6;

async function connectToMongo() {
  const client = new MongoClient(url);
  await client.connect();
  console.log('Connected successfully to MongoDB');
  const db = client.db(dbName);
  collectionIPv4 = db.collection(collectionNameIPv4);
  collectionIPv6 = db.collection(collectionNameIPv6);
}

function ipToNumber(ipAddress) {
  const addr = ipaddr.parse(ipAddress);
  if (addr.kind() === 'ipv4') {
    return addr.toByteArray().reduce((acc, byte) => acc * 256 + byte, 0);
  } else {
    return BigInt(`0x${addr.toNormalizedString().replace(/:/g, '')}`).toString();
  }
}

async function lookupIP(ipAddress) {
  const parsedIP = ipaddr.parse(ipAddress);
  const isIPv4 = parsedIP.kind() === 'ipv4';
  const ipNum = ipToNumber(ipAddress);

  const query = {
    start_ip: { $lte: ipNum },
    end_ip: { $gte: ipNum }
  };

  const projection = {
    _id: 0,
    country: 1
  };
  console.log(JSON.stringify(query));
  const collection = isIPv4 ? collectionIPv4 : collectionIPv6;
  const result = await collection.findOne(query, { projection });

  if (result) {
    console.log(`Found ${result.country} for ${ipAddress}`);
    return result;
  } else {
    return null;
  }
}

function generateRandomIPv4() {
  return Array(4).fill().map(() => Math.floor(Math.random() * 256)).join('.');
}

function generateRandomIPv6() {
  return Array(8).fill().map(() => Math.floor(Math.random() * 65536).toString(16)).join(':');
}

async function runPerformanceTest(count) {
  const ipv4Addresses = Array(count).fill().map(() => generateRandomIPv4());
  const ipv6Addresses = Array(count).fill().map(() => generateRandomIPv6());

  console.log(`Testing ${count} IPv4 addresses...`);
  const startIPv4 = Date.now();
  for (const ip of ipv4Addresses) {
    await lookupIP(ip);
  }
  const endIPv4 = Date.now();
  console.log(`IPv4 lookup time: ${endIPv4 - startIPv4}ms  avg: ${((endIPv4 - startIPv4) / count).toFixed(2)}ms`);

  console.log(`Testing ${count} IPv6 addresses...`);
  const startIPv6 = Date.now();
  for (const ip of ipv6Addresses) {
    await lookupIP(ip);
  }
  const endIPv6 = Date.now();
  console.log(`IPv6 lookup time: ${endIPv6 - startIPv6}ms  avg: ${((endIPv6 - startIPv6) / count).toFixed(2)}ms`);
}

async function main() {
  await connectToMongo();

  const testCount = 10;
  await runPerformanceTest(testCount);

  process.exit(0);
}

main().catch(console.error);