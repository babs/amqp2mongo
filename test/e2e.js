'use strict';

// E2E harness for CI: declare the exchange, publish N messages, then assert they
// all land in MongoDB. Endpoints come from the same env vars the bridge reads, so
// the bridge and this harness talk to the same broker/db.
//   node test/e2e.js setup        declare the exchange (bridge binds to it)
//   node test/e2e.js publish 10000 publish N messages
//   node test/e2e.js verify 10000  poll mongo until N docs, then exit non-zero on mismatch
const amqp = require('amqplib');
const { MongoClient } = require('mongodb');

const AMQP = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672/%2F';
const MONGO = process.env.MONGODB || 'mongodb://localhost:27017/amqp2mongo?retryWrites=false';
const EXCH = process.env.RABBITMQ_CONS_EXCH || 'consume';
const COLL = process.env.MONGOCOLLECTION || 'amqp2mongo';
const KEY = 'a.b.c';

async function setup() {
  const conn = await amqp.connect(AMQP);
  const ch = await conn.createChannel();
  // The bridge binds its queue to this exchange but never declares it.
  await ch.assertExchange(EXCH, 'topic', { durable: true });
  await ch.close();
  await conn.close();
  console.log('exchange declared');
}

async function publish(n) {
  const conn = await amqp.connect(AMQP);
  const ch = await conn.createConfirmChannel();
  for (let i = 0; i < n; i += 1) {
    const body = Buffer.from(JSON.stringify({ seq: i }));
    // publish() returns false under write backpressure — wait for drain so a
    // large run doesn't balloon memory.
    if (!ch.publish(EXCH, KEY, body, { persistent: true })) {
      await new Promise((resolve) => ch.once('drain', resolve));
    }
  }
  await ch.waitForConfirms();
  await ch.close();
  await conn.close();
  console.log('published ' + n);
}

async function verify(expected) {
  const client = new MongoClient(MONGO);
  await client.connect();
  const col = client.db().collection(COLL);
  let n = 0;
  // estimatedDocumentCount is O(1) — safe to poll while the bridge drains.
  for (let tries = 0; tries < 120; tries += 1) {
    n = await col.estimatedDocumentCount();
    if (n >= expected) {
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  const exact = await col.countDocuments();
  await client.close();
  console.log(JSON.stringify({ ok: exact === expected, exact: exact, expected: expected }));
  if (exact !== expected) {
    process.exit(1);
  }
}

const cmd = process.argv[2];
const arg = parseInt(process.argv[3], 10);
const run = { setup: setup, publish: () => publish(arg), verify: () => verify(arg) }[cmd];
if (!run) {
  console.error('usage: setup | publish N | verify N');
  process.exit(2);
}
run().catch((e) => {
  console.error(e);
  process.exit(1);
});
