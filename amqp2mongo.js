#!/usr/bin/env node
'use strict';

require('dotenv').config({ path: __dirname + '/.env' });
require('console-stamp')(console, 'yyyy-mm-dd HH:MM:ss.l');

const amqpcb = require('amqplib/callback_api');
const MongoClient = require('mongodb').MongoClient;

// Concurrency knob: AMQP prefetch caps in-flight (unacked) messages — here, how
// many can be buffered toward the next bulk write. maxPoolSize is sized from the
// same value (one env, can't drift): batching keeps concurrent writes low, but
// the per-document isolation fallback can fan out up to PREFETCH parallel inserts,
// so the pool must cover that worst case. Default 25 = author's sweet spot (25-50).
const PREFETCH = parseInt(process.env.PREFETCH, 10) || 25;

// Batch writes: the old path did one insertOne + one ack per message, so the
// per-message AMQP/Mongo round-trip (not the DB) was the throughput ceiling.
// Instead, accumulate up to BATCH_SIZE docs (capped at PREFETCH, else the
// unacked window can never fill a batch) or flush after BATCH_MAX_WAIT_MS of
// idle — whichever comes first — and write them in one insertMany.
const BATCH_MAX_WAIT_MS = parseInt(process.env.BATCH_MAX_WAIT_MS, 10) || 200;
const BATCH_SIZE = Math.min(parseInt(process.env.BATCH_SIZE, 10) || PREFETCH, PREFETCH);

// Pause before requeueing after an infrastructure (retryable) write failure, so a
// MongoDB outage retries at a bounded pace instead of hot-looping the broker.
const RETRY_BACKOFF_MS = parseInt(process.env.RETRY_BACKOFF_MS, 10) || 1000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Canonical MongoDB retryable server error codes: failover, step-down, shutdown,
// not-primary, transient network. A DocDB failover surfaces as one of these, and
// must be retried — not dead-lettered as if the document were at fault.
const RETRYABLE_CODES = new Set([6, 7, 63, 89, 91, 133, 134, 150, 189, 234, 262, 9001, 10107, 11600, 11602, 13435, 13436]);

// Distinguishes infrastructure failures (worth requeueing + retrying) from
// deterministic per-document failures (oversized BSON, serialization, validation
// — requeueing those would hot-loop forever). Anything not matched here is treated
// as permanent and dead-lettered.
const isRetryable = (err) => {
  if (!err) {
    return false;
  }
  // Trust the driver's own classification first; it labels transient, safe-to-retry
  // write failures. DocDB with retryWrites=false often omits the label, hence the
  // explicit code/name fallbacks below.
  if (typeof err.hasErrorLabel === 'function' && err.hasErrorLabel('RetryableWriteError')) {
    return true;
  }
  const name = err.name || '';
  if (/MongoNetwork|MongoServerSelection|MongoTimeout|MongoNotConnected|PoolClear|WriteConcern/.test(name)) {
    return true;
  }
  return RETRYABLE_CODES.has(err.code)
    || ['ETIMEDOUT', 'ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'EAI_AGAIN'].indexOf(err.code) !== -1;
};

// Set once the consumer is live; cancels delivery and flushes the buffered batch
// so a graceful stop doesn't strand in-flight messages (they'd otherwise only be
// recovered via broker redelivery).
let drain = null;

const shutdown = (signal) => {
  console.info(` [+] ${signal} received, draining`);
  Promise.resolve(drain && drain())
    .catch((e) => console.error(' [-] drain failed: ' + (e && e.message)))
    .then(() => process.exit(0));
};
// SIGTERM: Kubernetes rollout / scale-down / spot reclaim. SIGINT: Ctrl-C.
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// A fire-and-forget rejection or unexpected throw must not silently stall the
// consumer: log and exit non-zero so the orchestrator restarts a clean process.
process.on('unhandledRejection', (reason) => {
  console.error(' [-] Unhandled rejection: ' + (reason && reason.stack ? reason.stack : reason));
  process.exit(1);
});
process.on('uncaughtException', (err) => {
  console.error(' [-] Uncaught exception: ' + (err && err.stack ? err.stack : err));
  process.exit(1);
});

const removeEmpty = (obj) => {
  Object.keys(obj).forEach((key) => (obj[key] == null) && delete obj[key]);
};

var counters = { success: 0, failure: 0, requeued: 0 };
console.info(' [+] Starting amqp2mongo');
console.info(' [0/2] Establishing connections');
const client = new MongoClient(process.env.MONGODB, { maxPoolSize: PREFETCH });
client.connect()
  .then(() => {
    console.info(' [1/2] Connected to MongoDB');
    startConsumeMessage(client.db());
  })
  .catch((err) => {
    console.error(' [-] MongoDB connection failed: ' + err);
    process.exit(1);
  });

const startConsumeMessage = db => {
  amqpcb.connect(process.env.RABBITMQ_URL, (err, conn) => {
    if (err) {
      console.error(' [-] RabbitMQ connection failed: ' + err);
      process.exit(1);
    }
    conn.on('error', (err) => {
      if (err == undefined) {
        err = 'no reason given';
      }
      console.error(' [-] RabbitMQ Connection error received: ' + err);
      process.exit(1);
    });
    conn.on('close', (err) => {
      if (err == undefined) {
        err = 'no reason given';
      }
      console.error(' [-] RabbitMQ Connection closed. (' + err + ')');
      process.exit(1);
    });
    console.info(' [2/2] Connected to RabbitMQ');
    console.info(' [+] All connections established');
    setInterval(() => { console.info(`message processed: ${JSON.stringify(counters)}`); }, 60000);
    conn.createConfirmChannel((err, ch) => {
      if (err) {
        console.error(' [-] RabbitMQ Channel creation failed: ' + err);
        process.exit(1);
      }
      ch.on('error', (err) => {
        if (err == undefined) {
          err = 'no reason given';
        }
        console.error(' [-] RabbitMQ Channel error received: ' + err);
        process.exit(1);
      });
      ch.on('close', (err) => {
        if (err == undefined) {
          err = 'no reason given';
        }
        console.error(' [-] RabbitMQ Channel closed. (' + err + ')');
        process.exit(1);
      });
      // Sweet spot ~ 25 - 50; tune via PREFETCH env. Mongo maxPoolSize matches.
      ch.prefetch(PREFETCH);
      ch.assertQueue(process.env.RABBITMQ_CONS_QUEUE, { exclusive: false }, (err, q) => {
        if (err) {
          console.error(' [-] RabbitMQ Queue assertion failed: ' + err);
          process.exit(1);
        }

        console.info(' [*] Waiting for msgs. To exit press CTRL+C');

        process.env.RABBITMQ_CONS_ROUTING_KEYS.split(',').forEach((key) => {
          ch.bindQueue(q.queue, process.env.RABBITMQ_CONS_EXCH, key);
        });

        const collection = db.collection(process.env.MONGOCOLLECTION);
        let batch = [];
        let flushTimer = null;

        // ack/nack throw if the channel is already gone (the channel 'close'/'error'
        // handlers above own the exit). Swallow + log so it never escapes the
        // fire-and-forget flush as an unhandled rejection.
        const safeAck = (msg) => {
          try {
            ch.ack(msg);
          } catch (e) {
            console.error(' [-] ack failed: ' + (e && e.message));
          }
        };
        const safeNack = (msg, requeue) => {
          try {
            ch.nack(msg, false, requeue);
          } catch (e) {
            console.error(' [-] nack failed: ' + (e && e.message));
          }
        };

        // Batch-level failure with no per-document detail: we can't tell which doc
        // is at fault, so retry each alone. Good docs land and are acked; a
        // deterministically-bad doc is dead-lettered (never requeued, so no loop);
        // if MongoDB is down, every isolated insert is retryable and is requeued.
        const isolate = (current) => Promise.all(current.map(async (entry) => {
          try {
            await collection.insertOne(entry.document, { checkKeys: false });
            counters.success += 1;
            safeAck(entry.msg);
          } catch (docErr) {
            if (isRetryable(docErr)) {
              counters.requeued += 1;
              safeNack(entry.msg, true);
            } else {
              counters.failure += 1;
              safeNack(entry.msg, false);
              console.error(JSON.stringify({
                error: 'document rejected by mongodb (dead-lettered)',
                message: docErr && docErr.message,
              }));
            }
          }
        }));

        const flush = async () => {
          if (flushTimer) {
            clearTimeout(flushTimer);
            flushTimer = null;
          }
          if (batch.length === 0) {
            return;
          }
          // Snapshot and reset so messages arriving during the await land in the
          // next batch instead of this one.
          const current = batch;
          batch = [];

          try {
            // ordered:false — docs are independent; one bad doc must not block the rest.
            await collection.insertMany(current.map((e) => e.document), { ordered: false, checkKeys: false });
            counters.success += current.length;
            current.forEach((e) => safeAck(e.msg));
            return;
          } catch (e) {
            if (e.writeErrors && e.writeErrors.length > 0) {
              // Per-document server rejects: the unordered insert already persisted
              // the rest, so ack those (avoids re-insert duplicates on redelivery)
              // and dead-letter only the offending ones by their reported index.
              const failed = new Set(e.writeErrors.map((we) => we.index));
              current.forEach((entry, i) => {
                if (failed.has(i)) {
                  counters.failure += 1;
                  safeNack(entry.msg, false);
                } else {
                  counters.success += 1;
                  safeAck(entry.msg);
                }
              });
              console.error(JSON.stringify({
                error: 'bulk insert partially failed (failures dead-lettered)',
                failed: failed.size,
                batch: current.length,
                message: e.message,
              }));
              return;
            }
            if (isRetryable(e)) {
              // Infrastructure failure (e.g. MongoDB unreachable): pace the retry to
              // avoid hot-looping the broker, then requeue the whole batch.
              counters.requeued += current.length;
              console.error(JSON.stringify({
                error: 'bulk insert failed, requeueing batch',
                batch: current.length,
                message: e.message,
              }));
              await sleep(RETRY_BACKOFF_MS);
              current.forEach((entry) => safeNack(entry.msg, true));
              return;
            }
            // Batch-level, non-retryable (e.g. a doc the driver refused to serialise):
            // isolate to land the good docs and dead-letter only the culprit.
            await isolate(current);
          }
        };

        ch.consume(q.queue, (msg) => {
          let document = {
            insertDate: new Date(),
            queue: q.queue,
            fields: msg.fields,
            properties: msg.properties,
          };
          removeEmpty(document.fields);
          removeEmpty(document.properties);

          try {
            document.content = JSON.parse(msg.content);
          } catch (e) {
            document.content = msg.content.toString('utf8');
            document.error = {
              message: e.message,
              stack: e.stack,
            };
          }

          batch.push({ msg: msg, document: document });
          if (batch.length >= BATCH_SIZE) {
            flush();
          } else if (!flushTimer) {
            // First message of an under-full batch: arm the idle-flush timer so
            // low-traffic messages don't wait indefinitely for the batch to fill.
            flushTimer = setTimeout(flush, BATCH_MAX_WAIT_MS);
          }
        }, { noAck: false }, (err, ok) => {
          if (err) {
            console.error(' [-] RabbitMQ consume failed: ' + err);
            process.exit(1);
          }
          const consumerTag = ok.consumerTag;
          // Graceful stop: stop new deliveries, then flush whatever is buffered.
          drain = async () => {
            await new Promise((resolve) => ch.cancel(consumerTag, () => resolve()));
            await flush();
          };
        });
      });
    });
  });
};
