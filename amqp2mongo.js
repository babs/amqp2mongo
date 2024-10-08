#!/usr/bin/env node
'use strict';

//require('./headdump.js').init('./dumps/');

require('dotenv').config({ path: __dirname + '/.env' });
require('console-stamp')(console, 'yyyy-mm-dd HH:MM:ss.l');

const amqpcb = require('amqplib/callback_api');
const MongoClient = require('mongodb').MongoClient;

process.on('SIGINT', () => {
  console.info('Interrupted');
  process.exit(0);
});

//const SDC = require('statsd-client');
//const statsd = new SDC({host: process.env.STATSD_HOST || 'localhost'});

/*
function hrt_to_ms(hrtresult) {
  return hrtresult[0]*1000+hrtresult[1]/1000000;
}

const Logger = require('mongodb').Logger;
Logger.setLevel('debug');
*/

const removeEmpty = (obj) => {
  Object.keys(obj).forEach((key) => (obj[key] == null) && delete obj[key]);
};

var counters = { success: 0, failure: 0 };
console.info(' [+] Starting amqp2mongo');
console.info(' [0/2] Establishing connections');
MongoClient(process.env.MONGODB, {
  useNewUrlParser: true,
  auto_reconnect: true,
  useUnifiedTopology: true,
}).connect((err, client) => {
  if (err) {
    console.error(' [-] MongoDB connection failed: ' + err);
    process.exit(1);
  }
  console.info(' [1/2] Connected to MongoDB');
  startConsumeMessage(client.db());
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
      // Sweet spot ~ 25 - 50
      ch.prefetch(10);
      ch.assertQueue(process.env.RABBITMQ_CONS_QUEUE, { exclusive: false }, (err, q) => {
        if (err) {
          console.error(' [-] RabbitMQ Queue assertion failed: ' + err);
          process.exit(1);
        }

        console.info(' [*] Waiting for msgs. To exit press CTRL+C');

        process.env.RABBITMQ_CONS_ROUTING_KEYS.split(',').forEach((key) => {
          ch.bindQueue(q.queue, process.env.RABBITMQ_CONS_EXCH, key);
        });

        ch.consume(q.queue, async (msg) => {
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
          try {
            await db.collection(process.env.MONGOCOLLECTION).insertOne(document, { checkKeys: false });
            counters.success += 1;
            ch.ack(msg);
          } catch (e) {
            counters.failure += 1;
            // Delay Nack to avoid consumption / ack loop
            console.error(JSON.stringify({
              error: 'unable to save document in mongodb',
              message: e.message,
              stack: e.stack,
              document: document,
            }));
            console.log(`message processed: ${JSON.stringify(counters)}`);
            throw e;
          }
        });
      }, { noAck: false });
    });
  });
};
