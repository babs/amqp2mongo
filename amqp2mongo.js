#!/usr/bin/env node
'use strict';

//require('./headdump.js').init('./dumps/');

require('dotenv').config({path: __dirname + '/.env'});
require('console-stamp')(console, 'yyyy-mm-dd HH:MM:ss.l');

const amqpcb = require('amqplib/callback_api');
var MongoClient = require('mongodb').MongoClient;

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

console.info(' [+] Starting amqp2mongo');
console.info(' [0/2] Establishing connections');
MongoClient(process.env.MONGODB, {
  useNewUrlParser: true,
  auto_reconnect: true,
  reconnectTries: Number.MAX_VALUE
}).connect((err, client) => {
  if (err) {
    console.error(' [-] MongoDB connection failed: ' + err);
    process.exit(1);
  }
  console.info(' [1/2] Connected to MongoDB');
  startConsumeMessage(client.db());
});

function startConsumeMessage(db) {
  amqpcb.connect(process.env.RABBITMQ_URL, function (err, conn) {
    if (err) {
      console.error(' [-] RabbitMQ connection failed: ' + err);
      process.exit(1);
    }
    conn.on('error', (err) => {
      if (err == undefined) {
        err = 'no reason given';
      }
      console.error(' [-] RabbitMQ Connection error received: ' + err);
      // close is emitted after error
    });
    conn.on('close', (err) => {
      if (err == undefined) {
        err = 'no reason given';
      }
      console.error(' [-] RabbitMQ Connection closed. (' + err + ')');
      process.exit(1);
    });
    console.info(' [2/2] Connected to RabbitMQ');
    console.info(' [+] All connetion established');
    conn.createConfirmChannel(function(err, ch) {
      if (err) {
        console.error(' [-] RabbitMQ Channel creation failed: ' + err);
        process.exit(1);
      }
      ch.on('error', (err) => {
        if (err == undefined) {
          err = 'no reason given';
        }
        console.error(' [-] RabbitMQ Channel error received: ' + err);
        // close is emitted after error
      });
      ch.on('close', (err) => {
        if (err == undefined) {
          err = 'no reason given';
        }
        console.error(' [-] RabbitMQ Channel closed. (' + err + ')');
      });
      // Sweet spot ~ 25 - 50
      ch.prefetch(10);
      ch.assertQueue(process.env.RABBITMQ_CONS_QUEUE, {exclusive: false}, function(err, q) {
        console.info(' [*] Waiting for msgs. To exit press CTRL+C');
      
        process.env.RABBITMQ_CONS_ROUTING_KEYS.split(',').forEach(function(key) {
          ch.bindQueue(q.queue, process.env.RABBITMQ_CONS_EXCH, key);
        });

        ch.consume(q.queue, function(msg){
          let document = {
            insertDate: new Date(),
            queue: q.queue,
            fields: msg.fields,
            properties: msg.properties,
          };
          removeEmpty(document.fields);
          removeEmpty(document.properties);
          if (document.properties.headers.timestamp_in_ms) {
            document.messageDate = new Date(document.properties.headers.timestamp_in_ms);
          }

          try {
            document.content = JSON.parse(msg.content);
          } catch (e) {
            document.content = msg.content.toString('utf8');
            document.error = {
              message: e.message,
              stack: e.stack,
            };
          }
          db.collection(process.env.MONGOCOLLECTION).insertOne(document);
          // console.dir(document);
          ch.ack(msg);
        });
      }, {noAck: false});
    });
  });
}

