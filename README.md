# amqp2mongo

Connects to a rabbitmq server, declare queue and bindings to the given exchange and consume messages, saving them to a mongodb collection.

## Usage

    node amqp2mongo.js
    # or
    ./amqp2mongo.js

## Configuration

Configuration is taken from environment variables and `.env` file

* `RABBITMQ_URL`=amqp://user:password@host/%2F?heartbeat=30
    > RabbitMQ DSN

* `RABBITMQ_CONS_QUEUE`=queue
    > Queue to consume

* `RABBITMQ_CONS_EXCH`=exchange
    > Exchange to bind the queue to

* `RABBITMQ_CONS_ROUTING_KEYS`=routing keys coma separated
    > Bindings to declare, can be several, coma separated, eg: a.b.d,xx.# 

* `MONGODB`=mongodb://user:pwd@host1,host2,host3/database?replicaSet=replicaset
    > MongoDB DSN

* `MONGOCOLLECTION`=collection
    > collection to store the messages to

* `PREFETCH`=25
    > AMQP prefetch (max in-flight unacked messages). Also drives the Mongo
    > connection pool size (`maxPoolSize`). Default `25`.

* `BATCH_SIZE`=25
    > Messages written per bulk `insertMany`. Capped at `PREFETCH`. Default = `PREFETCH`.

* `BATCH_MAX_WAIT_MS`=200
    > Flush an under-full batch after this many ms of idle, so low-traffic
    > messages aren't held back. Default `200`.

* `RETRY_BACKOFF_MS`=1000
    > Pause before requeueing a batch after an infrastructure write failure
    > (e.g. MongoDB unreachable), to avoid hot-looping the broker. Default `1000`.

## Throughput

Messages are buffered and written in bulk (`insertMany`, unordered): one
round-trip per batch instead of per message. Tune `PREFETCH` / `BATCH_SIZE` up
for higher throughput — the practical ceiling is the target DocDB/Mongo instance
CPU, not this process.

## Reliability

At-least-once. A message is acked only after its document is persisted, so a
crash or disconnect mid-batch causes redelivery — and, since writes are not
idempotent, possible duplicates. Failures are classified:

* **Infrastructure** (connection/timeout): the batch is requeued and retried
  after `RETRY_BACKOFF_MS` — no message loss.
* **Per-document reject** (e.g. oversized/invalid doc): that message is `nack`ed
  with `requeue=false`, i.e. routed to the queue's dead-letter exchange if one is
  bound, otherwise dropped. **Bind a DLX** to avoid losing poison messages.

`SIGTERM`/`SIGINT` stop consumption and flush the buffered batch before exit.
The periodic `message processed` log reports `{ success, failure, requeued }`.

## Document

### format

Keys:

* `_id`: auto-generated mongodb id
* `content` Message payload interpreted if JSON, text if not
* `error`: (optionnal) exception info if message can't be interpreted as JSON (contains self explaintatory `message` and `stack` keys)
* `insertDate`: date when the message is processed by `amqp2mongo`
* `queue`: queue the message was consumed from
* `properties`: message properties
* `fields`: message fields

### examples 

#### plain json interpreted

```json
{
    "_id" : ObjectId("5c8d5ad022b2b418cedeb91f"),
    "insertDate" : ISODate("2019-03-16T21:21:36.610+01:00"),
    "queue" : "queue",
    "fields" : {
        "consumerTag" : "amq.ctag-dGIx8npNHP_bL5pzRsqQpw",
        "deliveryTag" : 4,
        "redelivered" : false,
        "exchange" : "publish",
        "routingKey" : "a.b.c"
    },
    "properties" : {
        "headers" : {
            "timestamp_in_ms" : 1552767696580.0
        },
        "deliveryMode" : 2,
        "timestamp" : 1552767696
    },
    "content" : {
        "test" : true
    }
}
```

#### error at parsing

```json
{
    "_id" : ObjectId("5c8d5ae922b2b418cedeb920"),
    "insertDate" : ISODate("2019-03-16T21:22:01.519+01:00"),
    "queue" : "queue",
    "fields" : {
        "consumerTag" : "amq.ctag-dGIx8npNHP_bL5pzRsqQpw",
        "deliveryTag" : 5,
        "redelivered" : false,
        "exchange" : "publish",
        "routingKey" : "a.b.c"
    },
    "properties" : {
        "headers" : {
            "timestamp_in_ms" : 1552767721459.0
        },
        "deliveryMode" : 2,
        "timestamp" : 1552767721
    },
    "content" : "invalid json\n",
    "error" : {
        "message" : "Unexpected token i in JSON at position 0",
        "stack" : "SyntaxError: Unexpected token i in JSON at position 0\n    at JSON.parse (<anonymous>)\n    at ...",
        "code" : null
    }
}
```
