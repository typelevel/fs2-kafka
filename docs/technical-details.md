# Technical Details

In the following sections, technical aspects of the library are detailed.

## Implementation Notes

Following are some general library implementation notes.

- The library relies on the Java Kafka client, and does not re-implement the Kafka client. In particular, the library implements a consumer threading model similar to the 'decouple consumption and processing' model described in the [documentation](@KAFKA_API_BASE_URL@/?org/apache/kafka/clients/consumer/KafkaConsumer.html).

- Since the Java Kafka client is allowed to block (up to `pollTimeout` for `poll`), all Kafka client calls are run on a dedicated `ExecutionContext`. Unless explicitly provided when creating `ConsumerSettings`, a default single-threaded `ExecutionContext` will be created and used for this purpose.

## Consumer Streaming

To enable backpressured streaming of Kafka records, there are three pieces at work.

1. The 'consumer stream' continually issues fetch requests as long as there is demand. There is some room to issue fetch requests even when there isn't demand, to allow prefetching of records. This is controlled using the `maxPrefetchBatches` setting. Fetch requests will only be issued as long as processing is fast enough, so that less than `maxPrefetchBatches` batches are prefetched.
2. The 'consumer loop' handles fetch requests issued by the stream, as well as poll requests, and other requests (e.g. commit). Records are only fetched for partitions where there is demand (pull-based). `KafkaConsumerActor` mostly implements the consumer loop internally.
3. The 'poll scheduler' which schedules poll requests via a poll queue (bounded to 1 element).

These pieces are assembled in `KafkaConsumer` and run concurrently in independent `Fiber`s.

### Backpressure

There are a few important points to understanding how the backpressure works.

- FS2 streams are pull-based, meaning there isn't a messaging system instructing up-stream to slow down. Instead, up-stream is asked to produce elements whenever down-stream is pulling. It's possible to run the consumer and producer independently, and use an asynchronous non-blocking queue for communication. The producer can then slow down when the consumer isn't fast enough.
- For performance reasons, we should not issue a request to Kafka for every record requested by down-stream. Instead, we fetch records in batches (size controlled by `max.poll.records`) and keep the records on a queue until processed.
- Since the Java Kafka client `poll` blocks, we throttle poll requests to every `pollInterval`.
- The `KafkaConsumerActor` only requests records for partitions where there is a fetch request.
