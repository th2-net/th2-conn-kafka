# KafkaConnect (0.3.0)
The "KafkaConnect" component is responsible for the communication with Kafka;

## Configuration

This configuration should be specified in the custom configuration block in schema editor.
```yaml
  customConfig:

    topics:
      publish:
        - topic: "topic_01"
          key: "key_01"
          sessionAlias: "alias_01"
          sessionGroup: "group_01"
          book: "book_01"
      subscribe:
        - topic: "topic_02"
          sessionAlias: "alias_02"

    defaultSessionGroup: "session_alias_group_02"

    bootstrapServers: "cp-kafka:9092"
    groupId: "consumer_group_01"

    maxInactivityPeriod: 8
    maxInactivityPeriodUnit: "HOURS"
    batchSize: 100
    timeSpan: 1000
    timeSpanUnit : "MILLISECONDS"
    reconnectBackoffMs: 50
    reconnectBackoffMaxMs: 1000
    kafkaBatchSize: 524288
    kafkaLingerMillis: 200
    kafkaConnectionEvents: true
    messagePublishingEvents: true
```

Parameters:
+ topics - matches topics or topics+keys (separately for publishing and subscription) to th2 books, sessions and session groups
  + topic (required)
  + key (default - any key will match)
  + book (default - use box bookName)
  + sessionAlias (required)
  + sessionGroup (default - use default defaultSessionGroup)
+ defaultSessionGroup - this session group will be set for sessions not mentioned in `sessionGroups`
+ groupId - that ID will be used for Kafka connection
+ bootstrapServers - URL of one of the Kafka brokers which you give to fetch the initial metadata about your Kafka cluster
+ maxInactivityPeriod - if the period of inactivity is longer than this time, then start reading Kafka messages from the current moment. Should be positive.
+ maxInactivityPeriodUnit - time unit for `maxInactivityPeriod`
  + DAYS
  + HOURS
  + MINUTES
  + SECONDS
  + MILLISECONDS
  + MICROSECONDS
  + NANOSECONDS
+ batchSize - the size of one batch (number of messages). Should be positive.
+ timeSpan - the period router collects messages to batch before it should be sent. Should be positive.
+ eventBatchMaxBytes - the size of one event batch (number of bytes). Should be positive.
+ eventBatchMaxEvents - the size of one event batch (number of events). Should be positive.
+ eventBatchTimeSpan - the period router collects events to batch before it should be sent. Should be positive.
+ timeSpanUnit time unit for `timeSpan` and `eventBatchTimeSpan`.
+ reconnectBackoffMs - The amount of time in milliseconds to wait before attempting to reconnect to a given host. Should be positive.
+ reconnectBackoffMaxMs - The maximum amount of time in milliseconds to backoff/wait when reconnecting to a broker that has repeatedly failed to connect. If provided, the backoff per host will increase exponentially for each consecutive connection failure, up to this maximum. Once the maximum is reached, reconnection attempts will continue periodically with this fixed rate. To avoid connection storms, a randomization factor of 0.2 will be applied to the backoff resulting in a random range between 20% below and 20% above the computed value. Should be positive.
+ kafkaConnectionEvents - Generate TH2 events on lost connection and restore connection to Kafka. `false` by default.
+ messagePublishingEvents - Generate TH2 event on successful message publishing.
+ kafkaBatchSize - maximum number of bytes that will be included in a batch.
+ kafkaLingerMillis - number of milliseconds a producer is willing to wait before sending a batch out.
+ addExtraMetadata - Add extra metadata to messages (like topic, key. offset, original timestamp ...).
+ security.protocol - Protocol used to communicate with brokers.
+ sasl.kerberos.service.name - The Kerberos principal name that Kafka runs as.
+ sasl.mechanism - SASL mechanism used for client connections.
+ sasl.jaas.config - JAAS login context parameters for SASL connections in the format used by JAAS configuration files.

## Reconnect behaviour

If the consumer loses connection to which one, then it will try to reconnect to it indefinitely in accordance with `reconnectBackoffMs` and `reconnectBackoffMaxMs`You can see a warning in the log files

`[kafka-producer-network-thread | producer-1] org.apache.kafka.clients.NetworkClient - [Producer clientId=producer-1] Connection to node 1 (/127.0.0.1:9092) could not be established. Broker may not be available.`

## Pins

Messages that were received to 'to_send' pin will be send to Kafka.
Messages that were received from / sent to the Kafka will be sent to the `out_raw` pin:

Example of pins configuration:

```yaml
spec:
  imageName: ghcr.io/th2-net/th2-conn-kafka
  imageVersion: 0.3.0
  type: th2-conn

  pins:
    mq:
      subscribers:
        - name: to_send
          attributes: ["send", "raw", "subscribe"]
          linkTo:
            - box: script
              pin: to_conn

      publishers:
        - name: out_raw
          attributes: ["raw", "publish", "store"]
```

## Release notes

### 0.3.0

+ added ability to filter by "book", "session_group", "session_alias", "message_type", "direction", "protocol".
+ conditions inside the message and metadata now combined as "and".

### 0.2.0

+ Secure connection support
+ Kafka batching settings
+ Message events publishing setting
+ Added extra metadata to messages received from Kafka

### 0.1.1

+ bump library versions

### 0.1.0

+ Migrated to Books & Pages concept

### 0.0.4
+ th2-common upgrade to `3.44.1`
+ th2-bom upgrade to `4.2.0`

### 0.0.3

+ Publishing to Kafka support
+ Kafka keys support
+ Session groups support

### 0.0.2

+ Reusable workflow with dependency check

### 0.0.1

+ Initial version