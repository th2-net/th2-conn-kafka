# KafkaConnect (0.0.4)
The "KafkaConnect" component is responsible for the communication with Kafka;

## Configuration

This configuration should be specified in the custom configuration block in schema editor.
```yaml
  customConfig:

    aliasToTopic:
      session_alias_01:
        topic: "topic_01"
        subscribe: true
      session_alias_02:
        topic: "topic_02"
        subscribe: true

    aliasToTopicAndKey:
      session_alias_03:
        topic: "topic_03"
        key: "key_01"
        subscribe: false
      session_alias_04:
        topic: "topic_04"
        key: null

    sessionGroups:
      session_alias_group_01: ["session_alias_01", "session_alias_02"]
      session_alias_group_02: ["session_alias_03"]

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
    kafkaConnectionEvents: true
```

Parameters:
+ aliasToTopic - matches th2 sessions with Kafka topics **Note: Kafka does not guarantee message ordering within topic if topic contains more than one partition**
+ aliasToTopicAndKey - matches th2 sessions with Kafka topics and keys **Note: Kafka guarantees message ordering only within messages with the same non null key if topic contains more than one partition**
+ sessionGroups - match session group with sessions (key: session group, value: list of session aliases)
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
+ timeSpan - The period router collects messages before it should be sent. Should be positive.
+ timeSpanUnit time unit for `timeSpan`
+ reconnectBackoffMs - The amount of time in milliseconds to wait before attempting to reconnect to a given host. Should be positive.
+ reconnectBackoffMaxMs - The maximum amount of time in milliseconds to backoff/wait when reconnecting to a broker that has repeatedly failed to connect. If provided, the backoff per host will increase exponentially for each consecutive connection failure, up to this maximum. Once the maximum is reached, reconnection attempts will continue periodically with this fixed rate. To avoid connection storms, a randomization factor of 0.2 will be applied to the backoff resulting in a random range between 20% below and 20% above the computed value. Should be positive.
+ kafkaConnectionEvents - Generate TH2 events on lost connection and restore connection to Kafka. `false` by default.

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
  imageVersion: 0.0.3
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