# KafkaConnect (0.0.2)
The "KafkaConnect" component is responsible for the communication with Kafka;

## Configuration

This configuration should be specified in the custom configuration block in schema editor.

```json
{
  "topicToAlias": {
    "topicName" : "session-alias"
  },
  "groupId": 1,
  "acceptableBreak" : 8,
  "acceptableBreakTimeUnit": "HOURS",
  "batchSize": 100,
  "timespan": 1000,
  "timespanUnit" : "MILLISECONDS",
  "reconnectBackoffMs": 50,
  "reconnectBackoffMaxMs": 1000
}
```

Parameters:
+ session-alias - that session alias will be set for all messages sent by this component. **It should be unique for each "KafkaConnect" topic**;
+ groupId - that ID will be used for Kafka connection
+ bootstrapServers - URL of one of the Kafka brokers which you give to fetch the initial metadata about your Kafka cluster
+ acceptableBreak - if the period of inactivity is longer than this time, then start reading Kafka messages from the current moment. Should be positive.
+ timeUnit - time unit for `acceptableBreak` and `timespan` classification
  + DAYS
  + HOURS
  + MINUTES
  + SECONDS
  + MILLISECONDS
  + MICROSECONDS
  + NANOSECONDS
+ batchSize - the size of one batch. Should be positive.
+ timespan - The period router collects messages before it should be sent. Should be positive.
+ reconnectBackoffMs - The amount of time in milliseconds to wait before attempting to reconnect to a given host. Should be positive.
+ reconnectBackoffMaxMs - The maximum amount of time in milliseconds to backoff/wait when reconnecting to a broker that 
          has repeatedly failed to connect. If provided, the backoff per host will increase 
          exponentially for each consecutive connection failure, up to this maximum. 
          Once the maximum is reached, reconnection attempts will continue periodically with
          this fixed rate. To avoid connection storms, a randomization factor of 0.2 will be applied to
          the backoff resulting in a random range between 20% below and 20% above the computed value. Should be positive.
## Reconnect behaviour
If the consumer loses connection to which one, then it will try to reconnect to it indefinitely
in accordance with `reconnectBackoffMs` and `reconnectBackoffMaxMs`
You can see a warning in the log files 

`[kafka-producer-network-thread | producer-1] org.apache.kafka.clients.NetworkClient - [Producer clientId=producer-1] Connection to node 1 (/127.0.0.1:9092) could not be established. Broker may not be available.`
## Release notes
