# KStreams

Inspired from this [example](https://github.com/aseigneurin/kafka-tutorial-event-processing).


## Pre-requisites

Before launching the test, please create the topics :

```bash
$ kafka-topics --zookeeper localhost:2181 --create --topic log-states --replication-factor 1 --partitions 4
```

## Event Producer

The **Creator** function simulates the generator of 10 incident tickets.

Each message generated contains three information:

- Creation Date
- Ticket UUID
- Ticket State ( 0: Creted // 1 : Processing // 2 : Finished )

When a ticket has its state changed, a new message containing the new state will be sent to Kafka.

## Event Processor

The **Consumer** function will catchup every message of the topic from the beginning when it is launching.

The purpose of this function is to find tickets that have been created since more than three days but still not been processed (Not been picked by dev, forgotten, ...). 

It will send a console log with tickets UUIDs to alert that those tickets are still pending. In real case, you can replace the console log by any operation that you want : email alerting, ...

This function should be called periodically. For example, at the end of day, or every 10 hours, ...

## Running

1. Run **Creator** to generate messages in Kafka.
2. Execute **Consumer** to process messages.
