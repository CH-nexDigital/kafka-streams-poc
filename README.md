# KStreams

Inspired from this [example](https://github.com/aseigneurin/kafka-tutorial-event-processing).



## Event Producer

The **TimedMessageCreator** function simulates the generation of Kafka Messages.

Each generated message contains three information:

- Message creation date
- Functional UUID
- Content

The message key is assigned to its functional UUID.

For each functional UUID, two messages is expected within a period of 5 minutes.
That means, if everything goes well, the function will send two different messages having the same functional UUID within the period of 5 minutes.

However, anomaly is intentionnaly generated in this function in order to get randomly some UUID with only one message sent. This anomaly is simulating the loss of the second message for the corresponding functional UUID.


## Event Processor

The **TimedMessageProcessor** function will catchup messages of the concerning topic.

The purpose of this function is to find anormal functional UUIDs which have only one message. It will send a console log with functional UUID to alert about the anomaly.

The concept of this function is quite simple.
When the functional UUID appears for the first time, then a scheduler will be programmed in order to check 5 minutes later how many messages concerning this functional UUID will be received.

If the count is still equal to one which means there are no more messages (except the first one), that means there is some anomaly.

## Running

1. Run **TimedMessageCreator** to generate messages in Kafka.
2. Execute **TimedMessageProcessor** to process messages.
