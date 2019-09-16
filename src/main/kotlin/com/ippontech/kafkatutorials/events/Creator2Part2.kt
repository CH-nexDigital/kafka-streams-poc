package com.ippontech.kafkatutorials.events

import com.fasterxml.jackson.annotation.ObjectIdGenerators
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import java.util.*


fun main(args: Array<String>) {
    Consumer2("localhost:9092").process()
}

class Consumer2(val brokers: String) {
    private val producer = createProducer(brokers)

    private fun createProducer(brokers: String): Producer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["key.serializer"] = StringSerializer::class.java
        props["value.serializer"] = StringSerializer::class.java
        return KafkaProducer<String, String>(props)
    }

    private fun sendMessage(id: UUID, ts: Long, state: Int) {
        //State: 0 => Creation // 1 => Handling // 2 => Finished

        val value = "$id,$ts,$state"
        val futureResult = producer.send(ProducerRecord("log-states2", null, ts, id.toString(), value))
        println("Sent a record: [id: $id, date: ${Date(ts.toLong()).toString()}, status: $state")
        futureResult.get()
    }

    fun process() {
        val streamsBuilder = StreamsBuilder()
        val randomGenerator = Random()


        val eventStream: KStream<String, String> = streamsBuilder
                .stream("log-states2-uuid", Consumed.with(Serdes.String(), Serdes.String()))

        eventStream.foreach { key, value -> run {
            //println("Key: $key, Value: $value")
            var index = randomGenerator.nextInt(3)
            var uuid = UUID.fromString(key)
            if (index != 0) {
                sendMessage(uuid,System.currentTimeMillis(),1)
            }
            else {
                println("--------------------- L'id $uuid n'a pas de 2ème Log !")
            }
        } }


        val topology = streamsBuilder.build()

        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["application.id"] = "kafka-tutorial-events-4-system2}"
        props["auto.offset.reset"] = "earliest"
        props["commit.interval.ms"] = 0
        val streams = KafkaStreams(topology, props)
        streams.start()
    }
}
