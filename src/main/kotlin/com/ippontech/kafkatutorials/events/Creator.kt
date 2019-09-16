package com.ippontech.kafkatutorials.events

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.LogManager
import java.util.*
import java.util.Random








// $ kafka-topics --zookeeper localhost:2181 --create --topic log-states --replication-factor 1 --partitions 4

fun main(args: Array<String>) {
    Creator("localhost:9092").produce()
}

class Creator(brokers: String) {

    private val logger = LogManager.getLogger(javaClass)
    private val producer = createProducer(brokers)

    private fun createProducer(brokers: String): Producer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["key.serializer"] = StringSerializer::class.java
        props["value.serializer"] = StringSerializer::class.java
        return KafkaProducer<String, String>(props)
    }

    fun produce() {
        //Step 1 : Initializing

        val ids = Array<UUID>(10,{UUID.randomUUID()})
        val states = Array<Int>(10,{0})
        val dates = Array<Long>(10, {System.currentTimeMillis()})
        val randomGenerator = Random()

        //Step 1: Initializing
        for (i in 0..9) {
            ids[i] = UUID.randomUUID()
            states[i] = randomGenerator.nextInt(3)
            var daysBefore = randomGenerator.nextInt(90) * 10000000
            dates[i] = dates[i] - daysBefore
            sendMessage(ids[i], dates[i], 0)

        }


        // Step 2: Picking up some tickets
        for (i in 0..9){
            //Simulate processing the logs
            if (states[i] >= 1 ) {
                dates[i] = dates[i] + (randomGenerator.nextInt(4) + 1) * 10000000
                sendMessage(ids[i], dates[i], 1)
            }

        }

        //Step 3 : Finishing some tickets
        for (i in 0..9){
            //Simulate the process is terminating
            if (states[i] == 2 ){
                dates[i] = dates[i] + (randomGenerator.nextInt(4)+1) * 10000000
                sendMessage(ids[i], dates[i], 2)
            }

        }
    }

    private fun sendMessage(id: UUID, ts: Long, state: Int) {
        //State: 0 => Creation // 1 => Handling // 2 => Finished

        val value = "$id,$ts,$state"
        val futureResult = producer.send(ProducerRecord("log-states", null, ts, id.toString(), value))
        println("Sent a record: [id: $id, date: ${Date(ts.toLong()).toString()}, status: $state")
        futureResult.get()

    }
}
