package com.ippontech.kafkatutorials.events
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.LogManager
import java.util.*
import java.util.Random








// $ kafka-topics --zookeeper localhost:2181 --create --topic log-states2 --replication-factor 1 --partitions 4
// kafka-topics --zookeeper localhost:2181 --create --topic log-states2-uuid --replication-factor 1 --partitions 1

fun main(args: Array<String>) {
    Creator2("localhost:9092").produce()
}

class Creator2(brokers: String) {

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
        val ids = Array<UUID>(10,{UUID.randomUUID()})
        val states = Array<Int>(10,{0})
        val dates = Array<Long>(10, {System.currentTimeMillis()})
        val randomGenerator = Random()

        //Step 1: Initializing
        for (i in 0..9) {
            ids[i] = UUID.randomUUID()
            states[i] = randomGenerator.nextInt(3)
            //var daysBefore = randomGenerator.nextInt(90) * 10000000
            //dates[i] = dates[i] - daysBefore
            dates[i]=System.currentTimeMillis()
            sendUUID(ids[i],dates[i])
            sendMessage(ids[i], dates[i], 0)

        }
        System.out.println(Arrays.toString(ids))
    }

    private fun sendMessage(id: UUID, ts: Long, state: Int) {
        //State: 0 => Creation // 1 => Handling // 2 => Finished

        val value = "$id,$ts,$state"
        val futureResult = producer.send(ProducerRecord("log-states2", null, ts, id.toString(), value))
        println("Sent a record: [id: $id, date: ${Date(ts.toLong()).toString()}, status: $state")
        futureResult.get()
    }
    private fun sendUUID(id: UUID, ts: Long) {
        //State: 0 => Creation // 1 => Handling // 2 => Finished

        val value = "$id"
        val futureResult = producer.send(ProducerRecord("log-states2-uuid", null,ts, id.toString(), value))
        println("Sent a record: [id: $id, date: ${Date(ts.toLong()).toString()}, value:$value")
        futureResult.get()
    }
}
