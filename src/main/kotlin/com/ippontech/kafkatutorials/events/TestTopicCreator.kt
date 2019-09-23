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
    TestTopicCreator2("localhost:9092").produce()
}

class TestTopicCreator2(brokers: String) {

    //private val idsstring = "c9e562e5-4674-4adb-8a11-10a854d1c3ab,0cfd33b9-9040-45e1-b91c-95c7c72421fc,4e6c4cc6-4a02-4041-9a38-34aac7c9f64d".split(",")
    private val idsstring = "6df80d55-5d0e-4ef6-8a5d-2626d0a36cc1,b7c57fc7-88bc-4ab5-9d28-841647dec118,477e2b3e-23cf-4a3c-a9bb-a4137ebfb5ce".split(",")
    
    //private val idsstring = "b5a82dca-b70f-4434-ab87-78fb2b387ea5".split(",") //id 1

    //private val idsstring = "ef008650-6252-4a3e-80bf-03581304779d".split(",") //id 2
    //b5a82dca-b70f-4434-ab87-78fb2b387ea5, ef008650-6252-4a3e-80bf-03581304779d, 327b8bb9-d18c-46e4-9c85-bd335aecd5fa

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
        val size = idsstring.size
        //val size = 1
        val ids = Array<UUID>(size,{UUID.randomUUID()})
        val states = Array<Int>(size,{0})
        //val dates = Array<Long>(size, {System.currentTimeMillis()})
        val randomGenerator = Random()

        //Step 1: Initializing
        for (i in 0..size-1) {
            ids[i] = UUID.fromString(idsstring[i])
            states[i] = randomGenerator.nextInt(3)
            sendMessage(ids[i],System.currentTimeMillis().toString(), states[i].toString())
            //sendMessage(ids[i],System.currentTimeMillis().toString(), "3")

        }
    }

    private fun sendMessage(id: UUID, ts: String, content: String) {
        val value = "$id,$ts,$content"
        val futureResult = producer.send(ProducerRecord("test-topic-9", null, System.currentTimeMillis(), id.toString(), value))
        println("Sent a record: [key: ${id.toString()}, value:$value}]")
        futureResult.get()
    }
}
