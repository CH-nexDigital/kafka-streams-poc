package com.ippontech.kafkatutorials.events
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.LogManager
import java.util.*
import java.util.Random
import java.util.concurrent.TimeUnit


fun main(args: Array<String>) {
    TimedMessageCreator("localhost:9092").produce()
}

class TimedMessageCreator(brokers: String) {
    private val producer = createProducer(brokers)

    private fun createProducer(brokers: String): Producer<String, String> {
        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["key.serializer"] = StringSerializer::class.java
        props["value.serializer"] = StringSerializer::class.java
        return KafkaProducer<String, String>(props)
    }

    fun produce() {
        val size = 5
        val ids = Array<UUID>(size,{UUID.randomUUID()})
        val states = Array<Int>(size,{0})
        val randomGenerator = Random()
        val content0 = "0"
        val content1 = "1"

        for (i in 0..size-1) {
            ids[i] = UUID.randomUUID()
            states[i] = randomGenerator.nextInt(3)

            val delay1 = Math.random() * (2L) * TimeUnit.MINUTES.toMillis(1).toDouble()
            println("Will send 1st message of Id=${ids[i]} / State=${states[i]} after $delay1 milliseconds.")

            scheduleSend(delay1.toLong(),ids[i],content0)

            if (states[i] != 2) {
                val delay2 = Math.random() * (2L) * TimeUnit.MINUTES.toMillis(1).toDouble()
                println("Will send 2nd message of Id=${ids[i]} / State=${states[i]} after $delay2 milliseconds.")
                scheduleSend(delay1.toLong()+delay2.toLong(),ids[i],content1)
            }

        }
    }

    private fun scheduleSend(delay: Long,id: UUID, content: String){
        val timer = Timer()
        timer.schedule(object : TimerTask() {
            override fun run() {
                sendMessage(id,System.currentTimeMillis().toString(),content)
            }
        }, delay)
    }

    private fun sendMessage(id: UUID, ts: String, content: String) {
        val value = "$id,$ts,$content"
        val futureResult = producer.send(ProducerRecord("timed-messages", null, System.currentTimeMillis(), id.toString(), value))
        println("Sent a record: [key: ${id.toString()}, value:$value}]")
        futureResult.get()
    }
}
