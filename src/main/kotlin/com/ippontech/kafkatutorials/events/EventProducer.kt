package com.ippontech.kafkatutorials.events

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.LogManager
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*

// $ kafka-topics --zookeeper localhost:2181 --create --topic events --replication-factor 1 --partitions 4

fun main(args: Array<String>) {
    SimpleProducer("localhost:9092").produce()
}

class SimpleProducer(brokers: String) {

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
        val now = System.currentTimeMillis()
        val delay = 1200 - Math.floorMod(now, 1000)
        val timer = Timer()
        timer.schedule(object : TimerTask() {
            override fun run() {
                val ts = System.currentTimeMillis()
                val second = Math.floorMod(ts / 1000, 10)

                //println("parité:" + second.rem(2).toString() + " second :" + second.toString())
                /*
                if (second != 8L) {
                    sendMessage(second, ts, "on time")
                }
                if (second == 2L) {
                    // send the late record
                    //sendMessage(8, ts - 4000, "late")
                    println(ts.toString() + "Missing operations....")
                }
                 */
                sendMessage(second, ts, "on time")
            }
        }, delay, 1000L)
    }

    private fun sendMessage(id: Long, ts: Long, info: String) {
        val window = (ts / 10000) * 10000
        val value = "$window,$id,$info"
        val test = window / 10000

        if ( (test.rem(3).toInt()!=0 ) || (test.rem(3).toInt()==0 && id < 6) ) {
            val futureResult = producer.send(ProducerRecord("events3", null, ts, "$window", value))
            //logger.debug("Sent a record: $value")
            println("Normal - Sent a record: $value")
            futureResult.get()
        } else {
            println("!! id: "+window+" is missing its message number "+id)
        }
    }
}
