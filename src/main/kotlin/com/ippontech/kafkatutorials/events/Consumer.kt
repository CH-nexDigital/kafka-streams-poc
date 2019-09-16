package com.ippontech.kafkatutorials.events

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import java.util.*


fun main(args: Array<String>) {
    Consumer("localhost:9092").process()
}

class Consumer(val brokers: String) {

    fun process() {
        val streamsBuilder = StreamsBuilder()



        val eventStream: KStream<String, String> = streamsBuilder
                .stream("log-states", Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
                .reduce { value1, value2 -> value2 } // Take always the newest value
                .toStream()
                .filter { key, value -> run {
                    val state = value.split(",")[2]
                    val time = value.split(",")[1].toLong()
                    val limit = System.currentTimeMillis() - 259200000
                    println("$key //// ${Date(time)} //// $state")
                    (state.toInt() == 0) && (time < limit)
                } }


        eventStream.foreach { key, value -> run {
            val uuid = value.split(",")[0]
            val time = Date(value.split(",")[1].toLong())

            val state = value.split(",")[2]
            println("The ticket : $uuid, created on :$time, has been created since 3 days and is still suspending for picking up !! !!\n")
        } }


        val topology = streamsBuilder.build()

        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["application.id"] = "kafka-tutorial-events-${System.currentTimeMillis()}"
        props["auto.offset.reset"] = "earliest"
        props["commit.interval.ms"] = 0
        val streams = KafkaStreams(topology, props)
        streams.start()
    }
}
