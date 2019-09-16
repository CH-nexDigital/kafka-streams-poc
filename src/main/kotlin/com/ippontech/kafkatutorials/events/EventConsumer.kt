package com.ippontech.kafkatutorials.events

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import java.util.*
import java.util.concurrent.TimeUnit

// $ kafka-topics --zookeeper localhost:2181 --create --topic ages --replication-factor 1 --partitions 4

fun main(args: Array<String>) {
    EventConsumer("localhost:9092").process()
}

class EventConsumer(val brokers: String) {

    fun process() {
        val streamsBuilder = StreamsBuilder()

        val eventStream: KStream<String, String> = streamsBuilder
                .stream("events3", Consumed.with(Serdes.String(), Serdes.String()))


        //eventStream.foreach { key, value -> println(key + " => " + value) }


        val aggregates: KTable<Windowed<String>, Long> = eventStream
                //.groupBy({ k, v -> "dummy" }, Serialized.with(Serdes.String(), Serdes.String()))
                .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.of(10000))
                //.windowedBy(SessionWindows.with(TimeUnit.MINUTES.toMillis(10)))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                //.filter { key, value -> (key.key().toLong() > System.currentTimeMillis() - 100000) && ((key.key().toLong() < System.currentTimeMillis() - 50000))}


        aggregates.toStream().foreach { key, value -> run {
            println("key: "+key.key()+"     value: "+value)
            val time = key.key().toLong()
            val current = System.currentTimeMillis()
            val diff =  current.minus(time)
            println(key.toString() + "=>" + value + "==== current: " +current+"   ==== time : "+ time+"  ===== diff: " + diff)

//            if (diff > 1000 && value.toInt() < 10) {
                //println("----------------- id: "+key.key()+"has encounter a problem, the count is "+value)
//            }
        }
        }

        val topology = streamsBuilder.build()

        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["application.id"] = "kafka-tutorial-events-4"
        props["auto.offset.reset"] = "latest"
        props["commit.interval.ms"] = 0
        val streams = KafkaStreams(topology, props)
        streams.start()
    }
}
