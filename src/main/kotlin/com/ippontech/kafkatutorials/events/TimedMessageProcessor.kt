package com.ippontech.kafkatutorials.events

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.HashMap





// $ kafka-topics --zookeeper localhost:2181 --create --topic ages --replication-factor 1 --partitions 4

fun main(args: Array<String>) {
    TimedMessageProcessor("localhost:9092").process()
}

class TimedMessageProcessor(val brokers: String) {

    val countMap: HashMap<String, Long> = HashMap()



    fun process() {
        val streamsBuilder = StreamsBuilder()

        val eventStream: KStream<String, String> = streamsBuilder
                .stream("timed-messages", Consumed.with(Serdes.String(), Serdes.String()))

        val aggregates: KTable<String, Long> = eventStream
                .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))

        aggregates.toStream().foreach() { key, value -> run {
            println("key: $key    =>    count: $value")
            if (!countMap.containsKey(key) && value.toInt() == 1) {
                println("//// A scheduler is launching for the log with key: $key ")
                val timer = Timer()
                val delay = TimeUnit.MINUTES.toMillis(5)
                timer.schedule(object : TimerTask() {
                    override fun run() {
                        if(countMap.getValue(key).toInt()==1){
                            println("ALERT : 5 Minutes passed for the log with key: $key and there are no successive messages.");
                        } else {
                            countMap.remove(key)
                        }
                    }
                }, delay)
            }
            countMap.put(key,value)
        }

        }


        val topology = streamsBuilder.build()

        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["application.id"] = "kafka-tutorial-events-4-${System.currentTimeMillis()}"
        props["auto.offset.reset"] = "latest"
        props["commit.interval.ms"] = 0
        val streams = KafkaStreams(topology, props)
        streams.start()
    }
}
