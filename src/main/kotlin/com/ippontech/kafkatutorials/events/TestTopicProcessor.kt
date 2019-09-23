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
    EventProcessorTestTopic("localhost:9092").process()
}

class EventProcessorTestTopic(val brokers: String) {

    val countMap: HashMap<String, Long> = HashMap()



    fun process() {
        val streamsBuilder = StreamsBuilder()

        val eventStream: KStream<String, String> = streamsBuilder
                .stream("test-topic-9", Consumed.with(Serdes.String(), Serdes.String()))

        val aggregates: KTable<String, Long> = eventStream
                .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))

        aggregates.toStream().foreach() { key, value -> run {
            println("key: $key    =>    count: $value")
            if (!countMap.containsKey(key) && value.toInt() == 1) {
                println("//// Un scheduler est entrain de lancer pour key: $key ")
                val timer = Timer()
                val delay = TimeUnit.MINUTES.toMillis(5)
                timer.schedule(object : TimerTask() {
                    override fun run() {
                        if(countMap.getValue(key).toInt()==1){
                            println("ALERTE : 5 MINUTES EST PASSE POUR key: $key et le nombre de message recu actuel est ${countMap.getValue(key)}");
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
        props["auto.offset.reset"] = "earliest"
        props["commit.interval.ms"] = 0
        val streams = KafkaStreams(topology, props)
        streams.start()
    }
}
