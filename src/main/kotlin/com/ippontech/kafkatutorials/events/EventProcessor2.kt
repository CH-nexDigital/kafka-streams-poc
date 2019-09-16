package com.ippontech.kafkatutorials.events


import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import java.util.*


fun main(args: Array<String>) {
    EventProcessor2("localhost:9092").process()
}

class EventProcessor2(val brokers: String) {

    fun process() {
        val streamsBuilder = StreamsBuilder()

        val eventStream: KStream<String, String> = streamsBuilder
                .stream("log-states2", Consumed.with(Serdes.String(), Serdes.String()))



//        val a: KStream<String, String> = eventStream.selectKey { k, v ->
//            println("$k,$v")
//            "plop"
//        }

//        a.to("plop", Produced.with(Serdes.String(), Serdes.String()))

        val aggregates: KTable<Windowed<String>, Long> = eventStream
                .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.of(180000)) //Window of 3 minutes
//                .windowedBy(TimeWindows.of(10000).grace(10000))
                .count(Materialized.with(Serdes.String(), Serdes.Long()))

        aggregates.toStream().foreach { key, value -> run {
            println("key:$key   =>       value:$value")
        } }

        /*
        aggregates
//                .suppress(intermediateEvents(withEmitAfter(Duration.ofSeconds(10))))
//                .suppress(emitFinalResultsOnly(withBufferFullStrategy(SHUT_DOWN)))
                .toStream()
                .map { ws, i -> KeyValue("${ws.window().start()}", "$i") }
                .to("aggs3", Produced.with(Serdes.String(), Serdes.String()))

        aggregates
                .toStream()
                .map { ws, i -> KeyValue("${ws.window().start()}", "$i")}
                .toString()
         */

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
