package org.kafka.dataprocessor.kstreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

    public static void main(String[] args) {

        //Logger log = LoggerFactory.getLogger(StreamsStarterApp.class.getName());

        Properties config = getConfig();

        KStreamBuilder builder = new KStreamBuilder();
        // stream from kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        // map-values to lowercase
        KTable<String, Long> wordCounts = wordCountInput
                .mapValues(String::toLowerCase)
                .flatMapValues(lowerCasedTextLine -> Arrays.asList(lowerCasedTextLine.split(" ")))
                .selectKey((ignoredKey, word) -> word)
                .groupByKey()
                .count("Counts");

        wordCounts.to(Serdes.String(), Serdes.Long(),"word-count-output");

        //Start a kafka streaming
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        // printing the streams topology
        System.out.println("Streams started"+ streams.toString());

        // Shutdown gracefully --> correctly
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }

    public static Properties getConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return  config;
    }

}
