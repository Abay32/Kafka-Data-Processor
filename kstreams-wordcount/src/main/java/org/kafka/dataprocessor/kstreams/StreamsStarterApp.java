package org.kafka.dataprocessor.kstreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;

import org.apache.kafka.streams.kstream.*;


import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class StreamsStarterApp {

    public static void main(String[] args) {

        //Logger log = LoggerFactory.getLogger(StreamsStarterApp.class.getName());

        Properties config = getConfig();

        StreamsBuilder builder = new StreamsBuilder();
        // stream from kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");
        KStream<String, String> wordCountInput2 = builder.stream("word-count-input-2");

        //Value joiner
        ValueJoiner<String, String, String> valueJoiner = (left, right) -> {
            return left + "added" + right;
        };

        //define joinWindows
        JoinWindows tenSecondWindow = JoinWindows.ofTimeDifferenceAndGrace(
                Duration.ofSeconds(10), // Window Size: The span during which records must match
                Duration.ofSeconds(0)   // Grace Period: How long after the window ends we still accept late records
        );

        KStream<String, String> stringJoiner = wordCountInput.join(wordCountInput2, valueJoiner, tenSecondWindow);

        // map-values to lowercase
        KTable<String, Long> wordCounts = stringJoiner
                .mapValues(value -> value.toLowerCase())
                .flatMapValues(lowerCasedTextLine -> Arrays.asList(lowerCasedTextLine.split(" ")))
                .selectKey((ignoredKey, word) -> word)
                .groupByKey()
                .count(Materialized.as("Counts"));

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));

        System.out.println("I am here what's up"+wordCounts);

        //Start a kafka streaming
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // printing the streams topology
        System.out.println("Streams started"+ streams);

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
