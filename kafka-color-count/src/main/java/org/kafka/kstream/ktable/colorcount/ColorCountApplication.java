package org.kafka.kstream.ktable.colorcount;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class ColorCountApplication {
    public static void main(String[] args) {

        Properties config = getConfig();

        StreamsBuilder builder = new StreamsBuilder();
        // stream from kafka
        KStream<String, String> colorCountInput = builder.stream("fav-colour-input");

        KStream<String, String> userFColors = colorCountInput
                .filter((user, color) -> color.contains(","))
                .selectKey((user, color) -> color.split(",")[0].toLowerCase())
                .mapValues(color -> color.split(",")[1].toLowerCase())
                .filter((user, color) -> Arrays.asList("green", "red", "blue").contains(color)); //data sanitization

        // write to intermediate topic
        userFColors.to("fav-colour-intermediate");

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        // Read "color-count" topic as a KTable: updates are captured correctly (i.e, only the latest us here)
        KTable<String, String> userFColorsTable = builder.table("fav-colour-intermediate");

        // Count the occurrence  of colors
        KTable<String, Long> favColors = userFColorsTable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColours")
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde));

        // output the result to a kafka topic
        favColors.toStream().to("fav-colour-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // printing the streams topology
        System.out.println("Streams started"+ streams.toString());

        // Shutdown gracefully --> correctly
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    public static Properties getConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "fav-colour-java-app");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        return  config;
    }
}
