package org.kafka.dataprocessor.producer.consumer;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.kafka.dataprocessor.producer.model.LengthRevisionDifference;
import org.kafka.dataprocessor.producer.model.MergedJsonModel;
import org.kafka.dataprocessor.producer.model.Meta;
import org.kafka.dataprocessor.producer.model.RecentChangeEvent;
import org.kafka.dataprocessor.producer.sedes.JacksonDeserializer;
import org.kafka.dataprocessor.producer.sedes.JacksonSerde;
import org.kafka.dataprocessor.producer.sedes.JacksonSerializer;
import org.kafka.dataprocessor.producer.transformation.StreamJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.Duration;
import java.util.Properties;

public class KafkaStreamConsumer {
    // You will need an ObjectMapper instance available

    public static void main(String[] args) {
        //get kafka config properties
        Logger log = LoggerFactory.getLogger(KafkaStreamConsumer.class);

        ObjectMapper objectMapper = new ObjectMapper();

        Properties config = getProperties();
        StreamsBuilder builder = new StreamsBuilder();

        // Json->pojo
        Serde<RecentChangeEvent> eventSerde =  jacksonSerde(RecentChangeEvent.class);

        Serde<Meta> metaSerde = new JacksonSerde<>(Meta.class);
        Serde<MergedJsonModel>  mergedJsonModelSerde = new JacksonSerde<>(MergedJsonModel.class);
        Serde<LengthRevisionDifference> diffSerde = new JacksonSerde<>(LengthRevisionDifference.class);


        //Stream from Kafka
        KStream<String, String> stream = builder.stream("wikimedia.recent-change", Consumed.with(Serdes.String(), Serdes.String()));

        //stream.peek((key, value) -> System.out.println("Received raw JSON: " + value));


        //Filter out BOT edits
        KStream<String, String> humanEdits = stream.filter((key, jsonStringValue) -> {
            try {
                JsonNode node = objectMapper.readTree(jsonStringValue);
                // Safely check the 'bot' field, defaulting to false if the field doesn't exist
                boolean isBot = node.has("bot") && node.get("bot").asBoolean();

                // Filter keeps records where the condition is TRUE.
                // We want human edits (where isBot is FALSE), so we negate the check:
                return !isBot;

            } catch (Exception e) {
                // Handle parsing errors (e.g., log them, or treat as a bot/human)
                System.err.println("Error parsing JSON for filter: " + jsonStringValue);
                return false; // Filter out records that can't be parsed
            }
        });



        //humanEdits.to("wikimedia.recent-change-output" );

        // Get the difference of the length and revision (old-new)
        KStream<String, LengthRevisionDifference> diffsStream = stream.mapValues( jsonStringValue -> {

            try {
                JsonNode rootNode = objectMapper.readTree(jsonStringValue);
                LengthRevisionDifference difference = new LengthRevisionDifference();

                if (rootNode.has("length")) {
                    difference.lengthDiff = rootNode.get("length").has("old") ? rootNode.get("length").get("old").asInt() : 0;
                } else {
                    difference.lengthDiff = 0;
                }
                if (rootNode.has("revision")) {
                    difference.revisionDiff = rootNode.get("revision").has("old") ? rootNode.get("revision").get("old").asInt() : 0;

                } else {
                    difference.revisionDiff = 0;
                }



                return difference;
            } catch (Exception e) {
                System.err.println("Error parsing JSON for filter Difference: " + jsonStringValue);
                return null;
            }

        });

        diffsStream.to("wikimedia.recent-change-output-diff", Produced.with(Serdes.String(), diffSerde));
        KTable<String, LengthRevisionDifference> differenceKTable = builder.table("wikimedia.recent-change-output-diff", Consumed.with(Serdes.String(), diffSerde));

        // Extract useful fields :) if you have serialized json
        //KStream<String, Meta> metaData = humanEdits.mapValues(v -> v.meta ); // the forward way if we have schema or model

        KStream<String, Meta> metaData = humanEdits.mapValues( jsonStringValue -> {
            try {
                JsonNode rootNode = objectMapper.readTree(jsonStringValue);

                JsonNode metaNode = rootNode.get("meta");

                if (metaNode != null && metaNode.isObject()) {

                    Meta metaObject = new Meta();
                    metaObject.uri = metaNode.get("uri").asText();
                    metaObject.id = metaNode.get("id").asText();
                    metaObject.domain = metaNode.get("domain").asText();
                    metaObject.stream = metaNode.get("stream").asText();

                    return metaObject;
                }

                Meta m = new Meta();
                m.uri = rootNode.path("uri").asText(null);
                m.id = rootNode.path("id").asText(null);
                m.domain = rootNode.path("domain").asText(null);
                m.stream = rootNode.path("stream").asText(null);

                return m;

            } catch (Exception e) {
                System.err.println("Error parsing JSON for filter Meta : " + jsonStringValue);
                return null;
            }

        });
        metaData.filter((k,v)-> v != null)
                .to("wikimedia.recent-change-output-1", Produced.with(Serdes.String(), metaSerde));

        metaData.peek((k,v)-> System.out.println("Key is " + k + " -> and Value :" + v));

        KTable<String, Meta> metaKTable = builder.table("wikimedia.recent-change-output-1", Consumed.with(Serdes.String(), metaSerde));

        // You will need to use a different join method signature
        KTable<String, MergedJsonModel> mergedTable = metaKTable.join(differenceKTable,
                StreamJoiner.createJoiner() // Your joiner is fine
                // Note: You might need to specify a Materialized object for the state store
        );

        KStream<String, MergedJsonModel> mergedStream =  mergedTable.toStream();


        mergedStream.to("wikimedia.recent-change-output-KTable", Produced.with(Serdes.String(), mergedJsonModelSerde));


        mergedStream.peek((k,v)-> System.out.println("Merged output value " + k + ": " + v));

        // Define a time window for matching records (e.g., 5 seconds)
        /*JoinWindows joinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));

        KStream<String, MergedJsonModel> mergedStream = metaData.join(diffsStream,
                StreamJoiner.createJoiner(),
                joinWindows,
                StreamJoined.with(Serdes.String(), metaSerde, diffSerde)
        );
        mergedStream.to("wikimedia.recent-change-output", Produced.with(Serdes.String(), mergedJsonModelSerde));
        */
        //diffsStream.to("wikimedia.recent-change-output", Produced.with(Serdes.String(), diffSerde));
        //metaData.to("wikimedia.recent-change-output", Produced.with(Serdes.String(), metaSerde) );


        //Start a kafka streaming
        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.start();

        // printing the streams topology
        System.out.println("Streams started"+ streams);


    }

    public static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "WikimediaChangesProcessor");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndFailExceptionHandler.class);

        return props;
    }


    public static <T> Serde<T> jacksonSerde(Class<T> clazz) {
        Serializer<T> serializer = new JacksonSerializer<>();
        Deserializer<T> deserializer = new JacksonDeserializer<>(clazz);
        return Serdes.serdeFrom(serializer, deserializer);
    }





}
