package org.kafka.dataprocessor;

import com.google.gson.JsonParser;
import com.google.gson.annotations.JsonAdapter;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;

        URI connUri = URI.create("http://localhost:9200");

        //extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            //Rest client without security
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(
                            new HttpHost(
                                    connUri.getHost(),
                                    connUri.getPort(),
                                    connUri.getScheme()
                            )
                    )
            );
        } else {
            // RestClient with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder( new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                            )
            );


        }
        return restHighLevelClient;
    }

    public static void main(String[] args) throws IOException {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

        // Create an opensearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        //create our kafka Client
        KafkaConsumer<String, String>  consumer = createKafkaConsumer();

        try(openSearchClient; consumer) {

            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

            if (!indexExists) {
                // we need to create the index on the Opensearch if it doesn't exist
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Create Index Success");
            }else  {
                log.info("Index alreadyExists ");
            }

            // subscribe to the topic to read data from it
            consumer.subscribe(Collections.singletonList("wikimedia.recentchange"));

            while(true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordsCount = records.count();
                log.info("Consumer Records Count : {}", recordsCount);

                //BulkRequest
                BulkRequest bulkRequest = new BulkRequest();


                for (ConsumerRecord<String, String> record : records) {

                    try {
                        // send the record in to opensearch

                        // How to process/consume the message exactly-one not more not less
                        // strategy 1: define an Id using Record coordinates
                        // String id = record.topic() + "_" + record.partition() + "_" + record.offset(); // not best

                        //strategy 2: get the id from the coming metadata
                        String id = extractId(record.value()); // best


                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id); // using id

                        //IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        bulkRequest.add(indexRequest);


                        //log.info("Inserted 1 document into OpenSearch Index {}", response.getId());
                    }catch (Exception e) {

                    }

                }
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                    log.info("Bulk Response : {}", bulkResponse.getItems().length + "record(s).");
                    try {
                        Thread.sleep(1000);

                    }catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }


                //commit offsets after the batch is consumed
                consumer.commitSync();
                log.info("Offsets have been successfully committed");

            }




        }






        //main code logic

        //close things


    }

    private static String extractId(String json) {
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();

    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {

        String bootstrapServers = "localhost:9092";
        String groupId = "consumer-opensearch-demo";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //create the Kafka consumer
        KafkaConsumer<String, String> consumer;
        consumer = new KafkaConsumer<>(props);
        return consumer;


    }
}