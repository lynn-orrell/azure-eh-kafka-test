package com.example.appdevgbb;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.ConsumerConfig;

public class Consumer {

    private final static int NUM_CONSUMER_GROUP_THREADS = 1;
    private final static int NUM_RECORDS_TO_READ_BEFORE_COMMIT = System.getenv("NUM_RECORDS_TO_READ_BEFORE_COMMIT") == null ? 0 : Integer.parseInt(System.getenv("NUM_RECORDS_TO_READ_BEFORE_COMMIT"));

    public static void main(String[] args) {
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_CONSUMER_GROUP_THREADS);
        for(int x = 0; x < NUM_CONSUMER_GROUP_THREADS; x++) {
            executorService.execute(new ConsumerThread(getTopicFromEnvironment(), NUM_RECORDS_TO_READ_BEFORE_COMMIT, createConsumerConfig()));
        }
    }

    private static Properties createConsumerConfig() {
        Properties props = new Properties();
        try {
            props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, System.getenv("CONSUMER_GROUP_NAME"));
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("BOOTSTRAP_SERVER"));
            props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, System.getenv("MAX_POLL_RECORDS") == null ? "1000" : System.getenv("MAX_POLL_RECORDS"));
            props.setProperty("security.protocol", "SASL_SSL");
            props.setProperty("sasl.mechanism", "PLAIN");
            props.setProperty("sasl.jaas.config", System.getenv("SASL_JAAS_CONFIG"));
        } catch (Exception e) {
            System.out.println("The following environment variables must be set: BOOTSTRAP_SERVER, SASL_JAAS_CONFIG, CONSUMER_GROUP_NAME");
            System.exit(1);
        }

        return props;
    }

    private static String getTopicFromEnvironment() {
        //String topicName = System.getenv("TOPIC_NAME");
        String topicName = "test-topic";
        if(topicName == null) {
            System.out.println("You must specify the kafka topic to consume from using the TOPIC_NAME environment variable.");
            System.exit(1);
        }
        return topicName;
    }
}