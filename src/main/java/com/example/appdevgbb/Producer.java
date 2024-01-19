package com.example.appdevgbb;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.ProducerConfig;

public class Producer {

    private final static int NUM_PRODUCER_THREADS = 1;
    private final static int MESSAGE_SIZE_IN_BYTES = System.getenv("MESSAGE_SIZE_IN_BYTES") == null ? 1024 : Integer.parseInt(System.getenv("MESSAGE_SIZE_IN_BYTES"));
    private final static long SLEEP_TIME_MS = System.getenv("SLEEP_TIME_MS") == null ? 0 : Long.parseLong(System.getenv("SLEEP_TIME_MS"));

    public static void main(String[] args) {
        String topicName = getTopicFromEnvironment();
        System.out.println("Preparing to produce events to the " + topicName + " topic.");

        ExecutorService executorService = Executors.newFixedThreadPool(NUM_PRODUCER_THREADS);
        for(int x = 0; x < NUM_PRODUCER_THREADS; x++) {
            executorService.execute(new ProducerThread(getTopicFromEnvironment(), MESSAGE_SIZE_IN_BYTES, SLEEP_TIME_MS, createProducerConfig()));
        }
    }
    
    private static Properties createProducerConfig() {
        Properties props = new Properties();
        try {
            props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("BOOTSTRAP_SERVER"));
            props.setProperty("security.protocol", "SASL_SSL");
            props.setProperty("sasl.mechanism", "PLAIN");
            props.setProperty("sasl.jaas.config", System.getenv("SASL_JAAS_CONFIG"));
        } catch (Exception e) {
            System.out.println("The following environment variables must be set: BOOTSTRAP_SERVER, SASL_JAAS_CONFIG");
            System.exit(1);
        }

        return props;
    }

    private static String getTopicFromEnvironment() {
        String topicName = System.getenv("TOPIC_NAME");
        if(topicName == null) {
            System.out.println("You must specify the kafka topic to produce events to using the TOPIC_NAME environment variable.");
            System.exit(1);
        }
        return topicName;
    }
}