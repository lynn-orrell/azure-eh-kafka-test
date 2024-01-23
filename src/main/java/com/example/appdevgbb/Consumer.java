package com.example.appdevgbb;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;

public class Consumer {

    private final static int NUM_CONSUMER_GROUP_THREADS = 1;
    private final static int NUM_RECORDS_TO_READ_BEFORE_COMMIT = System.getenv("NUM_RECORDS_TO_READ_BEFORE_COMMIT") == null ? 0 : Integer.parseInt(System.getenv("NUM_RECORDS_TO_READ_BEFORE_COMMIT"));
    private final static boolean SHOULD_START_FROM_END = System.getenv("SHOULD_START_FROM_END") == null ? true : Boolean.parseBoolean(System.getenv("SHOULD_START_FROM_END"));
    private final static int NUM_REQUIRED_RECORDS_FOR_SUMMARY = System.getenv("NUM_REQUIRED_RECORDS_FOR_SUMMARY") == null ? 100000 : Integer.parseInt(System.getenv("NUM_REQUIRED_RECORDS_FOR_SUMMARY"));

    ExecutorService _executorService;
    private List<ConsumerThread> _consumerThreads;

    public Consumer() {
        _consumerThreads = new ArrayList<ConsumerThread>();

        _executorService = Executors.newFixedThreadPool(NUM_CONSUMER_GROUP_THREADS);
    }

    private void start() {
        ConsumerThread consumerThread;
        for(int x = 0; x < NUM_CONSUMER_GROUP_THREADS; x++) {
            consumerThread = new ConsumerThread(getTopicFromEnvironment(), NUM_RECORDS_TO_READ_BEFORE_COMMIT, SHOULD_START_FROM_END, NUM_REQUIRED_RECORDS_FOR_SUMMARY, createConsumerConfig());
            _consumerThreads.add(consumerThread);
            _executorService.execute(consumerThread);
        }
    }

    private void stop() {
        _executorService.shutdown();
        for(ConsumerThread consumerThread : _consumerThreads) {
            consumerThread.stop();
        }

        try {
            _executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {}
    }

    public static void main(String[] args) {
        Consumer consumer = new Consumer();
        consumer.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting down...");
                consumer.stop();
            }
        });
    }

    private static Properties createConsumerConfig() {
        Properties props = new Properties();
        try {
            props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, System.getenv("CONSUMER_GROUP_NAME"));
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("BOOTSTRAP_SERVER"));
            props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, System.getenv("MAX_POLL_RECORDS") == null ? Integer.toString(ConsumerConfig.DEFAULT_MAX_POLL_RECORDS) : System.getenv("MAX_POLL_RECORDS"));
            props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, SHOULD_START_FROM_END ? "latest" : "earliest");
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
        String topicName = System.getenv("TOPIC_NAME");
        if(topicName == null) {
            System.out.println("You must specify the kafka topic to consume from using the TOPIC_NAME environment variable.");
            System.exit(1);
        }
        return topicName;
    }
}