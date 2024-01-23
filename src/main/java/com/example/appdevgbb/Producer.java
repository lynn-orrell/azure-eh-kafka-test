package com.example.appdevgbb;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerConfig;

public class Producer {

    private final static int NUM_PRODUCER_THREADS = 1;
    private final static int MESSAGE_SIZE_IN_BYTES = System.getenv("MESSAGE_SIZE_IN_BYTES") == null ? 1024 : Integer.parseInt(System.getenv("MESSAGE_SIZE_IN_BYTES"));
    private final static long SLEEP_TIME_MS = System.getenv("SLEEP_TIME_MS") == null ? 0 : Long.parseLong(System.getenv("SLEEP_TIME_MS"));

    private ExecutorService _executorService;
    private List<ProducerThread> _producerThreads;

    public Producer() {
        _producerThreads = new ArrayList<ProducerThread>();
        _executorService = Executors.newFixedThreadPool(NUM_PRODUCER_THREADS);
    }

    private void start() {
        String topicName = getTopicFromEnvironment();
        System.out.println("Preparing to produce events to the " + topicName + " topic.");
        System.out.println("Using " + NUM_PRODUCER_THREADS + " producer threads each producing " + MESSAGE_SIZE_IN_BYTES + " byte messages with a " + SLEEP_TIME_MS + " ms sleep time between sends.");    

        ProducerThread producerThread;
        for(int x = 0; x < NUM_PRODUCER_THREADS; x++) {
            producerThread = new ProducerThread(topicName, MESSAGE_SIZE_IN_BYTES, SLEEP_TIME_MS, createProducerConfig());
            _producerThreads.add(producerThread);
            _executorService.execute(producerThread);
        }
    }

    private void stop() {
        _executorService.shutdown();
        for(ProducerThread producerThread : _producerThreads) {
            producerThread.stop();
        }

        try {
            _executorService.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {}
    }

    public static void main(String[] args) {
        Producer producer = new Producer();
        producer.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Shutting down...");
                producer.stop();
            }
        });
    }
    
    private static Properties createProducerConfig() {
        Properties props = new Properties();
        try {
            props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("BOOTSTRAP_SERVER"));
            props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
            props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(MESSAGE_SIZE_IN_BYTES * 50));
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