package com.example.appdevgbb;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;

public class Consumer {

    private final static Logger LOGGER = LogManager.getLogger(Consumer.class);
    private final static int NUM_CONSUMER_GROUP_THREADS = System.getenv("NUM_CONSUMER_GROUP_THREADS") == null ? 1 : Integer.parseInt(System.getenv("NUM_CONSUMER_GROUP_THREADS"));
    private final static int NUM_RECORDS_TO_READ_BEFORE_COMMIT = System.getenv("NUM_RECORDS_TO_READ_BEFORE_COMMIT") == null ? 0 : Integer.parseInt(System.getenv("NUM_RECORDS_TO_READ_BEFORE_COMMIT"));
    private final static boolean SHOULD_START_FROM_END = System.getenv("SHOULD_START_FROM_END") == null ? true : Boolean.parseBoolean(System.getenv("SHOULD_START_FROM_END"));

    private ExecutorService _consumersExecutorService;
    private List<ConsumerThread> _consumerThreads;
    private ExecutorService _consumerMetricsExecutorService;
    private ConsumerMetricsThread _consumerMetricsThread;
    private BlockingQueue<ConsumerMetric> _consumerMetrics;

    public Consumer() {
        _consumerThreads = new ArrayList<ConsumerThread>();
        _consumersExecutorService = Executors.newFixedThreadPool(NUM_CONSUMER_GROUP_THREADS);
        _consumerMetricsExecutorService = Executors.newSingleThreadExecutor();
        _consumerMetrics = new LinkedBlockingQueue<ConsumerMetric>();
    }

    private void start() {
        ConsumerThread consumerThread;
        for(int x = 0; x < NUM_CONSUMER_GROUP_THREADS; x++) {
            consumerThread = new ConsumerThread(getTopicFromEnvironment(), NUM_RECORDS_TO_READ_BEFORE_COMMIT, SHOULD_START_FROM_END, createConsumerConfig(), _consumerMetrics);
            _consumerThreads.add(consumerThread);
            _consumersExecutorService.execute(consumerThread);
        }

        _consumerMetricsThread = new ConsumerMetricsThread(_consumerMetrics);
        _consumerMetricsExecutorService.execute(_consumerMetricsThread);
    }

    private void stop() {
        _consumersExecutorService.shutdown();
        for(ConsumerThread consumerThread : _consumerThreads) {
            consumerThread.stop();
        }

        _consumerMetricsExecutorService.shutdown();
        _consumerMetricsThread.stop();
        try {
            _consumerMetricsExecutorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {}

        try {
            _consumersExecutorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {}

        if( LogManager.getContext() instanceof LoggerContext ) {
                LOGGER.debug("Shutting down log4j2");
                Configurator.shutdown((LoggerContext)LogManager.getContext());
        } else {
            LOGGER.warn("Unable to shutdown log4j2");
        }

        LOGGER.info("Shutdown complete.");
    }

    public static void main(String[] args) {
        Consumer consumer = new Consumer();
        consumer.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOGGER.info("Shutting down...");
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
            LOGGER.error("The following environment variables must be set: BOOTSTRAP_SERVER, SASL_JAAS_CONFIG, CONSUMER_GROUP_NAME");
            System.exit(1);
        }

        return props;
    }

    private static String getTopicFromEnvironment() {
        String topicName = System.getenv("TOPIC_NAME");
        if(topicName == null) {
            LOGGER.error("You must specify the kafka topic to consume from using the TOPIC_NAME environment variable.");
            System.exit(1);
        }
        return topicName;
    }
}