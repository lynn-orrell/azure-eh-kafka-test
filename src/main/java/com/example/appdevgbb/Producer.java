package com.example.appdevgbb;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;

public class Producer {
    
    private final static Logger LOGGER = LogManager.getLogger(Producer.class);
    private final static int NUM_PRODUCER_THREADS = System.getenv("NUM_PRODUCER_THREADS") == null ? 1 : Integer.parseInt(System.getenv("NUM_PRODUCER_THREADS"));
    private final static int MESSAGE_SIZE_IN_BYTES = System.getenv("MESSAGE_SIZE_IN_BYTES") == null ? 1024 : Integer.parseInt(System.getenv("MESSAGE_SIZE_IN_BYTES"));
    private final static int TARGET_RECORDS_PER_SECOND = System.getenv("TARGET_RECORDS_PER_SECOND") == null ? Integer.MAX_VALUE : Integer.parseInt(System.getenv("TARGET_RECORDS_PER_SECOND"));

    private ExecutorService _producersExecutorService;
    private List<ProducerThread> _producerThreads;
    private ExecutorService _producerMetricsExecutorService;
    private ProducerMetricsThread _producerMetricsThread;
    private BlockingQueue<ProducerMetric> _producerMetrics;

    public Producer() {
        _producerThreads = new ArrayList<ProducerThread>();
        _producersExecutorService = Executors.newFixedThreadPool(NUM_PRODUCER_THREADS + 1);
        _producerMetricsExecutorService = Executors.newSingleThreadExecutor();
        _producerMetrics = new java.util.concurrent.LinkedBlockingQueue<ProducerMetric>();
    }

    private void start() {
        String topicName = getTopicFromEnvironment();
        LOGGER.info("Preparing to produce events to the " + topicName + " topic.");
        LOGGER.info("Using " + NUM_PRODUCER_THREADS + " producer threads each producing " + MESSAGE_SIZE_IN_BYTES + " byte messages with a target message rate of " + TARGET_RECORDS_PER_SECOND + " per second.");    

        ProducerThread producerThread;
        for(int x = 0; x < NUM_PRODUCER_THREADS; x++) {
            producerThread = new ProducerThread(topicName, MESSAGE_SIZE_IN_BYTES, TARGET_RECORDS_PER_SECOND / NUM_PRODUCER_THREADS, createProducerConfig(), _producerMetrics);
            _producerThreads.add(producerThread);
            _producersExecutorService.execute(producerThread);
        }

        _producerMetricsThread = new ProducerMetricsThread(_producerMetrics);
        _producerMetricsExecutorService.execute(_producerMetricsThread);
    }

    private void stop() {
        _producersExecutorService.shutdown();
        for(ProducerThread producerThread : _producerThreads) {
            producerThread.stop();
        }
        try {
            _producersExecutorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {}

        _producerMetricsExecutorService.shutdown();
        _producerMetricsThread.stop();
        try {
            _producerMetricsExecutorService.awaitTermination(60, TimeUnit.SECONDS);
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
        Producer producer = new Producer();
        producer.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                LOGGER.info("Shutting down...");
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
            props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(MESSAGE_SIZE_IN_BYTES * 400));
            props.setProperty("security.protocol", "SASL_SSL");
            props.setProperty("sasl.mechanism", "PLAIN");
            props.setProperty("sasl.jaas.config", System.getenv("SASL_JAAS_CONFIG"));
        } catch (Exception e) {
            LOGGER.error("The following environment variables must be set: BOOTSTRAP_SERVER, SASL_JAAS_CONFIG");
            System.exit(1);
        }

        return props;
    }

    private static String getTopicFromEnvironment() {
        String topicName = System.getenv("TOPIC_NAME");
        if(topicName == null) {
            LOGGER.error("You must specify the kafka topic to produce events to using the TOPIC_NAME environment variable.");
            System.exit(1);
        }
        return topicName;
    }
}