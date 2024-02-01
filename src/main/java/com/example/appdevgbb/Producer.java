package com.example.appdevgbb;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;

public class Producer {
    
    private final static Logger LOGGER = LogManager.getLogger(Producer.class);
    private final static int NUM_PRODUCER_THREADS = System.getenv("NUM_PRODUCER_THREADS") == null ? 1 : Integer.parseInt(System.getenv("NUM_PRODUCER_THREADS"));
    private final static int MESSAGE_SIZE_IN_BYTES = System.getenv("MESSAGE_SIZE_IN_BYTES") == null ? 1024 : Integer.parseInt(System.getenv("MESSAGE_SIZE_IN_BYTES"));
    private final static int TARGET_RECORDS_PER_SECOND = System.getenv("TARGET_RECORDS_PER_SECOND") == null ? 1000000 : Integer.parseInt(System.getenv("TARGET_RECORDS_PER_SECOND"));
    private final static int LINGER_MS = System.getenv("LINGER_MS") == null ? 0 : Integer.parseInt(System.getenv("LINGER_MS"));

    private List<ProducerThread> _producerThreads;
    private ProducerMetricsThread _producerMetricsThread;

    public Producer() {
        _producerThreads = new ArrayList<ProducerThread>();
    }

    private void start() {
        String topicName = getTopicFromEnvironment();
        LOGGER.info("Preparing to produce events to the " + topicName + " topic.");
        LOGGER.info("Using " + NUM_PRODUCER_THREADS + " producer threads each producing " + MESSAGE_SIZE_IN_BYTES + " byte messages with a target message rate of " + TARGET_RECORDS_PER_SECOND + " per second.");    

        ProducerThread producerThread;
        for(int x = 0; x < NUM_PRODUCER_THREADS; x++) {
            producerThread = new ProducerThread(topicName, MESSAGE_SIZE_IN_BYTES, TARGET_RECORDS_PER_SECOND / NUM_PRODUCER_THREADS, createProducerConfig());
            _producerThreads.add(producerThread);
            producerThread.start();
        }

        _producerMetricsThread = new ProducerMetricsThread(_producerThreads);
        _producerMetricsThread.start();
    }

    private void stop() {
        for(ProducerThread producerThread : _producerThreads) {
            producerThread.stop();
        }

        _producerMetricsThread.stop();

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
            //props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("BOOTSTRAP_SERVER"));
            props.setProperty(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(LINGER_MS));
            //props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(MESSAGE_SIZE_IN_BYTES * 400));
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