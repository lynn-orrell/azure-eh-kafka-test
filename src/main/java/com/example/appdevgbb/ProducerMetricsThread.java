package com.example.appdevgbb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.MetricName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProducerMetricsThread {

    private final static Logger LOGGER = LogManager.getLogger(ProducerMetricsThread.class);

    private List<ProducerThread> _producerThreads;

    private ScheduledExecutorService _scheduledExecutorService;
    private ScheduledFuture<?> _future;

    public ProducerMetricsThread(List<ProducerThread> producerThreads) {
        _producerThreads = producerThreads;

        _scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }
    
    public void start() {
        _future = _scheduledExecutorService.scheduleAtFixedRate(() -> printMetrics(), 5, 5, java.util.concurrent.TimeUnit.SECONDS);
        LOGGER.info("ProducerMetricsThread started: " + _future.state());
    }
    
    public void stop() {
        _future.cancel(false);
        _scheduledExecutorService.shutdown();
        try {
            _scheduledExecutorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("An error occurred while waiting for the scheduled executor service to terminate.", e);
        }

        printMetrics();
    }

    private void printMetrics() {
        double totalRecordSendTotal = 0;
        double totalRecordSendRate = 0;
        for (ProducerThread producerThread : _producerThreads) {
            Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics = producerThread.getMetrics();
            totalRecordSendTotal += getRecordSendTotal(metrics);
            totalRecordSendRate += getRecordSendRate(metrics);
            // for (MetricName metricName : metrics.keySet()) {
            //     LOGGER.info(metricName.name() + ": " + metrics.get(metricName).metricValue());
            //     for (Map.Entry<String, String> tag : metricName.tags().entrySet()) {
            //         LOGGER.info("    " + tag.getKey() + ": " + tag.getValue());
            //     }
            // }
        }

        LOGGER.info("Producer metrics: " + totalRecordSendTotal + " records sent total, " + totalRecordSendRate + " records sent per second.");
    }

    private double getRecordSendTotal(Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics) {
        for (MetricName metricName : metrics.keySet()) {
            if (metricName.name().equals("record-send-total") && !metricName.tags().containsKey("topic")) {
                return (double)metrics.get(metricName).metricValue();
                // LOGGER.info(metricName.name() + ": " + metrics.get(metricName).metricValue());
                // for (Map.Entry<String, String> tag : metricName.tags().entrySet()) {
                //     LOGGER.info("    " + tag.getKey() + ": " + tag.getValue());
                // }
            }
        }

        return 0;
    }

    private double getRecordSendRate(Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics) {
        for (MetricName metricName : metrics.keySet()) {
            if (metricName.name().equals("record-send-rate") && !metricName.tags().containsKey("topic")) {
                return (double)metrics.get(metricName).metricValue();
                // LOGGER.info(metricName.name() + ": " + metrics.get(metricName).metricValue());
                // for (Map.Entry<String, String> tag : metricName.tags().entrySet()) {
                //     LOGGER.info("    " + tag.getKey() + ": " + tag.getValue());
                // }
            }
        }

        return 0;
    }
}
