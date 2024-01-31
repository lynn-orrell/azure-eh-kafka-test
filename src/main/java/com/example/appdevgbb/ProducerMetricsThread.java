package com.example.appdevgbb;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.Metric;
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
        LOGGER.info("Producer Metrics (across all threads)");
        LOGGER.info("------------------------------------------");
        _future = _scheduledExecutorService.scheduleAtFixedRate(() -> printMetrics(), 5, 5, java.util.concurrent.TimeUnit.SECONDS);
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
        printFinalMetrics();
    }

    private void printMetrics() {
        double totalRecordSendTotal = 0;
        double totalRecordSendRate = 0;
        double totalRecordQueueTimeAverage = 0;
        double totalRecordsPerRequestAverage = 0;

        for (ProducerThread producerThread : _producerThreads) {
            Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics = producerThread.getMetrics();
            totalRecordSendTotal += getRecordSendTotal(metrics);
            totalRecordSendRate += getRecordSendRate(metrics);
            totalRecordQueueTimeAverage += getRecordQueueTimeAverage(metrics);
            totalRecordsPerRequestAverage += getRecordsPerRequestAverage(metrics);
        }

        LOGGER.info("record-send-total:                " + Math.round(totalRecordSendTotal));
        LOGGER.info("record-send-rate:                 " + String.format("%.2f", totalRecordSendRate));
        LOGGER.info("record-queue-time-average (pt):   " + String.format("%.2f", totalRecordQueueTimeAverage / _producerThreads.size()));
        LOGGER.info("records-per-request-average (pt): " + String.format("%.2f", totalRecordsPerRequestAverage / _producerThreads.size()));
        LOGGER.info("------------------------------------------");
    }

    private void printFinalMetrics() {
        double totalRequestLatencyAverage = 0;

        for (ProducerThread producerThread : _producerThreads) {
            Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics = producerThread.getMetrics();
            totalRequestLatencyAverage += getRequestLatencyAverage(metrics);
        }

        LOGGER.info("request-latency-average:     " + String.format("%.2f", totalRequestLatencyAverage));
    }

    private double getRecordSendTotal(Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics) {
        for (MetricName metricName : metrics.keySet()) {
            if (metricName.name().equals("record-send-total") && !metricName.tags().containsKey("topic")) {
                return (double)metrics.get(metricName).metricValue();
            }
        }

        return 0;
    }

    private double getRecordSendRate(Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics) {
        for (MetricName metricName : metrics.keySet()) {
            if (metricName.name().equals("record-send-rate") && !metricName.tags().containsKey("topic")) {
                return (double)metrics.get(metricName).metricValue();
            }
        }

        return 0;
    }

    private double getRecordQueueTimeAverage(Map<MetricName, ? extends Metric> metrics) {
        for (MetricName metricName : metrics.keySet()) {
            if (metricName.name().equals("record-queue-time-avg") && !metricName.tags().containsKey("topic")) {
                return (double)metrics.get(metricName).metricValue();
            }
        }

        return 0;
    }

    private double getRecordsPerRequestAverage(Map<MetricName, ? extends Metric> metrics) {
        for (MetricName metricName : metrics.keySet()) {
            if (metricName.name().equals("records-per-request-avg") && !metricName.tags().containsKey("topic")) {
                return (double)metrics.get(metricName).metricValue();
            }
        }

        return 0;
    }

    private double getRequestLatencyAverage(Map<MetricName, ? extends Metric> metrics) {
        for (MetricName metricName : metrics.keySet()) {
            if (metricName.name().equals("request-latency-avg") && !metricName.tags().containsKey("topic")) {
                return (double)metrics.get(metricName).metricValue();
            }
        }

        return 0;
    }
}
