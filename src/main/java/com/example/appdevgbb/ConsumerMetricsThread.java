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

public class ConsumerMetricsThread {
    
    private final static Logger LOGGER = LogManager.getLogger(ConsumerMetricsThread.class);

    private List<ConsumerThread> _consumerThreads;

    private ScheduledExecutorService _scheduledExecutorService;
    private ScheduledFuture<?> _future;

    public ConsumerMetricsThread(List<ConsumerThread> consumerThreads) {
        _consumerThreads = consumerThreads;

        _scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() {
        LOGGER.info("Consumer Metrics (across all threads)");
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
    }

    private void printMetrics() {
        double recordsConsumedTotal = 0;
        double recordsConsumedRate = 0;
        double averageEndToEndLatency = 0;
        double averageLogAppendLatency = 0;
        double recordsPerRequestAverage = 0;
        double fetchLatencyAverage = 0;
        double fetchRate = 0;

        for (ConsumerThread consumerThread : _consumerThreads) {
            Map<MetricName, ? extends org.apache.kafka.common.Metric> metrics = consumerThread.getMetrics();
            recordsConsumedTotal += getRecordsConsumedTotal(metrics);
            recordsConsumedRate += getRecordsConsumedRate(metrics);
            averageEndToEndLatency += consumerThread.getAverageEndToEndLatency();
            averageLogAppendLatency += consumerThread.getAverageLogAppendLatency();
            recordsPerRequestAverage += getRecordsPerRequestAverage(metrics);
            fetchLatencyAverage += getFetchLatencyAverage(metrics);
            fetchRate += getFetchRate(metrics);
        }

        // LOGGER.info("Consumer metrics: " + (long)totalRecordsRead + " records read total, " + String.format("%.2f", totalRecordsReadRate) + " records read per second, " + String.format("%.2f", averageEndToEndLatency / _consumerThreads.size()) + " average end-to-end latency.");
        LOGGER.info("records-consumed-total:       " + (long)recordsConsumedTotal);
        LOGGER.info("records-consumed-rate:        " + String.format("%.2f", recordsConsumedRate));
        LOGGER.info("end-to-end-latency (pt):      " + String.format("%.2f", averageEndToEndLatency / _consumerThreads.size()));
        LOGGER.info("log-append-latency (pt):      " + String.format("%.2f", averageLogAppendLatency / _consumerThreads.size()));
        LOGGER.info("records-per-request-avg (pt): " + String.format("%.2f", recordsPerRequestAverage / _consumerThreads.size()));
        LOGGER.info("fetch-latency-avg (pt):       " + String.format("%.2f", fetchLatencyAverage / _consumerThreads.size()));
        LOGGER.info("fetch-rate (pt):              " + String.format("%.2f", fetchRate / _consumerThreads.size()));
        LOGGER.info("------------------------------------------");
    }

    private double getRecordsConsumedTotal(Map<MetricName, ? extends Metric> metrics) {
        for (MetricName metricName : metrics.keySet()) {
            if (metricName.name().equals("records-consumed-total") && !metricName.tags().containsKey("topic")) {
                return (double) metrics.get(metricName).metricValue();
            }
        }
        return 0;
    }

    private double getRecordsConsumedRate(Map<MetricName, ? extends Metric> metrics) {
        for (MetricName metricName : metrics.keySet()) {
            if (metricName.name().equals("records-consumed-rate") && !metricName.tags().containsKey("topic")) {
                return (double) metrics.get(metricName).metricValue();
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

    private double getFetchLatencyAverage(Map<MetricName, ? extends Metric> metrics) {
        for (MetricName metricName : metrics.keySet()) {
            if (metricName.name().equals("fetch-latency-avg") && !metricName.tags().containsKey("topic")) {
                return (double)metrics.get(metricName).metricValue();
            }
        }

        return 0;
    }

    private double getFetchRate(Map<MetricName, ? extends Metric> metrics) {
        for (MetricName metricName : metrics.keySet()) {
            if (metricName.name().equals("fetch-rate") && !metricName.tags().containsKey("topic")) {
                return (double)metrics.get(metricName).metricValue();
            }
        }

        return 0;
    }
}
