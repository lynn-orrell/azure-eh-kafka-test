package com.example.appdevgbb;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProducerMetricsThread implements Runnable {

    private final static Logger LOGGER = LogManager.getLogger(ProducerMetricsThread.class);

    private boolean _isRunning;
    private BlockingQueue<ProducerMetric> _producerMetrics;

    public ProducerMetricsThread(BlockingQueue<ProducerMetric> producerMetrics) {
        _producerMetrics = producerMetrics;
    }
    
    @Override
    public void run() {
        _isRunning = true;

        List<ProducerMetric> producerMetrics = new ArrayList<ProducerMetric>();
        Map<Long, List<ProducerMetric>> producerMetricsByThreadId = new HashMap<Long, List<ProducerMetric>>();
        long totalRecordsSent = 0;
        Duration totalDuration = Duration.ZERO;

        while(_isRunning || !_producerMetrics.isEmpty()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {}

            long totalRequestedSends = 0;

            _producerMetrics.drainTo(producerMetrics);

            if(!producerMetrics.isEmpty()) {
                for(ProducerMetric producerMetric : producerMetrics) {
                    if(!producerMetricsByThreadId.containsKey(producerMetric.getThreadId())) {
                        producerMetricsByThreadId.put(producerMetric.getThreadId(), new ArrayList<ProducerMetric>());
                    }
                    producerMetricsByThreadId.get(producerMetric.getThreadId()).add(producerMetric);
                }

                for(Long threadId : producerMetricsByThreadId.keySet()) {
                    List<ProducerMetric> producerMetricsForThread = producerMetricsByThreadId.get(threadId);
                    long threadTotalRequestedSends = 0;
                    for(ProducerMetric producerMetric : producerMetricsForThread) {
                        threadTotalRequestedSends = Math.max(producerMetric.getTotalRequestedSends(), threadTotalRequestedSends);
                        totalRecordsSent += producerMetric.getRecordsSent();
                        totalDuration = totalDuration.plus(producerMetric.getDuration());
                    }
                    totalRequestedSends += threadTotalRequestedSends;
                    producerMetricsForThread.clear();
                }
                

                LOGGER.info("Total requested sends: " + totalRequestedSends + ", total records sent: " + totalRecordsSent + ", total time: " + totalDuration.toMillis() + " ms, records per second per thread: " + String.format("%.2f", totalRecordsSent / (totalDuration.toMillis() / 1000.0)) + ", records per second total: " + String.format("%.2f", totalRecordsSent / (totalDuration.toMillis() / 1000.0) * producerMetricsByThreadId.keySet().size()));

                producerMetrics.clear();
            }
        }
    }
    
    public void stop() {
        _isRunning = false;
    }
}
