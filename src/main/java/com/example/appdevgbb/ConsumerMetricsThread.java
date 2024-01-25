package com.example.appdevgbb;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConsumerMetricsThread implements Runnable {
    
    private final static Logger LOGGER = LogManager.getLogger(ConsumerMetricsThread.class);

    private boolean _isRunning;
    private BlockingQueue<ConsumerMetric> _consumerMetrics;

    public ConsumerMetricsThread(BlockingQueue<ConsumerMetric> consumerMetrics) {
        _consumerMetrics = consumerMetrics;
    }

    @Override
    public void run() {
        _isRunning = true;

        List<ConsumerMetric> consumerMetrics = new ArrayList<ConsumerMetric>();
        Map<Long, List<ConsumerMetric>> consumerMetricsByThreadId = new HashMap<Long, List<ConsumerMetric>>();
        Map<Long, ConsumerMetric> maxConsumerMetricsByThreadId = new HashMap<Long, ConsumerMetric>();

        long totalRecordsReadBeforeCommit = 0;
        Duration totalDuration = Duration.ZERO;

        while(_isRunning || !_consumerMetrics.isEmpty()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {}

            _consumerMetrics.drainTo(consumerMetrics);

            if(!consumerMetrics.isEmpty()) {
                
                long totalRecordsRead = 0;
                long totalRecordsCommitted = 0;
                long totalEndToEndLatency = 0;

                for(ConsumerMetric consumerMetric : consumerMetrics) {
                    if(!consumerMetricsByThreadId.containsKey(consumerMetric.getThreadId())) {
                        consumerMetricsByThreadId.put(consumerMetric.getThreadId(), new ArrayList<ConsumerMetric>());
                    }
                    consumerMetricsByThreadId.get(consumerMetric.getThreadId()).add(consumerMetric);
                }

                for(Long threadId : consumerMetricsByThreadId.keySet()) {
                    List<ConsumerMetric> consumerMetricsForThread = consumerMetricsByThreadId.get(threadId);
                    long threadTotalRecordsRead = 0;
                    long threadTotalRecordsCommitted = 0;
                    long threadTotalEndToEndLatency = 0;

                    for(ConsumerMetric consumerMetric : consumerMetricsForThread) {
                        totalRecordsReadBeforeCommit += consumerMetric.getRecordsReadBeforeCommit();
                        threadTotalRecordsRead = Math.max(consumerMetric.getTotalRecordsRead(), threadTotalRecordsRead);
                        threadTotalRecordsCommitted = Math.max(consumerMetric.getTotalRecordsCommitted(), threadTotalRecordsCommitted);
                        threadTotalEndToEndLatency = Math.max(consumerMetric.getTotalEndToEndLatency(), threadTotalEndToEndLatency);
                        totalDuration = totalDuration.plus(consumerMetric.getDuration());
                    }
                    
                    totalRecordsRead += threadTotalRecordsRead;
                    totalRecordsCommitted += threadTotalRecordsCommitted;
                    totalEndToEndLatency += threadTotalEndToEndLatency;

                    if(!maxConsumerMetricsByThreadId.containsKey(threadId)) {
                        maxConsumerMetricsByThreadId.put(threadId, new ConsumerMetric(threadId, totalRecordsRead, totalRecordsCommitted, 0, 0, totalEndToEndLatency, Duration.ZERO));
                    } else {
                        ConsumerMetric maxConsumerMetric = maxConsumerMetricsByThreadId.get(threadId);
                        maxConsumerMetric.setTotalRecordsRead(Math.max(maxConsumerMetric.getTotalRecordsRead(), threadTotalRecordsRead));
                        maxConsumerMetric.setTotalRecordsCommitted(Math.max(maxConsumerMetric.getTotalRecordsCommitted(), threadTotalRecordsCommitted));
                        maxConsumerMetric.setTotalEndToEndLatency(Math.max(maxConsumerMetric.getTotalEndToEndLatency(), threadTotalEndToEndLatency));
                    }
                    
                    consumerMetricsForThread.clear();
                }

                // LOGGER.info("Total Records Read: " + totalRecordsRead);
                // LOGGER.info("Total Records Committed: " + totalRecordsCommitted);
                // LOGGER.info("Total End To End Latency: " + totalEndToEndLatency);

                LOGGER.info("Total Records Committed: " + getSumOfTotalCommits(maxConsumerMetricsByThreadId) + ". Avg read end-to-end latency: " + getSumOfTotalEndToEndLatency(maxConsumerMetricsByThreadId) / getSumOfTotalReads(maxConsumerMetricsByThreadId) + " ms. Read records/thread/sec: " + String.format("%.2f", totalRecordsReadBeforeCommit / (totalDuration.toSeconds() * 1.0)) + ". Committed records/thread/sec: " + String.format("%.2f", getSumOfTotalCommits(maxConsumerMetricsByThreadId) / (totalDuration.toSeconds() * 1.0)) + ". Read records/sec total: " + String.format("%.2f", totalRecordsReadBeforeCommit / (totalDuration.toSeconds() * 1.0) * maxConsumerMetricsByThreadId.keySet().size()) + ". Committed records/thread/sec: " + String.format("%.2f", getSumOfTotalCommits(maxConsumerMetricsByThreadId) / (totalDuration.toSeconds() * 1.0) * maxConsumerMetricsByThreadId.keySet().size()) + ".");
                
                consumerMetrics.clear();
                consumerMetricsByThreadId.clear();
            }
        }
    }

    private long getSumOfTotalCommits(Map<Long, ConsumerMetric> maxConsumerMetricsByThreadId) {
        long sum = 0;
        for(Long threadId : maxConsumerMetricsByThreadId.keySet()) {
            sum += maxConsumerMetricsByThreadId.get(threadId).getTotalRecordsCommitted();
        }
        return sum;
    }

    private long getSumOfTotalReads(Map<Long, ConsumerMetric> maxConsumerMetricsByThreadId) {
        long sum = 0;
        for(Long threadId : maxConsumerMetricsByThreadId.keySet()) {
            sum += maxConsumerMetricsByThreadId.get(threadId).getTotalRecordsRead();
        }
        return sum;
    }

    private long getSumOfTotalEndToEndLatency(Map<Long, ConsumerMetric> maxConsumerMetricsByThreadId) {
        long sum = 0;
        for(Long threadId : maxConsumerMetricsByThreadId.keySet()) {
            sum += maxConsumerMetricsByThreadId.get(threadId).getTotalEndToEndLatency();
        }
        return sum;
    }

    public void stop() {
        _isRunning = false;
    }
}
