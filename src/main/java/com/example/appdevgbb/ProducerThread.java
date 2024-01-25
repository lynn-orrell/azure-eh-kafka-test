package com.example.appdevgbb;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProducerThread implements Runnable, Callback {

    private final static Logger LOGGER = LogManager.getLogger(ProducerThread.class);
    private static final int PRINT_AFTER_BATCH_SIZE = 77;

    private boolean _isRunning;

    private String _topicName;
    private int _messageSizeInBytes;
    private int _targetRecordsPerSecond;
    private BlockingQueue<ProducerMetric> _producerMetrics;

    private long _totalRequestedSends;
    private AtomicInteger _numRecordsSent;
    
    private Instant _producerMetricsStart;
    private Properties _producerConfigProps;

    public ProducerThread(String topicName, int messageSizeInBytes, int targetRecordsPerSecond, Properties producerConfigProps, BlockingQueue<ProducerMetric> producerMetrics) {
        _topicName = topicName;
        _messageSizeInBytes = messageSizeInBytes;
        _targetRecordsPerSecond = targetRecordsPerSecond;
        _producerConfigProps = producerConfigProps;
        _producerMetrics = producerMetrics;
        _numRecordsSent = new AtomicInteger(0);
    }

    @Override
    public void run() {
        _isRunning = true;
        Instant recordSetStartTime;

        final Producer<String, SimpleEvent> producer = createProducer();

        _totalRequestedSends = 0;
        _producerMetricsStart = Instant.now();
        String message = generateRandomString(_messageSizeInBytes);
        try {
            while (_isRunning) {
                recordSetStartTime = Instant.now();
                for(int x = 0; x < (_targetRecordsPerSecond == Integer.MAX_VALUE ? 1 : _targetRecordsPerSecond); x++) {
                    final ProducerRecord<String, SimpleEvent> record = new ProducerRecord<String, SimpleEvent>(_topicName, new SimpleEvent(message));
                    producer.send(record, this);
                    _totalRequestedSends++;
                }

                if(_targetRecordsPerSecond != Integer.MAX_VALUE) {
                    Instant recordSetEndTime = Instant.now();
                    long sleepTime = 1000 - Duration.between(recordSetStartTime, recordSetEndTime).toMillis();
                    if(sleepTime > 0) {
                        try {
                            Thread.sleep(sleepTime);
                        } catch (InterruptedException e) {
                            LOGGER.error("Producer thread was interrupted while sleeping.", e);
                        }
                    }
                }
            }
        } finally {
            producer.close();
            if(_numRecordsSent.get() > 0) {
                LOGGER.info(_numRecordsSent.get());
                sendMetric();
            }
        }
    }

    public void stop() {
        _isRunning = false;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            LOGGER.error("An error occurred during publish.", exception);
        } else {
            final int numRecordsSent = _numRecordsSent.incrementAndGet();
            if (numRecordsSent % _targetRecordsPerSecond == 0 || !_isRunning) {
                sendMetric();
                _producerMetricsStart = Instant.now();
                _numRecordsSent.set(0);
            }
        }
    }

    private Producer<String, SimpleEvent> createProducer() {
        return new KafkaProducer<>(_producerConfigProps, new StringSerializer(), new JsonSerializer<SimpleEvent>());
    }

    private void sendMetric() {
        Instant end = Instant.now();
        Duration duration = Duration.between(_producerMetricsStart, end);
        ProducerMetric producerMetric = new ProducerMetric(Thread.currentThread().threadId(), _totalRequestedSends, _numRecordsSent.get(), duration);
        _producerMetrics.add(producerMetric);
    }

    private String generateRandomString(int length) {
        String source = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuilder result = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            int index = random.nextInt(source.length());
            result.append(source.charAt(index));
        }

        return result.toString();
    }
}
