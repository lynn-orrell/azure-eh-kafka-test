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
    private static final int PRINT_AFTER_BATCH_SIZE = 1000;

    private boolean _isRunning;

    private String _topicName;
    private int _messageSizeInBytes;
    private long _sleepTimeMs;
    private BlockingQueue<ProducerMetric> _producerMetrics;

    private long _totalRequestedSends;
    private AtomicInteger _numRecordsSent;
    private AtomicLong _totalRecordsSent;

    private Instant _start;
    private Properties _producerConfigProps;

    public ProducerThread(String topicName, int messageSizeInBytes, long sleepTimeMs, Properties producerConfigProps, BlockingQueue<ProducerMetric> producerMetrics) {
        _topicName = topicName;
        _messageSizeInBytes = messageSizeInBytes;
        _sleepTimeMs = sleepTimeMs;
        _producerConfigProps = producerConfigProps;
        _producerMetrics = producerMetrics;
        _numRecordsSent = new AtomicInteger(0);
        _totalRecordsSent = new AtomicLong(0);
    }

    @Override
    public void run() {
        _isRunning = true;

        final Producer<String, SimpleEvent> producer = createProducer();

        _totalRequestedSends = 0;
        _start = Instant.now();
        String message = generateRandomString(_messageSizeInBytes);
        try {
            while (_isRunning) {
                final ProducerRecord<String, SimpleEvent> record = new ProducerRecord<String, SimpleEvent>(_topicName, new SimpleEvent(message));
                producer.send(record, this);
                _totalRequestedSends++;

                if (_sleepTimeMs > 0) {
                    try {
                        Thread.sleep(_sleepTimeMs);
                    } catch (InterruptedException e) {
                        LOGGER.error("Producer thread [Thread: " + Thread.currentThread().threadId() + "] was interrupted while sleeping.", e);
                    }
                }
            }
        } finally {
            producer.close();
        }
    }

    public void stop() {
        _isRunning = false;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            System.out.println(exception);
        } else {
            final int numRecordsSent = _numRecordsSent.incrementAndGet();
            final long totalRecordsSent = _totalRecordsSent.incrementAndGet();
            if (numRecordsSent % PRINT_AFTER_BATCH_SIZE == 0 || (!_isRunning && totalRecordsSent == _totalRequestedSends)) {
                Instant end = Instant.now();
                Duration duration = Duration.between(_start, end);
                // System.out.println("Producer thread [Thread: " + Thread.currentThread().threadId() + "] total requested sends: " + _totalRequestedSends + ". Total records sent: " + totalRecordsSent + ". Records/sec: " + (numRecordsSent / (end.toEpochMilli() - _start.toEpochMilli() * 1.0) * 1000));
                ProducerMetric producerMetric = new ProducerMetric(Thread.currentThread().threadId(), _totalRequestedSends, numRecordsSent, duration);
                _producerMetrics.add(producerMetric);
                _start = Instant.now();
                _numRecordsSent.set(0);
            }
        }
    }

    private Producer<String, SimpleEvent> createProducer() {
        return new KafkaProducer<>(_producerConfigProps, new StringSerializer(), new JsonSerializer<SimpleEvent>());
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
