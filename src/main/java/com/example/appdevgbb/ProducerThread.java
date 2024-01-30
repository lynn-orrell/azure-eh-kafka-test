package com.example.appdevgbb;

import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProducerThread implements Callback {

    private final static Logger LOGGER = LogManager.getLogger(ProducerThread.class);

    private String _topicName;
    private int _messageSizeInBytes;
    private int _targetRecordsPerSecond;
    private Properties _producerConfigProps;
    private Producer<String, SimpleEvent> _producer;

    private ScheduledExecutorService _scheduledExecutorService;
    private ScheduledFuture<?> _future;

    public ProducerThread(String topicName, int messageSizeInBytes, int targetRecordsPerSecond,
            Properties producerConfigProps) {
        _topicName = topicName;
        _messageSizeInBytes = messageSizeInBytes;
        _targetRecordsPerSecond = targetRecordsPerSecond;
        _producerConfigProps = producerConfigProps;

        _scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        _producer = createProducer();
    }

    public void start() {
        long periodMicros = 1000000 / _targetRecordsPerSecond;

        String message = generateRandomString(_messageSizeInBytes);

        _future = _scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                final ProducerRecord<String, SimpleEvent> record = new ProducerRecord<String, SimpleEvent>(_topicName, new SimpleEvent(message));
                _producer.send(record, this);
            } catch (Exception e) {
                LOGGER.error("An error occurred while sending a record.", e);
            }
        }, 0, periodMicros, TimeUnit.MICROSECONDS);
    }

    public void stop() {
        _future.cancel(false);
        _scheduledExecutorService.shutdown();
        try {
            _scheduledExecutorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("An error occurred while waiting for the scheduled executor service to terminate.", e);
        }
        _producer.close();
    }

    public Map<MetricName, ? extends Metric> getMetrics() {
        return _producer.metrics();
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            LOGGER.error("An error occurred during publish.", exception);
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
