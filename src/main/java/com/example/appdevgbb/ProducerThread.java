package com.example.appdevgbb;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;


public class ProducerThread implements Runnable {

    private String _topicName;
    private int _messageSizeInBytes;
    private long _sleepTimeMs;
    private Properties _producerConfigProps;
    private static final int PRINT_AFTER_BATCH_SIZE = 1000;

    public ProducerThread(String topicName, int messageSizeInBytes, long sleepTimeMs, Properties producerConfigProps) {
        _topicName = topicName;
        _messageSizeInBytes = messageSizeInBytes;
        _sleepTimeMs = sleepTimeMs;
        _producerConfigProps = producerConfigProps;
    }

    @Override
    public void run() {
        final Producer<String, SimpleEvent> producer = createProducer();

        int numRecordsSent = 0;
        long totalRecordsSent = 0;
        Instant start = Instant.now();
        String message = generateRandomString(_messageSizeInBytes);
        while (true) {
            final ProducerRecord<String, SimpleEvent> record = new ProducerRecord<String, SimpleEvent>(_topicName, new SimpleEvent(message));
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println(exception);
                    }
                }
            });

            numRecordsSent++;
            totalRecordsSent++;
            if (numRecordsSent % PRINT_AFTER_BATCH_SIZE == 0) {
                Instant end = Instant.now();
                System.out.println("Sent " + PRINT_AFTER_BATCH_SIZE + " records. Record size: " + _messageSizeInBytes + " bytes. Total records sent: " + totalRecordsSent + ". Records/sec: " + (numRecordsSent / (end.toEpochMilli() - start.toEpochMilli() * 1.0) * 1000));
                start = Instant.now();
                numRecordsSent = 0;
            }

            if (_sleepTimeMs > 0) {
                try {
                    Thread.sleep(_sleepTimeMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
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
