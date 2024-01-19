package com.example.appdevgbb;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.core.type.TypeReference;

public class ConsumerThread implements Runnable, ConsumerRebalanceListener, OffsetCommitCallback {

    private static final int PRINT_AFTER_BATCH_SIZE = 1000;

    private String _topicName;
    private int _numRecordsToReadBeforeCommit;
    private Properties _consumerConfigProps;
    private final Consumer<String, SimpleEvent> _consumer;
    private boolean _isRunning;

    public ConsumerThread(String topicName, int numRecordsToReadBeforeCommit, Properties consumerConfigProps) {
        _topicName = topicName;
        _numRecordsToReadBeforeCommit = numRecordsToReadBeforeCommit;
        _consumerConfigProps = consumerConfigProps;
        _consumer = createConsumer();
    }

    @Override
    public void run() {
        _isRunning = true;

        System.out.println("Subscribing to topic: " + _topicName);

        _consumer.subscribe(Collections.singletonList(_topicName), this);

        try {
            SimpleEvent simpleEvent;
            long totalEndToEndLatency = 0;
            int numRecordsForCommit = 0;
            long totalRecordCount = 0;
            Instant start = Instant.now();

            while (_isRunning) {
                numRecordsForCommit = 0;

                final ConsumerRecords<String, SimpleEvent> records = _consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, SimpleEvent> consumerRecord : records) {
                    simpleEvent = consumerRecord.value();
                    if(simpleEvent != null) {
                        totalEndToEndLatency += Instant.now().toEpochMilli() - simpleEvent.get_createDate().toEpochMilli();
                        numRecordsForCommit++;
                    }
                    if (_numRecordsToReadBeforeCommit > 0 && numRecordsForCommit % _numRecordsToReadBeforeCommit == 0) {
                        _consumer.commitAsync(this);
                        numRecordsForCommit = 0;
                    }
                    totalRecordCount++;
                    if (totalRecordCount > 0 && totalRecordCount % PRINT_AFTER_BATCH_SIZE == 0) {
                        System.out.println("Total Record Count [Thread: " + Thread.currentThread().threadId() + "]: " + totalRecordCount + ". Avg end-to-end latency: " + totalEndToEndLatency / totalRecordCount + " ms. Records/sec: " + (PRINT_AFTER_BATCH_SIZE / (Instant.now().toEpochMilli() - start.toEpochMilli() * 1.0) * 1000) + ".");
                        start = Instant.now();
                    }
                }
                if(numRecordsForCommit > 0) {
                    _consumer.commitAsync(this);
                }
            }
        } catch (WakeupException e) {
            if(!_isRunning) {
                System.out.println("Consumer thread [Thread: " + Thread.currentThread().threadId() + "] received shutdown signal.");
            } else {
                System.out.println("Consumer thread [Thread: " + Thread.currentThread().threadId() + "] threw exception: " + e);
            }
        } finally {
            _consumer.commitAsync();
            _consumer.close();
        }
    }

    public void stop() {
        _isRunning = false;
        _consumer.wakeup();
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for(TopicPartition tp : partitions) {
            System.out.println("Revoked Partition on Topic: " + tp.topic() + " - Partition: " + tp.partition());
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for(TopicPartition tp : partitions) {
            System.out.println("Assigned Partition on Topic: " + tp.topic() + " - Partition: " + tp.partition());
        }
        _consumer.seekToEnd(partitions);
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if(exception != null) {
            System.out.println(exception);
        }
        else {
            for(Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                System.out.println("Committed offset " + entry.getValue().offset() + " for partition " + entry.getKey().partition());
            }
        }
    }

    private Consumer<String, SimpleEvent> createConsumer() {
        return new KafkaConsumer<>(_consumerConfigProps, new StringDeserializer(), new JsonDeserializer<SimpleEvent>(new TypeReference<SimpleEvent>() {}));
    }
}
