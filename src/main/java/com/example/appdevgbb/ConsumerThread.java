package com.example.appdevgbb;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.core.type.TypeReference;

public class ConsumerThread implements Runnable, ConsumerRebalanceListener {

    private static final int PRINT_AFTER_BATCH_SIZE = 1000;

    private String _topicName;
    private int _numRecordsToReadBeforeCommit;
    private Properties _consumerConfigProps;
    private final Consumer<String, SimpleEvent> _consumer;

    public ConsumerThread(String topicName, int numRecordsToReadBeforeCommit, Properties consumerConfigProps) {
        _topicName = topicName;
        _numRecordsToReadBeforeCommit = numRecordsToReadBeforeCommit;
        _consumerConfigProps = consumerConfigProps;
        _consumer = createConsumer();
    }

    @Override
    public void run() {
        System.out.println("Subscribing to topic: " + _topicName);

        _consumer.subscribe(Collections.singletonList(_topicName), this);

        try {
            SimpleEvent simpleEvent;
            long totalEndToEndLatency = 0;
            int numRecordsForCommit = 0;
            long totalRecordCount = 0;

            while (true) {
                numRecordsForCommit = 0;

                final ConsumerRecords<String, SimpleEvent> records = _consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, SimpleEvent> consumerRecord : records) {
                    simpleEvent = consumerRecord.value();
                    if(simpleEvent != null) {
                        totalEndToEndLatency += Instant.now().toEpochMilli() - simpleEvent.get_createDate().toEpochMilli();
                        numRecordsForCommit++;
                    }
                    if (_numRecordsToReadBeforeCommit > 0 && numRecordsForCommit % _numRecordsToReadBeforeCommit == 0) {
                        _consumer.commitAsync();
                        numRecordsForCommit = 0;
                    }
                    totalRecordCount++;
                    if (totalRecordCount > 0 && totalRecordCount % PRINT_AFTER_BATCH_SIZE == 0) {
                        System.out.println("Total Record Count [Thread: " + Thread.currentThread().threadId() + "]: " + totalRecordCount + ". Avg end-to-end latency: " + totalEndToEndLatency / totalRecordCount + " ms.");
                    }
                }
                _consumer.commitAsync();
            }
        } catch (Exception e) {
            System.out.println(e);
        } finally {
            _consumer.close();
        }
    }

    private Consumer<String, SimpleEvent> createConsumer() {
        return new KafkaConsumer<>(_consumerConfigProps, new StringDeserializer(), new JsonDeserializer<SimpleEvent>(new TypeReference<SimpleEvent>() {}));
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
}
