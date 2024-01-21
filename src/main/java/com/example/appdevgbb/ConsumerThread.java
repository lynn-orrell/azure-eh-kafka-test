package com.example.appdevgbb;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

    private String _topicName;
    private int _numRecordsToReadBeforeCommit;
    private boolean _shouldStartFromEnd;
    private Properties _consumerConfigProps;
    private final Consumer<String, SimpleEvent> _consumer;
    private boolean _isRunning;
    private Map<TopicPartition, Long> _partitionOffsets;
    private Instant _start;
    private long _totalRecordCount;
    private long _totalEndToEndLatency;
    private long _totalRecordsCommitted;

    public ConsumerThread(String topicName, int numRecordsToReadBeforeCommit, boolean shouldStartFromEnd, Properties consumerConfigProps) {
        _topicName = topicName;
        _numRecordsToReadBeforeCommit = numRecordsToReadBeforeCommit;
        _shouldStartFromEnd = shouldStartFromEnd;
        _consumerConfigProps = consumerConfigProps;
        _partitionOffsets = new HashMap<>();
        _consumer = createConsumer();
    }

    @Override
    public void run() {
        _isRunning = true;
        _start = Instant.now();

        System.out.println("Subscribing to topic: " + _topicName);

        _consumer.subscribe(Collections.singletonList(_topicName), this);

        SimpleEvent simpleEvent;
        int numRecordsForCommit = 0;

        try {
            while (_isRunning) {
                numRecordsForCommit = 0;

                final ConsumerRecords<String, SimpleEvent> records = _consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, SimpleEvent> consumerRecord : records) {
                    simpleEvent = consumerRecord.value();
                    if(simpleEvent != null) {
                        _totalEndToEndLatency += Instant.now().toEpochMilli() - simpleEvent.get_createDate().toEpochMilli();
                        numRecordsForCommit++;
                    }
                    if (_numRecordsToReadBeforeCommit > 0 && numRecordsForCommit % _numRecordsToReadBeforeCommit == 0) {
                        _consumer.commitAsync(this);
                        numRecordsForCommit = 0;
                    }
                    _totalRecordCount++;
                    // if (_totalRecordCount > 0 && _totalRecordCount % PRINT_AFTER_BATCH_SIZE == 0) {
                    //     System.out.println("Total Record Count [Thread: " + Thread.currentThread().threadId() + "]: " + _totalRecordCount + ". Avg end-to-end latency: " + _totalEndToEndLatency / _totalRecordCount + " ms. Records/sec: " + (PRINT_AFTER_BATCH_SIZE / (Instant.now().toEpochMilli() - _start.toEpochMilli() * 1.0) * 1000) + ".");
                    //     _start = Instant.now();
                    // }
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
            if(numRecordsForCommit > 0) {
                _consumer.commitAsync(this);
                Map<TopicPartition, OffsetAndMetadata> offsets = _consumer.committed(new HashSet<>(_consumer.assignment()));
                for(Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                    System.out.println("Committed offset " + entry.getValue().offset() + " for partition " + entry.getKey().partition());
                }
            }
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
            System.out.println("Revoked Partition on Topic: " + tp.topic() + " - Partition: " + tp.partition() + " - Last Committed Offset: " + _consumer.committed(new HashSet<>(Collections.singletonList(tp))).get(tp).offset());
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for(TopicPartition tp : partitions) {
            System.out.println("Assigned Partition on Topic: " + tp.topic() + " - Partition: " + tp.partition());
            if(_shouldStartFromEnd) {
                System.out.print("Seeking to end of partition " + tp.partition() + ". ");
                _consumer.seekToEnd(Collections.singletonList(tp));
            } else {
                System.out.print("Resuming last checkpoint of partition " + tp.partition() + ". ");
            }
            long position = _consumer.position(tp);
            _partitionOffsets.put(tp, Long.valueOf(position));
            System.out.println("Current offset: " + position);
        }
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if(exception != null) {
            System.out.println(exception);
        }
        else {
            int numRecordsCommitted = 0;
            for(Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                numRecordsCommitted += entry.getValue().offset() - _partitionOffsets.put(entry.getKey(), entry.getValue().offset());
            }
            _totalRecordsCommitted += numRecordsCommitted;
            System.out.println("Total Records Committed [Thread: " + Thread.currentThread().threadId() + "]: " + _totalRecordsCommitted + ". Avg read end-to-end latency: " + _totalEndToEndLatency / _totalRecordCount + " ms. Committed records/sec: " + (numRecordsCommitted / (Instant.now().toEpochMilli() - _start.toEpochMilli() * 1.0) * 1000) + ".");
            _start = Instant.now();
        }
    }

    private Consumer<String, SimpleEvent> createConsumer() {
        return new KafkaConsumer<>(_consumerConfigProps, new StringDeserializer(), new JsonDeserializer<SimpleEvent>(new TypeReference<SimpleEvent>() {}));
    }
}
