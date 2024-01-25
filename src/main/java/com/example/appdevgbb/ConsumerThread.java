package com.example.appdevgbb;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.core.type.TypeReference;

public class ConsumerThread implements Runnable, ConsumerRebalanceListener, OffsetCommitCallback {

    private final static Logger LOGGER = LogManager.getLogger(ConsumerThread.class);

    private String _topicName;
    private int _numRecordsToReadBeforeCommit;
    private boolean _shouldStartFromEnd;
    private Properties _consumerConfigProps;
    private final Consumer<String, SimpleEvent> _consumer;
    private boolean _isRunning;
    private Map<TopicPartition, Long> _partitionOffsets;
    private Instant _start;
    private long _recordsReadBeforeCommit;
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

        LOGGER.info("Subscribing to topic: " + _topicName);

        _consumer.subscribe(Collections.singletonList(_topicName), this);

        SimpleEvent simpleEvent;
        boolean isCommitNeeded = false;

        try {
            while (_isRunning) {

                final ConsumerRecords<String, SimpleEvent> records = _consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord<String, SimpleEvent> consumerRecord : records) {
                    simpleEvent = consumerRecord.value();
                    consumerRecord.partition();
                    if(simpleEvent != null) {
                        _totalEndToEndLatency += Instant.now().toEpochMilli() - simpleEvent.get_createDate().toEpochMilli();
                        isCommitNeeded = true;
                    }
                    if (_numRecordsToReadBeforeCommit > 0 && (_totalRecordCount > 0 && _totalRecordCount % _numRecordsToReadBeforeCommit == 0)) {
                        _consumer.commitAsync(this);
                        isCommitNeeded = false;
                    }
                    _totalRecordCount++;
                    _recordsReadBeforeCommit++;
                }
                if (_numRecordsToReadBeforeCommit == 0) {
                    _consumer.commitAsync(this);
                }
            }
        } catch (WakeupException e) {
            if(!_isRunning) {
                LOGGER.info("Consumer thread received shutdown signal.");
            } else {
                LOGGER.error("Consumer thread threw exception: " + e);
            }
        } finally {
            if(isCommitNeeded) {
                int numRecordsCommitted = 0;
                Set<TopicPartition> ownedPartitions = _consumer.assignment();
                for(TopicPartition tp : ownedPartitions) {
                    _consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(_consumer.position(tp))));
                    numRecordsCommitted += _consumer.position(tp) - _partitionOffsets.put(tp, _consumer.position(tp));
                    LOGGER.info("Committed offset: " + _consumer.position(tp) + " for partition: " + tp.partition());
                }
                _totalRecordsCommitted += numRecordsCommitted;

                printUpdate(numRecordsCommitted);
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
            LOGGER.info("Revoked Partition on Topic: " + tp.topic() + " - Partition: " + tp.partition() + " - Last Committed Offset: " + _consumer.committed(new HashSet<>(Collections.singletonList(tp))).get(tp).offset());
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for(TopicPartition tp : partitions) {
            LOGGER.info("Assigned Partition on Topic: " + tp.topic() + " - Partition: " + tp.partition());
            if(_shouldStartFromEnd) {
                LOGGER.info("Seeking to end of partition " + tp.partition() + ".");
                _consumer.seekToEnd(Collections.singletonList(tp));
            } else {
                LOGGER.info("Resuming last checkpoint of partition " + tp.partition() + ".");
            }
            long position = _consumer.position(tp);
            _partitionOffsets.put(tp, Long.valueOf(position));
            LOGGER.info("Current offset: " + position);
        }
    }

    @Override
    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if(exception != null) {
            LOGGER.error("An error occurred during commit.", exception);
        }
        else if (_totalRecordCount > 0) {
            int numRecordsCommitted = 0;
            for(Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                numRecordsCommitted += entry.getValue().offset() - _partitionOffsets.put(entry.getKey(), entry.getValue().offset());
            }
            _totalRecordsCommitted += numRecordsCommitted;
            printUpdate(numRecordsCommitted);
            _recordsReadBeforeCommit = 0;
            _start = Instant.now();
        }
    }

    private void printUpdate(int numRecordsCommitted) {
        double ellapsedMillis = Instant.now().toEpochMilli() - _start.toEpochMilli() * 1.0;
        LOGGER.info("Total Records Committed: " + _totalRecordsCommitted + ". Avg read end-to-end latency: " + _totalEndToEndLatency / _totalRecordCount + " ms. Read records/sec: " + String.format("%.2f", _recordsReadBeforeCommit / ellapsedMillis * 1000) + ". Committed records/sec: " + String.format("%.2f", numRecordsCommitted / ellapsedMillis * 1000) + ".");
    }

    private Consumer<String, SimpleEvent> createConsumer() {
        return new KafkaConsumer<>(_consumerConfigProps, new StringDeserializer(), new JsonDeserializer<SimpleEvent>(new TypeReference<SimpleEvent>() {}));
    }
}
