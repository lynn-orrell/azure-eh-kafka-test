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
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
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
    private long _sumLatencyRecordsRead;
    private long _sumEndToEndLatency;

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

        SimpleEvent simpleEvent;
        long numRecordsReadForPoll = 0;

        LOGGER.info("Subscribing to topic: " + _topicName);
        _consumer.subscribe(Collections.singletonList(_topicName), this);

        try {
            while (_isRunning) {
                numRecordsReadForPoll = 0;
                final ConsumerRecords<String, SimpleEvent> records = _consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord<String, SimpleEvent> consumerRecord : records) {
                    simpleEvent = consumerRecord.value();
                    consumerRecord.partition();
                    if(simpleEvent != null) {
                        _sumEndToEndLatency += Instant.now().toEpochMilli() - simpleEvent.get_createDate().toEpochMilli();
                        _sumLatencyRecordsRead++;
                    }
                    if (_numRecordsToReadBeforeCommit > 0 && (_sumLatencyRecordsRead > 0 && _sumLatencyRecordsRead % _numRecordsToReadBeforeCommit == 0)) {
                        _consumer.commitAsync(this);
                    }
                    numRecordsReadForPoll++;
                }
                if (_numRecordsToReadBeforeCommit == 0 && numRecordsReadForPoll > 0) {
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
            Set<TopicPartition> ownedPartitions = _consumer.assignment();
            for(TopicPartition tp : ownedPartitions) {
                _consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(_consumer.position(tp))));
                LOGGER.info("Committed offset: " + _consumer.position(tp) + " for partition: " + tp.partition());
            }

            _consumer.close();
        }
    }

    public void stop() {
        _isRunning = false;
        _consumer.wakeup();
    }

    public Map<MetricName, ? extends Metric> getMetrics() {
        return _consumer.metrics();
    }

    public double getAverageEndToEndLatency() {
        return _sumEndToEndLatency == 0 ? 0d : _sumEndToEndLatency / (_sumLatencyRecordsRead * 1.0);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        OffsetAndMetadata offset;
        for(TopicPartition tp : partitions) {
            offset = _consumer.committed(new HashSet<>(Collections.singletonList(tp))).get(tp);
            LOGGER.info("Revoked Partition on Topic: " + tp.topic() + " - Partition: " + tp.partition() + " - Last Committed Offset: " + offset == null ? "UNKNOWN" : offset.offset());
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
    }

    private Consumer<String, SimpleEvent> createConsumer() {
        return new KafkaConsumer<>(_consumerConfigProps, new StringDeserializer(), new JsonDeserializer<SimpleEvent>(new TypeReference<SimpleEvent>() {}));
    }
}
