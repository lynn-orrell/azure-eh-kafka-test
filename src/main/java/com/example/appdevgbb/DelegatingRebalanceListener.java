package com.example.appdevgbb;

import java.util.Collection;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

public class DelegatingRebalanceListener  implements ConsumerRebalanceListener{

    private Function<Collection<TopicPartition>, Void> _onPartitionsAssigned;
    private Function<Collection<TopicPartition>, Void> _onPartitionsRevoked;

    public DelegatingRebalanceListener(Function<Collection<TopicPartition>, Void> onPartitionsAssigned, Function<Collection<TopicPartition>, Void> onPartitionsRevoked) {
        _onPartitionsAssigned = onPartitionsAssigned;
        _onPartitionsRevoked = onPartitionsRevoked;
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        _onPartitionsAssigned.apply(partitions);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        _onPartitionsRevoked.apply(partitions);
    }
}
