package com.example.kafkademo.consumer;

import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;

/**
 * Both listeners are ready when every consumer thread owns at least one partition. With as many
 * partitions as threads (3 and 3) that means every partition is assigned.
 */
@Component
public class ListenerReadiness {

    private final KafkaListenerEndpointRegistry registry;

    public ListenerReadiness(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    public boolean ready() {
        for (String id : List.of(MessageListener.UNCOMMITTED_ID, MessageListener.COMMITTED_ID)) {
            MessageListenerContainer container = registry.getListenerContainer(id);
            if (!(container instanceof ConcurrentMessageListenerContainer<?, ?> concurrent) || !concurrent.isRunning()) {
                return false;
            }
            var children = concurrent.getContainers();
            if (children.isEmpty()) return false;
            for (var child : children) {
                Collection<TopicPartition> assigned = child.getAssignedPartitions();
                if (assigned == null || assigned.isEmpty()) return false;
            }
        }
        return true;
    }
}
