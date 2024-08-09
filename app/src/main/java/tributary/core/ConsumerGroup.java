package tributary.core;

import java.util.ArrayList;
import java.util.List;
import tributary.core.rebalancingStrategies.RebalancingStrategy;

public class ConsumerGroup {
    private String id;
    private Topic subscribedTopic;
    private RebalancingStrategy rebalancingStrategy;
    private List<Consumer> consumers;

    public List<Consumer> getConsumers() {
        return consumers;
    }

    public String getId() {
        return id;
    }

    public ConsumerGroup(String id, Topic subscribedTopic, RebalancingStrategy rebalancingStrategy) {
        this.id = id;
        this.subscribedTopic = subscribedTopic;
        this.rebalancingStrategy = rebalancingStrategy;
        this.consumers = new ArrayList<>();
    }

    public void addConsumer(Consumer consumer) {
        consumers.add(consumer);
        consumer.setTopic(subscribedTopic);
        rebalancingStrategy.allocatePartition(subscribedTopic.getPartitions(), consumers);
    }

    public void deleteConsumer(Consumer consumer) {
        String id = consumer.getId();
        consumers.remove(consumer);
        if (consumers.size() == 0) {
            System.out.println("Deleting consumer (id: " + id + ")...");
            return;
        }
        System.out.println("Deleting consumer (id: " + id + ")...");
        rebalancingStrategy.allocatePartition(subscribedTopic.getPartitions(), consumers);
    }

    public void setRebalancingStrategy(RebalancingStrategy rebalancingStrategy) {
        this.rebalancingStrategy = rebalancingStrategy;
        System.out.println("New rebalancing strategy has been set!\n");
    }

    public String show() {
        StringBuilder sb = new StringBuilder();
        sb.append("Consumer Group ID: ").append(id).append("\n");

        sb.append("Consumers:\n");
        for (Consumer consumer : consumers) {
            sb.append(consumer.toString()).append("\n");
        }

        return sb.toString();
    }
}
