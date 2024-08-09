package tributary.core;

import java.util.ArrayList;
import java.util.List;
import tributary.core.rebalancingStrategies.RebalancingStrategy;

public class ConsumerGroup {
    private String id;
    private Topic subscribedTopic;
    private RebalancingStrategy rebalancingStrategy;
    private List<Consumer> consumers;

    public ConsumerGroup(String id, Topic subscribedTopic, RebalancingStrategy rebalancingStrategy) {
        this.id = id;
        this.subscribedTopic = subscribedTopic;
        this.rebalancingStrategy = rebalancingStrategy;
        this.consumers = new ArrayList<>();
    }

    public void addConsumer(Consumer consumer) {
        consumers.add(consumer);
        rebalancingStrategy.allocatePartition(subscribedTopic.getPartitions(), consumers);
    }

    public void deleteConsumer(Consumer consumer) {
        consumers.remove(consumer);
        rebalancingStrategy.allocatePartition(subscribedTopic.getPartitions(), consumers);
    }

    public String show() {
        return "hihi";
    }

    public void setRebalancingStrategy(RebalancingStrategy rebalancingStrategy) {
        this.rebalancingStrategy = rebalancingStrategy;
    }
}
