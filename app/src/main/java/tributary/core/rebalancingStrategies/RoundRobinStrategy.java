package tributary.core.rebalancingStrategies;

import java.util.List;

import tributary.core.Consumer;
import tributary.core.Partition;

public class RoundRobinStrategy implements RebalancingStrategy {
    @Override
    public void allocatePartition(List<Partition> partitions, List<Consumer> consumers) {
        for (Consumer consumer : consumers) {
            consumer.clearPartitions();
        }
        for (int i = 0; i < partitions.size(); i++) {
            int consumerIndex = i % consumers.size();
            consumers.get(consumerIndex).addPartition(partitions.get(i));
        }
    }

}
