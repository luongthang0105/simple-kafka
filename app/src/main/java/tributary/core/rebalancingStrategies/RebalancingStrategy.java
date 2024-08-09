package tributary.core.rebalancingStrategies;

import java.util.List;

import tributary.core.Consumer;
import tributary.core.Partition;

public interface RebalancingStrategy {
    public void allocatePartition(List<Partition> partitions, List<Consumer> consumers);
}
