package tributary.core.rebalancingStrategies;

import java.util.List;

import tributary.core.Consumer;
import tributary.core.Partition;

public class RangeStrategy implements RebalancingStrategy {
    private void allocateUnevenly(List<Partition> partitions, List<Consumer> consumers) {
        int partitionsPerConsumer = partitions.size() / consumers.size();
        int extraPartitions = partitions.size() % consumers.size();
        int curPartition = 0;
        for (int i = 0; i < consumers.size(); ++i) {
            int upperBound = curPartition + partitionsPerConsumer;
            for (; curPartition < upperBound; ++curPartition) {
                consumers.get(i).addPartition(partitions.get(curPartition));
            }
        }
        for (int i = 0; i < extraPartitions; ++i, ++curPartition) {
            consumers.get(i).addPartition(partitions.get(curPartition));
        }
        // while (curConsumer < consumers.size()) {
        //     if (remainingExtra > 0) {
        //         for (int i = 0; i < numPartitions + extraPartitions - 1; i++) {
        //             consumers.get(curConsumer).addPartition(partitions.get(curPartition));
        //             curPartition += 1;
        //         }
        //         remainingExtra -= 1;
        //     } else {
        //         for (int i = 0; i < numPartitions; i++) {
        //             consumers.get(curConsumer).addPartition(partitions.get(curPartition));
        //             curPartition += 1;
        //         }
        //     }
        //     curConsumer++;
        // }
    }

    private void allocateEvenly(List<Partition> partitions, List<Consumer> consumers) {
        int numPartitions = partitions.size() / consumers.size();
        int curConsumer = 0;
        while (curConsumer < consumers.size()) {
            for (int i = 0; i < numPartitions; i++) {
                consumers.get(curConsumer).addPartition(partitions.get(i));
            }
            curConsumer++;
        }
    }

    @Override
    public void allocatePartition(List<Partition> partitions, List<Consumer> consumers) {
        for (Consumer consumer : consumers) {
            consumer.clearPartitions();
        }
        if (partitions.size() % consumers.size() != 0) {
            allocateUnevenly(partitions, consumers);
        } else {
            allocateEvenly(partitions, consumers);
        }

    }

}
