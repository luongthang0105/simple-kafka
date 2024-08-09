package tributary.core;

import java.util.ArrayList;
import java.util.List;

public class Topic<T> {
    private String id;
    private List<Partition> partitions;
    private List<ConsumerGroup> consumerGroups;

    public Topic(String id) {
        this.id = id;
        this.partitions = new ArrayList<>();
        this.consumerGroups = new ArrayList<>();
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public Partition getPartition(String partitionId) {
        return partitions.stream().filter(part -> part.getId().equals(partitionId)).findAny().orElse(null);
    }

    public void addPartition(Partition partition) {
        partitions.add(partition);
    }

    public void addConsumerGroup(ConsumerGroup consumerGroup) {
        consumerGroups.add(consumerGroup);
    }

    public String show() {
        // TODO
        return "hi";
    }
}
