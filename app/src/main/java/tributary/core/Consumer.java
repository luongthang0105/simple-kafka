package tributary.core;

import java.util.HashMap;

public class Consumer {
    private String id;
    private Topic<?> topic;
    private HashMap<String, Boolean> partitions;

    public Consumer(String id) {
        this.id = id;
    }

    public void setTopic(Topic<?> topic) {
        this.topic = topic;
    }

    private Partition extractPartition(String partitionId) {
        Boolean isValidPartition = partitions.get(partitionId);
        if (isValidPartition == null)
            return null;
        return topic.getPartition(partitionId);
    }

    public void consumeMessage(String partitionId) {
        Partition partition = extractPartition(partitionId);
        if (partition == null)
            return;
        partition.consumeMessage();
    }

    public void consumeMessages(String partitionId, int numMessages) {
        Partition partition = extractPartition(partitionId);
        if (partition == null)
            return;
        partition.consumeMessages(numMessages);
    }

    public void parallelConsume(Partition partition) {
        // TODO
    }

    public void addPartition(Partition partition) {
        partitions.put(partition.getId(), true);
    }

    public void clearPartitions() {
        partitions = new HashMap<>();
    }

    public void playback(Partition partition, int offset) {
        partition.playback(offset);
    }
}
