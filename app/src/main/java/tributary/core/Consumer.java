package tributary.core;

import java.util.HashMap;

public class Consumer {
    private String id;
    private Topic topic;
    private HashMap<String, Integer> partitions;
    public Consumer(String id) {
        this.id = id;
    }
    public void consumeMessage(Partition partition) {
        partition.consumeMessage();
    }
    public void consumeMessages(Partition partition, int numMessages) {
        partition.consumeMessages(numMessages);
    }
    public void parallelConsume(Partition partition) {
        // TODO
    }
    public void addPartition(Partition partition) {
        partitions.put(partition.getId(), 0);
    }
    public void clearPartitions() {
        partitions = new HashMap<>();
    }
    public void playback(Partition partition, int offset) {
        partition.playback(offset);
    }
}
