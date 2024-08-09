package tributary.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Consumer {
    private String id;
    private Topic<?> topic;
    private HashMap<String, Boolean> partitions;
    private List<Message<?>> consumedMessages;
    public Consumer(String id) {
        this.id = id;
        partitions = new HashMap<>();
        consumedMessages = new ArrayList<>();
    }

    public int getNumConsumedMessages() {
        return consumedMessages.size();
    }

    public String getId() {
        return id;
    }

    public void setTopic(Topic<?> topic) {
        this.topic = topic;
    }
    public void addConsumedMessage(Message<?> message) {
        consumedMessages.add(message);
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
        partition.consumeMessage(this);
    }

    public void consumeMessages(String partitionId, int numMessages) {
        Partition partition = extractPartition(partitionId);
        if (partition == null)
            return;
        partition.consumeMessages(numMessages, this);
    }

    public void addPartition(Partition partition) {
        partitions.put(partition.getId(), true);
    }

    public void clearPartitions() {
        partitions = new HashMap<>();
    }

    public void playback(Partition partition, int offset) {
        partition.playback(offset, this);
    }
}
