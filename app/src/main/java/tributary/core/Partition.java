package tributary.core;

import java.util.ArrayList;
import java.util.List;

public class Partition {
    private String id;
    private List<Message> messages;
    private int latestConsumedOffset = 0;

    public Partition(String id) {
        this.id = id;
        this.messages = new ArrayList<>();
    }

    public List<Message> getMessages() {
        return messages;
    }

    public synchronized void addMessage(Message message) {
        messages.add(message);
    }

    public String getId() {
        return id;
    }

    public int getNumConsumedMessages() {
        return latestConsumedOffset;
    }

    public int getPartitionSize() {
        return messages.size();
    }

    public synchronized <T> void consumeMessage(Consumer consumer) {
        Message<T> message = messages.get(latestConsumedOffset);
        consumer.addConsumedMessage(message);
        System.out.printf("- message (id: %s, value: %s)", message.getId(), message.getValue());
        latestConsumedOffset += 1;
    }

    public synchronized <T> void consumeMessages(int numMessages, Consumer consumer) {
        int numMessageConsumed = 0;
        while (numMessageConsumed < numMessages && latestConsumedOffset < messages.size()) {
            Message<T> message = messages.get(latestConsumedOffset);
            consumer.addConsumedMessage(message);
            System.out.printf("- message (id: %s, value: %s)");
            latestConsumedOffset += 1;
            numMessageConsumed += 1;
        }
    }

    public synchronized <T> void playback(int offset, Consumer consumer) {
        // playback from offset to the latest consumed message
        for (int i = offset; i < latestConsumedOffset; i++) {
            Message<T> message = messages.get(i);
            consumer.addConsumedMessage(message);
            System.out.printf("- message (id: %s, value: %s)");
        }
    }
}
