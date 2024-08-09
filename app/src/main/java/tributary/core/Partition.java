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

    public synchronized void consumeMessage() {
        // TODO: implement consume mechanism
        latestConsumedOffset += 1;
    }

    public synchronized void consumeMessages(int numMessages) {
        int numMessageConsumed = 0;
        while (numMessageConsumed < numMessages && latestConsumedOffset < messages.size()) {
            // TODO: implement consume mechanism
            latestConsumedOffset += 1;
            numMessageConsumed += 1;
        }
    }

    public synchronized void playback(int offset) {
        // playback from offset to the latest consumed message
        for (int i = offset; i < latestConsumedOffset; i++) {
            // TODO: implement consume mechanism
        }
    }
}
