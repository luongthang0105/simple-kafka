package tributary.core;

import tributary.core.allocateStrategies.AllocateStrategy;

public class Producer<T> {
    private AllocateStrategy allocateStrategy;
    private String id;

    public Producer(String id, AllocateStrategy allocateStrategy) {
        this.id = id;
        this.allocateStrategy = allocateStrategy;
    }

    public void produceMessage(Topic<T> topic, Message<T> message, Partition partition) {
        allocateStrategy.allocateMessagesToPartition(topic, message, partition);
        System.out.printf("Message (id: %s) has been produced by Producer (id: %s) inside Partition (id: %s).%n",
                message.getId(), id, partition.getId());
    }
}
