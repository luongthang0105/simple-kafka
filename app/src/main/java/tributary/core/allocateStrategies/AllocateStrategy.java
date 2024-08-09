package tributary.core.allocateStrategies;

import tributary.core.Message;
import tributary.core.Partition;
import tributary.core.Topic;

public interface AllocateStrategy {
    public void allocateMessagesToPartition(Topic topic, Message message, Partition partition);
}
