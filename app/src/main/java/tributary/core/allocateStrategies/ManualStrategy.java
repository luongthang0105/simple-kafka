package tributary.core.allocateStrategies;

import tributary.core.Message;
import tributary.core.Partition;
import tributary.core.Topic;

public class ManualStrategy implements AllocateStrategy {

    @Override
    public void allocateMessagesToPartition(Topic topic, Message message, Partition partition) {
        partition.addMessage(message);
    }

}
