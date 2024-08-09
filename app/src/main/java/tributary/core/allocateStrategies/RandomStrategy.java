package tributary.core.allocateStrategies;

import java.util.List;
import java.util.Random;
import java.util.random.*;

import tributary.core.Message;
import tributary.core.Partition;
import tributary.core.Topic;

public class RandomStrategy implements AllocateStrategy {
    @Override
    public void allocateMessagesToPartition(Topic topic, Message message, Partition partition) {
        // get the list of partitions the producer currently has
        List<Partition> curPartitions = topic.getPartitions();
        Random ran = new Random();
        Partition pickedPartition = curPartitions.get(ran.nextInt(curPartitions.size() - 1));
        pickedPartition.addMessage(message);
    }
}
