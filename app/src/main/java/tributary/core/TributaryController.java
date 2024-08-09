package tributary.core;

import java.util.HashMap;
import tributary.core.allocateStrategies.AllocateStrategy;
import tributary.core.allocateStrategies.ManualStrategy;
import tributary.core.allocateStrategies.RandomStrategy;
import tributary.core.rebalancingStrategies.RangeStrategy;
import tributary.core.rebalancingStrategies.RebalancingStrategy;
import tributary.core.rebalancingStrategies.RoundRobinStrategy;

public class TributaryController {
    private HashMap<String, Topic<?>> topics;
    private HashMap<String, Producer<?>> producers;
    private HashMap<String, ConsumerGroup> consumerGroups;
    private HashMap<String, Consumer> consumers;
    private HashMap<String, Partition> partitions;

    public TributaryController() {
        topics = new HashMap<>();
        producers = new HashMap<>();
        consumerGroups = new HashMap<>();
        consumers = new HashMap<>();
        partitions = new HashMap<>();
    }

    public void createIntegerTopic(String id) {
        topics.put(id, new Topic<Integer>(id));
    }

    public void createStringTopic(String id) {
        topics.put(id, new Topic<String>(id));
    }

    public void createPartition(String topicId, String partitionId) {
        Partition newPartition = new Partition(partitionId);
        partitions.put(partitionId, newPartition);
        Topic<?> topic = topics.get(topicId);
        topic.addPartition(newPartition);
    }

    public RebalancingStrategy findRebalancingStrategy(String strategy) {
        if (strategy.equals("RoundRobin")) {
            return new RoundRobinStrategy();
        }
        return new RangeStrategy();
    }

    public AllocateStrategy findAllocateStrategy(String strategy) {
        if (strategy.equals("Manual")) {
            return new ManualStrategy();
        }
        return new RandomStrategy();
    }

    public void createConsumerGroup(String consumerGroupId, String topicId, String rebalancingStrategy) {
        Topic<?> topic = topics.get(topicId);
        ConsumerGroup consumerGroup = new ConsumerGroup(topicId, topic, findRebalancingStrategy(rebalancingStrategy));
        consumerGroups.put(consumerGroupId, consumerGroup);
    }

    public void createConsumer(String consumerGroupId, String consumerId) {
        Consumer newConsumer = new Consumer(consumerId);
        ConsumerGroup consumerGroup = consumerGroups.get(consumerGroupId);
        consumers.put(consumerId, newConsumer);
        consumerGroup.addConsumer(newConsumer);
    }

    public void deleteConsumer(String consumerGroupId, String consumerId) {
        ConsumerGroup consumerGroup = consumerGroups.get(consumerGroupId);
        Consumer consumer = consumers.get(consumerId);
        consumerGroup.deleteConsumer(consumer);
    }

    public void createIntegerProducer(String id, String allocateStrategy) {
        producers.put(id, new Producer<Integer>(id, findAllocateStrategy(allocateStrategy)));
    }

    public void createStringProducer(String id, String allocateStrategy) {
        producers.put(id, new Producer<String>(id, findAllocateStrategy(allocateStrategy)));
    }

    public <T> void produceMessage(String producerId, String topicId, Message<T> message, String partitionId) {
        Producer<T> producer = (Producer<T>) producers.get(producerId);
        Topic<T> topic = (Topic<T>) topics.get(topicId);
        Partition partition = partitions.get(partitionId);
        producer.produceMessage(topic, message, partition);
    }

    public <T> void produceMessage(String producerId, String topicId, Message<T> message) {
        Producer<T> producer = (Producer<T>) producers.get(producerId);
        Topic<T> topic = (Topic<T>) topics.get(topicId);
        Partition partition = partitions.get(message.getDesirePartition());
        producer.produceMessage(topic, message, partition);
    }

    public void consumeMessage(String consumerId, String partitionId) {
        Consumer consumer = consumers.get(consumerId);
        consumer.consumeMessage(partitionId);
    }

    public void consumeMessages(String consumerId, String partitionId, int numMessages) {
        Consumer consumer = consumers.get(consumerId);
        consumer.consumeMessages(partitionId, numMessages);
    }

    public String showTopic(String topicId) {
        Topic topic = topics.get(topicId);
        return topic.show();
    }

    public String showConsumerGroup(String consumerGroupId) {
        ConsumerGroup consumerGroup = consumerGroups.get(consumerGroupId);
        return consumerGroup.show();
    }
    // public void parallelProduce(String producer, String topic, List<Message> messages) {

    //     producer.parallelProduce(topic, messages);
    // }
    // public void parallelConsume(String consumer, String partition) {
    //     consumer.parallelConsume(partition);
    // }
    public void setConsumerGroupRebalancing(String consumerGroupId, String rebalancingStrategy) {
        ConsumerGroup consumerGroup = consumerGroups.get(consumerGroupId);
        consumerGroup.setRebalancingStrategy(findRebalancingStrategy(rebalancingStrategy));
    }

    // Helpers for Testing
    public int getNumConsumedMessages(String partitionId) {
        Partition p = partitions.get(partitionId);
        return p.getNumConsumedMessages();
    }

    public int getPartitionSize(String partitionId) {
        Partition p = partitions.get(partitionId);
        return p.getPartitionSize();
    }
}
