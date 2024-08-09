package tributary.cli;

import tributary.core.TributaryController;

public class Create {
    public static void processCreation(String command, TributaryController controller) {
        String[] args = command.split(" ");
        String function = args[1];

        if (function.equals("topic")) {
            createTopic(args[2], args[3], controller);
        } else if (function.equals("partition")) {
            createPartition(args[2], args[3], controller);
        } else if (function.equals("consumer")) {
            if (args[2].equals("group")) {
                createConsumerGroup(args[3], args[4], args[5], controller);
            } else {
                createConsumer(args[2], args[3], controller);
            }
        } else if (function.equals("producer")) {
            createProducer(args[2], args[3], args[4], controller);
        }
    }

    public static void createTopic(String topicId, String topicType, TributaryController controller) {
        if (topicType.equals("String")) {
            controller.createStringTopic(topicId);
        } else if (topicType.equals("Integer")) {
            controller.createIntegerTopic(topicId);
        }
        System.out.println("Topic (id: " + topicId + ", type: " + topicType + ") has been created.");
    }

    public static void createPartition(String topicId, String partitionId, TributaryController controller) {
        controller.createPartition(topicId, partitionId);
        System.out.println("Partition (id: " + partitionId + ") in topic (id: " + topicId + ") has been created.");
    }

    public static void createConsumerGroup(String consumerGroupId, String topicId, String rebalancingStrat,
            TributaryController controller) {
        controller.createConsumerGroup(consumerGroupId, topicId, rebalancingStrat);
        System.out.printf("Consumer Group (id: %s, rebalancingStrat: %s) in topic (id: %s) has been created.%n",
                consumerGroupId, rebalancingStrat, topicId);

    }

    public static void createConsumer(String consumerGroupId, String consumerId, TributaryController controller) {
        controller.createConsumer(consumerGroupId, consumerId);
        System.out.printf("Consumer (id: %s) in consumer group (id: %s) has been created.%n",
                consumerId, consumerGroupId);
    }

    public static void createProducer(String producerId, String eventType, String allocationStrat,
            TributaryController controller) {
        if (eventType.equals("String")) {
            controller.createStringProducer(producerId, allocationStrat);
        } else if (eventType.equals("Integer")) {
            controller.createIntegerProducer(producerId, allocationStrat);
        }
        System.out.printf("Producuer (id: %s, type: %s, allocateStrat: %s) has been created.%n",
                producerId, eventType, allocationStrat);
    }

}
