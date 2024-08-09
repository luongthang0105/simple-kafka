package tributary.cli;

public class Create {
    public static void processCreation(String command) {
        String[] args = command.split(" ");
        String function = args[1];

        if (function.equals("topic")) {
            createTopic(args[2], args[3]);
        } else if (function.equals("partition")) {
            createPartition(args[2], args[3]);
        } else if (function.equals("consumer")) {
            if (args[2].equals("group")) {
                createConsumerGroup(args[3], args[4], args[5]);
            } else {
                createConsumer(args[2], args[3]);
            }
        } else if (function.equals("producer")) {
            createProducer(args[2], args[3], args[4]);
        }
    }

    public static void createTopic(String topicId, String topicType) {

    }

    public static void createPartition(String topicId, String partitionId) {

    }

    public static void createConsumerGroup(String consumerGroupId, String topicId, String rebalancingStrat) {

    }

    public static void createConsumer(String consumerGroupId, String consumerId) {

    }

    public static void createProducer(String producerId, String eventType, String allocationStrat) {

    }

}
