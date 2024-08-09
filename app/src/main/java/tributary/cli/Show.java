package tributary.cli;

public class Show {
    public static void processShow(String command) {
        String[] args = command.split(" ");
        String target = args[1];
        if (target.equals("consumer_group")) {
            showConsumerGroup(args[2]);
        } else if (target.equals("topic")) {
            showTopic(args[2]);
        }
    }

    public static void showConsumerGroup(String consumerGroupId) {
        return;
    }

    public static void showTopic(String topicId) {
        return;
    }
}
