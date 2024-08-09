package tributary.cli;

import tributary.core.TributaryController;

public class Show {
    public static void processShow(String command, TributaryController controller) {
        String[] args = command.split(" ");
        String target = args[1];
        if (target.equals("consumer_group")) {
            showConsumerGroup(args[2], controller);
        } else if (target.equals("topic")) {
            showTopic(args[2], controller);
        }
    }

    public static void showConsumerGroup(String consumerGroupId, TributaryController controller) {
        controller.showConsumerGroup(consumerGroupId);
    }

    public static void showTopic(String topicId, TributaryController controller) {
        System.out.println(controller.showTopic(topicId));
    }
}
