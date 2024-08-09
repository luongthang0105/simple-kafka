package tributary.cli;

import tributary.core.TributaryController;

public class Consume {
    public static void processConsume(String command, TributaryController controller) {
        String[] args = command.split(" ");
        String function = args[1];
        if (function.equals("event")) {
            consumeEvent(args[2], args[3], controller);
        } else if (function.equals("events")) {
            consumeMultipleEvents(args[2], args[3], Integer.parseInt(args[4]), controller);
        }
    }

    public static void consumeEvent(String consumerId, String partitionId, TributaryController controller) {
        controller.consumeMessage(consumerId, partitionId);
    }

    public static void consumeMultipleEvents(String consumerId, String partitionId, int numberOfEvents,
            TributaryController controller) {
        controller.consumeMessages(consumerId, partitionId, numberOfEvents);
    }
}
