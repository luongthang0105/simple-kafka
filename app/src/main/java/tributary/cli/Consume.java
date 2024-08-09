package tributary.cli;

public class Consume {
    public static void processConsume(String command) {
        String[] args = command.split(" ");
        String function = args[1];
        if (function.equals("event")) {
            consumeEvent(args[2], args[3]);
        } else if (function.equals("events")) {
            consumeMultipleEvents(args[2], args[3], Integer.parseInt(args[4]));
        }
    }

    public static void consumeEvent(String consumerId, String partitionId) {

    }

    public static void consumeMultipleEvents(String consumerId, String partitionId, int numberOfEvents) {
        for (int i = 0; i < numberOfEvents; ++i) {
            consumeEvent(consumerId, partitionId);
        }
    }
}
