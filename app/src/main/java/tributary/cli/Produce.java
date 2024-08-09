package tributary.cli;

import tributary.core.Message;
import tributary.core.TributaryController;

public class Produce {
    public static void processProduce(String command, TributaryController controller) {
        String[] args = command.split(" ");
        produceEvent(args[2], args[3], args[4], args[5], controller);
    }

    public static <T> void produceEvent(String producerId, String topicId, String eventFilePath, String partitionId,
            TributaryController controller) {
        MessageBuilder builder = new MessageBuilder(eventFilePath);
        try {
            Message<T> message = builder.constructMessage();
            controller.produceMessage(producerId, topicId, message, partitionId);
        } catch (Exception e) {
            System.err.println("Failed to construct or produce message: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
