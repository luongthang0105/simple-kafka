package tributary.cli;

import tributary.core.TributaryController;

public class Delete {
    public static void processDelete(String command, TributaryController controller) {
        String[] args = command.split(" ");
        String function = args[1];
        if (function.equals("consumer")) {
            deleteConsumer(args[2], controller);
        }
    }

    public static void deleteConsumer(String consumerId, TributaryController controller) {
        controller.deleteConsumer(consumerId);
    }
}
