package tributary.cli;

public class Delete {
    public static void processDelete(String command) {
        String[] args = command.split(" ");
        String function = args[1];
        if (function.equals("consumer")) {
            deleteConsumer(args[2]);
        }
    }

    public static void deleteConsumer(String consumerId) {
        return;
    }
}
