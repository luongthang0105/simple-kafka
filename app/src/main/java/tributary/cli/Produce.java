package tributary.cli;

public class Produce {
    public static void processProduce(String command) {
        String[] args = command.split(" ");
        produceEvent(args[2], args[3], args[4]);
    }

    public static void produceEvent(String producerId, String topicId, String eventFilePath) {

    }
}
