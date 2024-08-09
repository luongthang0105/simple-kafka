package tributary.cli;

public class Playback {
    public static void playbackEvents(String command) {
        String[] args = command.split(" ");
        String consumerId = args[1];
        String partitionId = args[2];
        String offset = args[3];

    }
}
