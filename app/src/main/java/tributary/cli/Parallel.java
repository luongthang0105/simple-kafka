package tributary.cli;

public class Parallel {
    public static void processParallel(String command) {
        String[] args = command.split(" ");
        String function = args[1];
        if (function.equals("produce")) {
            parallelProduce(args[2]);
        } else if (function.equals("consume")) {
            parallelConsume(args[2]);
        }
    }

    public static void parallelProduce(String allProducersInfo) {
        String[] producersInfo = allProducersInfo.split("|");
        for (String producerInfo : producersInfo) {
            String[] info = producerInfo.split(",");
            String producerId = info[0];
            String topicId = info[1];
            String eventJSONFilePath = info[2];
            Produce.produceEvent(producerId, topicId, eventJSONFilePath);
        }
    }

    public static void parallelConsume(String allConsumersInfo) {
        String[] consumersInfo = allConsumersInfo.split("|");
        for (String consumerInfo : consumersInfo) {
            String[] info = consumerInfo.split(",");
            String consumerId = info[0];
            String partitionId = info[1];
            Consume.consumeEvent(consumerId, partitionId);
        }
    }
}
