package tributary.cli;

import tributary.core.Message;
import tributary.core.TributaryController;
import tributary.core.input.ConsumeInput;
import tributary.core.input.ProduceInput;

public class Parallel {
    public static void processParallel(String command, TributaryController controller) {
        String[] args = command.split(" ");
        String function = args[1];
        if (function.equals("produce")) {
            parallelProduce(args[2], controller);
        } else if (function.equals("consume")) {
            parallelConsume(args[2], controller);
        }
    }

    public static void parallelProduce(String allProducersInfo, TributaryController controller) {
        String[] producersInfo = allProducersInfo.split("\\|");
        // System.out.println(producersInfo);
        ProduceInput[] inputs = new ProduceInput[producersInfo.length];
        int index = 0;
        for (String producerInfo : producersInfo) {
            String[] info = producerInfo.split(",");
            String producerId = info[0];
            String topicId = info[1];
            String eventJSONFilePath = info[2];
            String partitionId = info[3];

            MessageBuilder builder = new MessageBuilder(eventJSONFilePath);

            try {
                Message<?> message = builder.constructMessage();
                inputs[index] = new ProduceInput(producerId, topicId, partitionId, message);
                ++index;
            } catch (Exception e) {
                System.err.println("Failed to construct or produce message: " + e.getMessage());
                e.printStackTrace();
            }
        }
        controller.parallelProduce(inputs);
    }

    public static void parallelConsume(String allConsumersInfo, TributaryController controller) {
        String[] consumersInfo = allConsumersInfo.split("\\|");
        ConsumeInput[] inputs = new ConsumeInput[consumersInfo.length];
        int index = 0;
        for (String consumerInfo : consumersInfo) {
            String[] info = consumerInfo.split(",");
            String consumerId = info[0];
            String partitionId = info[1];

            inputs[index] = new ConsumeInput(consumerId, partitionId);
            index++;
        }
        controller.parallelConsume(inputs);
    }
}
