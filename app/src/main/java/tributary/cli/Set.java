package tributary.cli;

public class Set {
    public static void processSet(String command) {
        String[] args = command.split(" ");
        String function = args[1];
        if (function.equals("consumer_group_rebalancing")) {
            setConsumerGroupRebalancingStrategy(args[2], args[3]);
        }

    }

    public static void setConsumerGroupRebalancingStrategy(String consumerGroupId, String rebalancingStrat) {

    }
}
