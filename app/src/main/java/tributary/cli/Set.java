package tributary.cli;

import tributary.core.TributaryController;

public class Set {
    public static void processSet(String command, TributaryController controller) {
        String[] args = command.split(" ");
        String function = args[1];
        if (function.equals("consumer_group_rebalancing")) {
            setConsumerGroupRebalancingStrategy(args[2], args[3], controller);
        }

    }

    public static void setConsumerGroupRebalancingStrategy(String consumerGroupId, String rebalancingStrat,
            TributaryController controller) {
        controller.setConsumerGroupRebalancing(consumerGroupId, rebalancingStrat);
    }
}
