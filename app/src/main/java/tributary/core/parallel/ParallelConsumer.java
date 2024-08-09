package tributary.core.parallel;

import tributary.core.TributaryController;

public class ParallelConsumer extends Thread {
    private TributaryController controller;
    private String consumerId;
    private String partitionId;

    public ParallelConsumer(TributaryController controller, String consumerId, String partitionId) {
        this.controller = controller;
        this.consumerId = consumerId;
        this.partitionId = partitionId;
    }

    @Override
    public void run() {
        controller.consumeMessage(consumerId, partitionId);
    }
}
