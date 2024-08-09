package tributary.core.parallel;

import tributary.core.Message;
import tributary.core.TributaryController;

public class ParallelProducer extends Thread {
    private TributaryController controller;
    private String producerId;
    private String topicId;
    private Message<?> message;
    private String key;

    public ParallelProducer(TributaryController controller, String producerId, String topicId, Message<?> message,
            String key) {
        this.controller = controller;
        this.producerId = producerId;
        this.topicId = topicId;
        this.message = message;
        this.key = key;
    }

    @Override
    public void run() {
        controller.produceMessage(producerId, topicId, message, key);
    }
}
