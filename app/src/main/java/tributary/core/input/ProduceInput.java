package tributary.core.input;

import tributary.core.Message;

public class ProduceInput {
    private String producerId;
    private String topicId;
    // Key maybe null (for Random allocation)
    private String key;
    private Message<?> message;

    public ProduceInput(String producerId, String topicId, String key, Message<?> message) {
        this.producerId = producerId;
        this.topicId = topicId;
        this.key = key;
        this.message = message;
    }

    public ProduceInput(String producerId, String topicId, Message<?> message) {
        this.producerId = producerId;
        this.topicId = topicId;
        this.message = message;
        this.key = message.getDesirePartition();
    }

    public String getProducerId() {
        return producerId;
    }

    public String getTopicId() {
        return topicId;
    }

    public String getKey() {
        return key;
    }

    public Message<?> getMessage() {
        return message;
    }

}
