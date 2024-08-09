package tributary.core.input;

public class ConsumeInput {
    private String consumerId;
    private String partitionId;

    public ConsumeInput(String consumerId, String partitionId) {
        this.consumerId = consumerId;
        this.partitionId = partitionId;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public String getPartitionId() {
        return partitionId;
    }
}
