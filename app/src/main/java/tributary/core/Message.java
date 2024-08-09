package tributary.core;

public class Message<T> {
    private String id;
    private String key;
    private T value;

    public Message(String id, String key, T value) {
        this.id = id;
        this.key = key;
        this.value = value;
    }

    public String getDesirePartition() {
        return key;
    }
}
