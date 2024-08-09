package tributary.cli;

import java.io.IOException;

import org.json.JSONObject;

import tributary.core.Message;

public class MessageBuilder {
    private String configName;
    private JSONObject message;

    public MessageBuilder(String configName) {
        this.configName = configName;
    }

    public MessageBuilder setConfigName(String configName) {
        this.configName = configName;
        return this;
    }

    private void loadConfig() {
        String configFile = String.format("/configs/%s.json", configName);
        try {
            message = new JSONObject(FileLoader.loadResourceFile(configFile));
        } catch (IOException e) {
            e.printStackTrace();
            message = null;
        }
    }
    public <T> Message<T> constructMessage() {
        loadConfig();
        String id = message.getString("id");
        String key = message.getString("key");
        if (message.has("value")) {
            Object jsonValue = message.get("value");
            if (jsonValue instanceof String) {
                String value = (String) jsonValue;
                return new Message<>(id, key, (T) value);
            } else if (jsonValue instanceof Integer) {
                Integer value = (Integer) Integer.valueOf((Integer) jsonValue);
                return new Message<>(id, key, (T) value);
            } else {
                // Handle other types if necessary, or throw an exception
                throw new IllegalArgumentException("Unsupported type for value: " + jsonValue.getClass());
            }
        }
        // Return null or throw an exception if "value" is missing
        throw new IllegalArgumentException("JSON object must contain a value field.");
    }

}
