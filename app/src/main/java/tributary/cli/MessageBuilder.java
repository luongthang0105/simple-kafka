package tributary.cli;

import java.io.File;
import java.io.IOException;

import org.json.JSONObject;

import tributary.core.Message;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MessageBuilder {
    private String configName;
    private JSONObject message;

    public MessageBuilder(String configName) {
        this.configName = configName;
    }

    public <T> Message<T> constructMessage() throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode message = objectMapper.readTree(new File("cli_messages/" + configName));
        String id = message.get("id").asText();
        String key = message.get("key").asText();
        if (message.has("value")) {
            JsonNode jsonValue = message.get("value");
            if (jsonValue.isTextual()) {
                String value = message.get("value").asText();
                return new Message<>(id, key, (T) value);
            } else if (jsonValue.isInt()) {
                Integer value = message.get("value").asInt();
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
