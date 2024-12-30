package group.ventis.utils;


import group.ventis.dto.Operation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String toJson(Operation operation) {
        try {
            return objectMapper.writeValueAsString(operation);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}