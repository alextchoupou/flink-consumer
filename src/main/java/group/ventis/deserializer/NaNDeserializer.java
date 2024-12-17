package group.ventis.deserializer;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JacksonException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;

public class NaNDeserializer extends JsonDeserializer<Double> {

    @Override
    public Double deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JacksonException {
        String value = jsonParser.getText();
        if ("NaN".equals(value)) {
           return Double.NaN;
        } else if ("Infinity".equals(value)) {
            return Double.POSITIVE_INFINITY;
        } else if ("-Infinity".equals(value)) {
            return Double.NEGATIVE_INFINITY;
        }
        return jsonParser.getDoubleValue();
    }
}
