package group.ventis.deserializer;

import group.ventis.dto.Operation;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;


import java.io.IOException;

public class JSONValueDeserializationSchema implements DeserializationSchema<Operation> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Operation deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Operation.class);
    }

    @Override
    public boolean isEndOfStream(Operation nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Operation> getProducedType() {
        return TypeInformation.of(Operation.class);
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }
}
