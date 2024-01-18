package com.example.appdevgbb;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class JsonDeserializer<T> implements Deserializer<T> {

    private ObjectMapper _mapper;
    private TypeReference<T> _typeReference;

    public JsonDeserializer(TypeReference<T> typeReference) {
        _mapper = new ObjectMapper();
        _mapper.registerModule(new JavaTimeModule());
        _typeReference = typeReference;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return _mapper.readValue(data, _typeReference);
        } catch (Exception e) {
            System.out.println(e);
            return null;
        }
    }
}
