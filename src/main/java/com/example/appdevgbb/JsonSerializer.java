package com.example.appdevgbb;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class JsonSerializer<T> implements Serializer<T> {

    private ObjectMapper _mapper;

    public JsonSerializer() {
        _mapper = new ObjectMapper();
        _mapper.registerModule(new JavaTimeModule());
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            return _mapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
