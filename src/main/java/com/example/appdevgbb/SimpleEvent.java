package com.example.appdevgbb;

import java.time.Instant;

public class SimpleEvent {

    private Instant _createDate;
    private String _message;

    public SimpleEvent() {
        super();
    }

    public SimpleEvent(String message) {
        _message = message;
        _createDate = Instant.now();
    }

    public String get_message() {
        return _message;
    }

    public void set_message(String message) {
        _message = message;
    }

    public Instant get_createDate() {
        return _createDate;
    }

    public void set_createDate(Instant createDate) {
        _createDate = createDate;
    }
}
