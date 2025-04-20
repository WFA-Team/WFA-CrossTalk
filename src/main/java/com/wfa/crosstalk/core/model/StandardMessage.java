package com.wfa.crosstalk.core.model;

import java.io.Serializable;
import java.util.UUID;

public class StandardMessage implements Serializable {
	private static final long serialVersionUID = -3718746484830543464L;
	private final String messageId;
    private final long timestamp;
    private final String message;

    public StandardMessage() {
        this.messageId = UUID.randomUUID().toString(); // Auto-generate unique messageId
        this.timestamp = System.currentTimeMillis();
        this.message = "StandardMessage";
    }

    public StandardMessage(String message) {
        this.messageId = UUID.randomUUID().toString(); // Auto-generate unique messageId
        this.timestamp = System.currentTimeMillis();
        this.message = message;
    }

    public String getMessageId() {
        return messageId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getMessage() {
        return message;
    }
}
