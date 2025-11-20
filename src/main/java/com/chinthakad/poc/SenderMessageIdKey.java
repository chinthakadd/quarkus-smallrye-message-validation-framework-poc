package com.chinthakad.poc;

import java.util.UUID;

/**
 * Composite key for (sender, message_id) combinations used in duplicate detection.
 */
public class SenderMessageIdKey {
    private final String sender;
    private final UUID messageId;

    public SenderMessageIdKey(String sender, UUID messageId) {
        this.sender = sender;
        this.messageId = messageId;
    }

    public String getSender() {
        return sender;
    }

    public UUID getMessageId() {
        return messageId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        SenderMessageIdKey that = (SenderMessageIdKey) o;
        return sender.equals(that.sender) &&
                messageId.equals(that.messageId);
    }

    @Override
    public int hashCode() {
        int result = sender.hashCode();
        result = 31 * result + messageId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SenderMessageIdKey{" +
                "sender='" + sender + '\'' +
                ", messageId=" + messageId +
                '}';
    }
}

