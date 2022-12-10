package com.backend.sentiment.Enums;

public enum eRedisPrefix {
    count("_count"),
    sentiment("_sentiment"),
    stocks("stocks");

    private final String keyPrefix;

    private eRedisPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }
}
