package com.backend.sentiment.models;

public class TweetWithSentiment {
    private String text;
    private double sentiment;

    public TweetWithSentiment(String text, double sentiment) {
        this.text = text;
        this.sentiment = sentiment;
    }

    public void setText(String text) {
        this.text = text;
    }

    public void setSentiment(double sentiment) {
        this.sentiment = sentiment;
    }

    public String getText() {
        return text;
    }

    public double getSentiment() {
        return sentiment;
    }
}
