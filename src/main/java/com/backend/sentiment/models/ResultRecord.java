package com.backend.sentiment.models;

public class ResultRecord {
    private String stock;
    private double sentiment;
    private int count;

    public ResultRecord() {
    }

    public String getStock() {
        return stock;
    }

    public void setStock(String stock) {
        this.stock = stock;
    }

    public double getSentiment() {
        return sentiment;
    }

    public void setSentiment(double sentiment) {
        this.sentiment = sentiment;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public ResultRecord(String stock, double sentiment, int count) {
        this.stock = stock;
        this.sentiment = sentiment;
        this.count = count;
    }
}
