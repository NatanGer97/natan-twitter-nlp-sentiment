package com.backend.sentiment.twitter;

import com.backend.sentiment.Enums.*;
import com.backend.sentiment.services.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.Service;
import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import javax.annotation.PostConstruct;
import java.text.*;

@Service
public class AppTwitterStream implements StatusListener {

    Logger logger = org.slf4j.LoggerFactory.getLogger(AppTwitterStream.class);
    EmitterProcessor<String> emitterProcessor;
    public TwitterStream twitterStream;
    @Autowired
    private RedisService redisService;

    @Autowired
    private ObjectMapper objectMapper;

    private final DecimalFormat decimalFormat = new DecimalFormat("0.00");

    @PostConstruct
    public void init() {
        ConfigurationBuilder configBuilder = new ConfigurationBuilder();
        configBuilder.setDebugEnabled(true);
        configBuilder.setOAuthConsumerKey("0ybPhDyGyIMkXVNsTNJh3PDve");
        configBuilder.setOAuthConsumerSecret("u73QTeH7uUAyeI0D6jew3dQ5k7N0pweaAf4fLYDAXGD3uRnGhE");
        configBuilder.setOAuthAccessToken("902505417510715393-S3i0p6oQS3rtRL7GV05rims7kgLzSYY");
        configBuilder.setOAuthAccessTokenSecret("8SUqDCahQXLcgapFMgqNv5bZR0gWz3ng8T8xg7pOPYoh9");

        twitterStream = new TwitterStreamFactory(configBuilder.build()).getInstance();
    }

    /**
     * This method is starts the twitter stream and returns a flux of tweets that match
     * the given keyword filter.
     */

    public Flux<String> filter(String keyword) {

        FilterQuery filterQuery = new FilterQuery();
        String[] keys = {keyword};
        filterQuery.track(keys);
        twitterStream.addListener(this);
        twitterStream.filter(filterQuery);
        emitterProcessor = EmitterProcessor.create();
        emitterProcessor.map(x -> x);

        return emitterProcessor;
    }

    public Flux<String> filterStocks(String[] stocks) {
        FilterQuery filterQuery = new FilterQuery();
        filterQuery.track(stocks);
        twitterStream.addListener(this);
        twitterStream.filter(filterQuery);
        emitterProcessor = EmitterProcessor.create();
        emitterProcessor.map(x -> x);

        return emitterProcessor;
    }

    // TODO: 10/12/2022 may need to delete this method
    public Flux<String> filterStocks1(String[] stocks) {
        FilterQuery filterQuery = new FilterQuery();
        filterQuery.track(stocks);
        twitterStream.addListener(this);
        twitterStream.filter(filterQuery);
        return Flux.create(sink -> {
            twitterStream.onStatus(status -> sink.next(status.getText()));
            twitterStream.onException(sink::error);
            twitterStream.filter(filterQuery);
            sink.onCancel(() -> twitterStream.shutdown());
        });
    }

    /**
     * This method is called inorder to stop the twitter stream.
     */
    public void shutdown() {
        logger.info("Shutting down twitter stream");

        this.twitterStream.shutdown();
        emitterProcessor.onComplete();
    }

    public String shutdown1() {
        logger.info("Shutting down twitter stream");

        this.twitterStream.shutdown();
        emitterProcessor.onComplete();
//        twitterStream.removeListener(this);


        StringBuilder res = new StringBuilder();
        Object stocksFromRedis = redisService.get("stocks");
        try {
            String[] stocks = objectMapper.readValue(stocksFromRedis.toString(), String[].class);
            for (String stock : stocks) {
                String stockCount = redisService.get(stock + eRedisPrefix.count.getKeyPrefix()).toString();
                Double stockAvgSentiment = Double.parseDouble(redisService.get(stock + eRedisPrefix.sentiment.getKeyPrefix()).toString())
                        / Double.parseDouble(stockCount);

                String recorde = String.format("Stock: %s, Count: %s, Avg Sentiment: %s", stock, stockCount, decimalFormat.format(stockAvgSentiment));
                res.append(recorde).append("<br/>").append("<br/>");

            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        return res.toString();

    }

    /**
     * This method is called when a new tweet is received.
     *
     * @param status incoming tweet status with data.
     */
    @Override
    public void onStatus(Status status) {
        emitterProcessor.onNext(status.getText());
        System.out.println(">>> [Twitter STATUS]" + status);

    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
    }

    @Override
    public void onTrackLimitationNotice(int i) {
    }

    @Override
    public void onScrubGeo(long l, long l1) {
    }

    @Override
    public void onStallWarning(StallWarning stallWarning) {
    }

    @Override
    public void onException(Exception e) {
    }
}
