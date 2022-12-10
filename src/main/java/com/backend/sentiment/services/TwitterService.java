package com.backend.sentiment.services;

import com.backend.sentiment.Enums.*;
import com.backend.sentiment.kafka.*;
import com.backend.sentiment.models.*;
import com.backend.sentiment.nlp.*;
import com.backend.sentiment.twitter.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;
import reactor.core.publisher.*;
import reactor.kafka.receiver.*;

import java.text.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.*;


@Service
public class TwitterService {

    private final Logger logger = LoggerFactory.getLogger(TwitterService.class);
    private final DecimalFormat decimalFormat = new DecimalFormat("0.00");

    @Autowired
    private AppTwitterStream twitterStream;

    @Autowired
    private SentimentAnalyzer sentimentAnalyzer;

    @Autowired
    private RedisService redisService;

    @Autowired
    private KafkaReceiver<String, String> kafkaReceiver;
    @Autowired
    private AppKafkaSender kafkaSender;

    @Autowired
    private ObjectMapper objectMapper;
    private String htmlResponse = "<htmlResponse><head>" +
            "<link href=\"https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css\" rel=\"stylesheet\">\n" +
            "<head>" + "<body class=container><h6 class=text-center> %s </h6></body></htmlResponse>";
    /**
     * 1. get tweet and calc the sentiment of the twit
     * 2. create & return  a TweetStockStat object with the tweet sentiment
     * and with the stocks which the twit contains
     *
     * @param tweet given tweet from twitter
     * @param stocks the stocks to filter
     * @return stockStat object
     */
    public TweetStockStat calcTwitResult(String tweet, List<String> stocks) {
        Double analyze = sentimentAnalyzer.analyze(tweet);
        return new TweetStockStat(stocks.stream().filter(s -> tweet.contains(s)).collect(Collectors.toList()), analyze);
    }

    /**
     * method to get the tweets from kafka and to calc the sentiment of the twits
     * @param tweetKeyword the keyword to filter the tweets
     * @param parallelism  the level of parallelism
     * @param windowSize  the size of the window
     * @return  current window sentiment analysis
     */
    public Flux<String> startGettingSentiment(String tweetKeyword, int parallelism, int windowSize) {
        Flux<String> fluxFromKafka = kafkaReceiver.receive().map(message -> message.value());

        // flux of tweets from twitter which contains the given keyword
        Flux<String> fluxFromTwitter = twitterStream.filter(tweetKeyword);

        /*
            1. calc each tweet sentiment
            2. send the tweet to kafka
        */
        fluxFromTwitter.parallel(parallelism)
                .map(tweet -> toTweetWithSentiment(tweet))
                .map(tweetWithSentiment -> kafkaSender.send(twitWithSentimentToJson(tweetWithSentiment),
                        KafkaTopicConfig.APP_TOPIC))
                .subscribe();

        Flux<ArrayList<String>> groupedTweetsByWindow = fluxFromKafka
                .window(Duration.ofSeconds(windowSize)).flatMap(this::toArrayList);

        // calc the average sentiment of the tweets in the current time window
        return groupedTweetsByWindow.map(tweets -> {
            String response = "Empty" + "<br/>";

            if (!tweets.isEmpty()) { // receive tweets from kafka as json and map to TweetWithSentiment object
                List<TweetWithSentiment> currentWindowTweets = tweets.stream()
                        .map(tweet -> toTweetWithSentimentFromJson(tweet))
                        .collect(Collectors.toList());

                List<Double> sentimentListOfTweets = currentWindowTweets.stream().map(twit -> twit.getSentiment()).collect(Collectors.toList());
                Double averageSentiment = sentimentListOfTweets.stream().mapToDouble(x -> x).average().orElse(0.0);
                String formattedAvgSentiment = decimalFormat.format(averageSentiment);
                response = "Contains: " + currentWindowTweets.size() + " tweets Sentiment of the last " + windowSize + " seconds is: " + formattedAvgSentiment +
                        " for: " + "<strong>" + tweetKeyword + "</strong> <br/>";
            }

            return String.format(htmlResponse, response);
        });

    }

    /**
     * method to calc average sentiment of given tweet
     * @param tweet the tweet to calc the sentiment
     * @return TweetWithSentiment object
     */
    private TweetWithSentiment toTweetWithSentiment(String tweet) {
        Double analyze = sentimentAnalyzer.analyze(tweet);
        return new TweetWithSentiment(tweet, analyze);
    }


    public Flux<String> startTwitterStream(String[] stocks, int parallelism, int windowSize) {


        // start receiving "messages" from kafka
        Flux<String> fluxOfTweetsFromKafka = kafkaReceiver.receive()
                .map(messageReceivedFromKafka -> messageReceivedFromKafka.value());

        // starts receiving tweets from twitter
//        Flux<String> stringFluxFromTwitter = filter(stocks);
        Flux<String> stringFluxFromTwitter = twitterStream.filterStocks(stocks);

        // send tweets to kafka with calculated sentiment in parallel
        stringFluxFromTwitter.parallel(parallelism)
                .map(twit -> this.calcTwitResult(twit, List.of(stocks)))
                .map(stockStat -> kafkaSender
                        .send(twitWithSentimentToJson(stockStat),
                                KafkaTopicConfig.APP_TOPIC)).subscribe();

        /* groups records using time intervals and flattens the flux of fluxes into a flux of records */
        Flux<ArrayList<String>> groupedTweetsByWindow = fluxOfTweetsFromKafka
                .window(Duration.ofSeconds(windowSize)).flatMap(this::toArrayList);

        return groupedTweetsByWindow.map(currWindowTweets -> {
            Map<String, Double> stockToSentiment = new HashMap<>();
            Map<String, Double> stockToCount = new HashMap<>();

            if (!currWindowTweets.isEmpty()) {
                List<TweetStockStat> currentWindowTweetsAsObj =
                        currWindowTweets.stream().map(tweet ->
                                toStockStatFromJson(tweet)).collect(Collectors.toList());

                /* analysing the current window of tweets
                  1. for each tweet in the list
                  2. get the stocks which the tweet contains
                  3. for each stock in the list
                  4. add the sentiment of the tweet to the stock sentiment
                  5. add 1 to the stock count
                 */
                currentWindowTweetsAsObj.forEach(stockStat -> {
                    List<String> stockNames = stockStat.getStockNames();
                    stockNames.forEach(currentStockName -> {
                        if (stockToSentiment.containsKey(currentStockName)) {
                            stockToSentiment.put(currentStockName,
                                    stockToSentiment.get(currentStockName) + stockStat.getStockSentiment());
                            stockToCount.put(currentStockName, stockToCount.get(currentStockName) + 1);
                        } else {
                            stockToSentiment.put(currentStockName, stockStat.getStockSentiment());
                            stockToCount.put(currentStockName, 1.0);
                        }
                    });
                });

                StringBuilder responseStringBuilder = new StringBuilder();

                for (String stockName : stocks) {

                    double sentiment = stockToSentiment.get(stockName) == null ? 0.0 : stockToSentiment.get(stockName);
                    int count = stockToCount.get(stockName) == null ? 0 : stockToCount.get(stockName).intValue();

                    // max -> avoid division by zero in case of no currWindowTweets with this stock
                    double avgSentiment = (sentiment / Math.max(1, count));

                    responseStringBuilder.append(String.format("<strong> %s </strong>", stockName)).append(": ");
                    responseStringBuilder.append("count: ").append(count).append("-");
                    responseStringBuilder.append("sentiment (avg): ").append(decimalFormat.format(avgSentiment)).append(" | ");

                    // update stocks sentiment and count in redis
                    redisService.increment(stockName + eRedisPrefix.count.getKeyPrefix(), count);
                    redisService.increment(stockName + eRedisPrefix.sentiment.getKeyPrefix(), sentiment);
                }
                return htmlResponse.replace("%s", responseStringBuilder.toString());

            }
            // in case no incoming tweets
            return htmlResponse.replace("%s", "No tweets in the last " + windowSize + " seconds");
        });

    }

    /**
     * parse json to TweetWithSentiment object
     * @param tweetAsJson
     * @return
     */
    private TweetWithSentiment toTweetWithSentimentFromJson(String tweetAsJson) {
        try {
            return objectMapper.readValue(tweetAsJson, TweetWithSentiment.class);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * map data from kafka (from json) to a TweetStockStat object
     *
     * @param tweet json string
     * @return TweetStockStat object
     */
    private TweetStockStat toStockStatFromJson(String tweet) {
        try {
            return objectMapper.readValue(tweet, TweetStockStat.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * reduce flux to mono
     *
     * @param source flux
     * @param <T>    type of flux
     * @return mono
     */
    private <T> Mono<ArrayList<T>> toArrayList(Flux<T> source) {
        return source.reduce(new ArrayList(), (a, b) -> {
            a.add(b);
            return a;
        });
    }

    /**
     * convert TweetStockStat object to json string
     *
     * @param stockStat TweetStockStat object
     * @return json string
     */
    private String twitWithSentimentToJson(TweetStockStat stockStat) {
        try {
            return objectMapper.writeValueAsString(stockStat);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    private String twitWithSentimentToJson(TweetWithSentiment tweetWithSentiment) {
        try {
            return objectMapper.writeValueAsString(tweetWithSentiment);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * object to hold the sentiment of the twit and the stocks which the twit contains
     */
    public static class TweetStockStat {
        List<String> stockNames;
        Double stockSentiment;

        public TweetStockStat(List<String> stockNames, Double stockSentiment) {
            this.stockNames = stockNames;
            this.stockSentiment = stockSentiment;
        }

        public List<String> getStockNames() {
            return stockNames;
        }

        public void setStockNames(List<String> stockNames) {
            this.stockNames = stockNames;
        }

        public Double getStockSentiment() {
            return stockSentiment;
        }

        public void setStockSentiment(Double stockSentiment) {
            this.stockSentiment = stockSentiment;
        }
    }
}
