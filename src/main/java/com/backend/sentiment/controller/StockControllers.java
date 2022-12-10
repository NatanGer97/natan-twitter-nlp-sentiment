package com.backend.sentiment.controller;

import com.backend.sentiment.Enums.*;
import com.backend.sentiment.kafka.*;
import com.backend.sentiment.nlp.*;
import com.backend.sentiment.services.*;
import com.backend.sentiment.twitter.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import org.slf4j.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.*;
import reactor.kafka.receiver.*;

@RestController
@RequestMapping("/stock")
public class StockControllers {
    @Autowired
    SentimentAnalyzer sentimentAnalyzer;

    @Autowired
    private AppTwitterStream twitterStream;

    @Autowired
    private AppKafkaSender kafkaSender;
    @Autowired
    private TwitterService twitterService;

    @Autowired
    private KafkaReceiver<String, String> kafkaReceiver;

    private final Logger logger = LoggerFactory.getLogger(StockControllers.class);

    @Autowired
    private RedisService redisService;


    @Autowired
    private ObjectMapper objectMapper;


    /**
     * @param stockA      - first filter
     * @param stockB      - second filter
     * @param stockC      - third filter
     * @param parallelism - parallelism of the stream
     * @param window      - size of the window - in seconds
     * @return - flux of stock stats in window of X seconds
     */

    @GetMapping("/sentiment")
    public @ResponseBody
    Flux<String> getStocksSentiment(@RequestParam(value = "f1") String stockA,
                                    @RequestParam(value = "f2") String stockB,
                                    @RequestParam(value = "f3") String stockC,
                                    @RequestParam(value = "par", defaultValue = "5") int parallelism,
                                    @RequestParam(value = "window", defaultValue = "5") int window) {
        logger.info("enter getSentiment");
        String[] stocks = {stockA, stockB, stockC};

        try {
            redisService.set(eRedisPrefix.stocks.getKeyPrefix(), objectMapper.writeValueAsString(stocks));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        for (String stock : stocks) {
            // initialize the stocks keys in redis (count and sentiment keys)
            redisService.set(stock + eRedisPrefix.count.getKeyPrefix(), 0);
            redisService.set(stock + eRedisPrefix.sentiment.getKeyPrefix(), 0.0);
        }

        return twitterService.startTwitterStream(stocks, parallelism, window);
    }

    @GetMapping("/getSentiment")
    public @ResponseBody
    Flux<String> getFlux(@RequestParam String keyword, @RequestParam(defaultValue = "3", value = "w") int window,
                         @RequestParam(defaultValue = "3", value = "p") int parallelism) {
        return twitterService.startGettingSentiment(keyword, parallelism, window);
    }

    // TODO: should be deleted
    @GetMapping("/stop")
    public @ResponseBody
    Mono<String> stop() {
        logger.info("stop was requested");
        String response = twitterStream.shutdown1();
        return Mono.just(response);

    }


}
