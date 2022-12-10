package com.backend.sentiment.controller;

import com.backend.sentiment.Enums.*;
import com.backend.sentiment.models.*;
import com.backend.sentiment.services.*;
import com.backend.sentiment.twitter.*;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;

import org.springframework.beans.factory.annotation.*;
import org.springframework.stereotype.*;
import org.springframework.ui.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.method.support.*;

import java.text.*;
import java.util.*;

/**
 * Controller for the results page
 */
@Controller
@RequestMapping("/results")
public class ResultsController {

    @Autowired
    private RedisService redisService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    AppTwitterStream twitterStream;

    /**
     * This method is used to get the  report of the sentiment analysis as a html page
     * @param model the model to be used to pass the data to the view
     * @return the name of the view to be rendered
     */
    @GetMapping("/stop")
    public String greeting(Model model) {
        final DecimalFormat decimalFormat = new DecimalFormat("0.00");
        // stop listening to the twitter stream
        twitterStream.shutdown1();

        ModelAndViewContainer modelAndViewContainer  = new ModelAndViewContainer();
        Object stocksFromRedis = redisService.get("stocks");

        ArrayList<ResultRecord> resultRecords = new ArrayList<>();

        try {
            String[] stocks = objectMapper.readValue(stocksFromRedis.toString(), String[].class);

            for (String stock : stocks) {
                String stockCount = redisService.get(stock + eRedisPrefix.count.getKeyPrefix()).toString();
                Double stockAvgSentiment = Double.parseDouble(redisService.get(stock + eRedisPrefix.sentiment.getKeyPrefix()).toString())
                        / Double.parseDouble(stockCount);

                resultRecords.add(new ResultRecord(stock,
                        Double.parseDouble(decimalFormat.format(stockAvgSentiment)),
                        Integer.parseInt(stockCount)));

            }
            model.addAttribute("results", resultRecords);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        model.addAttribute("name", "name");
        return "results";
    }

}


