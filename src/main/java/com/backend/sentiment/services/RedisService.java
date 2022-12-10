package com.backend.sentiment.services;

import org.slf4j.*;
import org.springframework.beans.factory.annotation.*;
import org.springframework.data.redis.core.*;
import org.springframework.stereotype.*;

import java.text.*;

@Service
public class RedisService {
    @Autowired
    private RedisTemplate<String, Object> redisTemplate;
    private final DecimalFormat df = new DecimalFormat("0.00");

    Logger logger = LoggerFactory.getLogger(RedisService.class);


    /**
     * @param key   the key to save
     * @param value the value to save
     */
    public void set(String key, int value) {
        redisTemplate.opsForValue().set(key, String.valueOf(value));
    }

    public void set(String key, double value) {
        redisTemplate.opsForValue().set(key, String.valueOf(df.format(value)));
    }

    public void set(String key, String value) {
        redisTemplate.opsForValue().set(key, value);
    }

    /**
     * return the value of the key or null if the key does not exist
     *
     * @param key the key to get
     * @return the value of the key or null if the key does not exist
     */
    public Object get(String key) {
        if (Boolean.TRUE.equals(redisTemplate.hasKey(key)))
            return redisTemplate.opsForValue().get(key);

        // if the key is not exist return null
        return null;

    }

    public void increment(String key) {
        redisTemplate.opsForValue().increment(key);
    }

    public void increment(String key, double value) {

        redisTemplate.opsForValue().increment(key, value);


    }


}
