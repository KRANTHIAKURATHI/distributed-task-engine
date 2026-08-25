package com.taskengine.queue;

import org.springframework.data.redis.connection.RedisListCommands;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.UUID;

@Component
public class JobQueue {

    private static final String READY_QUEUE =
            "task-engine:queue:ready";

    private static final String PROCESSING_QUEUE =
            "task-engine:queue:processing";

    private final StringRedisTemplate redisTemplate;

    public JobQueue(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void enqueue(UUID jobId) {
        redisTemplate.opsForList()
                .rightPush(READY_QUEUE, jobId.toString());
    }

    public String claimJob() {

        ListOperations<String, String> operations =
                redisTemplate.opsForList();

        return operations.move(
                READY_QUEUE,
                RedisListCommands.Direction.LEFT,
                PROCESSING_QUEUE,
                RedisListCommands.Direction.RIGHT,
                Duration.ofSeconds(2)
        );
    }
}