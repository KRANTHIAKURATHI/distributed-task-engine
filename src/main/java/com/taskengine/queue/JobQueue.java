package com.taskengine.queue;

import com.taskengine.entity.Job;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
public class JobQueue {

    private static final String JOB_STREAM =
            "task-engine:jobs";

    private static final String CONSUMER_GROUP =
            "task-workers";

    private final StringRedisTemplate redisTemplate;

    public JobQueue(StringRedisTemplate redisTemplate) {

        this.redisTemplate = redisTemplate;

        createConsumerGroup();
    }

    /*
     * ==========================================
     * ADD JOB TO REDIS STREAM
     * ==========================================
     */
    public void enqueue(UUID jobId) {

        Map<String, String> message =
                Map.of(
                        "jobId",
                        jobId.toString()
                );

        redisTemplate.opsForStream()
                .add(
                        JOB_STREAM,
                        message
                );

        System.out.println(
                "JOB ADDED TO REDIS STREAM: "
                        + jobId
        );
    }

    /*
     * ==========================================
     * CREATE CONSUMER GROUP
     * ==========================================
     */
    private void createConsumerGroup() {

        try {

            redisTemplate.opsForStream()
                    .createGroup(
                            JOB_STREAM,
                            ReadOffset.from("0-0"),
                            CONSUMER_GROUP
                    );

            System.out.println(
                    "REDIS CONSUMER GROUP CREATED: "
                            + CONSUMER_GROUP
            );

        } catch (Exception e) {

            /*
             * Group already exists.
             */
            System.out.println(
                    "REDIS CONSUMER GROUP ALREADY EXISTS"
            );
        }
    }

    /*
     * ==========================================
     * CLAIM JOB
     * ==========================================
     *
     * IMPORTANT:
     *
     * This is a BLOCKING READ.
     *
     * The worker waits for a new Redis Stream
     * message instead of repeatedly polling.
     */
    public String claimJob(String workerId) {

        Consumer consumer =
                Consumer.from(
                        CONSUMER_GROUP,
                        workerId
                );

        StreamReadOptions options =
                StreamReadOptions.empty()
                        .count(1)
                        .block(Duration.ofSeconds(5));

        StreamOffset<String> offset =
                StreamOffset.create(
                        JOB_STREAM,
                        ReadOffset.lastConsumed()
                );

        List<MapRecord<String, Object, Object>> records =
                redisTemplate.opsForStream()
                        .read(
                                consumer,
                                options,
                                offset
                        );

        if (records == null || records.isEmpty()) {

            return null;
        }

        MapRecord<String, Object, Object> record =
                records.get(0);

        Object jobId =
                record.getValue()
                        .get("jobId");

        if (jobId == null) {

            System.out.println(
                    "REDIS MESSAGE HAS NO jobId"
            );

            return null;
        }

        System.out.println(
                "========================================"
        );

        System.out.println(
                "REDIS JOB CLAIMED"
        );

        System.out.println(
                "Worker: "
                        + workerId
        );

        System.out.println(
                "Redis Record: "
                        + record.getId()
        );

        System.out.println(
                "Job ID: "
                        + jobId
        );

        System.out.println(
                "========================================"
        );

        return jobId.toString();
    }

    /*
     * ==========================================
     * REQUEUE JOB
     * ==========================================
     */
    public void enqueueJob(Job job) {

        enqueue(job.getId());

        System.out.println(
                "JOB REQUEUED TO REDIS: "
                        + job.getId()
        );
    }
}