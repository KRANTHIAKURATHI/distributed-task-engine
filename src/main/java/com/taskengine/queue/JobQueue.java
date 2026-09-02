package com.taskengine.queue;

import com.taskengine.entity.Job;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.data.domain.Range;
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

    public JobQueue(
            StringRedisTemplate redisTemplate
    ) {

        this.redisTemplate =
                redisTemplate;

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

        RecordId recordId =
                redisTemplate
                        .opsForStream()
                        .add(
                                JOB_STREAM,
                                message
                        );

        System.out.println(
                "JOB ADDED TO REDIS STREAM: "
                        + jobId
        );

        System.out.println(
                "REDIS RECORD ID: "
                        + recordId
        );
    }

    /*
     * ==========================================
     * CREATE CONSUMER GROUP
     * ==========================================
     */

    private void createConsumerGroup() {

        try {

            redisTemplate
                    .opsForStream()
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
             * Usually means the group already exists.
             */
            System.out.println(
                    "REDIS CONSUMER GROUP ALREADY EXISTS"
            );
        }
    }

    /*
     * ==========================================
     * RECOVER PENDING JOBS
     * ==========================================
     *
     * Uses Redis XAUTOCLAIM.
     *
     * Finds messages that have been pending for
     * longer than the configured idle time and
     * transfers ownership to the current worker.
     */
    public List<ClaimedJob> recoverPendingJobs(
            String workerId,
            long minIdleTimeMs
    ) {

        Consumer consumer =
                Consumer.from(
                        CONSUMER_GROUP,
                        workerId
                );

        /*
         * Find pending messages that have been idle
         * longer than the configured threshold.
         *
         * Start from the beginning of the PEL.
         */
        var pendingMessages =
                redisTemplate
                        .opsForStream()
                        .pending(
                                JOB_STREAM,
                                CONSUMER_GROUP,
                                Range.unbounded(),
                                10
                        );

        if (pendingMessages == null
                || pendingMessages.isEmpty()) {

            return List.of();
        }

        /*
         * Collect Redis record IDs that are old enough
         * to be recovered.
         */
        List<RecordId> recordIds =
                new java.util.ArrayList<>();

        for (var pendingMessage :
                pendingMessages) {

            if (pendingMessage
                    .getElapsedTimeSinceLastDelivery()
                    .toMillis()
                    >= minIdleTimeMs) {

                recordIds.add(
                        pendingMessage
                                .getId()
                );
            }
        }

        if (recordIds.isEmpty()) {

            return List.of();
        }

        /*
         * ==========================================
         * CLAIM OWNERSHIP
         * ==========================================
         *
         * Transfer ownership of stale messages from
         * the failed/old consumer to this worker.
         */
        List<MapRecord<String, Object, Object>>
                claimedRecords =
                redisTemplate
                        .opsForStream()
                        .claim(
                                JOB_STREAM,
                                CONSUMER_GROUP,
                                workerId,
                                Duration.ofMillis(
                                        minIdleTimeMs
                                ),
                                recordIds.toArray(
                                        new RecordId[0]
                                )
                        );

        if (claimedRecords == null
                || claimedRecords.isEmpty()) {

            return List.of();
        }

        List<ClaimedJob> recoveredJobs =
                new java.util.ArrayList<>();

        /*
         * ==========================================
         * CONVERT REDIS RECORDS
         * ==========================================
         */
        for (MapRecord<String, Object, Object> record :
                claimedRecords) {

            Object jobId =
                    record
                            .getValue()
                            .get("jobId");

            if (jobId == null) {

                System.out.println(
                        "RECOVERED REDIS MESSAGE "
                                + "HAS NO jobId: "
                                + record.getId()
                );

                continue;
            }

            String recordId =
                    record.getId().getValue();

            System.out.println(
                    "========================================"
            );

            System.out.println(
                    "REDIS JOB RECOVERED"
            );

            System.out.println(
                    "Worker: "
                            + workerId
            );

            System.out.println(
                    "Redis Record: "
                            + recordId
            );

            System.out.println(
                    "Job ID: "
                            + jobId
            );

            System.out.println(
                    "========================================"
            );

            recoveredJobs.add(
                    new ClaimedJob(
                            jobId.toString(),
                            recordId
                    )
            );
        }

        return recoveredJobs;
    }

    /*
     * ==========================================
     * CLAIMED JOB
     * ==========================================
     *
     * Contains BOTH:
     *
     * 1. Database Job ID
     * 2. Redis Stream Record ID
     *
     * We need the Redis Record ID later for XACK.
     */

    public static class ClaimedJob {

        private final String jobId;
        private final String recordId;

        public ClaimedJob(
                String jobId,
                String recordId
        ) {

            this.jobId = jobId;
            this.recordId = recordId;
        }

        public String getJobId() {

            return jobId;
        }

        public String getRecordId() {

            return recordId;
        }
    }

    /*
     * ==========================================
     * CLAIM JOB
     * ==========================================
     *
     * Consumer-group read.
     *
     * lastConsumed() allows the consumer group
     * to continue from its last delivered position.
     *
     * The Redis record is NOT acknowledged here.
     *
     * It remains pending until the worker
     * successfully finishes processing.
     */

    public ClaimedJob claimJob(
            String workerId
    ) {

        Consumer consumer =
                Consumer.from(
                        CONSUMER_GROUP,
                        workerId
                );

        StreamReadOptions options =
                StreamReadOptions.empty()
                        .count(1)
                        .block(
                                Duration.ofSeconds(5)
                        );

        StreamOffset<String> offset =
                StreamOffset.create(
                        JOB_STREAM,
                        ReadOffset.lastConsumed()
                );

        List<MapRecord<String, Object, Object>>
                records =
                redisTemplate
                        .opsForStream()
                        .read(
                                consumer,
                                options,
                                offset
                        );

        if (records == null
                || records.isEmpty()) {

            return null;
        }

        MapRecord<String, Object, Object>
                record =
                records.get(0);

        Object jobId =
                record
                        .getValue()
                        .get("jobId");

        if (jobId == null) {

            System.out.println(
                    "REDIS MESSAGE HAS NO jobId"
            );

            return null;
        }

        String recordId =
                record.getId().getValue();

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
                        + recordId
        );

        System.out.println(
                "Job ID: "
                        + jobId
        );

        System.out.println(
                "========================================"
        );

        return new ClaimedJob(
                jobId.toString(),
                recordId
        );
    }

    /*
     * ==========================================
     * ACKNOWLEDGE JOB
     * ==========================================
     *
     * Removes the Redis Stream record from
     * the consumer group's Pending Entries List.
     *
     * IMPORTANT:
     *
     * Call this ONLY after the database job has
     * been successfully handled.
     */

    public boolean acknowledge(
            String recordId
    ) {

        Long acknowledged =
                redisTemplate
                        .opsForStream()
                        .acknowledge(
                                JOB_STREAM,
                                CONSUMER_GROUP,
                                recordId
                        );

        boolean success =
                acknowledged != null
                        && acknowledged == 1;

        if (success) {

            System.out.println(
                    "REDIS JOB ACKNOWLEDGED: "
                            + recordId
            );

        } else {

            System.out.println(
                    "REDIS ACK FAILED: "
                            + recordId
            );
        }

        return success;
    }

    /*
     * ==========================================
     * REQUEUE JOB
     * ==========================================
     */

    public void enqueueJob(Job job) {

        enqueue(
                job.getId()
        );

        System.out.println(
                "JOB REQUEUED TO REDIS: "
                        + job.getId()
        );
    }
}