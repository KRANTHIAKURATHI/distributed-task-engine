package com.taskengine.worker;

import com.taskengine.entity.Job;
import com.taskengine.enums.JobStatus;
import com.taskengine.queue.JobQueue;
import com.taskengine.repository.JobRepository;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class JobWorker implements SmartLifecycle {

    private final WorkerIdentity workerIdentity;
    private final JobQueue jobQueue;
    private final JobRepository jobRepository;

    /*
     * true  -> worker accepts new jobs
     * false -> shutdown has started
     */
    private final AtomicBoolean running =
            new AtomicBoolean(true);

    /*
     * true -> worker is currently executing a job
     */
    private final AtomicBoolean jobRunning =
            new AtomicBoolean(false);

    /*
     * Current job being executed.
     *
     * JobHeartbeat uses this to renew the
     * database lease.
     */
    private volatile Job currentJob;

    public JobWorker(
            WorkerIdentity workerIdentity,
            JobQueue jobQueue,
            JobRepository jobRepository
    ) {
        this.workerIdentity = workerIdentity;
        this.jobQueue = jobQueue;
        this.jobRepository = jobRepository;
    }

    /*
     * ==========================================
     * WORKER LOOP
     * ==========================================
     */

    @Scheduled(fixedDelay = 1000)
    public void run() {

        /*
         * Do not accept new jobs after shutdown.
         */
        if (!running.get()) {
            return;
        }

        processNextJob();
    }

    /*
     * ==========================================
     * PROCESS NEXT NEW JOB
     * ==========================================
     *
     * Normal Redis path:
     *
     * XREADGROUP
     *     ↓
     * ClaimedJob
     *     ↓
     * processClaimedJob()
     */

    public void processNextJob() {

        if (!running.get()) {
            return;
        }

        String workerId =
                workerIdentity.getWorkerId();

        System.out.println(
                "WORKER WAITING FOR JOB: "
                        + workerId
        );

        JobQueue.ClaimedJob claimedJob;

        try {

            claimedJob =
                    jobQueue.claimJob(
                            workerId
                    );

        } catch (Exception e) {

            /*
             * Redis may be shutting down.
             */
            if (!running.get()) {

                System.out.println(
                        "WORKER SHUTDOWN: "
                                + "Redis connection closed"
                );

                return;
            }

            System.out.println(
                    "REDIS CLAIM ERROR: "
                            + e.getMessage()
            );

            return;
        }

        if (claimedJob == null) {
            return;
        }

        processClaimedJob(
                claimedJob
        );
    }

    /*
     * ==========================================
     * PROCESS CLAIMED JOB
     * ==========================================
     *
     * This method is used by BOTH:
     *
     * 1. Normal XREADGROUP jobs
     *
     * 2. Recovered XCLAIM jobs
     *
     * The recovered flag tells us whether the
     * PostgreSQL job should already be PROCESSING.
     */

    public void processClaimedJob(
            JobQueue.ClaimedJob claimedJob
    ) {

        String workerId =
                workerIdentity.getWorkerId();

        String jobId =
                claimedJob.getJobId();

        String redisRecordId =
                claimedJob.getRecordId();

        /*
         * ==========================================
         * REDIS INFORMATION
         * ==========================================
         */

        System.out.println(
                "========================================"
        );

        System.out.println(
                "PROCESSING REDIS MESSAGE"
        );

        System.out.println(
                "WORKER: "
                        + workerId
        );

        System.out.println(
                "JOB ID: "
                        + jobId
        );

        System.out.println(
                "REDIS RECORD ID: "
                        + redisRecordId
        );

        System.out.println(
                "========================================"
        );

        jobRunning.set(true);

        UUID id;

        try {

            id =
                    UUID.fromString(
                            jobId
                    );

        } catch (IllegalArgumentException e) {

            System.out.println(
                    "INVALID JOB ID FROM REDIS: "
                            + jobId
            );

            /*
             * This message cannot be processed.
             */
            jobQueue.acknowledge(
                    redisRecordId
            );

            jobRunning.set(false);

            return;
        }

        /*
         * ==========================================
         * LOAD JOB FROM POSTGRESQL
         * ==========================================
         */

        Job job =
                jobRepository.findById(id)
                        .orElse(null);

        if (job == null) {

            System.out.println(
                    "JOB NOT FOUND IN DATABASE: "
                            + jobId
            );

            /*
             * Orphan Redis message.
             */
            jobQueue.acknowledge(
                    redisRecordId
            );

            jobRunning.set(false);

            return;
        }

        /*
         * ==========================================
         * CANCELLED JOB
         * ==========================================
         */

        if (job.getStatus()
                == JobStatus.CANCELLED) {

            System.out.println(
                    "SKIPPING CANCELLED JOB: "
                            + job.getId()
            );

            /*
             * No processing is required.
             */
            jobQueue.acknowledge(
                    redisRecordId
            );

            jobRunning.set(false);

            return;
        }

        /*
         * ==========================================
         * CURRENT JOB
         * ==========================================
         */

        currentJob = job;

        try {

            /*
             * ======================================
             * REFRESH DATABASE STATE
             * ======================================
             */

            Job latestJob =
                    jobRepository.findById(id)
                            .orElse(null);

            if (latestJob == null) {

                System.out.println(
                        "JOB DISAPPEARED BEFORE PROCESSING: "
                                + id
                );

                jobQueue.acknowledge(
                        redisRecordId
                );

                return;
            }

            /*
             * ======================================
             * CHECK CANCELLATION
             * ======================================
             */

            if (latestJob.getStatus()
                    == JobStatus.CANCELLED) {

                System.out.println(
                        "JOB CANCELLED BEFORE PROCESSING: "
                                + id
                );

                jobQueue.acknowledge(
                        redisRecordId
                );

                return;
            }

            /*
             * ======================================
             * DETERMINE JOB TYPE
             * ======================================
             *
             * NORMAL:
             *
             * PENDING / DISPATCHED
             *
             * RECOVERED:
             *
             * PROCESSING + expired lease
             */

            boolean normalJob =
                    latestJob.getStatus()
                            == JobStatus.PENDING
                            ||
                            latestJob.getStatus()
                                    == JobStatus.DISPATCHED;

            boolean recoveredJob =
                    latestJob.getStatus()
                            == JobStatus.PROCESSING;

            /*
             * ======================================
             * NORMAL JOB
             * ======================================
             */

            if (normalJob) {

                job =
                        latestJob;

                currentJob = job;

                LocalDateTime now =
                        LocalDateTime.now();

                /*
                 * Claim the job in PostgreSQL.
                 */
                job.setStatus(
                        JobStatus.PROCESSING
                );

                job.setWorkerId(
                        workerId
                );

                job.setClaimedAt(
                        now
                );

                /*
                 * 30-second database lease.
                 */
                job.setLeaseUntil(
                        now.plusSeconds(30)
                );

                job.setUpdatedAt(
                        now
                );

                jobRepository.save(job);

                System.out.println(
                        "========================================"
                );

                System.out.println(
                        "NEW JOB CLAIMED"
                );

                System.out.println(
                        "WORKER: "
                                + workerId
                );

                System.out.println(
                        "JOB: "
                                + job.getId()
                );

                System.out.println(
                        "ATTEMPT: "
                                + (job.getAttemptCount() + 1)
                                + "/"
                                + job.getMaxAttempts()
                );

                System.out.println(
                        "LEASE UNTIL: "
                                + job.getLeaseUntil()
                );

                System.out.println(
                        "========================================"
                );
            }

            /*
             * ======================================
             * RECOVERED JOB
             * ======================================
             *
             * The Redis message was reclaimed using
             * XCLAIM.
             *
             * The PostgreSQL job should therefore
             * already be PROCESSING.
             *
             * Only recover it if its database lease
             * has expired.
             */

            else if (recoveredJob) {

                LocalDateTime now =
                        LocalDateTime.now();

                /*
                 * Safety check:
                 *
                 * Do NOT steal a job from a worker
                 * whose PostgreSQL lease is still valid.
                 */
                if (latestJob.getLeaseUntil() != null
                        && latestJob
                        .getLeaseUntil()
                        .isAfter(now)) {

                    System.out.println(
                            "========================================"
                    );

                    System.out.println(
                            "RECOVERY REJECTED"
                    );

                    System.out.println(
                            "JOB STILL HAS VALID DATABASE LEASE"
                    );

                    System.out.println(
                            "JOB: "
                                    + latestJob.getId()
                    );

                    System.out.println(
                            "LEASE UNTIL: "
                                    + latestJob
                                    .getLeaseUntil()
                    );

                    System.out.println(
                            "========================================"
                    );

                    /*
                     * Do not process the same job while
                     * another worker may still own it.
                     *
                     * IMPORTANT:
                     *
                     * We also do NOT acknowledge this
                     * Redis message.
                     *
                     * It remains pending and can be
                     * considered again later.
                     */
                    return;
                }

                /*
                 * Database lease has expired.
                 *
                 * Take ownership.
                 */
                job =
                        latestJob;

                currentJob = job;

                job.setWorkerId(
                        workerId
                );

                job.setClaimedAt(
                        now
                );

                /*
                 * Give the recovered job a fresh lease.
                 */
                job.setLeaseUntil(
                        now.plusSeconds(30)
                );

                job.setUpdatedAt(
                        now
                );

                /*
                 * Keep status PROCESSING.
                 */
                job.setStatus(
                        JobStatus.PROCESSING
                );

                jobRepository.save(job);

                System.out.println(
                        "========================================"
                );

                System.out.println(
                        "JOB RECOVERED"
                );

                System.out.println(
                        "WORKER: "
                                + workerId
                );

                System.out.println(
                        "JOB: "
                                + job.getId()
                );

                System.out.println(
                        "ATTEMPT: "
                                + (job.getAttemptCount() + 1)
                                + "/"
                                + job.getMaxAttempts()
                );

                System.out.println(
                        "NEW LEASE UNTIL: "
                                + job.getLeaseUntil()
                );

                System.out.println(
                        "========================================"
                );
            }

            /*
             * ======================================
             * UNKNOWN DATABASE STATE
             * ======================================
             */

            else {

                System.out.println(
                        "JOB NOT ELIGIBLE FOR PROCESSING: "
                                + id
                );

                System.out.println(
                        "CURRENT STATUS: "
                                + latestJob.getStatus()
                );

                /*
                 * The Redis delivery has already been
                 * received, but the database state
                 * cannot be processed by this worker.
                 */
                jobQueue.acknowledge(
                        redisRecordId
                );

                return;
            }

            /*
             * ======================================
             * EXECUTE JOB
             * ======================================
             */

            System.out.println(
                    "========================================"
            );

            System.out.println(
                    "PROCESSING JOB: "
                            + job.getId()
            );

            boolean cancelled =
                    executeJob(job);

            /*
             * ======================================
             * CANCELLATION DETECTED
             * ======================================
             */

            if (cancelled) {

                System.out.println(
                        "========================================"
                );

                System.out.println(
                        "JOB EXECUTION CANCELLED: "
                                + job.getId()
                );

                System.out.println(
                        "JOB WILL NOT BE COMPLETED"
                );

                System.out.println(
                        "========================================"
                );

                /*
                 * Cancellation is a handled outcome.
                 */
                jobQueue.acknowledge(
                        redisRecordId
                );

                return;
            }

            /*
             * ======================================
             * FINAL DATABASE CHECK
             * ======================================
             */

            Job finalJob =
                    jobRepository.findById(
                            job.getId()
                    ).orElse(null);

            if (finalJob == null) {

                System.out.println(
                        "JOB NO LONGER EXISTS: "
                                + job.getId()
                );

                jobQueue.acknowledge(
                        redisRecordId
                );

                return;
            }

            /*
             * Check cancellation immediately
             * before completion.
             */
            if (finalJob.getStatus()
                    == JobStatus.CANCELLED) {

                System.out.println(
                        "CANCELLATION DETECTED "
                                + "BEFORE COMPLETION"
                );

                jobQueue.acknowledge(
                        redisRecordId
                );

                return;
            }

            /*
             * ======================================
             * COMPLETE JOB ATOMICALLY
             * ======================================
             *
             * The repository verifies:
             *
             * 1. Job ID
             * 2. Worker ID
             * 3. PROCESSING status
             * 4. Valid lease
             */
            LocalDateTime completionTime =
                    LocalDateTime.now();

            int updated =
                    jobRepository.completeJobIfOwned(
                            job.getId(),
                            workerId,
                            JobStatus.PROCESSING,
                            JobStatus.COMPLETED,
                            completionTime
                    );

            if (updated == 1) {

                System.out.println(
                        "========================================"
                );

                System.out.println(
                        "COMPLETED JOB: "
                                + job.getId()
                );

                System.out.println(
                        "========================================"
                );

                /*
                 * ==================================
                 * ACK ONLY AFTER DB COMPLETION
                 * ==================================
                 */
                jobQueue.acknowledge(
                        redisRecordId
                );

            } else {

                /*
                 * ==================================
                 * OWNERSHIP LOST
                 * ==================================
                 *
                 * DO NOT ACK.
                 *
                 * Redis keeps the message pending.
                 *
                 * Another worker can recover it later.
                 */
                System.out.println(
                        "========================================"
                );

                System.out.println(
                        "JOB COMPLETION REJECTED"
                );

                System.out.println(
                        "LEASE OR OWNERSHIP LOST: "
                                + job.getId()
                );

                System.out.println(
                        "REDIS MESSAGE REMAINS PENDING: "
                                + redisRecordId
                );

                System.out.println(
                        "========================================"
                );
            }

        } catch (Exception e) {

            /*
             * ======================================
             * FAILURE HANDLING
             * ======================================
             */

            if (isJobCancelled(job)) {

                System.out.println(
                        "JOB WAS CANCELLED: "
                                + job.getId()
                );

                /*
                 * Cancellation is handled.
                 */
                jobQueue.acknowledge(
                        redisRecordId
                );

                return;
            }

            /*
             * Handle RETRYING / DEAD.
             */
            handleFailure(
                    job,
                    e
            );

            /*
             * Database state has been persisted.
             *
             * RetryScheduler will create a new Redis
             * message when the retry becomes eligible.
             */
            jobQueue.acknowledge(
                    redisRecordId
            );

        } finally {

            currentJob = null;

            jobRunning.set(false);
        }
    }

    /*
     * ==========================================
     * CHECK DATABASE CANCELLATION
     * ==========================================
     */

    private boolean isJobCancelled(
            Job job
    ) {

        try {

            Job latest =
                    jobRepository.findById(
                            job.getId()
                    ).orElse(null);

            return latest != null
                    && latest.getStatus()
                    == JobStatus.CANCELLED;

        } catch (Exception e) {

            /*
             * Do not incorrectly assume cancellation
             * if PostgreSQL temporarily fails.
             */
            System.out.println(
                    "CANCELLATION CHECK FAILED: "
                            + e.getMessage()
            );

            return false;
        }
    }

    /*
     * ==========================================
     * FAILURE HANDLING
     * ==========================================
     */

    private void handleFailure(
            Job job,
            Exception e
    ) {

        /*
         * Check cancellation before changing
         * the job to RETRYING or DEAD.
         */
        if (isJobCancelled(job)) {

            System.out.println(
                    "JOB CANCELLED - "
                            + "SKIPPING FAILURE HANDLING: "
                            + job.getId()
            );

            return;
        }

        int nextAttempt =
                job.getAttemptCount() + 1;

        job.setAttemptCount(
                nextAttempt
        );

        job.setLastError(
                e.getMessage()
        );

        job.setWorkerId(null);

        job.setClaimedAt(null);

        job.setLeaseUntil(null);

        /*
         * ======================================
         * RETRY AVAILABLE
         * ======================================
         */

        if (nextAttempt < job.getMaxAttempts()) {

            /*
             * Exponential backoff:
             *
             * attempt 1 -> 2 sec
             * attempt 2 -> 4 sec
             * attempt 3 -> 8 sec
             * attempt 4 -> 16 sec
             */
            long delaySeconds =
                    (long) Math.pow(
                            2,
                            nextAttempt
                    );

            LocalDateTime retryAt =
                    LocalDateTime.now()
                            .plusSeconds(
                                    delaySeconds
                            );

            job.setStatus(
                    JobStatus.RETRYING
            );

            job.setScheduledAt(
                    retryAt
            );

            job.setUpdatedAt(
                    LocalDateTime.now()
            );

            jobRepository.save(job);

            System.out.println(
                    "========================================"
            );

            System.out.println(
                    "JOB FAILED: "
                            + job.getId()
            );

            System.out.println(
                    "ERROR: "
                            + e.getMessage()
            );

            System.out.println(
                    "ATTEMPT: "
                            + nextAttempt
                            + "/"
                            + job.getMaxAttempts()
            );

            System.out.println(
                    "RETRY DELAY: "
                            + delaySeconds
                            + " seconds"
            );

            System.out.println(
                    "RETRY AT: "
                            + retryAt
            );

            System.out.println(
                    "========================================"
            );

        } else {

            /*
             * ======================================
             * NO RETRIES LEFT
             * ======================================
             */

            job.setStatus(
                    JobStatus.DEAD
            );

            job.setScheduledAt(
                    null
            );

            job.setUpdatedAt(
                    LocalDateTime.now()
            );

            jobRepository.save(job);

            System.out.println(
                    "========================================"
            );

            System.out.println(
                    "JOB PERMANENTLY FAILED: "
                            + job.getId()
            );

            System.out.println(
                    "STATUS: DEAD"
            );

            System.out.println(
                    "ATTEMPTS: "
                            + nextAttempt
            );

            System.out.println(
                    "========================================"
            );
        }
    }

    /*
     * ==========================================
     * JOB EXECUTION
     * ==========================================
     *
     * Returns:
     *
     * true  -> cancelled
     * false -> completed execution normally
     */

    private boolean executeJob(
            Job job
    ) {

        switch (job.getType()) {

            case "SEND_EMAIL" -> {

                System.out.println(
                        "Sending email: "
                                + job.getPayload()
                );

                /*
                 * 15-second test job.
                 *
                 * Check cancellation every 500ms.
                 */
                long totalDuration =
                        15000;

                long checkInterval =
                        500;

                long elapsed =
                        0;

                while (elapsed < totalDuration) {

                    /*
                     * ==================================
                     * CHECK WORKER SHUTDOWN
                     * ==================================
                     */

                    if (!running.get()) {

                        System.out.println(
                                "JOB EXECUTION INTERRUPTED "
                                        + "BY WORKER SHUTDOWN: "
                                        + job.getId()
                        );

                        /*
                         * Graceful shutdown normally waits
                         * for this job to finish, so this
                         * branch should rarely be reached.
                         */
                        return false;
                    }

                    /*
                     * ==================================
                     * CHECK DATABASE CANCELLATION
                     * ==================================
                     */

                    if (isJobCancelled(job)) {

                        System.out.println(
                                "========================================"
                        );

                        System.out.println(
                                "CANCELLATION DETECTED"
                        );

                        System.out.println(
                                "JOB: "
                                        + job.getId()
                        );

                        System.out.println(
                                "STOPPING JOB EXECUTION"
                        );

                        System.out.println(
                                "========================================"
                        );

                        return true;
                    }

                    try {

                        Thread.sleep(
                                checkInterval
                        );

                    } catch (InterruptedException e) {

                        Thread.currentThread()
                                .interrupt();

                        if (isJobCancelled(job)) {

                            System.out.println(
                                    "JOB CANCELLED "
                                            + "DURING EXECUTION: "
                                            + job.getId()
                            );

                            return true;
                        }

                        throw new RuntimeException(
                                "Job interrupted",
                                e
                        );
                    }

                    elapsed +=
                            checkInterval;
                }

                return false;
            }

            default -> throw new IllegalArgumentException(
                    "Unknown job type: "
                            + job.getType()
            );
        }
    }

    /*
     * ==========================================
     * SMART LIFECYCLE
     * ==========================================
     */

    @Override
    public boolean isRunning() {

        return running.get();
    }

    @Override
    public boolean isAutoStartup() {

        return true;
    }

    @Override
    public int getPhase() {

        return 100;
    }

    @Override
    public void start() {

        running.set(true);

        System.out.println(
                "WORKER STARTED: "
                        + workerIdentity.getWorkerId()
        );
    }

    @Override
    public void stop() {

        running.set(false);

        System.out.println(
                "WORKER STOPPED"
        );
    }

    /*
     * ==========================================
     * GRACEFUL SHUTDOWN
     * ==========================================
     */

    @Override
    public void stop(
            Runnable callback
    ) {

        System.out.println(
                "========================================"
        );

        System.out.println(
                "GRACEFUL SHUTDOWN STARTED"
        );

        System.out.println(
                "WORKER: "
                        + workerIdentity.getWorkerId()
        );

        /*
         * Stop accepting NEW jobs.
         */
        running.set(false);

        System.out.println(
                "WORKER STOPPED ACCEPTING NEW JOBS"
        );

        /*
         * Wait for the current job to finish.
         *
         * JobHeartbeat continues renewing the
         * database lease while the job runs.
         */
        while (jobRunning.get()) {

            System.out.println(
                    "WAITING FOR CURRENT JOB "
                            + "TO FINISH..."
            );

            try {

                Thread.sleep(500);

            } catch (InterruptedException e) {

                Thread.currentThread()
                        .interrupt();

                System.out.println(
                        "SHUTDOWN WAIT INTERRUPTED"
                );

                break;
            }
        }

        System.out.println(
                "CURRENT JOB FINISHED"
        );

        System.out.println(
                "WORKER SHUTDOWN COMPLETE"
        );

        System.out.println(
                "========================================"
        );

        callback.run();
    }

    /*
     * ==========================================
     * CURRENT JOB ACCESS
     * ==========================================
     */

    public Job getCurrentJob() {

        return currentJob;
    }

    public boolean isJobRunning() {

        return jobRunning.get();
    }
}