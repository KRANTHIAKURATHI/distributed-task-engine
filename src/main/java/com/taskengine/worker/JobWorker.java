package com.taskengine.worker;

import com.taskengine.entity.Job;
import com.taskengine.enums.JobStatus;
import com.taskengine.queue.JobQueue;
import com.taskengine.repository.JobRepository;
import com.taskengine.service.IdempotencyService;
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
    private final IdempotencyService idempotencyService;

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
     * JobHeartbeat uses this to renew
     * the database lease.
     */
    private volatile Job currentJob;

    public JobWorker(
            WorkerIdentity workerIdentity,
            JobQueue jobQueue,
            JobRepository jobRepository,
            IdempotencyService idempotencyService
    ) {

        this.workerIdentity =
                workerIdentity;

        this.jobQueue =
                jobQueue;

        this.jobRepository =
                jobRepository;

        this.idempotencyService =
                idempotencyService;
    }

    /*
     * ==========================================
     * WORKER LOOP
     * ==========================================
     */

    @Scheduled(fixedDelay = 1000)
    public void run() {

        if (!running.get()) {
            return;
        }

        processNextJob();
    }

    /*
     * ==========================================
     * PROCESS NEXT NEW JOB
     * ==========================================
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
     * Used by:
     *
     * 1. Normal Redis XREADGROUP
     *
     * 2. PendingJobRecovery / XCLAIM
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

            jobQueue.acknowledge(
                    redisRecordId
            );

            jobRunning.set(false);

            return;
        }

        /*
         * ==========================================
         * LOAD JOB
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

            jobQueue.acknowledge(
                    redisRecordId
            );

            jobRunning.set(false);

            return;
        }

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
             * CANCELLATION CHECK
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
             * DETERMINE DATABASE STATE
             * ======================================
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

                currentJob =
                        job;

                LocalDateTime now =
                        LocalDateTime.now();

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
                 * 30-second lease.
                 */
                job.setLeaseUntil(
                        now.plusSeconds(30)
                );

                job.setUpdatedAt(
                        now
                );

                jobRepository.save(
                        job
                );

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
             * Redis XCLAIM transferred ownership.
             *
             * The PostgreSQL job should already be
             * PROCESSING.
             */

            else if (recoveredJob) {

                LocalDateTime now =
                        LocalDateTime.now();

                /*
                 * Another worker may still own this job.
                 *
                 * Never steal a valid lease.
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
                            "JOB STILL HAS VALID LEASE"
                    );

                    System.out.println(
                            "JOB: "
                                    + latestJob.getId()
                    );

                    System.out.println(
                            "CURRENT WORKER: "
                                    + latestJob.getWorkerId()
                    );

                    System.out.println(
                            "LEASE UNTIL: "
                                    + latestJob.getLeaseUntil()
                    );

                    System.out.println(
                            "========================================"
                    );

                    /*
                     * IMPORTANT:
                     *
                     * Do not ACK.
                     *
                     * Redis message remains pending.
                     */
                    return;
                }

                /*
                 * ==================================
                 * LEASE EXPIRED
                 * ==================================
                 *
                 * This worker can take ownership.
                 */

                job =
                        latestJob;

                currentJob =
                        job;

                job.setWorkerId(
                        workerId
                );

                job.setClaimedAt(
                        now
                );

                job.setLeaseUntil(
                        now.plusSeconds(30)
                );

                job.setUpdatedAt(
                        now
                );

                job.setStatus(
                        JobStatus.PROCESSING
                );

                jobRepository.save(
                        job
                );

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
                        "PREVIOUS WORKER: "
                                + latestJob.getWorkerId()
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
             * UNKNOWN STATE
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

                jobQueue.acknowledge(
                        redisRecordId
                );

                return;
            }

            /*
             * ======================================
             * IDEMPOTENCY CHECK
             * ======================================
             *
             * IMPORTANT:
             *
             * We do NOT simply check whether an
             * execution record exists.
             *
             * A PROCESSING execution may belong to
             * a worker that crashed.
             *
             * Therefore IdempotencyService must
             * distinguish an active execution from
             * a completed execution.
             */

            boolean executionStarted =
                    idempotencyService.tryStartExecution(
                            job.getId()
                    );

            if (!executionStarted) {

                /*
                 * The job has already been successfully
                 * registered as an execution.
                 *
                 * Do not execute it again.
                 */
                System.out.println(
                        "========================================"
                );

                System.out.println(
                        "DUPLICATE EXECUTION DETECTED"
                );

                System.out.println(
                        "SKIPPING JOB: "
                                + job.getId()
                );

                System.out.println(
                        "========================================"
                );

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
                    "EXECUTING JOB"
            );

            System.out.println(
                    "JOB: "
                            + job.getId()
            );

            System.out.println(
                    "TYPE: "
                            + job.getType()
            );

            System.out.println(
                    "========================================"
            );

            boolean cancelled =
                    executeJob(job);

            /*
             * ======================================
             * CANCELLATION
             * ======================================
             */

            if (cancelled) {

                System.out.println(
                        "JOB EXECUTION CANCELLED: "
                                + job.getId()
                );

                jobQueue.acknowledge(
                        redisRecordId
                );

                return;
            }

            /*
             * ======================================
             * MARK EXECUTION COMPLETED
             * ======================================
             */

            idempotencyService.markCompleted(
                    job.getId()
            );

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
             * Cancellation may have happened while
             * the job was executing.
             */
            if (finalJob.getStatus()
                    == JobStatus.CANCELLED) {

                System.out.println(
                        "CANCELLATION DETECTED "
                                + "BEFORE JOB COMPLETION"
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
             */

            LocalDateTime completionTime =
                    LocalDateTime.now();

            int completed =
                    jobRepository.completeJobIfOwned(
                            job.getId(),
                            workerId,
                            JobStatus.PROCESSING,
                            JobStatus.COMPLETED,
                            completionTime
                    );

            if (completed == 1) {

                System.out.println(
                        "========================================"
                );

                System.out.println(
                        "JOB COMPLETED"
                );

                System.out.println(
                        "JOB: "
                                + job.getId()
                );

                System.out.println(
                        "========================================"
                );

                /*
                 * ACK only after database completion.
                 */
                jobQueue.acknowledge(
                        redisRecordId
                );

            } else {

                /*
                 * Ownership was lost.
                 *
                 * Do NOT ACK.
                 *
                 * Recovery can process the Redis
                 * message again.
                 */
                System.out.println(
                        "========================================"
                );

                System.out.println(
                        "JOB COMPLETION REJECTED"
                );

                System.out.println(
                        "OWNERSHIP OR LEASE LOST"
                );

                System.out.println(
                        "JOB: "
                                + job.getId()
                );

                System.out.println(
                        "REDIS MESSAGE REMAINS PENDING"
                );

                System.out.println(
                        "RECORD: "
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

                jobQueue.acknowledge(
                        redisRecordId
                );

                return;
            }

            handleFailure(
                    job,
                    e
            );

            /*
             * RetryScheduler will create the next
             * Redis message.
             */
            jobQueue.acknowledge(
                    redisRecordId
            );

        } finally {

            currentJob =
                    null;

            jobRunning.set(false);
        }
    }

    /*
     * ==========================================
     * CANCELLATION CHECK
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

        job.setWorkerId(
                null
        );

        job.setClaimedAt(
                null
        );

        job.setLeaseUntil(
                null
        );

        /*
         * ======================================
         * RETRY
         * ======================================
         */

        if (nextAttempt < job.getMaxAttempts()) {

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

            jobRepository.save(
                    job
            );

            System.out.println(
                    "========================================"
            );

            System.out.println(
                    "JOB FAILED"
            );

            System.out.println(
                    "JOB: "
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
                    "RETRY AT: "
                            + retryAt
            );

            System.out.println(
                    "========================================"
            );

        } else {

            /*
             * ======================================
             * DEAD LETTER QUEUE
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

            jobRepository.save(
                    job
            );

            System.out.println(
                    "========================================"
            );

            System.out.println(
                    "JOB PERMANENTLY FAILED"
            );

            System.out.println(
                    "JOB: "
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
                 */
                long totalDuration =
                        15000;

                long checkInterval =
                        500;

                long elapsed =
                        0;

                while (elapsed < totalDuration) {

                    /*
                     * Worker shutdown.
                     */
                    if (!running.get()) {

                        System.out.println(
                                "JOB EXECUTION INTERRUPTED "
                                        + "BY WORKER SHUTDOWN: "
                                        + job.getId()
                        );

                        return false;
                    }

                    /*
                     * Database cancellation.
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
                                    "JOB CANCELLED DURING EXECUTION: "
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
         * Stop accepting new jobs.
         */
        running.set(false);

        System.out.println(
                "WORKER STOPPED ACCEPTING NEW JOBS"
        );

        /*
         * Wait for current job.
         */
        while (jobRunning.get()) {

            System.out.println(
                    "WAITING FOR CURRENT JOB TO FINISH..."
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

