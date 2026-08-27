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
     * PROCESS NEXT JOB
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

        String jobId;

        try {

            jobId =
                    jobQueue.claimJob(workerId);

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

        if (jobId == null) {
            return;
        }

        /*
         * ==========================================
         * JOB RECEIVED
         * ==========================================
         */

        jobRunning.set(true);

        System.out.println(
                "WORKER RECEIVED JOB: "
                        + jobId
        );

        UUID id;

        try {

            id =
                    UUID.fromString(jobId);

        } catch (IllegalArgumentException e) {

            System.out.println(
                    "INVALID JOB ID FROM REDIS: "
                            + jobId
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

            jobRunning.set(false);

            return;
        }

        /*
         * ==========================================
         * IMPORTANT:
         * CHECK CANCELLATION BEFORE CLAIMING
         * ==========================================
         *
         * This protects against a job that was
         * cancelled while it was sitting in Redis.
         */

        if (job.getStatus() == JobStatus.CANCELLED) {

            System.out.println(
                    "SKIPPING CANCELLED JOB: "
                            + job.getId()
            );

            currentJob = null;

            jobRunning.set(false);

            return;
        }

        /*
         * Make current job visible to shutdown logic.
         */
        currentJob = job;

        try {

            /*
             * ======================================
             * CLAIM JOB
             * ======================================
             */

            LocalDateTime now =
                    LocalDateTime.now();

            /*
             * Re-check cancellation immediately
             * before changing state.
             */
            Job latestJob =
                    jobRepository.findById(id)
                            .orElse(null);

            if (latestJob == null) {

                System.out.println(
                        "JOB DISAPPEARED BEFORE CLAIM: "
                                + id
                );

                return;
            }

            if (latestJob.getStatus()
                    == JobStatus.CANCELLED) {

                System.out.println(
                        "JOB CANCELLED BEFORE CLAIM: "
                                + id
                );

                return;
            }

            /*
             * Use the latest database entity.
             */
            job = latestJob;

            currentJob = job;

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

            jobRepository.save(job);

            System.out.println(
                    "========================================"
            );

            System.out.println(
                    "WORKER: "
                            + workerId
            );

            System.out.println(
                    "CLAIMED JOB: "
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

            /*
             * ======================================
             * EXECUTE JOB
             * ======================================
             */

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
                        "WORKER: "
                                + workerId
                );

                System.out.println(
                        "JOB WILL NOT BE COMPLETED"
                );

                System.out.println(
                        "========================================"
                );

                return;
            }

            /*
             * ======================================
             * FINAL CANCELLATION CHECK
             * ======================================
             *
             * The job might have been cancelled
             * immediately after executeJob() returned.
             *
             * Check PostgreSQL one final time before
             * attempting COMPLETED.
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

                return;
            }

            if (finalJob.getStatus()
                    == JobStatus.CANCELLED) {

                System.out.println(
                        "========================================"
                );

                System.out.println(
                        "CANCELLATION DETECTED "
                                + "BEFORE COMPLETION"
                );

                System.out.println(
                        "JOB: "
                                + job.getId()
                );

                System.out.println(
                        "JOB WILL NOT BE COMPLETED"
                );

                System.out.println(
                        "========================================"
                );

                return;
            }

            /*
             * ======================================
             * COMPLETE JOB
             * ======================================
             *
             * Atomic ownership + lease check.
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
                        "COMPLETED JOB: "
                                + job.getId()
                );

            } else {

                /*
                 * Another worker/reaper/cancellation
                 * changed the job.
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
                        "WORKER: "
                                + workerId
                );

                System.out.println(
                        "The job may have been "
                                + "cancelled or reclaimed."
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

            /*
             * Do not treat cancellation as a normal
             * retryable failure.
             */
            if (isJobCancelled(job)) {

                System.out.println(
                        "JOB WAS CANCELLED: "
                                + job.getId()
                );

                return;
            }

            handleFailure(
                    job,
                    e
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

    private boolean isJobCancelled(Job job) {

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
             * If the database temporarily fails,
             * don't incorrectly assume cancellation.
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
         * Check cancellation before changing the
         * job to RETRYING or DEAD.
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

            job.setScheduledAt(null);

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
     * true  -> job was cancelled
     * false -> job completed execution normally
     */

    private boolean executeJob(Job job) {

        switch (job.getType()) {

            case "SEND_EMAIL" -> {

                System.out.println(
                        "Sending email: "
                                + job.getPayload()
                );

                /*
                 * 15-second test job.
                 *
                 * Instead of one Thread.sleep(15000),
                 * sleep in 500ms intervals and check
                 * PostgreSQL for cancellation.
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
                         * Do not mark it completed.
                         *
                         * Graceful shutdown waits for the
                         * current job, so normally this
                         * branch won't occur during a normal
                         * shutdown.
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

                        /*
                         * Check whether the interruption
                         * happened because the job was
                         * cancelled.
                         */
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
    public void stop(Runnable callback) {

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
         * Wait for the current job to finish.
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