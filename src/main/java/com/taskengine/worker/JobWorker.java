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
     * true -> worker currently executing a job
     */
    private final AtomicBoolean jobRunning =
            new AtomicBoolean(false);

    /*
     * Current job being executed.
     *
     * JobHeartbeat uses this during graceful
     * shutdown to keep the lease alive.
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
         * Do not accept new jobs after shutdown
         * begins.
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

        /*
         * Double-check shutdown state before
         * touching Redis.
         */
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
             *
             * Do not turn this into a failed job.
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
         * We now own a job that was received
         * from Redis.
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
         * Make current job visible to heartbeat.
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

            executeJob(job);

            /*
             * ======================================
             * COMPLETE JOB
             * ======================================
             *
             * IMPORTANT:
             *
             * Do NOT use:
             *
             * job.setStatus(COMPLETED);
             * jobRepository.save(job);
             *
             * because another worker may have
             * reclaimed this job.
             *
             * Instead perform an atomic ownership
             * check in PostgreSQL.
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

                /*
                 * We still own the job.
                 */
                System.out.println(
                        "COMPLETED JOB: "
                                + job.getId()
                );

            } else {

                /*
                 * Another worker/reaper changed
                 * the job before we completed it.
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
                                + "reclaimed by another worker."
                );

                System.out.println(
                        "========================================"
                );
            }

        } catch (Exception e) {

            /*
             * ======================================
             * JOB FAILED
             * ======================================
             */

            handleFailure(
                    job,
                    e
            );

        } finally {

            /*
             * Job execution is finished.
             */
            currentJob = null;

            jobRunning.set(false);
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
         * If the lease was already lost, don't
         * blindly overwrite the state that another
         * worker may have established.
         *
         * For now we preserve the existing retry
         * behavior.
         */

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
             * attempt 1 -> 2 seconds
             * attempt 2 -> 4 seconds
             * attempt 3 -> 8 seconds
             * attempt 4 -> 16 seconds
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
     */

    private void executeJob(Job job) {

        switch (job.getType()) {

            case "SEND_EMAIL" -> {

                System.out.println(
                        "Sending email: "
                                + job.getPayload()
                );

                try {

                    /*
                     * 15-second test job.
                     *
                     * Keep this for crash/shutdown
                     * testing.
                     */
                    Thread.sleep(15000);

                } catch (InterruptedException e) {

                    Thread.currentThread()
                            .interrupt();

                    throw new RuntimeException(
                            "Job interrupted",
                            e
                    );
                }
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
         * Stop accepting NEW jobs.
         */
        running.set(false);

        System.out.println(
                "WORKER STOPPED ACCEPTING NEW JOBS"
        );

        /*
         * Wait for the current job to finish.
         *
         * JobHeartbeat continues renewing its
         * lease while this happens.
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

        /*
         * Tell Spring lifecycle processing that
         * this worker has completely stopped.
         */
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