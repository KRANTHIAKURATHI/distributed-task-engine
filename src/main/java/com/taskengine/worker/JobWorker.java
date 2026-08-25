package com.taskengine.worker;

import com.taskengine.entity.Job;
import com.taskengine.enums.JobStatus;
import com.taskengine.queue.JobQueue;
import com.taskengine.repository.JobRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
public class JobWorker {

    private final WorkerIdentity workerIdentity;
    private final JobQueue jobQueue;
    private final JobRepository jobRepository;

    public JobWorker(
            WorkerIdentity workerIdentity,
            JobQueue jobQueue,
            JobRepository jobRepository
    ) {
        this.workerIdentity = workerIdentity;
        this.jobQueue = jobQueue;
        this.jobRepository = jobRepository;
    }

    @Scheduled(fixedDelay = 1000)
    public void run() {
        processNextJob();
    }

    public void processNextJob() {

        // Take a job from Redis
        String jobId = jobQueue.claimJob();

        if (jobId == null) {
            return;
        }

        UUID id = UUID.fromString(jobId);

        // Find the job in PostgreSQL
        Job job = jobRepository.findById(id)
                .orElse(null);

        if (job == null) {
            System.out.println(
                    "Job not found: " + jobId
            );
            return;
        }

        try {

            /*
             * ==============================
             * CLAIM THE JOB
             * ==============================
             */

            LocalDateTime now = LocalDateTime.now();

            job.setStatus(JobStatus.PROCESSING);

            // Identify which worker owns this job
            job.setWorkerId(
                    workerIdentity.getWorkerId()
            );

            // Record when the job was claimed
            job.setClaimedAt(now);

            // TEMPORARY: 10-second lease
            // We are using 10 seconds so that
            // the 40-second test job expires.
            job.setLeaseUntil(
                    now.plusSeconds(30)
            );

            job.setUpdatedAt(now);

            jobRepository.save(job);

            System.out.println(
                    "========================================"
            );

            System.out.println(
                    "Worker: "
                            + workerIdentity.getWorkerId()
            );

            System.out.println(
                    "Claimed job: "
                            + job.getId()
            );

            System.out.println(
                    "Lease until: "
                            + job.getLeaseUntil()
            );

            /*
             * ==============================
             * EXECUTE JOB
             * ==============================
             */

            System.out.println(
                    "Processing job: "
                            + job.getId()
            );

            executeJob(job);

            /*
             * ==============================
             * JOB COMPLETED
             * ==============================
             */

            job.setStatus(JobStatus.COMPLETED);

            // Remove worker ownership
            job.setWorkerId(null);

            // Remove claim timestamp
            job.setClaimedAt(null);

            // Remove lease
            job.setLeaseUntil(null);

            job.setUpdatedAt(
                    LocalDateTime.now()
            );

            jobRepository.save(job);

            System.out.println(
                    "Completed job: "
                            + job.getId()
            );

            System.out.println(
                    "========================================"
            );

        } catch (Exception e) {

            /*
             * ==============================
             * JOB FAILED
             * ==============================
             */

            job.setStatus(JobStatus.FAILED);

            job.setLastError(
                    e.getMessage()
            );

            // Remove worker ownership
            job.setWorkerId(null);

            // Remove claim timestamp
            job.setClaimedAt(null);

            // Remove lease
            job.setLeaseUntil(null);

            job.setUpdatedAt(
                    LocalDateTime.now()
            );

            jobRepository.save(job);

            System.out.println(
                    "Failed job: "
                            + job.getId()
            );

            System.out.println(
                    "Error: "
                            + e.getMessage()
            );
        }
    }

    private void executeJob(Job job) {

        switch (job.getType()) {

            case "SEND_EMAIL" -> {

                System.out.println(
                        "Sending email: "
                                + job.getPayload()
                );

                try {

                    /*
                     * TEMPORARY TEST
                     *
                     * Job takes 40 seconds.
                     * Lease lasts 10 seconds.
                     *
                     * Therefore, the lease should
                     * expire while the job is running.
                     */

                    Thread.sleep(60000);

                } catch (InterruptedException e) {

                    Thread.currentThread().interrupt();

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
}