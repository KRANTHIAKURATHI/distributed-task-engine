package com.taskengine.service;

import com.taskengine.dto.CreateJobRequest;
import com.taskengine.entity.Job;
import com.taskengine.enums.JobStatus;
import com.taskengine.queue.JobQueue;
import com.taskengine.repository.JobRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class JobService {

    private final JobRepository jobRepository;
    private final JobQueue jobQueue;

    public JobService(
            JobRepository jobRepository,
            JobQueue jobQueue
    ) {
        this.jobRepository = jobRepository;
        this.jobQueue = jobQueue;
    }

    /*
     * ==========================================
     * CREATE JOB
     * ==========================================
     */

    public Job createJob(CreateJobRequest request) {

        Job job = new Job();

        job.setType(request.getType());
        job.setPayload(request.getPayload());
        job.setPriority(request.getPriority());
        job.setScheduledAt(request.getScheduledAt());
        job.setMaxAttempts(request.getMaxAttempts());

        job.setStatus(JobStatus.PENDING);
        job.setAttemptCount(0);
        job.setCreatedAt(LocalDateTime.now());
        job.setUpdatedAt(LocalDateTime.now());

        Job savedJob =
                jobRepository.save(job);

        jobQueue.enqueue(
                savedJob.getId()
        );

        return savedJob;
    }

    /*
     * ==========================================
     * DLQ INSPECTION
     * ==========================================
     *
     * Returns all permanently failed jobs.
     */

    public List<Job> getDeadJobs() {

        return jobRepository.findByStatus(
                JobStatus.DEAD
        );
    }

    /*
     * ==========================================
     * REPROCESS DEAD JOB
     * ==========================================
     *
     * DEAD → PENDING
     *
     * Reset execution state and put the job
     * back into Redis.
     */

    @Transactional
    public Job reprocessJob(UUID jobId) {

        Job job =
                jobRepository.findById(jobId)
                        .orElseThrow(
                                () -> new RuntimeException(
                                        "Job not found: "
                                                + jobId
                                )
                        );

        /*
         * Only DEAD jobs can be reprocessed.
         */

        if (job.getStatus() != JobStatus.DEAD) {

            throw new IllegalStateException(
                    "Only DEAD jobs can be reprocessed. "
                            + "Current status: "
                            + job.getStatus()
            );
        }

        /*
         * ======================================
         * RESET JOB STATE
         * ======================================
         */

        job.setStatus(
                JobStatus.PENDING
        );

        /*
         * Start a completely new retry cycle.
         */
        job.setAttemptCount(0);

        /*
         * Remove previous worker ownership.
         */
        job.setWorkerId(null);

        /*
         * Remove previous claim information.
         */
        job.setClaimedAt(null);

        /*
         * Remove old lease.
         */
        job.setLeaseUntil(null);

        /*
         * Remove previous failure reason.
         */
        job.setLastError(null);

        /*
         * Make it immediately eligible.
         */
        job.setScheduledAt(
                LocalDateTime.now()
        );

        job.setUpdatedAt(
                LocalDateTime.now()
        );

        /*
         * Save PostgreSQL state first.
         */
        Job savedJob =
                jobRepository.save(job);

        /*
         * Then put the job back into Redis.
         */
        jobQueue.enqueue(
                savedJob.getId()
        );

        System.out.println(
                "========================================"
        );

        System.out.println(
                "DLQ JOB REPROCESSED: "
                        + savedJob.getId()
        );

        System.out.println(
                "STATUS: "
                        + savedJob.getStatus()
        );

        System.out.println(
                "ATTEMPT COUNT RESET TO: "
                        + savedJob.getAttemptCount()
        );

        System.out.println(
                "JOB REQUEUED TO REDIS"
        );

        System.out.println(
                "========================================"
        );

        return savedJob;
    }
}