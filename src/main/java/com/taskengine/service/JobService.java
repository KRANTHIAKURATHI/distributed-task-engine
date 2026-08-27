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

        job.setAttemptCount(0);

        job.setWorkerId(null);

        job.setClaimedAt(null);

        job.setLeaseUntil(null);

        job.setLastError(null);

        job.setScheduledAt(
                LocalDateTime.now()
        );

        job.setUpdatedAt(
                LocalDateTime.now()
        );

        /*
         * Save PostgreSQL first.
         */

        Job savedJob =
                jobRepository.save(job);

        /*
         * Put the job back into Redis.
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

    /*
     * ==========================================
     * CANCEL JOB
     * ==========================================
     */

    @Transactional
    public Job cancelJob(UUID jobId) {

        LocalDateTime now =
                LocalDateTime.now();

        /*
         * ======================================
         * STEP 1
         * ======================================
         *
         * Try to cancel PENDING or RETRYING job.
         */

        int cancelled =
                jobRepository.cancelPendingOrRetryingJob(
                        jobId,
                        JobStatus.PENDING,
                        JobStatus.RETRYING,
                        JobStatus.CANCELLED,
                        now
                );

        if (cancelled == 1) {

            System.out.println(
                    "JOB CANCELLED: "
                            + jobId
            );

            return jobRepository.findById(
                    jobId
            ).orElseThrow();
        }

        /*
         * ======================================
         * STEP 2
         * ======================================
         *
         * If it wasn't PENDING/RETRYING,
         * check whether it is PROCESSING.
         */

        Job job =
                jobRepository.findById(jobId)
                        .orElseThrow(
                                () -> new RuntimeException(
                                        "Job not found: "
                                                + jobId
                                )
                        );

        /*
         * Only PROCESSING jobs need worker
         * ownership validation.
         */

        if (job.getStatus()
                == JobStatus.PROCESSING) {

            String workerId =
                    job.getWorkerId();

            if (workerId == null) {

                throw new IllegalStateException(
                        "Processing job has no worker owner"
                );
            }

            int processingCancelled =
                    jobRepository.cancelProcessingJob(
                            jobId,
                            workerId,
                            JobStatus.PROCESSING,
                            JobStatus.CANCELLED,
                            now
                    );

            if (processingCancelled == 1) {

                System.out.println(
                        "PROCESSING JOB CANCELLED: "
                                + jobId
                );

                return jobRepository.findById(
                        jobId
                ).orElseThrow();
            }

            /*
             * The worker may have lost the lease
             * or another state transition happened
             * concurrently.
             */

            throw new IllegalStateException(
                    "Job could not be cancelled because "
                            + "ownership or lease was lost"
            );
        }

        /*
         * ======================================
         * OTHER STATES
         * ======================================
         */

        if (job.getStatus()
                == JobStatus.CANCELLED) {

            throw new IllegalStateException(
                    "Job is already CANCELLED"
            );
        }

        if (job.getStatus()
                == JobStatus.COMPLETED) {

            throw new IllegalStateException(
                    "Completed job cannot be cancelled"
            );
        }

        if (job.getStatus()
                == JobStatus.DEAD) {

            throw new IllegalStateException(
                    "DEAD job cannot be cancelled. "
                            + "Use the reprocess API first."
            );
        }

        throw new IllegalStateException(
                "Job cannot be cancelled. "
                        + "Current status: "
                        + job.getStatus()
        );
    }
}