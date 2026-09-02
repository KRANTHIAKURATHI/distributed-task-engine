package com.taskengine.service;

import com.taskengine.dto.CreateJobRequest;
import com.taskengine.entity.Job;
import com.taskengine.enums.JobStatus;
import com.taskengine.queue.JobQueue;
import com.taskengine.repository.JobRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
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
     *
     * The job is saved to PostgreSQL first.
     *
     * PriorityScheduler will dispatch the job
     * to Redis.
     */
    public Job createJob(
            CreateJobRequest request
    ) {

        Job job = new Job();

        job.setType(
                request.getType()
        );

        job.setPayload(
                request.getPayload()
        );

        job.setPriority(
                request.getPriority()
        );

        job.setScheduledAt(
                request.getScheduledAt()
        );

        job.setMaxAttempts(
                request.getMaxAttempts()
        );

        job.setStatus(
                JobStatus.PENDING
        );

        job.setAttemptCount(
                0
        );

        job.setCreatedAt(
                LocalDateTime.now()
        );

        job.setUpdatedAt(
                LocalDateTime.now()
        );

        /*
         * Save to PostgreSQL.
         */
        Job savedJob =
                jobRepository.save(job);

        System.out.println(
                "========================================"
        );

        System.out.println(
                "JOB CREATED: "
                        + savedJob.getId()
        );

        System.out.println(
                "PRIORITY: "
                        + savedJob.getPriority()
        );

        System.out.println(
                "STATUS: "
                        + savedJob.getStatus()
        );

        System.out.println(
                "WAITING FOR PRIORITY SCHEDULER"
        );

        System.out.println(
                "========================================"
        );

        return savedJob;
    }

    /*
     * ==========================================
     * GET JOB BY ID
     * ==========================================
     *
     * GET /api/v1/jobs/{id}
     */
    public Job getJobById(
            UUID jobId
    ) {

        return jobRepository
                .findById(jobId)
                .orElseThrow(
                        () -> new RuntimeException(
                                "Job not found: "
                                        + jobId
                        )
                );
    }

    /*
     * ==========================================
     * GET JOBS
     * ==========================================
     *
     * Supports:
     *
     * GET /api/v1/jobs
     *
     * GET /api/v1/jobs?page=0&size=10
     *
     * GET /api/v1/jobs?status=COMPLETED
     *
     * GET /api/v1/jobs?status=DEAD&page=0&size=5
     *
     * Jobs are sorted by newest first.
     */
    public Page<Job> getJobs(
            JobStatus status,
            int page,
            int size
    ) {

        /*
         * ======================================
         * VALIDATE PAGE
         * ======================================
         */

        if (page < 0) {

            throw new IllegalArgumentException(
                    "Page number cannot be negative"
            );
        }

        /*
         * ======================================
         * VALIDATE SIZE
         * ======================================
         */

        if (size < 1 || size > 100) {

            throw new IllegalArgumentException(
                    "Page size must be between 1 and 100"
            );
        }

        /*
         * ======================================
         * CREATE PAGINATION
         * ======================================
         *
         * Newest jobs first.
         */
        Pageable pageable =
                PageRequest.of(
                        page,
                        size,
                        Sort.by(
                                Sort.Direction.DESC,
                                "createdAt"
                        )
                );

        /*
         * ======================================
         * STATUS FILTER
         * ======================================
         *
         * If status is supplied:
         *
         * SELECT jobs WHERE status = ?
         *
         * Otherwise:
         *
         * SELECT all jobs.
         */
        if (status != null) {

            return jobRepository.findJobsByStatus(
                    status,
                    pageable
            );
        }

        /*
         * ======================================
         * ALL JOBS
         * ======================================
         */

        return jobRepository.findAll(
                pageable
        );
    }

    /*
     * ==========================================
     * DLQ INSPECTION
     * ==========================================
     *
     * Returns all DEAD jobs.
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
    public Job reprocessJob(
            UUID jobId
    ) {

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
        if (job.getStatus()
                != JobStatus.DEAD) {

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

        job.setAttemptCount(
                0
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

        job.setLastError(
                null
        );

        job.setScheduledAt(
                LocalDateTime.now()
        );

        job.setUpdatedAt(
                LocalDateTime.now()
        );

        /*
         * Save PostgreSQL.
         *
         * PriorityScheduler will dispatch it.
         */
        Job savedJob =
                jobRepository.save(job);

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
                "WAITING FOR PRIORITY SCHEDULER"
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
    public Job cancelJob(
            UUID jobId
    ) {

        LocalDateTime now =
                LocalDateTime.now();

        /*
         * ======================================
         * STEP 1
         * ======================================
         *
         * Try to cancel PENDING or RETRYING.
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
         * Check PROCESSING.
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
         * ======================================
         * PROCESSING JOB
         * ======================================
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

