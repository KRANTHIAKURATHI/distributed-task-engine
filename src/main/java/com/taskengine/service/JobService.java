package com.taskengine.service;

import com.taskengine.dto.CreateJobRequest;
import com.taskengine.entity.Job;
import com.taskengine.enums.JobStatus;
import com.taskengine.metrics.TaskEngineMetrics;
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
    private final TaskEngineMetrics metrics;

    public JobService(
            JobRepository jobRepository,
            JobQueue jobQueue,
            TaskEngineMetrics metrics
    ) {

        this.jobRepository =
                jobRepository;

        this.jobQueue =
                jobQueue;

        this.metrics =
                metrics;
    }

    /*
     * ==========================================
     * CREATE JOB
     * ==========================================
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

        /*
         * Record metric only after
         * successful database save.
         */
        metrics.jobCreated();

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
     */

    public Page<Job> getJobs(
            JobStatus status,
            int page,
            int size
    ) {

        if (page < 0) {

            throw new IllegalArgumentException(
                    "Page number cannot be negative"
            );
        }

        if (size < 1 || size > 100) {

            throw new IllegalArgumentException(
                    "Page size must be between 1 and 100"
            );
        }

        Pageable pageable =
                PageRequest.of(
                        page,
                        size,
                        Sort.by(
                                Sort.Direction.DESC,
                                "createdAt"
                        )
                );

        if (status != null) {

            return jobRepository.findJobsByStatus(
                    status,
                    pageable
            );
        }

        return jobRepository.findAll(
                pageable
        );
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

        if (job.getStatus()
                != JobStatus.DEAD) {

            throw new IllegalStateException(
                    "Only DEAD jobs can be reprocessed. "
                            + "Current status: "
                            + job.getStatus()
            );
        }

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
         * PENDING / RETRYING
         * ======================================
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

            /*
             * Record cancellation only after
             * the database update succeeds.
             */
            metrics.jobCancelled();

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
         * PROCESSING
         * ======================================
         */

        Job job =
                jobRepository.findById(jobId)
                        .orElseThrow(
                                () -> new RuntimeException(
                                        "Job not found: "
                                                + jobId
                                )
                        );

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

                /*
                 * Record cancellation only after
                 * ownership and lease validation
                 * succeed.
                 */
                metrics.jobCancelled();

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
         * ALREADY CANCELLED
         * ======================================
         */

        if (job.getStatus()
                == JobStatus.CANCELLED) {

            throw new IllegalStateException(
                    "Job is already CANCELLED"
            );
        }

        /*
         * ======================================
         * COMPLETED
         * ======================================
         */

        if (job.getStatus()
                == JobStatus.COMPLETED) {

            throw new IllegalStateException(
                    "Completed job cannot be cancelled"
            );
        }

        /*
         * ======================================
         * DEAD
         * ======================================
         */

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
