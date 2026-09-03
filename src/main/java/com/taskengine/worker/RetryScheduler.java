package com.taskengine.worker;

import com.taskengine.entity.Job;
import com.taskengine.enums.JobStatus;
import com.taskengine.metrics.TaskEngineMetrics;
import com.taskengine.repository.JobRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Component
public class RetryScheduler {

    private final JobRepository jobRepository;
    private final TaskEngineMetrics metrics;

    public RetryScheduler(
            JobRepository jobRepository,
            TaskEngineMetrics metrics
    ) {

        this.jobRepository =
                jobRepository;

        this.metrics =
                metrics;
    }

    /*
     * ==========================================
     * RETRY SCHEDULER
     * ==========================================
     *
     * Checks every second for jobs whose
     * retry delay has expired.
     *
     * RETRYING → PENDING
     *
     * PriorityScheduler will then handle:
     *
     * PENDING → DISPATCHED → Redis
     *
     * This keeps retry jobs inside the same
     * priority scheduling pipeline as new jobs.
     */
    @Scheduled(fixedDelay = 1000)
    public void processRetries() {

        LocalDateTime now =
                LocalDateTime.now();

        List<Job> retryJobs =
                jobRepository
                        .findByStatusAndScheduledAtBefore(
                                JobStatus.RETRYING,
                                now
                        );

        if (retryJobs.isEmpty()) {
            return;
        }

        System.out.println(
                "========================================"
        );

        System.out.println(
                "RETRY SCHEDULER"
        );

        System.out.println(
                "RETRY JOBS READY: "
                        + retryJobs.size()
        );

        /*
         * ======================================
         * PROCESS RETRY JOBS
         * ======================================
         */

        for (Job job : retryJobs) {

            try {

                /*
                 * ==================================
                 * MOVE RETRYING → PENDING
                 * ==================================
                 *
                 * Do NOT enqueue directly into Redis.
                 *
                 * PriorityScheduler will pick this
                 * job according to priority.
                 */

                job.setStatus(
                        JobStatus.PENDING
                );

                /*
                 * scheduledAt is no longer needed
                 * once the retry becomes pending.
                 */
                job.setScheduledAt(
                        null
                );

                job.setUpdatedAt(
                        now
                );

                jobRepository.save(
                        job
                );

                /*
                 * ==================================
                 * RECORD RETRY
                 * ==================================
                 *
                 * The retry was successfully released
                 * back into the scheduling pipeline.
                 */
                metrics.jobRetried();

                System.out.println(
                        "RETRY JOB MOVED TO PENDING: "
                                + job.getId()
                );

                System.out.println(
                        "PRIORITY: "
                                + job.getPriority()
                );

                System.out.println(
                        "ATTEMPT: "
                                + job.getAttemptCount()
                                + "/"
                                + job.getMaxAttempts()
                );

                System.out.println(
                        "WAITING FOR PRIORITY SCHEDULER"
                );

            } catch (Exception e) {

                System.out.println(
                        "========================================"
                );

                System.out.println(
                        "RETRY SCHEDULER ERROR"
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
                        "========================================"
                );
            }
        }

        System.out.println(
                "========================================"
        );
    }
}

