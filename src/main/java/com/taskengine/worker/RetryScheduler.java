package com.taskengine.worker;

import com.taskengine.entity.Job;
import com.taskengine.enums.JobStatus;
import com.taskengine.queue.JobQueue;
import com.taskengine.repository.JobRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Component
public class RetryScheduler {

    private final JobRepository jobRepository;
    private final JobQueue jobQueue;

    public RetryScheduler(
            JobRepository jobRepository,
            JobQueue jobQueue
    ) {
        this.jobRepository = jobRepository;
        this.jobQueue = jobQueue;
    }

    /*
     * Check for jobs ready for retry every second.
     */
    @Scheduled(fixedDelay = 1000)
    public void processRetries() {

        LocalDateTime now = LocalDateTime.now();

        System.out.println(
                "RETRY CHECK: " + now
        );

        List<Job> retryJobs =
                jobRepository.findByStatusAndScheduledAtBefore(
                        JobStatus.RETRYING,
                        now
                );

        System.out.println(
                "RETRY JOBS READY: "
                        + retryJobs.size()
        );

        for (Job job : retryJobs) {

            try {

                /*
                 * ==============================
                 * MOVE RETRYING → PENDING
                 * ==============================
                 */

                job.setStatus(JobStatus.PENDING);

                job.setUpdatedAt(now);

                jobRepository.save(job);

                /*
                 * ==============================
                 * PUT JOB BACK INTO REDIS
                 * ==============================
                 */

                jobQueue.enqueueJob(job);

                System.out.println(
                        "RETRYING JOB REQUEUED: "
                                + job.getId()
                );

                System.out.println(
                        "ATTEMPT: "
                                + job.getAttemptCount()
                                + "/"
                                + job.getMaxAttempts()
                );

            } catch (Exception e) {

                System.out.println(
                        "Failed to requeue retry job: "
                                + job.getId()
                );

                System.out.println(
                        "Error: "
                                + e.getMessage()
                );
            }
        }
    }
}