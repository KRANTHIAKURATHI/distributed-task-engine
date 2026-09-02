package com.taskengine.scheduler;

import com.taskengine.entity.Job;
import com.taskengine.enums.JobStatus;
import com.taskengine.queue.JobQueue;
import com.taskengine.repository.JobRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Component
public class DispatchedJobRecovery {

    private final JobRepository jobRepository;
    private final JobQueue jobQueue;

    public DispatchedJobRecovery(
            JobRepository jobRepository,
            JobQueue jobQueue
    ) {
        this.jobRepository = jobRepository;
        this.jobQueue = jobQueue;
    }

    /*
     * ==========================================
     * DISPATCHED JOB RECOVERY
     * ==========================================
     *
     * Finds jobs that were changed:
     *
     * PENDING → DISPATCHED
     *
     * but never reached Redis.
     *
     * A job is considered stale when it has been
     * DISPATCHED for more than 5 seconds.
     */
    //@Scheduled(fixedDelay = 5000)
    public void recoverDispatchedJobs() {

        LocalDateTime cutoff =
                LocalDateTime.now()
                        .minusSeconds(5);

        List<Job> jobs =
                jobRepository
                        .findByStatusAndUpdatedAtBefore(
                                JobStatus.DISPATCHED,
                                cutoff
                        );

        if (jobs.isEmpty()) {
            return;
        }

        System.out.println(
                "========================================"
        );

        System.out.println(
                "DISPATCHED JOB RECOVERY"
        );

        System.out.println(
                "STALE JOBS FOUND: "
                        + jobs.size()
        );

        for (Job job : jobs) {

            System.out.println(
                    "RECOVERING DISPATCHED JOB: "
                            + job.getId()
            );

            /*
             * Put the job back into Redis.
             */
            jobQueue.enqueue(
                    job.getId()
            );

            /*
             * Refresh timestamp so another recovery
             * cycle doesn't immediately enqueue the
             * same job again.
             */
            job.setUpdatedAt(
                    LocalDateTime.now()
            );

            jobRepository.save(job);

            System.out.println(
                    "DISPATCHED JOB REQUEUED: "
                            + job.getId()
            );
        }

        System.out.println(
                "========================================"
        );
    }
}