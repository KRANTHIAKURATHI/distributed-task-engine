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
public class JobReaper {

    private final JobRepository jobRepository;
    private final JobQueue jobQueue;

    public JobReaper(
            JobRepository jobRepository,
            JobQueue jobQueue
    ) {
        this.jobRepository = jobRepository;
        this.jobQueue = jobQueue;
    }

    @Scheduled(fixedDelay = 5000)
    public void reclaimExpiredJobs() {

        LocalDateTime now = LocalDateTime.now();

        System.out.println(
                "========================================"
        );

        System.out.println(
                "REAPER CHECK: " + now
        );

        List<Job> expiredJobs =
                jobRepository.findByStatusAndLeaseUntilBefore(
                        JobStatus.PROCESSING,
                        now
                );

        System.out.println(
                "EXPIRED JOBS FOUND: " + expiredJobs.size()
        );

        for (Job job : expiredJobs) {

            System.out.println(
                    "RECLAIMING JOB: " + job.getId()
            );

            System.out.println(
                    "OLD WORKER: " + job.getWorkerId()
            );

            System.out.println(
                    "OLD LEASE: " + job.getLeaseUntil()
            );

            // Change job state
            job.setStatus(JobStatus.PENDING);

            // Remove previous worker ownership
            job.setWorkerId(null);

            // Remove previous claim timestamp
            job.setClaimedAt(null);

            // Remove expired lease
            job.setLeaseUntil(null);

            job.setUpdatedAt(now);

            // Save state to PostgreSQL
            jobRepository.save(job);

            System.out.println(
                    "JOB MARKED PENDING: " + job.getId()
            );

            // Put job back into Redis
            jobQueue.enqueue(job.getId());

            System.out.println(
                    "JOB REQUEUED TO REDIS: " + job.getId()
            );
        }

        System.out.println(
                "========================================"
        );
    }
}