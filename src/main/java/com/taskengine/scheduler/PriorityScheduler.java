package com.taskengine.scheduler;

import com.taskengine.entity.Job;
import com.taskengine.enums.JobStatus;
import com.taskengine.metrics.TaskEngineMetrics;
import com.taskengine.queue.JobQueue;
import com.taskengine.repository.JobRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Component
public class PriorityScheduler {

    private final JobRepository jobRepository;
    private final JobQueue jobQueue;
    private final TaskEngineMetrics metrics;

    public PriorityScheduler(
            JobRepository jobRepository,
            JobQueue jobQueue,
            TaskEngineMetrics metrics
    ) {
        this.jobRepository = jobRepository;
        this.jobQueue = jobQueue;
        this.metrics = metrics;
    }

    /*
     * ==========================================
     * PRIORITY DISPATCHER
     * ==========================================
     *
     * Runs every 1 second.
     *
     * Higher priority jobs are dispatched first.
     *
     * Same priority:
     * older jobs are dispatched first.
     */
    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void dispatchJobs() {

        List<Job> pendingJobs =
                jobRepository
                        .findByStatusOrderByPriorityDescCreatedAtAsc(
                                JobStatus.PENDING
                        );

        if (pendingJobs.isEmpty()) {
            return;
        }

        System.out.println(
                "========================================"
        );

        System.out.println(
                "PRIORITY SCHEDULER"
        );

        System.out.println(
                "PENDING JOBS FOUND: "
                        + pendingJobs.size()
        );

        /*
         * Process jobs in priority order.
         */
        for (Job job : pendingJobs) {

            LocalDateTime now =
                    LocalDateTime.now();

            /*
             * ======================================
             * ATOMIC DISPATCH
             * ======================================
             *
             * PENDING → DISPATCHED
             *
             * Only one scheduler instance can
             * successfully change this job.
             */
            int dispatched =
                    jobRepository.dispatchJob(
                            job.getId(),
                            JobStatus.PENDING,
                            JobStatus.DISPATCHED,
                            now
                    );

            /*
             * ======================================
             * THIS SCHEDULER WON
             * ======================================
             */
            if (dispatched == 1) {

                System.out.println(
                        "DISPATCHING JOB: "
                                + job.getId()
                );

                System.out.println(
                        "PRIORITY: "
                                + job.getPriority()
                );

                System.out.println(
                        "CREATED AT: "
                                + job.getCreatedAt()
                );

                /*
                 * ==================================
                 * PUT JOB INTO REDIS
                 * ==================================
                 */

                jobQueue.enqueue(
                        job.getId()
                );

                /*
                 * ==================================
                 * RECORD DISPATCH METRIC
                 * ==================================
                 *
                 * Only increment after the Redis
                 * enqueue call succeeds.
                 */
                metrics.jobDispatched();

                System.out.println(
                        "JOB DISPATCHED TO REDIS: "
                                + job.getId()
                );

            } else {

                /*
                 * Another scheduler instance already
                 * dispatched this job.
                 */
                System.out.println(
                        "JOB ALREADY DISPATCHED BY "
                                + "ANOTHER SCHEDULER: "
                                + job.getId()
                );
            }
        }

        System.out.println(
                "========================================"
        );
    }
}
