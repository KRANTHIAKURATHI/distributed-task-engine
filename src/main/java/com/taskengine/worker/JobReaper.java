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
public class JobReaper {

    private final JobRepository jobRepository;
    private final TaskEngineMetrics metrics;

    public JobReaper(
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
     * REAPER
     * ==========================================
     *
     * Checks every 5 seconds for PROCESSING jobs
     * whose lease has expired.
     *
     * Expired jobs are returned to PENDING.
     *
     * PriorityScheduler will then handle:
     *
     * PENDING → DISPATCHED → Redis
     *
     * This keeps all dispatching inside the
     * priority scheduling pipeline.
     */
    @Scheduled(fixedDelay = 5000)
    public void reclaimExpiredJobs() {

        LocalDateTime now =
                LocalDateTime.now();

        List<Job> expiredJobs =
                jobRepository
                        .findByStatusAndLeaseUntilBefore(
                                JobStatus.PROCESSING,
                                now
                        );

        if (expiredJobs.isEmpty()) {
            return;
        }

        System.out.println(
                "========================================"
        );

        System.out.println(
                "JOB REAPER"
        );

        System.out.println(
                "EXPIRED JOBS FOUND: "
                        + expiredJobs.size()
        );

        /*
         * ======================================
         * RECLAIM EXPIRED JOBS
         * ======================================
         */

        for (Job job : expiredJobs) {

            try {

                System.out.println(
                        "RECLAIMING JOB: "
                                + job.getId()
                );

                System.out.println(
                        "OLD WORKER: "
                                + job.getWorkerId()
                );

                System.out.println(
                        "OLD LEASE: "
                                + job.getLeaseUntil()
                );

                /*
                 * ==================================
                 * RETURN JOB TO PENDING
                 * ==================================
                 */

                job.setStatus(
                        JobStatus.PENDING
                );

                /*
                 * Remove previous worker ownership.
                 */
                job.setWorkerId(
                        null
                );

                /*
                 * Remove previous claim timestamp.
                 */
                job.setClaimedAt(
                        null
                );

                /*
                 * Remove expired lease.
                 */
                job.setLeaseUntil(
                        null
                );

                job.setUpdatedAt(
                        now
                );

                /*
                 * ==================================
                 * SAVE TO POSTGRESQL
                 * ==================================
                 */

                jobRepository.save(
                        job
                );

                /*
                 * ==================================
                 * RECORD RECOVERY
                 * ==================================
                 *
                 * The expired processing job has
                 * successfully been returned to the
                 * scheduling pipeline.
                 */
                metrics.jobRecovered();

                System.out.println(
                        "JOB MARKED PENDING: "
                                + job.getId()
                );

                System.out.println(
                        "PRIORITY: "
                                + job.getPriority()
                );

                System.out.println(
                        "WAITING FOR PRIORITY SCHEDULER"
                );

            } catch (Exception e) {

                System.out.println(
                        "========================================"
                );

                System.out.println(
                        "JOB REAPER ERROR"
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
