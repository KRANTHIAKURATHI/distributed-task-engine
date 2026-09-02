package com.taskengine.scheduler;

import com.taskengine.queue.JobQueue;
import com.taskengine.worker.JobWorker;
import com.taskengine.worker.WorkerIdentity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class PendingJobRecovery {

    /*
     * ==========================================
     * RECOVERY THRESHOLD
     * ==========================================
     *
     * A Redis message must be idle for at least
     * 20 seconds before another worker can claim it.
     *
     * Your test SEND_EMAIL job runs for 15 seconds,
     * so 20 seconds gives us some safety margin.
     */
    private static final long MIN_IDLE_TIME_MS =
            20_000;

    private final JobQueue jobQueue;
    private final WorkerIdentity workerIdentity;
    private final JobWorker jobWorker;

    public PendingJobRecovery(
            JobQueue jobQueue,
            WorkerIdentity workerIdentity,
            JobWorker jobWorker
    ) {

        this.jobQueue = jobQueue;
        this.workerIdentity = workerIdentity;
        this.jobWorker = jobWorker;
    }

    /*
     * ==========================================
     * RECOVER PENDING REDIS JOBS
     * ==========================================
     *
     * Runs every 5 seconds.
     *
     * Finds Redis messages that have been pending
     * for at least 20 seconds.
     *
     * Then:
     *
     * 1. XCLAIM the Redis message
     * 2. Give ownership to this worker
     * 3. Process the recovered job
     * 4. JobWorker handles XACK after success
     */
    @Scheduled(fixedDelay = 5000)
    public void recoverPendingJobs() {

        String workerId =
                workerIdentity.getWorkerId();

        try {

            List<JobQueue.ClaimedJob> recoveredJobs =
                    jobQueue.recoverPendingJobs(
                            workerId,
                            MIN_IDLE_TIME_MS
                    );

            if (recoveredJobs.isEmpty()) {
                return;
            }

            System.out.println(
                    "========================================"
            );

            System.out.println(
                    "PENDING JOB RECOVERY"
            );

            System.out.println(
                    "WORKER: "
                            + workerId
            );

            System.out.println(
                    "RECOVERED JOBS: "
                            + recoveredJobs.size()
            );

            /*
             * ======================================
             * PROCESS RECOVERED JOBS
             * ======================================
             */
            for (JobQueue.ClaimedJob claimedJob :
                    recoveredJobs) {

                System.out.println(
                        "RECOVERED JOB: "
                                + claimedJob.getJobId()
                );

                System.out.println(
                        "REDIS RECORD: "
                                + claimedJob.getRecordId()
                );

                /*
                 * Send the recovered Redis message
                 * through the SAME processing pipeline
                 * used by normal jobs.
                 */
                jobWorker.processClaimedJob(
                        claimedJob
                );
            }

            System.out.println(
                    "========================================"
            );

        } catch (Exception e) {

            System.out.println(
                    "========================================"
            );

            System.out.println(
                    "PENDING JOB RECOVERY ERROR"
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
}