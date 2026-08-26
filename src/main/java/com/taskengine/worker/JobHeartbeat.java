package com.taskengine.worker;

import com.taskengine.entity.Job;
import com.taskengine.enums.JobStatus;
import com.taskengine.repository.JobRepository;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Component
public class JobHeartbeat {

    private final JobRepository jobRepository;
    private final WorkerIdentity workerIdentity;
    private final JobWorker jobWorker;

    public JobHeartbeat(
            JobRepository jobRepository,
            WorkerIdentity workerIdentity,
            JobWorker jobWorker
    ) {
        this.jobRepository = jobRepository;
        this.workerIdentity = workerIdentity;
        this.jobWorker = jobWorker;
    }

    @Scheduled(fixedRate = 5000)
    @Transactional
    public void renewLeases() {

        String workerId =
                workerIdentity.getWorkerId();

        LocalDateTime now =
                LocalDateTime.now();

        /*
         * ==========================================
         * NORMAL PROCESSING JOBS
         * ==========================================
         */

        List<Job> jobs =
                jobRepository.findByStatusAndWorkerId(
                        JobStatus.PROCESSING,
                        workerId
                );

        for (Job job : jobs) {

            renewJob(job, workerId, now);
        }

        /*
         * ==========================================
         * GRACEFUL SHUTDOWN
         * ==========================================
         *
         * During shutdown, the scheduled heartbeat
         * may stop before the current job finishes.
         *
         * Explicitly check the current job so its
         * lease remains alive.
         */

        Job currentJob =
                jobWorker.getCurrentJob();

        if (currentJob != null
                && jobWorker.isJobRunning()
                && currentJob.getStatus()
                == JobStatus.PROCESSING) {

            /*
             * Avoid duplicate renewal if the job was
             * already included in the database query.
             */
            boolean alreadyRenewed =
                    jobs.stream()
                            .anyMatch(
                                    job ->
                                            job.getId()
                                                    .equals(
                                                            currentJob.getId()
                                                    )
                            );

            if (!alreadyRenewed) {

                renewJob(
                        currentJob,
                        workerId,
                        now
                );
            }
        }
    }

    /*
     * ==========================================
     * RENEW ONE JOB
     * ==========================================
     */

    private void renewJob(
            Job job,
            String workerId,
            LocalDateTime now
    ) {

        LocalDateTime newLease =
                now.plusSeconds(10);

        int updated =
                jobRepository.renewLease(
                        job.getId(),
                        workerId,
                        JobStatus.PROCESSING,
                        newLease,
                        now
                );

        if (updated == 1) {

            System.out.println(
                    "HEARTBEAT: renewed job "
                            + job.getId()
                            + " until "
                            + newLease
            );

        } else {

            System.out.println(
                    "HEARTBEAT FAILED: lease lost for job "
                            + job.getId()
            );
        }
    }
}