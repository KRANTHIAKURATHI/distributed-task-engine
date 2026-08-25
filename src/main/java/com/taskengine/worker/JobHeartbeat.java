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

    public JobHeartbeat(
            JobRepository jobRepository,
            WorkerIdentity workerIdentity
    ) {
        this.jobRepository = jobRepository;
        this.workerIdentity = workerIdentity;
    }

    @Scheduled(fixedRate = 5000)
    @Transactional
    public void renewLeases() {

        String workerId =
                workerIdentity.getWorkerId();

        LocalDateTime now =
                LocalDateTime.now();

        List<Job> jobs =
                jobRepository.findByStatusAndWorkerId(
                        JobStatus.PROCESSING,
                        workerId
                );

        for (Job job : jobs) {

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
}