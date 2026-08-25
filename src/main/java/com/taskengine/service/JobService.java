package com.taskengine.service;

import com.taskengine.dto.CreateJobRequest;
import com.taskengine.entity.Job;
import com.taskengine.enums.JobStatus;
import com.taskengine.queue.JobQueue;
import com.taskengine.repository.JobRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class JobService {

    private final JobRepository jobRepository;
    private final JobQueue jobQueue;

    public JobService(
            JobRepository jobRepository,
            JobQueue jobQueue
    ) {
        this.jobRepository = jobRepository;
        this.jobQueue = jobQueue;
    }

    public Job createJob(CreateJobRequest request) {

        Job job = new Job();

        job.setType(request.getType());
        job.setPayload(request.getPayload());
        job.setPriority(request.getPriority());
        job.setScheduledAt(request.getScheduledAt());
        job.setMaxAttempts(request.getMaxAttempts());

        job.setStatus(JobStatus.PENDING);
        job.setAttemptCount(0);
        job.setCreatedAt(LocalDateTime.now());
        job.setUpdatedAt(LocalDateTime.now());

        Job savedJob = jobRepository.save(job);

        jobQueue.enqueue(savedJob.getId());

        return savedJob;
    }
}