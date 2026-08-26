package com.taskengine.controller;

import com.taskengine.dto.CreateJobRequest;
import com.taskengine.entity.Job;
import com.taskengine.service.JobService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    /*
     * ==========================================
     * CREATE JOB
     * ==========================================
     */

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Job createJob(
            @Valid @RequestBody CreateJobRequest request
    ) {

        return jobService.createJob(
                request
        );
    }

    /*
     * ==========================================
     * DLQ INSPECTION
     * ==========================================
     *
     * GET /api/v1/jobs/dlq
     */

    @GetMapping("/dlq")
    public List<Job> getDeadJobs() {

        return jobService.getDeadJobs();
    }

    /*
     * ==========================================
     * REPROCESS DEAD JOB
     * ==========================================
     *
     * POST /api/v1/jobs/{id}/reprocess
     */

    @PostMapping("/{id}/reprocess")
    public Job reprocessJob(
            @PathVariable UUID id
    ) {

        return jobService.reprocessJob(
                id
        );
    }
}