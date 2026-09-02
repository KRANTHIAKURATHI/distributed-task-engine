package com.taskengine.controller;

import com.taskengine.dto.CreateJobRequest;
import com.taskengine.entity.Job;
import com.taskengine.enums.JobStatus;
import com.taskengine.service.JobService;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(
            JobService jobService
    ) {

        this.jobService =
                jobService;
    }

    /*
     * ==========================================
     * CREATE JOB
     * ==========================================
     *
     * POST /api/v1/jobs
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
     * GET ALL JOBS
     * ==========================================
     *
     * GET /api/v1/jobs
     *
     * Optional parameters:
     *
     * status
     * page
     * size
     *
     * Examples:
     *
     * GET /api/v1/jobs
     *
     * GET /api/v1/jobs?page=0&size=10
     *
     * GET /api/v1/jobs?status=COMPLETED
     *
     * GET /api/v1/jobs?status=DEAD&page=0&size=5
     */

    @GetMapping
    public Page<Job> getJobs(

            @RequestParam(
                    required = false
            )
            JobStatus status,

            @RequestParam(
                    defaultValue = "0"
            )
            int page,

            @RequestParam(
                    defaultValue = "10"
            )
            int size

    ) {

        return jobService.getJobs(
                status,
                page,
                size
        );
    }

    /*
     * ==========================================
     * GET JOB BY ID
     * ==========================================
     *
     * GET /api/v1/jobs/{id}
     */

    @GetMapping("/{id}")
    public Job getJobById(
            @PathVariable UUID id
    ) {

        return jobService.getJobById(
                id
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

    /*
     * ==========================================
     * CANCEL JOB
     * ==========================================
     *
     * POST /api/v1/jobs/{id}/cancel
     */

    @PostMapping("/{id}/cancel")
    public Job cancelJob(
            @PathVariable UUID id
    ) {

        return jobService.cancelJob(
                id
        );
    }
}
