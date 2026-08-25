package com.taskengine.controller;

import com.taskengine.dto.CreateJobRequest;
import com.taskengine.entity.Job;
import com.taskengine.service.JobService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

    private final JobService jobService;

    public JobController(JobService jobService) {
        this.jobService = jobService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Job createJob(
            @Valid @RequestBody CreateJobRequest request
    ) {
        return jobService.createJob(request);
    }
}