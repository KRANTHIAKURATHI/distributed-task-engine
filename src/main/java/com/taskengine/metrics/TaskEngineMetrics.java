package com.taskengine.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

@Component
public class TaskEngineMetrics {

    /*
     * ==========================================
     * JOB COUNTERS
     * ==========================================
     */

    private final Counter jobsCreated;

    private final Counter jobsDispatched;

    private final Counter jobsClaimed;

    private final Counter jobsCompleted;

    private final Counter jobsFailed;

    private final Counter jobsRetried;

    private final Counter jobsDead;

    private final Counter jobsCancelled;

    private final Counter jobsRecovered;

    private final Counter duplicateJobs;


    /*
     * ==========================================
     * JOB EXECUTION TIMER
     * ==========================================
     */

    private final Timer jobExecutionDuration;


    /*
     * ==========================================
     * CONSTRUCTOR
     * ==========================================
     */

    public TaskEngineMetrics(
            MeterRegistry registry
    ) {

        /*
         * ======================================
         * JOB CREATED
         * ======================================
         */

        jobsCreated =
                Counter.builder(
                                "task_engine_jobs_created_total"
                        )
                        .description(
                                "Total number of jobs created"
                        )
                        .register(registry);


        /*
         * ======================================
         * JOB DISPATCHED
         * ======================================
         */

        jobsDispatched =
                Counter.builder(
                                "task_engine_jobs_dispatched_total"
                        )
                        .description(
                                "Total number of jobs dispatched to Redis"
                        )
                        .register(registry);


        /*
         * ======================================
         * JOB CLAIMED
         * ======================================
         */

        jobsClaimed =
                Counter.builder(
                                "task_engine_jobs_claimed_total"
                        )
                        .description(
                                "Total number of jobs claimed by workers"
                        )
                        .register(registry);


        /*
         * ======================================
         * JOB COMPLETED
         * ======================================
         */

        jobsCompleted =
                Counter.builder(
                                "task_engine_jobs_completed_total"
                        )
                        .description(
                                "Total number of successfully completed jobs"
                        )
                        .register(registry);


        /*
         * ======================================
         * JOB FAILED
         * ======================================
         */

        jobsFailed =
                Counter.builder(
                                "task_engine_jobs_failed_total"
                        )
                        .description(
                                "Total number of failed job executions"
                        )
                        .register(registry);


        /*
         * ======================================
         * JOB RETRIED
         * ======================================
         */

        jobsRetried =
                Counter.builder(
                                "task_engine_jobs_retried_total"
                        )
                        .description(
                                "Total number of jobs scheduled for retry"
                        )
                        .register(registry);


        /*
         * ======================================
         * JOB DEAD
         * ======================================
         */

        jobsDead =
                Counter.builder(
                                "task_engine_jobs_dead_total"
                        )
                        .description(
                                "Total number of jobs moved to the dead letter queue"
                        )
                        .register(registry);


        /*
         * ======================================
         * JOB CANCELLED
         * ======================================
         */

        jobsCancelled =
                Counter.builder(
                                "task_engine_jobs_cancelled_total"
                        )
                        .description(
                                "Total number of cancelled jobs"
                        )
                        .register(registry);


        /*
         * ======================================
         * JOB RECOVERED
         * ======================================
         */

        jobsRecovered =
                Counter.builder(
                                "task_engine_jobs_recovered_total"
                        )
                        .description(
                                "Total number of jobs recovered after failure"
                        )
                        .register(registry);


        /*
         * ======================================
         * DUPLICATE JOB
         * ======================================
         */

        duplicateJobs =
                Counter.builder(
                                "task_engine_jobs_duplicate_total"
                        )
                        .description(
                                "Total number of duplicate execution attempts"
                        )
                        .register(registry);


        /*
         * ======================================
         * EXECUTION DURATION
         * ======================================
         *
         * Timer automatically records:
         *
         * count
         * total time
         * maximum
         *
         * and can support percentile/histogram
         * configuration later.
         */

        jobExecutionDuration =
                Timer.builder(
                                "task_engine_job_execution_duration"
                        )
                        .description(
                                "Time spent executing jobs"
                        )
                        .register(registry);
    }


    /*
     * ==========================================
     * RECORD JOB CREATED
     * ==========================================
     */

    public void jobCreated() {

        jobsCreated.increment();
    }


    /*
     * ==========================================
     * RECORD JOB DISPATCHED
     * ==========================================
     */

    public void jobDispatched() {

        jobsDispatched.increment();
    }


    /*
     * ==========================================
     * RECORD JOB CLAIMED
     * ==========================================
     */

    public void jobClaimed() {

        jobsClaimed.increment();
    }


    /*
     * ==========================================
     * RECORD JOB COMPLETED
     * ==========================================
     */

    public void jobCompleted() {

        jobsCompleted.increment();
    }


    /*
     * ==========================================
     * RECORD JOB FAILED
     * ==========================================
     */

    public void jobFailed() {

        jobsFailed.increment();
    }


    /*
     * ==========================================
     * RECORD JOB RETRIED
     * ==========================================
     */

    public void jobRetried() {

        jobsRetried.increment();
    }


    /*
     * ==========================================
     * RECORD JOB DEAD
     * ==========================================
     */

    public void jobDead() {

        jobsDead.increment();
    }


    /*
     * ==========================================
     * RECORD JOB CANCELLED
     * ==========================================
     */

    public void jobCancelled() {

        jobsCancelled.increment();
    }


    /*
     * ==========================================
     * RECORD JOB RECOVERED
     * ==========================================
     */

    public void jobRecovered() {

        jobsRecovered.increment();
    }


    /*
     * ==========================================
     * RECORD DUPLICATE EXECUTION
     * ==========================================
     */

    public void duplicateJob() {

        duplicateJobs.increment();
    }


    /*
     * ==========================================
     * RECORD EXECUTION DURATION
     * ==========================================
     */

    public Timer getJobExecutionDuration() {

        return jobExecutionDuration;
    }
}

