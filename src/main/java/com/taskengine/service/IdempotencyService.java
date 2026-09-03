package com.taskengine.service;

import com.taskengine.entity.JobExecution;
import com.taskengine.repository.JobExecutionRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class IdempotencyService {

    /*
     * ==========================================
     * EXECUTION TIMEOUT
     * ==========================================
     *
     * If an execution has been PROCESSING for
     * longer than this amount of time, it is
     * considered stale and can be recovered.
     *
     * Your SEND_EMAIL test job runs for 15 seconds.
     *
     * 20 seconds gives enough margin for the
     * normal execution while still allowing
     * crash recovery.
     */
    private static final long EXECUTION_TIMEOUT_SECONDS =
            20;

    private final JobExecutionRepository jobExecutionRepository;

    public IdempotencyService(
            JobExecutionRepository jobExecutionRepository
    ) {
        this.jobExecutionRepository =
                jobExecutionRepository;
    }

    /*
     * ==========================================
     * CHECK WHETHER JOB WAS COMPLETED
     * ==========================================
     *
     * COMPLETED means the actual execution has
     * already finished successfully.
     *
     * A PROCESSING execution is NOT considered
     * completed.
     */
    public boolean alreadyExecuted(UUID jobId) {

        return jobExecutionRepository
                .findByJobIdAndStatus(
                        jobId,
                        "COMPLETED"
                )
                .isPresent();
    }

    /*
     * ==========================================
     * TRY TO START / RECOVER EXECUTION
     * ==========================================
     *
     * Cases:
     *
     * 1. No execution record
     *       → create PROCESSING
     *
     * 2. COMPLETED
     *       → reject duplicate
     *
     * 3. PROCESSING and still fresh
     *       → reject duplicate
     *
     * 4. PROCESSING and stale
     *       → recover execution
     *
     * Database UNIQUE(job_id) protects against
     * concurrent workers.
     */
    @Transactional(
            propagation = Propagation.REQUIRES_NEW
    )
    public boolean tryStartExecution(
            UUID jobId
    ) {

        LocalDateTime now =
                LocalDateTime.now();

        /*
         * ======================================
         * FIND EXISTING EXECUTION
         * ======================================
         */

        JobExecution existing =
                jobExecutionRepository
                        .findByJobId(jobId)
                        .orElse(null);

        /*
         * ======================================
         * NO EXECUTION
         * ======================================
         */

        if (existing == null) {

            try {

                JobExecution execution =
                        new JobExecution();

                execution.setJobId(
                        jobId
                );

                execution.setStatus(
                        "PROCESSING"
                );

                execution.setCreatedAt(
                        now
                );

                execution.setCompletedAt(
                        null
                );

                jobExecutionRepository.saveAndFlush(
                        execution
                );

                System.out.println(
                        "IDEMPOTENCY: NEW EXECUTION STARTED: "
                                + jobId
                );

                return true;

            } catch (DataIntegrityViolationException e) {

                /*
                 * Another worker won the race.
                 */
                System.out.println(
                        "IDEMPOTENCY: CONCURRENT "
                                + "EXECUTION DETECTED: "
                                + jobId
                );

                return false;
            }
        }

        /*
         * ======================================
         * ALREADY COMPLETED
         * ======================================
         */

        if ("COMPLETED".equals(
                existing.getStatus()
        )) {

            System.out.println(
                    "IDEMPOTENCY: JOB ALREADY COMPLETED: "
                            + jobId
            );

            return false;
        }

        /*
         * ======================================
         * CURRENTLY PROCESSING
         * ======================================
         */

        if ("PROCESSING".equals(
                existing.getStatus()
        )) {

            LocalDateTime createdAt =
                    existing.getCreatedAt();

            /*
             * Defensive check.
             */
            if (createdAt == null) {

                System.out.println(
                        "IDEMPOTENCY: PROCESSING "
                                + "EXECUTION HAS NO CREATED_AT: "
                                + jobId
                );

                return false;
            }

            LocalDateTime staleAt =
                    createdAt.plusSeconds(
                            EXECUTION_TIMEOUT_SECONDS
                    );

            /*
             * ==================================
             * STILL ACTIVE
             * ==================================
             */

            if (staleAt.isAfter(now)) {

                System.out.println(
                        "IDEMPOTENCY: EXECUTION "
                                + "STILL ACTIVE: "
                                + jobId
                );

                return false;
            }

            /*
             * ==================================
             * STALE EXECUTION
             * ==================================
             *
             * Previous worker probably crashed.
             *
             * Reuse the existing execution record
             * instead of creating another one.
             */

            existing.setStatus(
                    "PROCESSING"
            );

            existing.setCreatedAt(
                    now
            );

            existing.setCompletedAt(
                    null
            );

            jobExecutionRepository.saveAndFlush(
                    existing
            );

            System.out.println(
                    "========================================"
            );

            System.out.println(
                    "IDEMPOTENCY: STALE EXECUTION RECOVERED"
            );

            System.out.println(
                    "JOB: "
                            + jobId
            );

            System.out.println(
                    "========================================"
            );

            return true;
        }

        /*
         * ======================================
         * UNKNOWN STATUS
         * ======================================
         */

        System.out.println(
                "IDEMPOTENCY: UNKNOWN EXECUTION STATUS: "
                        + existing.getStatus()
        );

        return false;
    }

    /*
     * ==========================================
     * MARK EXECUTION COMPLETED
     * ==========================================
     */

    @Transactional
    public void markCompleted(
            UUID jobId
    ) {

        JobExecution execution =
                jobExecutionRepository
                        .findByJobId(jobId)
                        .orElseThrow(
                                () -> new IllegalStateException(
                                        "Execution record not found: "
                                                + jobId
                                )
                        );

        execution.setStatus(
                "COMPLETED"
        );

        execution.setCompletedAt(
                LocalDateTime.now()
        );

        jobExecutionRepository.save(
                execution
        );

        System.out.println(
                "IDEMPOTENCY: EXECUTION COMPLETED: "
                        + jobId
        );
    }
}

