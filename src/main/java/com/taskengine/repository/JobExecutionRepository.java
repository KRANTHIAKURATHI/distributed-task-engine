package com.taskengine.repository;

import com.taskengine.entity.JobExecution;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface JobExecutionRepository
        extends JpaRepository<JobExecution, UUID> {

    /*
     * ==========================================
     * FIND EXECUTION BY JOB ID
     * ==========================================
     *
     * Used by IdempotencyService.
     */
    Optional<JobExecution> findByJobId(
            UUID jobId
    );

    /*
     * ==========================================
     * FIND EXECUTION BY JOB ID + STATUS
     * ==========================================
     *
     * Used to distinguish:
     *
     * PROCESSING
     * COMPLETED
     *
     * This is important for crash recovery.
     */
    Optional<JobExecution> findByJobIdAndStatus(
            UUID jobId,
            String status
    );

    /*
     * ==========================================
     * CHECK WHETHER EXECUTION EXISTS
     * ==========================================
     */
    boolean existsByJobId(
            UUID jobId
    );
}

