package com.taskengine.repository;

import com.taskengine.entity.Job;
import com.taskengine.enums.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public interface JobRepository
        extends JpaRepository<Job, UUID> {

    /*
     * ==========================================
     * FIND EXPIRED PROCESSING JOBS
     * ==========================================
     *
     * Used by JobReaper.
     */
    List<Job> findByStatusAndLeaseUntilBefore(
            JobStatus status,
            LocalDateTime time
    );

    /*
     * ==========================================
     * FIND RETRYABLE JOBS
     * ==========================================
     *
     * Used by RetryScheduler.
     */
    List<Job> findByStatusAndScheduledAtBefore(
            JobStatus status,
            LocalDateTime time
    );

    /*
     * ==========================================
     * FIND JOBS OWNED BY WORKER
     * ==========================================
     *
     * Used by JobHeartbeat.
     */
    List<Job> findByStatusAndWorkerId(
            JobStatus jobStatus,
            String workerId
    );

    /*
     * ==========================================
     * FIND DEAD JOBS / DLQ
     * ==========================================
     *
     * Used by:
     *
     * GET /api/v1/jobs/dlq
     */
    List<Job> findByStatus(
            JobStatus status
    );

    /*
     * ==========================================
     * RENEW LEASE
     * ==========================================
     *
     * Only the worker that currently owns the
     * job can renew its lease.
     *
     * The lease must still be valid.
     */
    @Modifying
    @Transactional
    @Query("""
        UPDATE Job j
        SET j.leaseUntil = :newLease,
            j.updatedAt = :now
        WHERE j.id = :id
          AND j.workerId = :workerId
          AND j.status = :status
          AND j.leaseUntil > :now
    """)
    int renewLease(
            @Param("id") UUID id,
            @Param("workerId") String workerId,
            @Param("status") JobStatus status,
            @Param("newLease") LocalDateTime newLease,
            @Param("now") LocalDateTime now
    );

    /*
     * ==========================================
     * COMPLETE JOB IF STILL OWNED
     * ==========================================
     *
     * This prevents an old worker from completing
     * a job after its lease has been lost and the
     * job has been reclaimed by another worker.
     *
     * Completion succeeds ONLY when:
     *
     * 1. Job ID matches
     * 2. Worker ID still matches
     * 3. Job is still PROCESSING
     * 4. Lease has not expired
     *
     * If another worker has already reclaimed the
     * job, this UPDATE affects 0 rows.
     */
    @Modifying
    @Transactional
    @Query("""
        UPDATE Job j
        SET j.status = :completedStatus,
            j.workerId = null,
            j.claimedAt = null,
            j.leaseUntil = null,
            j.updatedAt = :now
        WHERE j.id = :id
          AND j.workerId = :workerId
          AND j.status = :processingStatus
          AND j.leaseUntil > :now
    """)
    int completeJobIfOwned(
            @Param("id") UUID id,
            @Param("workerId") String workerId,
            @Param("processingStatus") JobStatus processingStatus,
            @Param("completedStatus") JobStatus completedStatus,
            @Param("now") LocalDateTime now
    );
}