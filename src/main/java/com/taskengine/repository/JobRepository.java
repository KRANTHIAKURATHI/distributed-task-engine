package com.taskengine.repository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
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
     * FIND JOBS BY STATUS
     * ==========================================
     *
     * Used by DLQ inspection.
     */
    /*
     * ==========================================
     * FIND JOBS BY STATUS
     * ==========================================
     *
     * Used by DLQ inspection.
     */
    List<Job> findByStatus(
            JobStatus status
    );

    /*
     * ==========================================
     * FIND JOBS BY STATUS WITH PAGINATION
     * ==========================================
     *
     * Used by Job Listing API.
     */
    Page<Job> findJobsByStatus(
            JobStatus status,
            Pageable pageable
    );

    /*
     * ==========================================
     * FIND PENDING JOBS BY PRIORITY
     * ==========================================
     *
     * Used by PriorityScheduler.
     *
     * Higher priority first.
     *
     * Same priority:
     * older jobs first.
     */
    List<Job> findByStatusOrderByPriorityDescCreatedAtAsc(
            JobStatus status
    );

    /*
     * ==========================================
     * FIND STUCK DISPATCHED JOBS
     * ==========================================
     *
     * Used by DispatchedJobRecovery.
     */
    List<Job> findByStatusAndUpdatedAtBefore(
            JobStatus status,
            LocalDateTime time
    );

    /*
     * ==========================================
     * DISPATCH JOB ATOMICALLY
     * ==========================================
     *
     * PENDING → DISPATCHED
     *
     * Only one scheduler instance can
     * successfully dispatch a job.
     */
    @Modifying(
            flushAutomatically = true,
            clearAutomatically = true
    )
    @Transactional
    @Query("""
        UPDATE Job j
        SET j.status = :dispatchedStatus,
            j.updatedAt = :now
        WHERE j.id = :id
          AND j.status = :pendingStatus
    """)
    int dispatchJob(
            @Param("id") UUID id,
            @Param("pendingStatus") JobStatus pendingStatus,
            @Param("dispatchedStatus") JobStatus dispatchedStatus,
            @Param("now") LocalDateTime now
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
     * Completion succeeds only when:
     *
     * 1. Job ID matches
     * 2. Worker ID still matches
     * 3. Job is PROCESSING
     * 4. Lease has not expired
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

    /*
     * ==========================================
     * CANCEL PENDING / RETRYING JOB
     * ==========================================
     *
     * PENDING   → CANCELLED
     * RETRYING  → CANCELLED
     */
    @Modifying
    @Transactional
    @Query("""
        UPDATE Job j
        SET j.status = :cancelledStatus,
            j.workerId = null,
            j.claimedAt = null,
            j.leaseUntil = null,
            j.updatedAt = :now
        WHERE j.id = :id
          AND (
              j.status = :pendingStatus
              OR j.status = :retryingStatus
          )
    """)
    int cancelPendingOrRetryingJob(
            @Param("id") UUID id,
            @Param("pendingStatus") JobStatus pendingStatus,
            @Param("retryingStatus") JobStatus retryingStatus,
            @Param("cancelledStatus") JobStatus cancelledStatus,
            @Param("now") LocalDateTime now
    );

    /*
     * ==========================================
     * CANCEL PROCESSING JOB
     * ==========================================
     *
     * PROCESSING → CANCELLED
     *
     * Only the worker that currently owns the
     * job can cancel it.
     *
     * The lease must still be valid.
     */
    @Modifying
    @Transactional
    @Query("""
        UPDATE Job j
        SET j.status = :cancelledStatus,
            j.workerId = null,
            j.claimedAt = null,
            j.leaseUntil = null,
            j.updatedAt = :now
        WHERE j.id = :id
          AND j.workerId = :workerId
          AND j.status = :processingStatus
          AND j.leaseUntil > :now
    """)
    int cancelProcessingJob(
            @Param("id") UUID id,
            @Param("workerId") String workerId,
            @Param("processingStatus") JobStatus processingStatus,
            @Param("cancelledStatus") JobStatus cancelledStatus,
            @Param("now") LocalDateTime now
    );
}

