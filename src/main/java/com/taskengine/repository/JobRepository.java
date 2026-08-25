package com.taskengine.repository;

import com.taskengine.entity.Job;
import com.taskengine.enums.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

public interface JobRepository
        extends JpaRepository<Job, UUID> {

    List<Job> findByStatusAndLeaseUntilBefore(
            JobStatus status,
            LocalDateTime time
    );

    List<Job> findByStatusAndWorkerId(
            JobStatus status,
            String workerId
    );

    @Modifying
    @Query("""
        UPDATE Job j
        SET j.leaseUntil = :newLease,
            j.updatedAt = :now
        WHERE j.id = :jobId
          AND j.workerId = :workerId
          AND j.status = :status
          AND j.leaseUntil > :now
    """)
    int renewLease(
            @Param("jobId") UUID jobId,
            @Param("workerId") String workerId,
            @Param("status") JobStatus status,
            @Param("newLease") LocalDateTime newLease,
            @Param("now") LocalDateTime now
    );
}