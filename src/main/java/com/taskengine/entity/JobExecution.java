package com.taskengine.entity;

import jakarta.persistence.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(
        name = "job_executions",
        uniqueConstraints = {
                @UniqueConstraint(
                        name = "uk_job_execution_job_id",
                        columnNames = "job_id"
                )
        }
)
public class JobExecution {

    /*
     * ==========================================
     * PRIMARY KEY
     * ==========================================
     */

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    /*
     * ==========================================
     * JOB ID
     * ==========================================
     *
     * One execution record belongs to one job.
     *
     * UNIQUE constraint prevents the same job
     * from having multiple execution records.
     */

    @Column(
            name = "job_id",
            nullable = false,
            unique = true
    )
    private UUID jobId;

    /*
     * ==========================================
     * EXECUTION STATUS
     * ==========================================
     *
     * Example:
     *
     * PROCESSING
     * COMPLETED
     */

    @Column(
            nullable = false
    )
    private String status;

    /*
     * ==========================================
     * CREATED AT
     * ==========================================
     */

    @Column(
            name = "created_at",
            nullable = false
    )
    private LocalDateTime createdAt;

    /*
     * ==========================================
     * COMPLETED AT
     * ==========================================
     */

    @Column(
            name = "completed_at"
    )
    private LocalDateTime completedAt;

    /*
     * ==========================================
     * GETTERS / SETTERS
     * ==========================================
     */

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getJobId() {
        return jobId;
    }

    public void setJobId(UUID jobId) {
        this.jobId = jobId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(LocalDateTime completedAt) {
        this.completedAt = completedAt;
    }
}

