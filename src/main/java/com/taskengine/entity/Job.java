package com.taskengine.entity;

import com.taskengine.enums.JobStatus;
import jakarta.persistence.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "jobs")
public class Job {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private String type;

    @Column(columnDefinition = "TEXT", nullable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private JobStatus status;

    @Column(nullable = false)
    private Integer priority;

    private LocalDateTime scheduledAt;

    @Column(nullable = false)
    private Integer attemptCount;

    @Column(nullable = false)
    private Integer maxAttempts;

    private String lastError;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    private LocalDateTime updatedAt;

    // Worker lease information
    private String workerId;

    private LocalDateTime claimedAt;

    private LocalDateTime leaseUntil;


    public Job() {
    }


    // =========================
    // ID
    // =========================

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }


    // =========================
    // TYPE
    // =========================

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }


    // =========================
    // PAYLOAD
    // =========================

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }


    // =========================
    // STATUS
    // =========================

    public JobStatus getStatus() {
        return status;
    }

    public void setStatus(JobStatus status) {
        this.status = status;
    }


    // =========================
    // PRIORITY
    // =========================

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }


    // =========================
    // SCHEDULED AT
    // =========================

    public LocalDateTime getScheduledAt() {
        return scheduledAt;
    }

    public void setScheduledAt(LocalDateTime scheduledAt) {
        this.scheduledAt = scheduledAt;
    }


    // =========================
    // ATTEMPT COUNT
    // =========================

    public Integer getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(Integer attemptCount) {
        this.attemptCount = attemptCount;
    }


    // =========================
    // MAX ATTEMPTS
    // =========================

    public Integer getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(Integer maxAttempts) {
        this.maxAttempts = maxAttempts;
    }


    // =========================
    // LAST ERROR
    // =========================

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }


    // =========================
    // CREATED AT
    // =========================

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }


    // =========================
    // UPDATED AT
    // =========================

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }


    // =========================
    // WORKER ID
    // =========================

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }


    // =========================
    // CLAIMED AT
    // =========================

    public LocalDateTime getClaimedAt() {
        return claimedAt;
    }

    public void setClaimedAt(LocalDateTime claimedAt) {
        this.claimedAt = claimedAt;
    }


    // =========================
    // LEASE UNTIL
    // =========================

    public LocalDateTime getLeaseUntil() {
        return leaseUntil;
    }

    public void setLeaseUntil(LocalDateTime leaseUntil) {
        this.leaseUntil = leaseUntil;
    }
}