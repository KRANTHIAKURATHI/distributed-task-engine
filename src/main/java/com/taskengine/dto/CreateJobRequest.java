package com.taskengine.dto;

import jakarta.validation.constraints.*;

import java.time.LocalDateTime;

public class CreateJobRequest {

    @NotBlank
    private String type;

    @NotBlank
    private String payload;

    @Min(1)
    @Max(10)
    private Integer priority = 5;

    private LocalDateTime scheduledAt;

    @Min(1)
    private Integer maxAttempts = 5;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public LocalDateTime getScheduledAt() {
        return scheduledAt;
    }

    public void setScheduledAt(LocalDateTime scheduledAt) {
        this.scheduledAt = scheduledAt;
    }

    public Integer getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(Integer maxAttempts) {
        this.maxAttempts = maxAttempts;
    }
}