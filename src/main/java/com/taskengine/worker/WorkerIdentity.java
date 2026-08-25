package com.taskengine.worker;

import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class WorkerIdentity {

    private final String workerId;

    public WorkerIdentity() {
        this.workerId =
                "worker-" + UUID.randomUUID();
    }

    public String getWorkerId() {
        return workerId;
    }
}