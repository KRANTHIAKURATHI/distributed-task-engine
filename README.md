# Distributed Task Execution Engine

A fault-tolerant distributed task execution system built with **Java and Spring Boot**, designed to process background jobs across multiple workers with reliable retries, recovery, duplicate-execution protection, dead-letter handling, persistence, and production-style observability.

The project demonstrates core **distributed systems, backend engineering, concurrency, fault tolerance, job scheduling, and observability** concepts.

---

## 🚀 Key Features

### Distributed Job Processing

* REST API for creating and managing jobs
* Redis-backed job dispatching
* Multiple worker execution
* Priority-based job processing
* Concurrent job execution
* Scheduled job support

### Fault Tolerance & Reliability

* Configurable retry attempts
* Retry and backoff handling
* Failed job tracking
* Dead Letter Queue (DLQ) handling
* Dead-job reprocessing
* Worker/job recovery
* Job cancellation
* Lease-based job ownership/recovery
* Duplicate execution prevention

### Persistence

* PostgreSQL-based job persistence
* Job lifecycle/state tracking
* Persistent retry and execution metadata
* Database-backed job history

### Observability

* Micrometer application metrics
* Prometheus metrics collection
* Grafana monitoring dashboard
* Job throughput monitoring
* Job success/failure tracking
* Retry and DLQ monitoring
* Duplicate execution monitoring
* Job execution duration
* Service health monitoring

### Alerting

Configured Grafana alerts for:

* TaskEngine service downtime
* New job failures
* New dead/DLQ jobs

The service-down alert was tested by stopping the application and verifying the **Normal → Pending → Alerting** lifecycle.

---

## 🏗️ Architecture

```text
                         REST API
                            │
                            ▼
                  ┌───────────────────┐
                  │   Spring Boot     │
                  │   Job Controller  │
                  └─────────┬─────────┘
                            │
                            ▼
                  ┌───────────────────┐
                  │     JobService    │
                  └───────┬─────┬─────┘
                          │     │
              ┌───────────┘     └────────────┐
              ▼                              ▼
       ┌──────────────┐              ┌──────────────┐
       │ PostgreSQL   │              │    Redis     │
       │ Job State    │              │ Job Dispatch │
       └──────────────┘              └──────┬───────┘
                                            │
                                  ┌─────────┴─────────┐
                                  ▼                   ▼
                           ┌────────────┐      ┌────────────┐
                           │  Worker 1  │      │  Worker 2  │
                           └──────┬─────┘      └──────┬─────┘
                                  │                   │
                                  └─────────┬─────────┘
                                            ▼
                                     Job Execution
                                            │
                       ┌────────────────────┼────────────────────┐
                       ▼                    ▼                    ▼
                    Success              Retry                  DLQ
                       │                    │                    │
                       └────────────────────┴────────────────────┘
                                            │
                                            ▼
                                     Metrics / Events
                                            │
                                            ▼
                                  ┌───────────────────┐
                                  │    Prometheus     │
                                  └─────────┬─────────┘
                                            │
                                            ▼
                                  ┌───────────────────┐
                                  │      Grafana      │
                                  │ Dashboard + Alerts│
                                  └───────────────────┘
```

---

## 🛠️ Tech Stack

| Category             | Technology                  |
| -------------------- | --------------------------- |
| Language             | Java                        |
| Framework            | Spring Boot                 |
| API                  | REST                        |
| Database             | PostgreSQL                  |
| Persistence          | Spring Data JPA / Hibernate |
| Message/Coordination | Redis                       |
| Build Tool           | Maven                       |
| Containerization     | Docker                      |
| Metrics              | Micrometer                  |
| Monitoring           | Prometheus                  |
| Visualization        | Grafana                     |
| Validation           | Jakarta Bean Validation     |

---

## 📋 Job Lifecycle

A job can move through the following states:

```text
              ┌──────────┐
              │  PENDING │
              └────┬─────┘
                   │
                   ▼
              ┌──────────┐
              │ RUNNING  │
              └────┬─────┘
                   │
          ┌────────┼─────────┐
          │        │         │
          ▼        ▼         ▼
      COMPLETED  RETRY      DEAD
                   │
                   │
                   ▼
                RUNNING

Other terminal/management states:

PENDING/RUNNING → CANCELLED
DEAD → REPROCESS → PENDING
```

---

## 🔄 Retry & Failure Handling

Jobs support configurable maximum attempts.

When execution fails:

```text
Job Execution
      │
      ▼
   Failure
      │
      ▼
Attempts Remaining?
   │           │
  Yes          No
   │           │
   ▼           ▼
 Retry         DEAD
   │
   ▼
Backoff
   │
   ▼
Re-execution
```

This prevents immediately retrying failed work and provides a controlled failure path through the DLQ.

---

## 🔁 Duplicate Execution Prevention

The system includes mechanisms for coordinating workers and preventing the same job from being executed concurrently by multiple workers.

This is important in distributed processing environments where multiple workers may attempt to claim the same pending job.

The design uses job ownership/lease concepts so that abandoned work can eventually become eligible for recovery.

---

## ♻️ Job Recovery

If a worker fails while processing a job, the job can be recovered rather than remaining permanently stuck.

High-level flow:

```text
Worker claims job
       │
       ▼
Job becomes RUNNING
       │
       ▼
Worker failure
       │
       ▼
Lease expires / recovery detects abandoned job
       │
       ▼
Job becomes eligible for recovery
       │
       ▼
Another worker processes the job
```

---

## ☠️ Dead Letter Queue

Jobs that exhaust their configured retry attempts are moved into the dead-job/DLQ flow.

Dead jobs can be inspected through the API and manually reprocessed when appropriate.

Example:

```http
GET /api/v1/jobs/dlq
```

Reprocess a dead job:

```http
POST /api/v1/jobs/{id}/reprocess
```

---

## 📡 REST API

### Create Job

```http
POST /api/v1/jobs
Content-Type: application/json
```

Example:

```json
{
  "type": "EMAIL",
  "payload": "send-welcome-email",
  "priority": 5,
  "maxAttempts": 5
}
```

### Get Jobs

```http
GET /api/v1/jobs
```

Optional filters:

```http
GET /api/v1/jobs?page=0&size=10
GET /api/v1/jobs?status=COMPLETED
GET /api/v1/jobs?status=DEAD&page=0&size=5
```

### Get Job

```http
GET /api/v1/jobs/{id}
```

### Inspect Dead Jobs

```http
GET /api/v1/jobs/dlq
```

### Reprocess Dead Job

```http
POST /api/v1/jobs/{id}/reprocess
```

### Cancel Job

```http
POST /api/v1/jobs/{id}/cancel
```

---

## 📊 Observability

The application exposes Prometheus metrics through:

```text
/actuator/prometheus
```

Example application metrics include:

```text
task_engine_jobs_total
task_engine_jobs_completed_total
task_engine_jobs_failed_total
task_engine_jobs_retried_total
task_engine_jobs_dead_total
task_engine_jobs_cancelled_total
task_engine_jobs_recovered_total
task_engine_jobs_duplicate_total
```

Execution duration is also tracked through Prometheus summary metrics.

### Grafana Dashboard

The dashboard provides visibility into:

* Jobs Created
* Jobs Completed
* Jobs Failed
* Jobs Retried
* Jobs Dead
* Jobs Cancelled
* Jobs Recovered
* Duplicate Executions
* Job Execution Duration
* Job Throughput
* Job Success Rate
* TaskEngine Health

---

## 🚨 Monitoring & Alerts

Grafana alerts are configured for critical operational conditions.

### TaskEngine Down

```promql
up{job="task-engine"} < 1
```

### Job Failures

```promql
increase(task_engine_jobs_failed_total[5m]) > 0
```

### Dead Jobs / DLQ

```promql
increase(task_engine_jobs_dead_total[5m]) > 0
```

The service-down alert was tested by stopping the application and verifying that the alert transitioned from:

```text
Normal
   ↓
Pending
   ↓
Alerting
```

and returned to normal after the service recovered.

---

## 🐳 Running the Project

### Prerequisites

Make sure the following are installed:

* Java
* Maven
* PostgreSQL
* Redis
* Docker
* Prometheus
* Grafana

### Start the application

```bash
./mvnw spring-boot:run
```

On Windows:

```powershell
.\mvnw.cmd spring-boot:run
```

The application runs on:

```text
http://localhost:8091
```

### Verify Prometheus metrics

Open:

```text
http://localhost:8091/actuator/prometheus
```

### Prometheus

Prometheus scrapes:

```text
http://host.docker.internal:8091/actuator/prometheus
```

### Grafana

Open:

```text
http://localhost:3000
```

Configure the Prometheus data source and import/create the TaskEngine dashboard.

---

## 🧪 Testing

The system has been tested across the major job-processing paths, including:

* Job creation
* Successful execution
* Failed execution
* Retry handling
* Dead/DLQ handling
* Job recovery
* Job cancellation
* Duplicate execution detection
* Metrics collection
* Grafana visualization
* Service-down alerting

The observability pipeline was also verified end-to-end:

```text
TaskEngine
    ↓
Micrometer
    ↓
/actuator/prometheus
    ↓
Prometheus
    ↓
PromQL
    ↓
Grafana
```

---

## 📈 Performance

The system is designed around a multi-worker architecture so processing capacity can be increased by adding workers.

Performance should be evaluated through controlled load testing rather than claiming an unverified throughput number.

Future benchmarking can measure:

* Jobs/sec
* Jobs/min
* p95/p99 latency
* Worker utilization
* Redis latency
* PostgreSQL latency
* Retry rate
* DLQ rate
* Duplicate executions

---

## 🎯 Engineering Concepts Demonstrated

This project focuses on practical backend and distributed-systems concepts:

* Distributed job processing
* Concurrent workers
* Job scheduling
* Priority queues
* Retry strategies
* Backoff
* Fault tolerance
* Failure recovery
* Lease-based ownership
* Duplicate execution prevention
* Dead Letter Queues
* Persistent job state
* REST API design
* Database consistency
* Redis-based coordination
* Observability
* Metrics
* Monitoring
* Alerting
* Containerized infrastructure

---

## 📁 Project Structure

```text
src/
└── main/
    └── java/
        └── com/
            └── taskengine/
                ├── controller/
                ├── dto/
                ├── entity/
                ├── enums/
                ├── repository/
                ├── service/
                ├── scheduler/
                ├── worker/
                └── config/
```

---

## 🔮 Future Improvements

Potential future improvements include:

* Automated load-testing pipeline
* Horizontal worker deployment
* Docker Compose for the complete stack
* CI/CD pipeline
* Kubernetes deployment
* Distributed tracing with OpenTelemetry
* Advanced rate limiting
* Worker autoscaling
* Partitioned job queues
* More comprehensive integration/load testing

---

## 📌 Project Goal

The goal of this project is to explore how reliable background-job processing systems are designed and operated in distributed environments, with particular focus on **concurrency, fault tolerance, recovery, duplicate execution prevention, persistence, and observability**.

---

## License

This project is intended for educational and portfolio purposes.
