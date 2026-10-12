# BullMQ Events: Worker Events and Queue Events

Beginner-friendly notes on how to **watch what happens to a job** in BullMQ, using an `email-queue` example.

## Table of Contents

1. [Big Picture](#1-big-picture)
2. [Job Lifecycle](#2-job-lifecycle)
3. [Project Setup](#3-project-setup)
4. [Worker Events](#4-worker-events)
5. [Queue Events](#5-queue-events)
6. [Worker Events vs Queue Events](#6-worker-events-vs-queue-events)
7. [Failed vs Stalled vs Error](#7-failed-vs-stalled-vs-error)
8. [Worker Concurrency](#8-worker-concurrency)
9. [Monitoring with `getJobCounts`](#9-monitoring-with-getjobcounts)
10. [Production Tips](#10-production-tips)
11. [Quick Cheat Sheet](#11-quick-cheat-sheet)

---

## 1. Big Picture

BullMQ has three main parts:

| Part | What it does | Example |
|------|--------------|---------|
| **Producer** | Adds jobs to the queue | "Send a welcome email to this user" |
| **Queue** (stored in Redis) | Holds the jobs until a worker is free | `email-queue` |
| **Worker** | Picks jobs and processes them | Actually sends the email |

Events let us **observe** what is happening to jobs. There are two places to listen:

- **Worker events**: listen inside the worker process.
- **Queue events**: listen using `QueueEvents`, from any process connected to the same Redis.

```text
Producer ──► Queue (Redis) ──► Worker
   │              │               │
   │              │               └── Worker Events  (worker.on(...))
   │              └────────────────── Queue Events   (queueEvent.on(...))
   └── adds jobs
```

---

## 2. Job Lifecycle

```text
Producer adds a job
        ↓
Job enters the queue
        ↓
Job waits for a worker
        ↓
     WAITING
        ↓
Worker picks the job
        ↓
     ACTIVE
        ↓
Worker processes the job
        ↓
  ┌─────┴─────┐
  ↓           ↓
SUCCESS     ERROR
  ↓           ↓
COMPLETED   FAILED
```

### What each state means

| State | Meaning |
|-------|---------|
| **WAITING** | The job is in the queue. No worker has started it yet. |
| **ACTIVE** | A worker picked the job and is processing it now. |
| **COMPLETED** | The worker finished the job successfully. |
| **FAILED** | The worker tried, but something went wrong. |
| **DELAYED** | The job is scheduled to run later. |

> **Example:** If 10 jobs are added and the worker can process only 1 job at a time, the other 9 jobs stay **WAITING**.

---

## 3. Project Setup

Both the worker file and the queue events file import a shared Redis `connection`.

```text
project/
├── config/
│   └── connection.js
├── queues/
│   └── emailQueue.js
├── workers/
│   └── emailWorker.js
├── events/
│   └── emailQueueEvents.js
└── monitoring/
    └── emailQueueStats.js
```

> File names are only a suggestion. Use the structure you already have.

---

## 4. Worker Events

### Worker Event Lifecycle

```text
A worker takes a job from the queue
              ↓
           ACTIVE
              ↓
       Processing job
              ↓
        ┌─────┴─────┐
        ↓           ↓
    COMPLETED     FAILED
        ↓           ↓
     SUCCESS       ERROR
```

### The Worker (with the processor)

The worker below sends an email. To practice the `failed` event, the fake `sendEmail` function **rejects on purpose** for one email address.

```javascript
import { Worker } from "bullmq";
import { connection } from "../config/connection.js";

// Fake email sender (takes 2 seconds)
const sendEmail = async (name, email) => {
    return new Promise((res, rej) => {
        setTimeout(() => {

            // Simulating a failure for testing
            if (email === "vinay123@gmail.com") {
                return rej(new Error("Email provider timeout"));
            }

            console.log(`📧 Email sent to ${email}`);
            res({ name, email });

        }, 2000);
    });
};

const worker = new Worker(
    "email-queue",

    // The processor: runs once for every job
    async (job) => {
        try {
            console.log(`PROCESSOR → Processing job ${job.id}`);

            const { name, email } = job.data;

            // Report progress (this triggers the "progress" queue event)
            await job.updateProgress({
                processed: 50,
                total: 100,
            });

            console.log("Email job started", {
                jobId: job.id,
                jobName: job.name,
                email,
            });

            const emailResult = await sendEmail(name, email);

            // Whatever we return here becomes the job's result
            return emailResult;

        } catch (error) {
            console.error("Email job failed", {
                jobId: job.id,
                attempt: job.attemptsMade + 1,
                email: job.data.email,
                error: error.message,
            });

            // Always re-throw so BullMQ knows the job failed
            throw error;
        }
    },

    { connection }
);
```

**Key points**

- The processor is the `async (job) => { ... }` function.
- If the processor **returns** a value, the job is **COMPLETED**.
- If the processor **throws** an error, the job is **FAILED**.
- Inside `catch`, we log and then `throw error` again. If we forget to re-throw, BullMQ thinks the job succeeded.

---

### Worker Event 1: `active`

**When it runs:** when the worker starts processing a job.

```text
Job is WAITING in queue
        ↓
Worker picks it
        ↓
      ACTIVE
        ↓
Worker starts work
```

**Example**

```text
Job 1
  ↓
Worker picks Job 1
  ↓
Worker starts sending email
  ↓
ACTIVE event
```

**What we can observe:** which job started, the job name, the job data, and how many attempts were already made.

```javascript
worker.on("active", (job) => {
    console.log("================================");
    console.log("🟢 WORKER EVENT → ACTIVE");
    console.log({
        jobId: job.id,
        name: job.name,
        data: job.data,
        attemptsMade: job.attemptsMade,
    });
    console.log("================================");
});
```

---

### Worker Event 2: `completed`

**When it runs:** when the worker successfully finishes a job.

```text
ACTIVE
  ↓
Worker works
  ↓
SUCCESS
  ↓
COMPLETED
```

**Example**

```text
Job 1
  ↓
Worker sends email
  ↓
Email sent successfully
  ↓
Worker returns result
  ↓
COMPLETED event
```

**What we can observe:** which job completed, job data, number of attempts, and the result returned by the processor.

Example result:

```javascript
{
    message: "Email sent",
    email: "user@gmail.com"
}
```

```javascript
worker.on("completed", (job, result) => {
    console.log("================================");
    console.log("✅ WORKER EVENT → COMPLETED");
    console.log({
        jobId: job.id,
        name: job.name,
        data: job.data,
        attemptsMade: job.attemptsMade,
        result,
    });
    console.log("================================");
});
```

---

### Worker Event 3: `failed`

**When it runs:** when the worker cannot successfully finish a job.

```text
ACTIVE
  ↓
Worker processes
  ↓
ERROR
  ↓
FAILED
```

**Example**

```text
Job 5
  ↓
Worker sends email
  ↓
Email provider timeout
  ↓
sendEmail() throws error
  ↓
Worker reports FAILED
```

**What we can observe:** which job failed, job data, number of attempts, and the error message.

```javascript
worker.on("failed", (job, error) => {
    console.log("================================");
    console.log("❌ WORKER EVENT → FAILED");
    console.log({
        jobId: job?.id,
        name: job?.name,
        data: job?.data,
        attemptsMade: job?.attemptsMade,
        error: error.message,
    });
    console.log("================================");
});
```

> **Note:** We use `job?.` (optional chaining) because `job` can be `undefined` in some failure cases.

> **IMPORTANT: Retries**
>
> If retries are configured, the job does **not** permanently fail after the first error:
>
> ```text
> Attempt 1
>    ↓
>  FAILED
>    ↓
>  Retry
>    ↓
> Attempt 2
>    ↓
>  SUCCESS
> ```

---

### Worker Event 4: `error`

This is different from `failed`.

| Event | Meaning |
|-------|---------|
| `failed` | **A particular job** failed while being processed. |
| `error` | **The Worker itself** had an error. |

```text
Job error:                      Worker error:

Job 5                           Worker
  ↓                               ↓
Email failed                    Worker-level problem
  ↓                               ↓
"failed" event                  "error" event
```

In short:

- `failed` → job processing failed
- `error` → the worker itself had an error

This event is useful for **monitoring the health of your worker**.

```javascript
worker.on("error", (error) => {
    console.error("🚨 WORKER EVENT → ERROR");
    console.error(error.message);
    console.log("================================");
});
```

---

### Worker Events Summary

| Event | Callback arguments | Fires when |
|-------|--------------------|------------|
| `active` | `(job)` | Worker starts a job |
| `completed` | `(job, result)` | Job finished successfully |
| `failed` | `(job, error)` | Job processing failed |
| `error` | `(error)` | The worker itself had an error |

---

## 5. Queue Events

`QueueEvents` listens to events from the **queue** (through Redis). It is useful for dashboards, logging, and monitoring services.

### Setup

```javascript
import { QueueEvents } from "bullmq";
import { connection } from "../config/connection.js";

const queueEvent = new QueueEvents("email-queue", { connection });
```

### Queue Event Flow

```text
Producer adds a job
        ↓
Job enters the queue
        ↓
Job waits for a worker
        ↓
     WAITING ──► worker picks job ──► ACTIVE ──► (PROGRESS) ──┬──► COMPLETED
                                                             └──► FAILED

(If the worker crashes or stops responding while ACTIVE → STALLED)
```

---

### Queue Event 1: `waiting`

**When it happens:** when a job is waiting in the queue. The worker has not started processing it yet.

```text
Producer
    ↓
Job 1 added
    ↓
Queue
    ↓
Job 1 is WAITING
```

**Example:** If 10 jobs are added and the worker can process only 1 job at a time, the other jobs stay **WAITING**.

```javascript
queueEvent.on("waiting", ({ jobId }) => {
    console.log("⏳ QUEUE EVENT → WAITING", { jobId });
});
```

---

### Queue Event 2: `active`

**When it happens:** the worker picks a waiting job. The job is now being processed.

```text
WAITING
   ↓
Worker picks the job
   ↓
ACTIVE
```

**Example**

```text
Job 1 → WAITING
          ↓
        ACTIVE
          ↓
    Worker processes it
```

The event also gives `prev`, the **previous state** of the job.

```javascript
queueEvent.on("active", ({ jobId, prev }) => {
    console.log("🔵 QUEUE EVENT → ACTIVE", {
        jobId,
        previousState: prev,
    });
});
```

---

### Queue Event 3: `progress`

**When it happens:** when the worker updates the progress of a job.

The worker does this with:

```javascript
await job.updateProgress(...)
```

**Example:** processing 10,000 leads

```text
Job 1
  ↓
Processing 10,000 leads
  ↓
2,000 processed   → progress = 20%
  ↓
5,000 processed   → progress = 50%
  ↓
10,000 processed  → progress = 100%
```

`QueueEvents` can listen to every one of these progress updates.

**Very useful for:**

- File imports
- Lead imports
- Video processing
- Report generation
- Large background tasks

```javascript
queueEvent.on("progress", ({ jobId, data }) => {
    console.log("📊 QUEUE EVENT → PROGRESS", {
        jobId,
        progress: data,
    });
});
```

---

### Queue Event 4: `completed`

**When it happens:** the worker successfully finishes the job.

```text
WAITING
   ↓
ACTIVE
   ↓
Worker processes job
   ↓
SUCCESS
   ↓
COMPLETED
```

**Example**

```text
Job 1
  ↓
Send email
  ↓
Email sent successfully
  ↓
COMPLETED
```

This event tells us: *"This job has successfully finished."*

```javascript
queueEvent.on("completed", ({ jobId, returnvalue }) => {
    console.log("✅ QUEUE EVENT → COMPLETED", {
        jobId,
        returnvalue,
    });
});
```

---

### Queue Event 5: `failed`

**When it happens:** the worker tried to process the job, but something went wrong.

```text
WAITING
   ↓
ACTIVE
   ↓
Worker processes job
   ↓
ERROR
   ↓
FAILED
```

**Example**

```text
Job 5
  ↓
Send email
  ↓
Email provider timeout
  ↓
FAILED
```

`failedReason` tells us **why** the job failed.

> If retries are configured, the job may be tried again before it finally becomes FAILED.

```javascript
queueEvent.on("failed", ({ jobId, failedReason }) => {
    console.log("❌ QUEUE EVENT → FAILED", {
        jobId,
        failedReason,
    });
});
```

---

### Queue Event 6: `stalled`

A job becomes **STALLED** when the worker was processing the job but **stopped responding properly**.

```text
Job
  ↓
ACTIVE
  ↓
Worker is processing
  ↓
Worker crashes / stops responding
  ↓
BullMQ detects the problem
  ↓
STALLED
```

**Useful in production monitoring.** It can help us detect:

- Worker crashes
- Worker becoming unresponsive
- Long-running or problematic jobs

```javascript
queueEvent.on("stalled", ({ jobId }) => {
    console.log("⚠️ QUEUE EVENT → STALLED", { jobId });
});
```

---

### Queue Events Summary

| Event | Data you receive | Meaning |
|-------|------------------|---------|
| `waiting` | `{ jobId }` | Job is in the queue, waiting for a worker |
| `active` | `{ jobId, prev }` | Worker started processing the job |
| `progress` | `{ jobId, data }` | Worker reported progress |
| `completed` | `{ jobId, returnvalue }` | Job finished successfully |
| `failed` | `{ jobId, failedReason }` | Job failed, with the reason |
| `stalled` | `{ jobId }` | BullMQ lost track of the worker processing the job |

---

## 6. Worker Events vs Queue Events

| | Worker Events | Queue Events |
|---|---|---|
| **Class** | `Worker` | `QueueEvents` |
| **Where it listens** | Inside the worker | Through Redis, separate from the worker |
| **Callback style** | `(job, result)` / `(job, error)` | `({ jobId, ... })` (an object) |
| **Gives full `job` object?** | Yes | No, only `jobId` and event data |
| **Has `waiting` / `progress` / `stalled`?** | No | Yes |
| **Has `error` (worker health)?** | Yes | No |
| **Good for** | Logging inside the worker | Monitoring and dashboards |

---

## 7. Failed vs Stalled vs Error

These three are easy to mix up.

| Term | What happened | Example |
|------|---------------|---------|
| **FAILED** | The worker processed the job and an **error happened** in the job. | Email provider timeout |
| **STALLED** | BullMQ **lost track** of the worker processing the job. | Worker crashed in the middle of a job |
| **ERROR** (worker event) | The **worker itself** had a problem. | Worker-level problem |

```text
FAILED   → The job broke while the worker was running fine.
STALLED  → The worker disappeared while running the job.
error    → The worker itself had a problem.
```

---

## 8. Worker Concurrency

**Concurrency** means: how many jobs **one worker** can process **at the same time**.

```javascript
new Worker("email-queue", processor, {
    connection,
    concurrency: 3,
});
```

### Example: 10 jobs, concurrency = 3

```text
Worker
  │
  ├── Job 1 → ACTIVE
  ├── Job 2 → ACTIVE
  └── Job 3 → ACTIVE
```

Maximum **3 jobs** are processed at one time. The remaining jobs stay **WAITING**:

```text
WAITING
├── Job 4
├── Job 5
├── Job 6
├── Job 7
├── Job 8
├── Job 9
└── Job 10
```

### When Job 1 completes

```text
Job 1
  ↓
COMPLETED
  ↓
Slot available
  ↓
Job 4
  ↓
ACTIVE
```

New state:

```text
Worker
  │
  ├── Job 2 → ACTIVE
  ├── Job 3 → ACTIVE
  └── Job 4 → ACTIVE
```

### Concurrency + Worker events

```text
concurrency = 3

Job 1 → ACTIVE
Job 2 → ACTIVE
Job 3 → ACTIVE
        ↓
Worker events:
active → Job 1
active → Job 2
active → Job 3
```

Suppose Job 1 fails:

```text
Job 1 → FAILED
   ↓
 RETRY
   ↓
Job 1 → ACTIVE
```

So after a retry, the `active` event can fire **again** for Job 1.

### What `concurrency = 3` does NOT mean

| ❌ Not this | ✅ This |
|------------|---------|
| 3 worker processes | One worker with **3 concurrent jobs** inside it |
| 3 threads | |

> `concurrency = 3` means: *"One worker can process a maximum of 3 jobs at the same time."*

### Multiple workers

```text
Worker 1 (concurrency = 3)
Worker 2 (concurrency = 3)

        Redis Queue
             │
        ┌────┴────┐
        ↓         ↓
    Worker 1   Worker 2
        │         │
     3 jobs     3 jobs
        │         │
        └────┬────┘
             ↓
    ~6 jobs processing
```

This is one of the ways to **scale background job processing in production**.

---

## 9. Monitoring with `getJobCounts`

Events tell you what is happening **right now**. `getJobCounts` gives you a **snapshot** of how many jobs are in each state.

```javascript
import { emailQueue } from "../queues/emailQueue.js";

export const getEmailQueueStats = async () => {
    const counts = await emailQueue.getJobCounts(
        "waiting",
        "active",
        "completed",
        "failed",
        "delayed"
    );

    console.log({
        waiting: counts.waiting ?? 0,
        active: counts.active ?? 0,
        completed: counts.completed ?? 0,
        failed: counts.failed ?? 0,
        delayed: counts.delayed ?? 0,
    });
};

getEmailQueueStats();
```

Example output:

```javascript
{ waiting: 7, active: 3, completed: 120, failed: 2, delayed: 0 }
```

| Count | How to read it |
|-------|----------------|
| `waiting` high | Jobs are piling up. You may need more workers or higher concurrency. |
| `active` | Jobs being processed right now (never more than total concurrency). |
| `completed` | Jobs that finished successfully. |
| `failed` | Jobs that failed. Check the reasons. |
| `delayed` | Jobs scheduled to run later. |

`?? 0` means "use 0 if the value is `null` or `undefined`".

---

## 10. Production Tips

The snippets below are **extra best practices**, added on top of the examples above.

### 10.1 Add retries and backoff when adding jobs

Without `attempts`, a failed job is not retried.

```javascript
// queues/emailQueue.js
import { Queue } from "bullmq";
import { connection } from "../config/connection.js";

export const emailQueue = new Queue("email-queue", { connection });

await emailQueue.add(
    "send-email",
    { name: "Vinay", email: "vinay@example.com" },
    {
        attempts: 3,                                    // try up to 3 times
        backoff: { type: "exponential", delay: 1000 },  // wait 1s, 2s, 4s...
        removeOnComplete: 1000,                         // keep only the last 1000 completed jobs
        removeOnFail: 5000,                             // keep only the last 5000 failed jobs
    }
);
```

### 10.2 Always attach an `error` listener to the Worker

If a worker has no `error` listener, a worker-level error can crash your Node process.

```javascript
worker.on("error", (error) => {
    console.error("🚨 WORKER EVENT → ERROR", error.message);
});
```

### 10.3 Also attach an `error` listener to `QueueEvents`

```javascript
queueEvent.on("error", (error) => {
    console.error("🚨 QUEUE EVENTS ERROR", error.message);
});
```

### 10.4 Always re-throw errors in the processor

```javascript
async (job) => {
    try {
        // ... work ...
    } catch (error) {
        console.error("Job failed", { jobId: job.id, error: error.message });
        throw error; // so BullMQ marks the job as FAILED
    }
}
```

### 10.5 Shut down the worker gracefully

This lets running jobs finish before the process exits.

```javascript
const shutdown = async () => {
    console.log("Shutting down worker...");
    await worker.close();
    await queueEvent.close();
    process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
```

### 10.6 Use stalled events for alerts

```javascript
queueEvent.on("stalled", ({ jobId }) => {
    // In production, send this to your logger or alert system
    console.warn("⚠️ Job stalled", { jobId });
});
```

### 10.7 Keep jobs small and safe to repeat

A stalled job can be picked up again, so the same job may run more than once. Try to write processors so that running twice does not cause harm (for example, do not charge a customer twice).

---

## 11. Quick Cheat Sheet

### Which event should I use?

| I want to know... | Use |
|-------------------|-----|
| Which job a worker just started | Worker `active` |
| What a job returned | Worker `completed` |
| Why a job failed (inside the worker) | Worker `failed` |
| Is my worker healthy? | Worker `error` |
| Which jobs are waiting in the queue | Queue `waiting` |
| How far a long job has progressed | Queue `progress` |
| Did a worker crash mid-job? | Queue `stalled` |
| How many jobs are in each state | `getJobCounts` |

### Full lifecycle in one picture

```text
Producer adds job
      ↓
   WAITING   ← Queue event: waiting
      ↓
   ACTIVE    ← Worker event: active  |  Queue event: active
      ↓
  PROGRESS   ← Queue event: progress  (only if job.updateProgress is used)
      ↓
┌─────┴──────┐
↓            ↓
COMPLETED    FAILED
↓            ↓
Worker:      Worker: failed
completed    Queue:  failed
Queue:
completed

Worker crashes during ACTIVE → Queue event: stalled
Worker itself has a problem  → Worker event: error
```

### Key takeaways

- **Worker events** → what the worker sees: `active`, `completed`, `failed`, `error`.
- **Queue events** → what the queue sees: `waiting`, `active`, `progress`, `completed`, `failed`, `stalled`.
- **`failed`** = the job had an error. **`stalled`** = the worker was lost. **`error`** = the worker itself had a problem.
- **`concurrency`** = number of jobs one worker handles at once. It is not threads or processes.
- Use **multiple workers** to scale.
- Use **`getJobCounts`** for a quick health check of the queue.