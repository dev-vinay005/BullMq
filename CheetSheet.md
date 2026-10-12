# BullMQ + Redis — Production Cheatsheet

> Quick revision: **Queue → Job → Worker → Retries → Scaling → Idempotency → Dead Letter Queue → Events → Shutdown**

> [!NOTE]
> **How to use these notes**
>
> - Each topic has: a simple explanation, a code example, a real-world example, and a production tip.
> - Code is written in **TypeScript**. If you use JavaScript, just remove the types (like `: string`).
> - Use the [30-Second Revision](#30-second-revision) at the end for a very quick recap, and the [Production Checklist](#production-checklist) before going live.

---

## 📑 Table of Contents

0. [Setup: Connection, Queue and Worker](#0-setup-connection-queue-and-worker)
1. [Queue](#1-queue)
2. [Job](#2-job)
3. [Job Lifecycle (States)](#3-job-lifecycle-states)
4. [Producer](#4-producer)
5. [Worker](#5-worker)
6. [Consumer](#6-consumer)
7. [Attempts](#7-attempts)
8. [Backoff](#8-backoff)
9. [Delay](#9-delay)
10. [Priority](#10-priority)
11. [Concurrency](#11-concurrency)
12. [Idempotency](#12-idempotency)
13. [Dead Letter Queue (DLQ)](#13-dead-letter-queue-dlq)
14. [Graceful Shutdown](#14-graceful-shutdown)
15. [QueueEvents](#15-queueevents)
16. [Worker Events](#16-worker-events)
17. [Job Cleanup (Retention)](#17-job-cleanup-retention)
18. [Scheduled Jobs (Job Schedulers)](#18-scheduled-jobs-job-schedulers)
19. [Useful Queue Commands](#19-useful-queue-commands)
20. [Role of Redis in BullMQ](#20-role-of-redis-in-bullmq)
21. [Common Production Mistakes](#21-common-production-mistakes)
22. [Final Production Flow](#final-production-flow)
23. [Production Checklist](#production-checklist)
24. [30-Second Revision](#30-second-revision)

---

## 0. Setup: Connection, Queue and Worker

Before using BullMQ, you need three things: a **Redis connection**, a **Queue**, and a **Worker**.

### Step 1 — Redis connection

BullMQ needs a Redis connection.

```ts
// config/connection.ts
import IORedis from "ioredis";

export const connection = new IORedis(process.env.REDIS_URL!, {
  maxRetriesPerRequest: null,
});
```

> [!IMPORTANT]
> `maxRetriesPerRequest: null` is important for workers. A BullMQ worker needs its Redis connection to be available all the time, so it must keep waiting instead of giving up.

### Step 2 — Create the Queue

The queue accepts and stores jobs.

```ts
// queues/emailQueue.ts
import { Queue } from "bullmq";
import { connection } from "../config/connection.js";

export const emailQueue = new Queue("email-queue", {
  connection,
});
```

```text
Producer
   │
   ▼
emailQueue
   │
   ▼
Redis
```

### Step 3 — Add a job

The producer adds a job to the queue.

```ts
const job = await emailQueue.add("welcome-email", {
  userId: "user_123",
  email: "user@example.com",
});

console.log("Job added:", job.id);
```

```text
API
 ↓
Producer
 ↓
Queue
 ↓
Redis
 ↓
Waiting Job
```

### Step 4 — Create the Worker

The worker takes jobs from the queue and processes them.

```ts
// workers/emailWorker.ts
import { Worker } from "bullmq";
import { connection } from "../config/connection.js";

export const emailWorker = new Worker(
  "email-queue",

  async (job) => {
    console.log("Processing:", job.id);

    const { userId, email } = job.data;

    await sendEmail(email);

    return {
      userId,
      sent: true,
    };
  },

  {
    connection,
    concurrency: 5,
  },
);
```

```text
Redis
  │
  ▼
email-queue
  │
  ▼
Worker
  │
  ▼
Processor
  │
  ▼
sendEmail()
```

### Step 5 — Add basic logging with Worker events

```ts
emailWorker.on("completed", (job, result) => {
  console.log("Job completed", {
    jobId: job.id,
    jobName: job.name,
    result,
  });
});

emailWorker.on("failed", (job, error) => {
  console.error("Job failed", {
    jobId: job?.id,
    jobName: job?.name,
    error: error.message,
  });
});

emailWorker.on("error", (error) => {
  console.error("Worker error", {
    error: error.message,
  });
});
```

### Minimal production folder structure

```text
src/
│
├── config/
│   └── connection.ts
│
├── queues/
│   └── emailQueue.ts
│
├── workers/
│   └── emailWorker.ts
│
├── producers/
│   └── emailProducer.ts
│
└── server.ts
```

Keep each part's job separate:

| Folder       | Responsibility                           |
| ------------ | ---------------------------------------- |
| `config/`    | Shared infrastructure (Redis connection) |
| `queues/`    | Queue configuration                      |
| `producers/` | Functions that add jobs                  |
| `workers/`   | Functions that process jobs              |

---

## 1. Queue

A **Queue** stores jobs until a worker processes them.

```ts
import { Queue } from "bullmq";
import { connection } from "./connection.js";

export const emailQueue = new Queue("email-queue", {
  connection,
});
```

### Real-world example

A user signs up:

```text
API
 ↓
Add email job
 ↓
Queue
 ↓
Worker
 ↓
Welcome email
```

The API **does not have to wait** for the email provider's response. The user gets a fast reply.

### Production rule

Give queues **meaningful names**:

```text
email-queue
payment-queue
notification-queue
report-queue
```

> [!TIP]
> 💡 **One queue per type of work.** Don't put emails, reports, and payments in one queue. Separate queues let you scale, pause, and monitor each type on its own. A slow report job should never block password-reset emails.

---

## 2. Job

A **Job** is one specific piece of work.

```ts
await emailQueue.add("welcome-email", {
  userId: "user_123",
  email: "user@example.com",
});
```

Here:

```text
"welcome-email"      → job name
{ userId, email }    → job data
```

### Real-world example

```text
Job name:  welcome-email

Job data:  {
             userId: "user_123",
             email: "user@example.com"
           }
```

### What does a job contain? 💡

When you get a job object inside a worker, these are the fields you will use most:

| Field              | Meaning                                         |
| ------------------ | ----------------------------------------------- |
| `job.id`           | Unique ID of the job                            |
| `job.name`         | Job name (like `welcome-email`)                 |
| `job.data`         | The data you sent                               |
| `job.attemptsMade` | How many attempts have been made                |
| `job.opts`         | The options you used (`attempts`, `delay`, ...) |
| `job.failedReason` | Why it failed (if it failed)                    |
| `job.returnvalue`  | What the worker returned (if it completed)      |

### Production rule

Don't put large objects inside job data.

❌ **Bad:**

```ts
{
  user: completeUserObject,
  orders: allOrders,
  products: allProducts
}
```

✅ **Better:**

```ts
{
  (userId, orderId);
}
```

The worker can fetch the data it needs from the database.

---

## 3. Job Lifecycle (States)

💡 Every job moves through **states**. Knowing them helps you debug and monitor.

```text
add job
  │
  ├── has delay?     → delayed ──(delay ends)──┐
  ├── has priority?  → prioritized ────────────┤
  │                                            ▼
  └── normal ──────────────────────────────→ waiting
                                               │
                                               ▼
                                            active   (a worker is processing it)
                                               │
                              ┌────────────────┴────────────────┐
                              ▼                                 ▼
                          success                            error
                              │                                 │
                              ▼                       attempts left?
                          completed                    │              │
                                                      YES             NO
                                                       │              │
                                          delayed (backoff wait)    failed
                                                       │
                                                       ▼
                                                    waiting (retry)
```

| State         | Meaning                                                    |
| ------------- | ---------------------------------------------------------- |
| `waiting`     | In the queue, ready for a worker                           |
| `delayed`     | Waiting for time to pass (a `delay`, or a retry `backoff`) |
| `prioritized` | Waiting, but ordered by `priority`                         |
| `active`      | A worker is processing it right now                        |
| `completed`   | Finished successfully                                      |
| `failed`      | Failed and no attempts are left                            |

> [!NOTE]
> **Stalled jobs:** if a worker dies in the middle of a job, BullMQ notices (the job's lock expires) and moves the job back to `waiting` so another worker can take it. This is one more reason your jobs must be **idempotent** (Section 12).

### Real-world example — job status endpoint

A user clicks "Generate report" and wants to see the progress:

```ts
app.get("/jobs/:id", async (req, res) => {
  const job = await reportQueue.getJob(req.params.id);

  if (!job) {
    return res.status(404).json({ message: "Job not found" });
  }

  const state = await job.getState(); // "waiting", "active", "completed", ...

  // Return only safe fields. Don't return job.data blindly.
  res.json({
    id: job.id,
    state,
    failedReason: job.failedReason,
  });
});
```

> [!NOTE]
> If cleanup (Section 17) has already removed an old job, `getJob` returns nothing. That is normal.

---

## 4. Producer

A **Producer** adds jobs to the queue.

```ts
// producers/emailProducer.ts
export const createWelcomeEmailJob = async (userId: string, email: string) => {
  const job = await emailQueue.add("welcome-email", {
    userId,
    email,
  });

  return job.id;
};
```

### Real-world example (Express controller)

```ts
const jobId = await createWelcomeEmailJob(user.id, user.email);

res.status(202).json({
  message: "Email queued",
  jobId,
});
```

> [!TIP]
> Use **`202 Accepted`**. It means "we received your request and will process it". The email is **queued**, but not necessarily **sent** yet.

---

## 5. Worker

A **Worker** picks jobs from the queue and processes them.

```ts
import { Worker } from "bullmq";

const worker = new Worker(
  "email-queue",
  async (job) => {
    const { userId, email } = job.data;

    await sendWelcomeEmail(email);

    return {
      userId,
      sent: true,
    };
  },
  {
    connection,
  },
);
```

Flow:

```text
Queue
  ↓
Worker picks Job
  ↓
Processor runs
  ↓
Success → completed
Failure → failed / retry
```

### Production rule

Don't do heavy background work **inside the API request**.

```text
API Server
    ↓
Queue
    ↓
Worker
```

> [!TIP]
> 💡 **How does a worker say "this job failed"?** Just **throw an error** inside the processor. BullMQ catches it and either retries the job or marks it as failed. If the processor finishes without an error, the job is `completed`.

---

## 6. Consumer

In BullMQ, the **Worker is the consumer**.

```ts
new Worker(
  "email-queue",
  async (job) => {
    // consume / process the job
  },
  { connection },
);
```

Simple difference:

| Role                  | What it does                                     |
| --------------------- | ------------------------------------------------ |
| **Producer**          | Puts jobs **into** the queue                     |
| **Consumer / Worker** | Takes jobs **from** the queue and processes them |

---

## 7. Attempts

`attempts` tells BullMQ **how many times** a job can be tried at most.

```ts
await emailQueue.add(
  "send-email",
  { email },
  {
    attempts: 3,
  },
);
```

All attempts fail:

```text
Attempt 1 ❌
     ↓
Attempt 2 ❌
     ↓
Attempt 3 ❌
     ↓
Failed
```

Attempt 2 succeeds:

```text
Attempt 1 ❌
     ↓
Attempt 2 ✅
     ↓
Completed
```

### Real-world example

The email provider is **temporarily down**. Retrying a few times is useful, because it will probably work again soon.

> [!NOTE]
> 💡 If you don't set `attempts`, the default is **1**. That means **no retries**: one failure and the job is `failed`.

---

## 8. Backoff

Backoff decides **how long to wait before the next retry**.

```ts
await emailQueue.add(
  "send-email",
  { email },
  {
    attempts: 3,
    backoff: {
      type: "exponential",
      delay: 1000, // start with 1 second
    },
  },
);
```

Conceptually:

```text
Attempt 1 ❌
     ↓
wait 1s
     ↓
Attempt 2 ❌
     ↓
wait 2s
     ↓
Attempt 3
```

**Exponential backoff** means the waiting time **grows** after each failure. This prevents your system from hitting a failing service again and again.

### 💡 Two types of backoff

| Type          | Waiting time       | Example (`delay: 1000`) |
| ------------- | ------------------ | ----------------------- |
| `fixed`       | Same every time    | 1s, 1s, 1s              |
| `exponential` | Doubles every time | 1s, 2s, 4s              |

```ts
backoff: { type: "fixed", delay: 5000 } // always wait 5 seconds
```

### Real-world example

```text
Email Provider
      ↓
503 Service Unavailable
      ↓
Wait
      ↓
Retry
```

### Production rule

| Type of error                           | What to do                       |
| --------------------------------------- | -------------------------------- |
| **Temporary** (timeout, `503`, `429`)   | ✅ Retry                         |
| **Permanent** (invalid email, bad data) | ❌ Retrying usually doesn't help |

---

## 9. Delay

`delay` means:

> Don't process this job immediately. Wait first.

```ts
await emailQueue.add(
  "send-reminder",
  {
    orderId: "order_123",
  },
  {
    delay: 60_000, // 60 seconds (in milliseconds)
  },
);
```

The job becomes available after about 60 seconds.

### Real-world example

```text
Order created
     ↓
delay: 24 hours
     ↓
Reminder job
     ↓
Send reminder
```

```ts
await emailQueue.add(
  "order-reminder",
  { orderId },
  {
    delay: 24 * 60 * 60 * 1000, // 24 hours
  },
);
```

### Important: delay vs backoff

They are **different**:

| Option    | When it waits                   |
| --------- | ------------------------------- |
| `delay`   | **Before the first** processing |
| `backoff` | **Between failed attempts**     |

> [!IMPORTANT]
> 💡 **The delay is stored in Redis, not in memory.** Even if your server restarts during the wait, the job is not lost. This is why we use `delay` instead of `setTimeout()` for important work.
>
> Also, when the delay ends, the worker should **check the current state** before acting. For example, before sending a "complete your payment" reminder, check that the order is still unpaid.

---

## 10. Priority

Priority lets **important jobs go first**.

```ts
// Important
await emailQueue.add("password-reset", { userId }, { priority: 1 });

// Less important
await emailQueue.add("marketing-email", { userId }, { priority: 10 });
```

A **lower number** means **higher priority**:

```text
priority: 1  → more important
priority: 10 → less important
```

### Real-world example

| Job                  | Priority |
| -------------------- | -------- |
| Password reset       | High     |
| Payment notification | High     |
| Marketing email      | Lower    |

### Production rule

Don't use priority on every job without thinking. Too many priority levels make things complicated and can affect queue performance. Keep it simple, for example **2 or 3 levels**.

> [!NOTE]
> Jobs that have **no priority** set are treated as the most important. If you use priority in a queue, set it on **all** jobs of that queue so the order stays clear.

### Job options at a glance

All the options from Sections 7–10 can be used together:

```ts
await emailQueue.add(
  "order-reminder",
  { orderId },
  {
    delay: 30 * 60 * 1000, // wait 30 minutes before first processing
    attempts: 3, // try at most 3 times
    backoff: {
      type: "exponential", // wait longer after every failure
      delay: 1000,
    },
    priority: 5, // medium importance
    jobId: `order-reminder-${orderId}`, // avoid duplicates (see Section 12)
  },
);
```

---

## 11. Concurrency

Concurrency controls how many jobs **one Worker instance** can process **at the same time**.

```ts
const worker = new Worker("email-queue", processEmail, {
  connection,
  concurrency: 5,
});
```

Meaning:

```text
Worker
 ├── Job 1
 ├── Job 2
 ├── Job 3
 ├── Job 4
 └── Job 5
```

At most **5 active jobs** at once.

### Multiple workers

```text
Worker 1 → concurrency 5
Worker 2 → concurrency 5
Worker 3 → concurrency 5
```

Possible total:

```text
5 + 5 + 5 = 15 jobs at the same time
```

> [!TIP]
> **Jobs at the same time = number of workers × concurrency.** To go faster, you can raise concurrency **or** run more worker processes.

### Real-world example

Your email provider allows only **50 requests per second**. Setting `concurrency: 1000` would send too many requests and you would get `429 Too Many Requests`. Choose a value your provider can handle.

### Production rule

Don't just set:

```ts
concurrency: 1000;
```

Think about:

- CPU
- Memory
- Database capacity
- External API rate limits
- Network
- Type of job

### 💡 Extra: Add a speed limit with `limiter`

BullMQ can also limit how many jobs **start** in a time window. This protects the provider even if you run many workers.

```ts
const worker = new Worker("email-queue", processEmail, {
  connection,
  concurrency: 10,

  // At most 50 jobs every 1 second (for the whole queue)
  limiter: {
    max: 50,
    duration: 1000,
  },
});
```

---

## 12. Idempotency

**Idempotency** means:

> Running the same operation many times should not create a wrong final result.

This is very important because a background job can sometimes run **more than once**.

### Example problem

```text
orderId = order_123
```

The job sends an order confirmation email. The worker **crashes after sending the email** but **before the job is marked completed**. The job runs again:

```text
Email 1 → sent
Email 2 → sent again ❌
```

### Solution 1 — Business-level protection (in the worker)

Check the database first, so a repeated run can safely stop.

```ts
const order = await Order.findById(orderId);

if (order.confirmationEmailSent) {
  return; // already done → skip
}

await sendEmail(order.email);

await Order.updateOne(
  { _id: orderId },
  {
    $set: {
      confirmationEmailSent: true,
    },
  },
);
```

### Solution 2 — Deterministic job ID (in the producer)

The same order always gets the same job ID:

```ts
await emailQueue.add(
  "order-confirmation",
  { orderId },
  {
    jobId: `order-confirmation-${orderId}`,
  },
);
```

> [!IMPORTANT]
> A unique `jobId` helps prevent **duplicate adding** of jobs. It is **not** a replacement for business-level idempotency.
>
> Use **both**:
>
> - `jobId` → stops duplicate **adding**
> - DB check in the worker → stops duplicate **processing**

> [!NOTE]
>
> - Use a hyphen (`-`) in custom job IDs. Some BullMQ versions don't allow `:`.
> - A custom job ID can't be only a number (like `"123"`), so always add text in front.
> - The duplicate check works only while the old job is still in Redis. If cleanup already removed it, the same ID can be added again. That's why the worker check is needed too.

---

## 13. Dead Letter Queue (DLQ)

A **Dead Letter Queue (DLQ)** is a **separate queue** where you keep jobs that have **failed for good**.

> "Failed for good" means: it will not succeed even if we retry.

### 🧒 Simple example

Think of the **"undelivered letters" box** at a post office. If a letter can't be delivered after many tries, it is not thrown away. It goes into a special box so someone can look at it and decide what to do.

### Why do we need a DLQ?

Without a DLQ, a failed job is just one of thousands of old records that may get cleaned up. With a DLQ, you can:

- **Inspect** why the job failed
- **Debug** the problem
- **Replay** the job after fixing the issue
- **Alert** your team
- **Investigate** permanent failures

### Flow

```text
Main Queue
    │
    ▼
 Worker
    │
    ├── success → Completed
    │
    └── failure
          │
          ▼
        Retry (attempts + backoff)
          │
          ├── success → Completed
          │
          └── no retries left / permanent error
                    │
                    ▼
                   DLQ
```

> [!NOTE]
> BullMQ does **not** have a built-in DLQ feature. A DLQ is simply **another queue that you create yourself** and fill when a job fails for good.
>
> **Simpler option:** keep failed jobs in the main queue's `failed` list for a longer time (see [Section 17](#17-job-cleanup-retention)). A separate DLQ is better when you want alerts, a separate dashboard, or a replay tool just for failures.

### Which errors go to the DLQ?

Not every failure belongs in the DLQ.

| Error type                | Examples                                 | What to do                                                       |
| ------------------------- | ---------------------------------------- | ---------------------------------------------------------------- |
| **Retryable** (temporary) | Timeout, `503`, `429`                    | Retry with backoff. Go to the DLQ **only if all attempts fail**. |
| **Permanent**             | Invalid email, bad payload, missing data | Don't retry. Go to the DLQ **right away**.                       |

### Step 1 — Create the main queue and the DLQ

```ts
// queues/emailQueue.ts
import { Queue } from "bullmq";
import { connection } from "../config/connection.js";

export const emailQueue = new Queue("email-queue", {
  connection,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: "exponential", delay: 1000 },
  },
});

// This queue only STORES failed jobs. No worker processes it.
export const deadLetterQueue = new Queue("email-dead-letter-queue", {
  connection,
});
```

### Step 2 — Mark permanent errors with `UnrecoverableError`

When you throw `UnrecoverableError`, BullMQ **fails the job immediately** and does **not retry**.

```ts
import { Worker, UnrecoverableError } from "bullmq";

export const emailWorker = new Worker(
  "email-queue",
  async (job) => {
    const { email } = job.data;

    // Permanent problem → retrying will not help
    if (!email) {
      throw new UnrecoverableError("Email is missing");
    }

    // Temporary problems (timeout, 503...) are normal errors.
    // BullMQ will retry them automatically.
    await sendEmail(email);

    return { sent: true };
  },
  { connection },
);
```

### Step 3 — Copy the job to the DLQ when it fails for good

```ts
emailWorker.on("failed", async (job, error) => {
  if (!job) return;

  // This event runs after EVERY failed attempt,
  // so we first check: is this the end of the road?
  const isPermanent = error instanceof UnrecoverableError;
  const isLastAttempt = job.attemptsMade >= (job.opts.attempts ?? 1);

  if (!isPermanent && !isLastAttempt) return; // BullMQ will retry. Do nothing.

  try {
    await deadLetterQueue.add(
      "failed-email",
      {
        originalQueue: "email-queue",
        originalJobId: job.id,
        originalJobName: job.name,
        data: job.data,
        failedReason: error.message,
        attemptsMade: job.attemptsMade,
        failedAt: new Date().toISOString(),
      },
      {
        // Same failed job → same DLQ entry (no duplicates)
        jobId: `dlq-${job.id}`,
      },
    );

    console.error("Job moved to DLQ:", job.id);
    // TIP: send an alert here (Slack / email)
  } catch (dlqError) {
    // Never let DLQ problems crash the worker
    console.error("Could not write to DLQ:", dlqError);
  }
});
```

### Step 4 — Look inside the DLQ

Jobs in the DLQ stay in the `waiting` state because no worker takes them. That is **intended**.

```ts
const deadJobs = await deadLetterQueue.getJobs(["waiting"], 0, 49); // first 50

for (const deadJob of deadJobs) {
  console.log({
    id: deadJob.id,
    originalJobId: deadJob.data.originalJobId,
    reason: deadJob.data.failedReason,
    failedAt: deadJob.data.failedAt,
  });
}
```

### Step 5 — Replay jobs after fixing the problem

```ts
export const replayDeadJobs = async () => {
  const deadJobs = await deadLetterQueue.getJobs(["waiting"], 0, 99);

  for (const deadJob of deadJobs) {
    const { originalJobName, data, originalJobId } = deadJob.data;

    // Add it back to the main queue with a NEW job ID
    await emailQueue.add(originalJobName, data, {
      jobId: `replay-${originalJobId}-${Date.now()}`,
    });

    // Remove it from the DLQ
    await deadJob.remove();
  }

  console.log(`Replayed ${deadJobs.length} jobs`);
};
```

> [!WARNING]
> Replay **only after the root problem is fixed** (the provider is back, the bug is fixed, the bad data is corrected). Otherwise the jobs will just fail again. Replaying is safe only if your worker is **idempotent** (Section 12).

### Real-world example

**Order confirmation email**

```text
Order paid
    ↓
order-confirmation job
    ↓
Email provider is down for 2 hours
    ↓
Attempt 1 ❌ → Attempt 2 ❌ → Attempt 3 ❌
    ↓
Job moved to DLQ + Slack alert
    ↓
Team waits for the provider to recover
    ↓
Run replayDeadJobs()
    ↓
Customers get their emails ✅
```

### DLQ tips

- **Keep payloads small** (IDs, not big objects) and avoid sensitive data like tokens or passwords.
- **Set up an alert** when jobs arrive in the DLQ. A DLQ nobody looks at is useless.
- **Clean old DLQ jobs** from time to time (for example, after 30 days) so it doesn't grow forever.
- DLQ is for **investigation and replay**, not a permanent business record. Important history belongs in your database.

### Retry vs DLQ vs Cleanup

Don't mix these three:

| Concept     | Question it answers                         |
| ----------- | ------------------------------------------- |
| **Retry**   | What should happen right after a failure?   |
| **DLQ**     | Where do we keep jobs that failed for good? |
| **Cleanup** | How long do we keep old jobs?               |

---

## 14. Graceful Shutdown

When a server or container is stopping, **don't kill the worker immediately**. A job may be running.

❌ **Bad:**

```ts
process.on("SIGTERM", () => {
  process.exit(0);
});
```

✅ **Better:**

```ts
const shutdown = async (signal: string) => {
  console.log(`${signal} received`);

  await worker.close();

  console.log("Worker closed");

  process.exit(0);
};

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
```

Concept:

```text
SIGTERM
   ↓
Stop taking new jobs
   ↓
Finish current jobs
   ↓
Close worker
   ↓
Exit
```

### Real-world example

Docker / Kubernetes deployment:

```text
Old worker
    ↓
SIGTERM
    ↓
Finish active jobs
    ↓
Shutdown
    ↓
New worker starts
```

This prevents jobs from being interrupted in the middle.

> [!TIP]
>
> - `SIGTERM` is sent by Docker/Kubernetes when stopping. `SIGINT` is what you send with `Ctrl + C`.
> - 💡 Docker and Kubernetes wait only for a limited time (the "grace period") before force-killing. Set it to be **longer than your longest job**.

### 💡 Extra: Close everything in the right order

```ts
const shutdown = async (signal: string) => {
  console.log(`${signal} received`);

  await worker.close(); // 1. finish running jobs first
  await emailQueue.close(); // 2. close the queue
  await connection.quit(); // 3. close Redis last

  process.exit(0);
};
```

Close **Redis last**, because the running jobs still need it.

---

## 15. QueueEvents

`QueueEvents` lets you watch the **lifecycle of jobs in a queue** from one central place.

```ts
import { QueueEvents } from "bullmq";

const queueEvents = new QueueEvents("email-queue", { connection });

queueEvents.on("completed", ({ jobId }) => {
  console.log("Job completed:", jobId);
});

queueEvents.on("failed", ({ jobId, failedReason }) => {
  console.log("Job failed:", {
    jobId,
    failedReason,
  });
});
```

Useful events:

```text
waiting
active
progress
completed
failed
stalled
```

### Real-world example

An admin dashboard:

```text
Waiting:   120
Active:     10
Completed: 950
Failed:      8
```

QueueEvents helps power **monitoring** and **real-time job status** in your application.

---

## 16. Worker Events

Worker events belong to **one specific Worker instance**.

```ts
worker.on("active", (job) => {
  console.log("Job active:", job.id);
});

worker.on("completed", (job) => {
  console.log("Job completed:", job.id);
});

worker.on("failed", (job, error) => {
  console.error("Job failed:", {
    jobId: job?.id,
    error: error.message,
  });
});

worker.on("error", (error) => {
  console.error("Worker error:", error);
});
```

### Worker Events vs QueueEvents

|                | Worker Events                     | QueueEvents                                |
| -------------- | --------------------------------- | ------------------------------------------ |
| **Belongs to** | One specific Worker instance      | The whole queue                            |
| **Best for**   | Worker-level logging and behavior | Central monitoring, dashboards, job status |

> [!TIP]
> Always listen to the worker's `error` event. Without it, Redis connection problems can go unnoticed.

### 💡 Extra: Show job progress

A long job (like a report) can report its own progress:

```ts
const worker = new Worker(
  "report-queue",
  async (job) => {
    await job.updateProgress(25);
    await loadData();

    await job.updateProgress(60);
    await buildReport();

    await job.updateProgress(100);
    return { done: true };
  },
  { connection },
);

// Listen to it
worker.on("progress", (job, progress) => {
  console.log(`Job ${job.id} is ${progress}% done`);
});
```

Use this to show a progress bar to the user.

---

## 17. Job Cleanup (Retention)

💡 By default, BullMQ keeps jobs in Redis after they finish. If you never clean them, Redis memory keeps growing.

```text
100 jobs → 10,000 jobs → 1,000,000 jobs → Redis memory problem
```

Use `removeOnComplete` and `removeOnFail`:

| Option                             | Meaning                                   |
| ---------------------------------- | ----------------------------------------- |
| `removeOnComplete: true`           | Delete the job as soon as it completes    |
| `removeOnComplete: 100`            | Keep only the last 100 completed jobs     |
| `removeOnComplete: { age, count }` | Keep by **time** (seconds) and **number** |
| `removeOnFail: { age, count }`     | Same idea, for failed jobs                |

### Recommended production setting

Set it **once** in the queue's defaults, so every job follows it:

```ts
export const emailQueue = new Queue("email-queue", {
  connection,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: "exponential", delay: 1000 },

    // Successful jobs: short retention
    removeOnComplete: {
      age: 3600, // 1 hour
      count: 1000, // at most 1000 jobs
    },

    // Failed jobs: longer retention (useful for debugging)
    removeOnFail: {
      age: 24 * 3600, // 24 hours
      count: 5000, // at most 5000 jobs
    },
  },
});
```

> [!WARNING]
> Avoid `removeOnFail: true` in production. Failed jobs help you debug problems. Keep them longer than successful jobs.

> [!NOTE]
> `age` and `count` are **limits**, not an exact deletion time. Think "old jobs should not pile up", not "this job disappears at exactly 10:00:00".

---

## 18. Scheduled Jobs (Job Schedulers)

💡 A **delayed job** runs **once** later. A **scheduled job** runs **again and again** on a schedule.

|             | Delayed job                 | Scheduled job                   |
| ----------- | --------------------------- | ------------------------------- |
| **Runs**    | Once                        | Repeatedly                      |
| **Example** | Reminder after 24 hours     | Daily report at 9 AM            |
| **How**     | `queue.add(..., { delay })` | `queue.upsertJobScheduler(...)` |

### Example 1 — Run every 24 hours

```ts
await reportQueue.upsertJobScheduler(
  "daily-report-scheduler", // stable ID
  {
    every: 86_400_000, // every 24 hours (in milliseconds)
  },
  {
    name: "daily-report",
    data: {
      reportType: "daily",
    },
  },
);
```

### Example 2 — Run every day at 9:00 AM (cron)

```ts
await reportQueue.upsertJobScheduler(
  "daily-report-9am",
  {
    pattern: "0 9 * * *", // minute hour day month weekday
    tz: "UTC", // change to your timezone
  },
  {
    name: "daily-report",
    data: { reportType: "daily" },
  },
);
```

> [!IMPORTANT]
> **Use a stable scheduler ID.** `upsert` means "update if it exists, create if it doesn't". Your app can restart many times and the scheduler stays **one**.
>
> Don't use `setInterval()` for important schedules. With 3 servers, the task would run 3 times. With BullMQ, the schedule is stored in Redis, so only **one job** is created per run.

---

## 19. Useful Queue Commands

💡 Commands you will often use when working with and monitoring queues.

### Count jobs in each state

```ts
const counts = await emailQueue.getJobCounts(
  "waiting",
  "active",
  "delayed",
  "failed",
  "completed",
);

console.log(counts);
// { waiting: 12, active: 5, delayed: 0, failed: 1, completed: 340 }
```

### Get one job

```ts
const job = await emailQueue.getJob("job-id-here");
```

### Retry a failed job manually

```ts
const job = await emailQueue.getJob(jobId);
await job?.retry(); // moves a failed job back to waiting
```

### Remove a job

```ts
const job = await emailQueue.getJob(jobId);
await job?.remove();
```

### Pause and resume a queue

Useful during maintenance or when a provider has a long outage.

```ts
await emailQueue.pause(); // workers stop taking new jobs
// ... do maintenance ...
await emailQueue.resume(); // workers continue
```

| Command                            | What it does               |
| ---------------------------------- | -------------------------- |
| `queue.getJobCounts(...)`          | Number of jobs per state   |
| `queue.getJob(id)`                 | Get one job                |
| `job.getState()`                   | Current state of the job   |
| `job.retry()`                      | Retry a failed job         |
| `job.remove()`                     | Delete the job             |
| `queue.pause()` / `queue.resume()` | Stop / continue processing |

> [!CAUTION]
> Some commands are dangerous in production. `queue.drain()` removes waiting jobs and `queue.obliterate()` can delete a whole queue. Never run them on a live queue unless you are 100% sure.

---

## 20. Role of Redis in BullMQ

BullMQ uses Redis as its **backend**. Redis **stores and coordinates** the queue state.

```text
Producer
   ↓
BullMQ
   ↓
Redis
   ↓
BullMQ
   ↓
Worker
```

Redis manages things like:

```text
waiting jobs
active jobs
delayed jobs
completed jobs
failed jobs
job data
queue state
```

### Important

> [!WARNING]
> **Redis is not your main business database.** Don't use it as a replacement for MongoDB or PostgreSQL.

| System                   | What it stores                                                   |
| ------------------------ | ---------------------------------------------------------------- |
| **MongoDB / PostgreSQL** | Orders, Users, Payments (business data)                          |
| **Redis / BullMQ**       | Background job state, queue coordination, retry/delay scheduling |

### Production rule

**Business-critical data should live in your proper database.**

### 💡 Extra: Redis settings for production

| Setting                       | Why                                                                                |
| ----------------------------- | ---------------------------------------------------------------------------------- |
| `maxmemory-policy noeviction` | If Redis is full, it should **refuse new data**, not silently **delete your jobs** |
| Persistence (AOF / RDB)       | Jobs survive a Redis restart                                                       |
| Enough memory + job cleanup   | Redis keeps jobs in memory (see Section 17)                                        |
| Password / private network    | Don't expose Redis to the internet                                                 |

---

## 21. Common Production Mistakes

### ❌ 1. Doing heavy work inside an API request

**Bad:**

```ts
app.post("/send-email", async (req, res) => {
  await sendEmail();
  res.json({ message: "done" });
});
```

**Better:**

```ts
app.post("/send-email", async (req, res) => {
  await emailQueue.add("send-email", { email: req.body.email });
  res.status(202).json({ message: "Email queued" });
});
```

```text
API
 ↓
Queue
 ↓
202 Accepted

Worker
 ↓
Send email
```

---

### ❌ 2. Retrying permanent errors

**Bad:**

```text
Invalid email
   ↓
Retry
   ↓
Retry
   ↓
Retry
```

**Better:**

```text
Invalid email
    ↓
Permanent failure
    ↓
DLQ / alert / manual handling
```

Use `UnrecoverableError` to say "don't retry this job", then move it to the **DLQ** (see [Section 13](#13-dead-letter-queue-dlq)):

```ts
import { UnrecoverableError } from "bullmq";

if (!email) {
  throw new UnrecoverableError("Email is missing");
}
```

---

### ❌ 3. No idempotency

Never assume:

```text
One job = exactly one execution
```

Design important business operations so they are **safe if they run twice** (see Section 12).

---

### ❌ 4. Huge job payloads

**Bad:**

```ts
{
  (entireUser, entireOrder, entireProductCatalog);
}
```

**Better:**

```ts
{
  (userId, orderId);
}
```

---

### ❌ 5. Unlimited concurrency

**Bad:**

```ts
concurrency: 1000;
```

without checking downstream limits (database, external API, CPU, memory).

---

### ❌ 6. No graceful shutdown

Don't do this while workers may have active jobs:

```ts
process.exit();
```

Close the worker first (see Section 14).

---

### ❌ 7. Running multiple schedulers by accident

Use a **stable scheduler ID**, so restarting your app doesn't create duplicates.

```ts
await queue.upsertJobScheduler(
  "daily-report-scheduler", // same ID every time
  {
    every: 86_400_000, // every 24 hours
  },
  {
    name: "daily-report",
    data: {
      reportType: "daily",
    },
  },
);
```

Avoid generating a random scheduler ID every time the application starts.

---

### ❌ 8. Logging sensitive data

**Avoid:**

```ts
console.log(job.data);
```

Job data may contain:

- Emails
- Tokens
- Customer information
- Payment-related information

Log only what is useful.

---

### ❌ 9. Using `console.log` everywhere in production

Prefer a **structured logger** (for example `pino` or `winston`):

```ts
logger.info(
  {
    jobId: job.id,
    jobName: job.name,
  },
  "Email job completed",
);
```

This makes logs easier to search and monitor.

---

### ❌ 10. Treating BullMQ as your database

Don't use queue data as the source of truth for business entities.

```text
PostgreSQL / MongoDB
       ↓
Source of truth

Redis / BullMQ
       ↓
Background processing
```

---

### 💡 Bonus mistakes

#### ❌ 11. No cleanup of old jobs

Completed and failed jobs pile up in Redis until memory runs out.

**Fix:** set `removeOnComplete` and `removeOnFail` (Section 17).

#### ❌ 12. Failed jobs that nobody looks at

Jobs fail quietly and the team finds out days later.

**Fix:** use a **DLQ** and **alerts** (Section 13), and watch `failed` counts (Section 19).

#### ❌ 13. Blocking Node.js with heavy work

A very heavy synchronous calculation can freeze the worker. BullMQ may think the worker died and give the job to another worker, so the job runs **twice**.

**Fix:** keep heavy work small, or run it in a separate worker process.

#### ❌ 14. Wrong Redis memory setting

If Redis is allowed to evict (delete) keys when it is full, it can **delete your queue data**.

**Fix:** use `maxmemory-policy noeviction` (Section 20).

---

## Final Production Flow

Remember this architecture:

```text
                         API
                          │
                          ▼
                     Producer
                          │
                          ▼
                       Queue
                          │
                          ▼
                        Redis
                          │
               ┌──────────┴──────────┐
               ▼                     ▼
           Worker 1               Worker 2
         concurrency 5          concurrency 5
               │                     │
               └──────────┬──────────┘
                          ▼
                    Business Logic
                          │
                 ┌────────┴────────┐
                 ▼                 ▼
             Database        External API
                 │                 │
                 └────────┬────────┘
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
          Completed             Failed for good
                                      │
                                      ▼
                                     DLQ
                                (inspect / replay / alert)
```

The production safety layer around it:

```text
Retries
Backoff
Delay
Priority
Concurrency
Idempotency
DLQ
Job Cleanup
QueueEvents
Worker Events
Graceful Shutdown
Structured Logging
```

---

## Production Checklist

Before going live, check:

**Setup**

- [ ] `maxRetriesPerRequest: null` on the Redis connection
- [ ] Meaningful queue names (one queue per type of work)
- [ ] API and workers run as separate processes

**Jobs**

- [ ] Job data contains IDs and small data only
- [ ] `attempts` and `backoff` are set for temporary failures
- [ ] Permanent errors use `UnrecoverableError`
- [ ] Workers are idempotent (DB check) and producers use stable `jobId`s

**Failures**

- [ ] Dead Letter Queue (or longer `removeOnFail` retention) is in place
- [ ] Alerts exist for failed jobs
- [ ] A replay process is ready

**Performance**

- [ ] Concurrency matches what the provider/database can handle
- [ ] `limiter` is set when the provider has a rate limit

**Cleanup and Redis**

- [ ] `removeOnComplete` and `removeOnFail` are set
- [ ] Redis uses `noeviction` and has persistence

**Operations**

- [ ] Graceful shutdown handles `SIGTERM` and `SIGINT`
- [ ] Worker `error` event is logged
- [ ] Queue counts are monitored (waiting, active, failed, delayed)
- [ ] Sensitive data is not logged
- [ ] Schedulers use a stable ID

---

## 30-Second Revision

| Concept                     | One-line meaning                                                                                        |
| --------------------------- | ------------------------------------------------------------------------------------------------------- |
| **Queue**                   | Stores background jobs                                                                                  |
| **Job**                     | One unit of work                                                                                        |
| **Job states**              | waiting → active → completed / failed (plus delayed, prioritized)                                       |
| **Producer**                | Adds jobs to the queue                                                                                  |
| **Worker**                  | Processes jobs                                                                                          |
| **Consumer**                | The worker that consumes jobs                                                                           |
| **Attempts**                | Maximum number of tries                                                                                 |
| **Backoff**                 | Waiting time between retries (`fixed` or `exponential`)                                                 |
| **Delay**                   | Waiting time before the first processing                                                                |
| **Priority**                | Important jobs get preference (lower number = more important)                                           |
| **Concurrency**             | Number of jobs one worker handles at the same time                                                      |
| **Idempotency**             | Running the same job twice is safe                                                                      |
| **Dead Letter Queue (DLQ)** | A separate queue that stores jobs that failed for good, so you can inspect, alert, and replay them      |
| **UnrecoverableError**      | Throw it to fail a job immediately without retries                                                      |
| **Graceful Shutdown**       | Finish current jobs, then stop safely                                                                   |
| **QueueEvents**             | Central queue lifecycle monitoring                                                                      |
| **Worker Events**           | Events from one Worker instance                                                                         |
| **Job Cleanup**             | `removeOnComplete` / `removeOnFail` keep Redis from filling up                                          |
| **Job Scheduler**           | Creates repeating jobs (`upsertJobScheduler` with a stable ID)                                          |
| **Redis**                   | BullMQ's backend for queue state and coordination                                                       |
| **Production**              | Retry smartly + scale workers + make operations idempotent + use a DLQ + monitor + shut down gracefully |

## Production Golden Rule

> **BullMQ makes background processing asynchronous. Production engineering makes that processing reliable, scalable, observable, and safe to repeat.**
