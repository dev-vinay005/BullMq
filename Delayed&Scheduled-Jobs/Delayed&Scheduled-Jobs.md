# BullMQ Day 5 — Delayed & Scheduled Jobs

> Revision notes on delayed jobs, scheduled jobs, retries, idempotency, cleanup, and production rules in BullMQ.

---

## 📑 Table of Contents

1. [Delayed Jobs](#1-delayed-jobs)
2. [Adding a Delayed Job](#2-adding-a-delayed-job)
3. [Production Example — Welcome Email](#3-production-example--welcome-email)
4. [Delayed Job + Retry](#4-delayed-job--retry)
5. [Scheduled Jobs](#5-scheduled-jobs)
6. [Production Scheduled Job](#6-production-scheduled-job)
7. [Scheduled Job vs Delayed Job](#7-scheduled-job-vs-delayed-job)
8. [Production Example — Order Reminder](#8-production-example--order-reminder)
9. [Delayed Jobs Need Idempotency](#9-delayed-jobs-need-idempotency)
10. [Cleanup Strategy](#10-cleanup-strategy)
11. [Production Cleanup Policy](#11-production-cleanup-policy)
12. [Scheduled Cleanup Job](#12-scheduled-cleanup-job)
13. [Delayed Job + Server Restart](#13-delayed-job--server-restart)
14. [Don't Put Huge Data Inside Delayed Jobs](#14-dont-put-huge-data-inside-delayed-jobs)
15. [Production Rules for Delayed/Scheduled Jobs](#15-production-rules-for-delayedscheduled-jobs)
16. [Production Architecture](#16-production-architecture)
17. [Production-Grade Code Snippets](#18-production-grade-code-snippets)
    - [18.1 Install](#181-install)
    - [18.2 Folder Structure](#182-folder-structure)
    - [18.3 Redis Connection & Logger](#183-redis-connection--logger)
    - [18.4 Queue Definitions](#184-queue-definitions-with-safe-defaults)
    - [18.5 Producers](#185-producers-adding-delayed-jobs)
    - [18.6 Email Worker](#186-email-worker-retry-aware--idempotent)
    - [18.7 Order Reminder Worker](#187-order-reminder-worker-checks-current-business-state)
    - [18.8 Cancel a Reminder](#188-cancel-a-reminder-when-its-no-longer-needed)
    - [18.9 Job Schedulers + Maintenance Worker](#189-scheduled-jobs-job-schedulers--maintenance-worker)
    - [18.10 Express Controller](#1810-express-controller-using-the-producer)
    - [18.11 Worker Entry Point & Graceful Shutdown](#1811-worker-entry-point-with-graceful-shutdown)
    - [18.12 What Makes This Production-Grade?](#1812-what-makes-this-production-grade)
    - [18.13 Environment Variables](#1813-quick-environment-variables-example)
18. [One-Line Mental Model](#-one-line-mental-model)

---

## 1. Delayed Jobs

A **delayed job** is a job that should **not** be processed immediately.

### Real-world examples

- Send a welcome email 5 minutes after signup
- Send a payment reminder after 24 hours
- Retry a business workflow at a specific time
- Send a notification after 30 seconds
- Schedule order cancellation after 15 minutes

### Flow

```text
API
 │
 │ add job + delay
 ▼
BullMQ Queue
 │
 │ waiting for delay
 ▼
Delayed Job
 │
 │ delay expires
 ▼
Waiting
 │
 ▼
Worker
 │
 ▼
Process Job
```

---

## 2. Adding a Delayed Job

Production-style producer:

```js
await emailQueue.add(
  "welcome-email",
  {
    userId,
    email,
    name,
  },
  {
    delay: 5 * 60 * 1000, // 5 minutes
  },
);
```

`delay: 5 * 60 * 1000` means:

```text
Current time
     ↓
  5 minutes
     ↓
Job becomes eligible
     ↓
Worker processes it
```

> [!IMPORTANT]
> **Important distinction:** `delay` does **not** mean the worker sleeps for 5 minutes.
>
> Instead, BullMQ keeps the job in a _delayed_ state until it becomes eligible for processing.

---

## 3. Production Example — Welcome Email

Suppose a user registers. We don't necessarily want to send the welcome email synchronously (right away, inside the API request).

### API

```js
export const registerUser = async (req, res, next) => {
  try {
    const user = await createUser(req.body);

    await emailQueue.add(
      "welcome-email",
      {
        userId: user.id,
        email: user.email,
        name: user.name,
      },
      {
        delay: 5 * 60 * 1000,
        attempts: 3,
        backoff: {
          type: "exponential",
          delay: 1000,
        },
      },
    );

    return res.status(201).json({
      message: "User registered successfully",
    });
  } catch (error) {
    next(error);
  }
};
```

### Worker

```js
const worker = new Worker(
  "email-queue",
  async (job) => {
    const { userId, email, name } = job.data;

    console.log("Processing welcome email", {
      jobId: job.id,
      userId,
      email,
    });

    await emailService.sendWelcomeEmail({
      email,
      name,
    });

    return {
      sent: true,
    };
  },
  {
    connection,
  },
);
```

### Production lifecycle

```text
User Signup
 │
 ▼
Create User in DB
 │
 ▼
Add Email Job
 │
 │ delay = 5 min
 ▼
Delayed
 │
 │ 5 min completed
 ▼
Waiting
 │
 ▼
Worker
 │
 ▼
Email Provider
 │
 ├── Success → Completed
 │
 └── Failure → Retry
```

---

## 4. Delayed Job + Retry

Delayed jobs and retries are **different concepts**.

| Concept   | What it controls                       |
| --------- | -------------------------------------- |
| **Delay** | _When_ the first processing can happen |
| **Retry** | _What happens after a failure_         |

### Delay

```js
{
  delay: 300000;
}
```

### Retry

```js
{
  attempts: 3,
  backoff: {
    type: "exponential",
    delay: 1000,
  },
}
```

### Using both together

```js
await queue.add("payment-reminder", data, {
  delay: 60 * 60 * 1000,

  attempts: 3,

  backoff: {
    type: "exponential",
    delay: 2000,
  },
});
```

Meaning:

```text
Wait 1 hour
     ↓
Attempt #1
     ↓
Failure
     ↓
Retry after backoff
     ↓
Attempt #2
     ↓
Failure
     ↓
Retry
     ↓
Attempt #3
```

---

## 5. Scheduled Jobs

A scheduled job is **different** from a one-time delayed job.

### Delayed job

Runs **once** after a specified delay.

```text
Now
 ↓
10 minutes
 ↓
Run once
```

### Scheduled job

Runs according to a **recurring schedule**.

```text
Every day
   ↓
  Run
   ↓
Tomorrow
   ↓
  Run
   ↓
Next day
   ↓
  Run
```

### Typical use cases

- Daily reports
- Cleanup
- Subscription checks
- Database maintenance
- Analytics aggregation
- Reminder systems
- Periodic synchronization

---

## 6. Production Scheduled Job

For modern BullMQ, prefer **Job Schedulers** for recurring workloads.

Conceptually:

```js
await queue.upsertJobScheduler(
  "daily-report",
  {
    pattern: "0 9 * * *", // every day at 9:00 AM
  },
  {
    name: "generate-daily-report",
    data: {
      type: "daily-report",
    },
  },
);
```

The important production idea:

```text
Scheduler
 │
 │ creates jobs according to schedule
 ▼
Queue
 │
 ▼
Worker
 │
 ▼
Generate Report
```

### Why `upsert` is useful

Your application may restart multiple times. You don't want every application startup to accidentally create another scheduler.

Using a **stable scheduler ID** (like `"daily-report"`) allows you to maintain/update the scheduler instead of blindly creating duplicates.

---

## 7. Scheduled Job vs Delayed Job

| Feature         | Delayed Job           | Scheduled Job      |
| --------------- | --------------------- | ------------------ |
| **Runs**        | Once                  | Repeatedly         |
| **Timing**      | Specific delay        | Recurring schedule |
| **Example**     | Reminder after 10 min | Daily report       |
| **Typical API** | `queue.add()`         | Job Scheduler      |
| **Repetition**  | No                    | Yes                |

Think of it like this:

- **Delayed:** _"Do this later."_
- **Scheduled:** _"Keep doing this according to this schedule."_

---

## 8. Production Example — Order Reminder

Suppose an order is created and you want to remind the user after **30 minutes**.

### Producer

```js
await notificationQueue.add(
  "order-reminder",
  {
    orderId,
    userId,
    email,
  },
  {
    delay: 30 * 60 * 1000,
    attempts: 3,
    backoff: {
      type: "exponential",
      delay: 2000,
    },
  },
);
```

### Worker

```js
const worker = new Worker(
  "notification-queue",
  async (job) => {
    const { orderId, userId, email } = job.data;

    const order = await orderRepository.findById(orderId);

    if (!order) {
      throw new Error("Order not found");
    }

    if (order.status !== "pending") {
      return {
        skipped: true,
        reason: "Order is no longer pending",
      };
    }

    await notificationService.sendReminder({
      userId,
      email,
      orderId,
    });

    return {
      sent: true,
    };
  },
  { connection },
);
```

### Important production insight

> [!WARNING]
> The worker must **not blindly send** the reminder. It should **verify the current business state** first.

**Why?** Look at this timeline:

```text
Order created
     ↓
Reminder scheduled
     ↓
Customer pays after 10 minutes
     ↓
30 minutes pass
     ↓
Reminder job executes
```

If you don't check the order status:

```text
Customer already paid
        ↓
Still receives: "Please complete your payment"
```

❌ Bad production behavior.

**Instead:**

```text
Job executes
     ↓
Fetch current order
     ↓
Is order still pending?
 │
 ├── YES → Send reminder
 │
 └── NO  → Skip job
```

---

## 9. Delayed Jobs Need Idempotency

> **Idempotency** = doing the same thing more than once should not cause extra harm or duplicate side effects.

This becomes especially important with scheduled/delayed workflows.

Imagine the `order-reminder` job gets accidentally added **twice**. You could get:

```text
Reminder #1 → sent
Reminder #2 → sent
```

The customer receives **duplicate notifications**.

### Solution: use a stable `jobId`

A stable `jobId` can help prevent duplicate job creation:

```js
await notificationQueue.add(
  "order-reminder",
  {
    orderId,
    userId,
  },
  {
    delay: 30 * 60 * 1000,
    jobId: `order-reminder:${orderId}`,
  },
);
```

Concept:

```text
Order 123
   │
   └── jobId: order-reminder:123
```

The application can use a **deterministic ID** (same input → same ID) for deduplication.

> [!NOTE]
> Job-level deduplication is **not the same thing** as complete business-level idempotency.
> Your worker should **still safely handle duplicate execution**.

---

## 10. Cleanup Strategy

Don't allow Redis to accumulate millions of completed jobs forever.

### ❌ Bad

```js
await queue.add("email", data);
```

...with **unlimited historical retention**. Over time:

```text
100 jobs
10,000 jobs
1,000,000 jobs
10,000,000 jobs
      ↓
Redis memory pressure
```

### ✅ For jobs where history isn't required

```js
{
  removeOnComplete: true,
  removeOnFail: true,
}
```

### ✅ For production systems that need some history

```js
{
  removeOnComplete: {
    age: 3600,    // keep for 1 hour
    count: 1000,  // keep at most 1000
  },

  removeOnFail: {
    age: 24 * 3600, // keep for 24 hours
    count: 5000,    // keep at most 5000
  },
}
```

Conceptually:

```text
Completed jobs
 │
 ├── Keep recent 1000
 └── Delete older jobs

Failed jobs
 │
 ├── Keep up to 5000
 └── Delete old failures
```

### Why keep failed jobs longer?

Failed jobs are useful for:

- Debugging
- Monitoring
- Incident investigation
- Identifying provider problems

---

## 11. Production Cleanup Policy

A reasonable starting strategy:

| Type of data               | Strategy         |
| -------------------------- | ---------------- |
| Successful jobs            | Short retention  |
| Failed jobs                | Longer retention |
| Important business history | Store in DB      |

> [!IMPORTANT]
> Don't use BullMQ's job storage as your **permanent business database**.

Example:

```text
Email job completed
        ↓
BullMQ history
        ↓
Temporary operational data

Email audit/history
        ↓
Database
        ↓
Permanent business data
```

---

## 12. Scheduled Cleanup Job

You can use a scheduler to periodically perform maintenance.

Example architecture:

```text
Daily Scheduler
      ↓
cleanup-jobs
      ↓
Worker
      ↓
Remove/Archive old application data
```

**Important production principle:**

> Don't depend on a single application instance's `setInterval()` for critical scheduling.

### ❌ Avoid

```js
setInterval(
  () => {
    cleanupDatabase();
  },
  24 * 60 * 60 * 1000,
);
```

### Why?

If you have:

```text
Server 1
Server 2
Server 3
```

each server may run the cleanup. You could accidentally execute the **same task 3 times**.

✅ Use a **centralized scheduler/queue approach** instead.

---

## 13. Delayed Job + Server Restart

One of the biggest reasons to use BullMQ instead of an in-memory timer.

### ❌ Bad

```js
setTimeout(
  () => {
    sendEmail();
  },
  5 * 60 * 1000,
);
```

Suppose:

```text
API Server
 │
 └── setTimeout()
        │
        │ 3 minutes
        ▼
   SERVER CRASH
```

👉 The timer **disappears**.

### ✅ BullMQ

```text
API
 │
 ▼
Redis
 │
 └── Delayed Job
        │
        ▼
 Server crashes
        │
        ▼
 Server restarts
        │
        ▼
 Worker processes job
```

That's the production advantage.

---

## 14. Don't Put Huge Data Inside Delayed Jobs

### ❌ Bad

```js
await queue.add("send-report", {
  entireUserObject,
  hugeOrderObject,
  hugeProductList,
  largePdfData,
});
```

### ✅ Better

```js
await queue.add("send-report", {
  reportId,
  userId,
});
```

### Worker

```js
const { reportId, userId } = job.data;

const report = await reportRepository.findById(reportId);
```

### Why?

Redis should not become a dumping ground for large application objects.

```text
Job
 │
 ├── IDs
 └── Small metadata

Worker
 │
 └── Fetch actual data from DB/storage
```

---

## 15. Production Rules for Delayed/Scheduled Jobs

### Rule 1 — Keep job payload small

```text
IDs > huge objects
```

### Rule 2 — Make workers idempotent

The same job should not cause dangerous duplicate side effects.

### Rule 3 — Verify current business state

Especially for reminders.

```text
Scheduled 30 min ago
        ≠
Still valid 30 min later
```

### Rule 4 — Configure retries intentionally

Not every failure should be retried.

| Situation                  | Retry?                  |
| -------------------------- | ----------------------- |
| Temporary provider failure | ✅ Retry                |
| Invalid email              | ❌ Probably don't retry |
| Missing required data      | ❌ Probably don't retry |

### Rule 5 — Configure retention

Don't keep unlimited completed/failed jobs.

### Rule 6 — Don't rely on `setTimeout` for durable workflows

Use BullMQ.

### Rule 7 — Don't use application-local cron for distributed production systems

Use centralized scheduling.

### Rule 8 — Use meaningful job names

| ✅ Good                    | ❌ Bad    |
| -------------------------- | --------- |
| `send-welcome-email`       | `job1`    |
| `order-payment-reminder`   | `task`    |
| `generate-daily-report`    | `process` |
| `cleanup-expired-sessions` | `test`    |

---

## 16. Production Architecture

A realistic Day-5 architecture:

```text
                ┌──────────────┐
                │   Express    │
                │     API      │
                └──────┬───────┘
                       │
                       │ add job
                       ▼
                ┌──────────────┐
                │   BullMQ     │
                │    Queue     │
                └──────┬───────┘
                       │
                ┌──────▼───────┐
                │    Redis     │
                │              │
                │ Delayed Jobs │
                │ Schedulers   │
                └──────┬───────┘
                       │
                delay expires
                       │
                       ▼
                ┌──────────────┐
                │    Worker    │
                └──────┬───────┘
                       │
                       ▼
             ┌──────────────────┐
             │ External Service │
             │ Email/Payment/   │
             │ Notification     │
             └──────────────────┘
```

---

## 17. Production-Grade Code Snippets

This section puts **everything from Sections 1–17 together** in real, ready-to-use code.

> [!NOTE]
>
> - Uses **ES Modules** (`import` / `export`), **BullMQ v5+**, **ioredis**, and **pino** (logger).
> - Replace helper names like `emailService`, `orderRepository`, `emailLogRepository` with your own code.
> - Each snippet follows the rules from [Section 15](#15-production-rules-for-delayedscheduled-jobs).

### 18.1 Install

```bash
npm install bullmq ioredis pino
```

### 18.2 Folder Structure

```text
src/
├── config/
│   ├── redis.js                  # Redis connection
│   └── logger.js                 # Logger
├── queues/
│   ├── index.js                  # All queue definitions
│   ├── email.producer.js         # Functions that add email jobs
│   └── notification.producer.js  # Functions that add/cancel reminder jobs
├── workers/
│   ├── worker.utils.js           # Shared worker logging
│   ├── email.worker.js
│   ├── notification.worker.js
│   └── maintenance.worker.js
├── schedulers/
│   └── index.js                  # Recurring jobs (Job Schedulers)
├── controllers/
│   └── auth.controller.js        # Express API
├── server.js                     # API process
└── worker.js                     # Worker process
```

> [!TIP]
> Run the **API** and the **workers** as **separate processes**. This way you can scale them independently, and a heavy job will never slow down your API.

---

### 18.3 Redis Connection & Logger

**`src/config/redis.js`**

```js
import IORedis from "ioredis";

export const connection = new IORedis({
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD,

  // Required by BullMQ for workers.
  // Without this, workers can throw errors when Redis is slow or reconnecting.
  maxRetriesPerRequest: null,
});
```

**`src/config/logger.js`**

```js
import pino from "pino";

export const logger = pino({
  level: process.env.LOG_LEVEL || "info",
});
```

**Why?**

- One shared config → no copy-pasted Redis settings.
- Credentials come from **environment variables**, never hard-coded.

---

### 18.4 Queue Definitions (with Safe Defaults)

Set the retry and cleanup options **once** in `defaultJobOptions`, so every job gets them automatically.

**`src/queues/index.js`**

```js
import { Queue } from "bullmq";
import { connection } from "../config/redis.js";

// Default options applied to every job in the queue.
// Individual jobs can still override them.
const defaultJobOptions = {
  attempts: 3,
  backoff: {
    type: "exponential",
    delay: 1000,
  },
  removeOnComplete: {
    age: 3600, // keep for 1 hour
    count: 1000, // keep at most 1000
  },
  removeOnFail: {
    age: 24 * 3600, // keep failed jobs for 24 hours
    count: 5000, // keep at most 5000
  },
};

export const emailQueue = new Queue("email-queue", {
  connection,
  defaultJobOptions,
});

export const notificationQueue = new Queue("notification-queue", {
  connection,
  defaultJobOptions,
});

export const maintenanceQueue = new Queue("maintenance-queue", {
  connection,
  defaultJobOptions,
});
```

**Covers:** retries (Rule 4), retention (Rule 5), meaningful names (Rule 8).

---

### 18.5 Producers (Adding Delayed Jobs)

Keep the "how to add a job" logic in **one place**. Your controllers then stay clean and every job is created the same way.

**`src/queues/email.producer.js`**

```js
import { emailQueue } from "./index.js";

const FIVE_MINUTES = 5 * 60 * 1000;

export const scheduleWelcomeEmail = ({ userId, email, name }) => {
  return emailQueue.add(
    "send-welcome-email",
    // Small payload: only what the worker needs
    { userId, email, name },
    {
      delay: FIVE_MINUTES,
      // Same user → same jobId → no duplicate welcome emails
      jobId: `welcome-email-${userId}`,
    },
  );
};
```

**`src/queues/notification.producer.js`**

```js
import { notificationQueue } from "./index.js";

const THIRTY_MINUTES = 30 * 60 * 1000;

// One place to build the ID, so adding and cancelling always match
const reminderJobId = (orderId) => `order-reminder-${orderId}`;

export const scheduleOrderReminder = ({ orderId, userId, email }) => {
  return notificationQueue.add(
    "order-reminder",
    { orderId, userId, email },
    {
      delay: THIRTY_MINUTES,
      jobId: reminderJobId(orderId),
    },
  );
};

export const cancelOrderReminder = async (orderId) => {
  const job = await notificationQueue.getJob(reminderJobId(orderId));
  if (!job) return;

  const state = await job.getState();

  // Only remove jobs that have not started yet
  if (state === "delayed" || state === "waiting") {
    await job.remove();
  }
};
```

> [!NOTE]
> **About `jobId`:**
>
> - Use a **hyphen** (`-`) in custom job IDs, like `order-reminder-123`. Some BullMQ versions do not allow `:` in a custom `jobId`, so the hyphen is the safe choice.
> - A custom `jobId` also **cannot be a plain number** (like `"123"`), so always add a text prefix.
> - Deduplication only works while the old job still exists in Redis. If a finished job was already removed by `removeOnComplete`, the same `jobId` can be added again. This is why the worker must stay idempotent (see 18.7).

---

### 18.6 Email Worker (Retry-Aware & Idempotent)

**`src/workers/worker.utils.js`** — shared logging for all workers

```js
import { logger } from "../config/logger.js";

export const attachWorkerEvents = (worker, name) => {
  worker.on("completed", (job, result) => {
    logger.info({ worker: name, jobId: job.id, result }, "Job completed");
  });

  worker.on("failed", (job, error) => {
    logger.error(
      {
        worker: name,
        jobId: job?.id,
        jobName: job?.name,
        attemptsMade: job?.attemptsMade,
        maxAttempts: job?.opts?.attempts,
        error: error.message,
      },
      "Job failed",
    );
    // TIP: when attemptsMade reaches maxAttempts, send an alert (Slack, email, etc.)
  });

  // Connection/Redis problems. Without this handler, errors can go unnoticed.
  worker.on("error", (error) => {
    logger.error({ worker: name, error: error.message }, "Worker error");
  });
};
```

**`src/workers/email.worker.js`**

```js
import { Worker, UnrecoverableError } from "bullmq";
import { connection } from "../config/redis.js";
import { logger } from "../config/logger.js";
import { attachWorkerEvents } from "./worker.utils.js";
import { emailService } from "../services/email.service.js";
import { emailLogRepository } from "../repositories/emailLog.repository.js";

export const emailWorker = new Worker(
  "email-queue",
  async (job) => {
    switch (job.name) {
      case "send-welcome-email": {
        const { userId, email, name } = job.data;

        // Bad data will never become good by retrying → fail immediately
        if (!email) {
          throw new UnrecoverableError("Email is missing");
        }

        // Idempotency: if this job runs twice, don't send twice
        const alreadySent = await emailLogRepository.exists({
          userId,
          type: "welcome",
        });

        if (alreadySent) {
          return { skipped: true, reason: "Welcome email already sent" };
        }

        logger.info({ jobId: job.id, userId }, "Processing welcome email");

        // Temporary errors here (provider down, timeout) → BullMQ retries
        await emailService.sendWelcomeEmail({ email, name });

        // Permanent business history belongs in the DB, not in BullMQ
        await emailLogRepository.create({ userId, type: "welcome" });

        return { sent: true };
      }

      default:
        throw new UnrecoverableError(`Unknown job name: ${job.name}`);
    }
  },
  {
    connection,
    concurrency: 5, // process up to 5 jobs at the same time
  },
);

attachWorkerEvents(emailWorker, "email-worker");
```

**What this does:**

| Situation              | What happens                                  |
| ---------------------- | --------------------------------------------- |
| Email provider is down | Error is thrown → **retried** with backoff    |
| `email` is missing     | `UnrecoverableError` → **no retry** (Rule 4)  |
| Job runs twice         | DB check → **second run is skipped** (Rule 2) |
| Success                | Saved in DB as **permanent history**          |

---

### 18.7 Order Reminder Worker (Checks Current Business State)

**`src/workers/notification.worker.js`**

```js
import { Worker, UnrecoverableError } from "bullmq";
import { connection } from "../config/redis.js";
import { attachWorkerEvents } from "./worker.utils.js";
import { orderRepository } from "../repositories/order.repository.js";
import { notificationService } from "../services/notification.service.js";

export const notificationWorker = new Worker(
  "notification-queue",
  async (job) => {
    if (job.name !== "order-reminder") {
      throw new UnrecoverableError(`Unknown job name: ${job.name}`);
    }

    const { orderId, userId, email } = job.data;

    // Always fetch the CURRENT order. Never trust old data.
    const order = await orderRepository.findById(orderId);

    // Missing data will not fix itself → don't retry
    if (!order) {
      throw new UnrecoverableError(`Order ${orderId} not found`);
    }

    // Customer may have paid or cancelled while the job was waiting
    if (order.status !== "pending") {
      return { skipped: true, reason: "Order is no longer pending" };
    }

    // Safety net: reminder already sent earlier (duplicate execution)
    if (order.reminderSentAt) {
      return { skipped: true, reason: "Reminder already sent" };
    }

    await notificationService.sendReminder({ userId, email, orderId });
    await orderRepository.markReminderSent(orderId);

    return { sent: true };
  },
  {
    connection,
    concurrency: 10,
  },
);

attachWorkerEvents(notificationWorker, "notification-worker");
```

**Covers:** verifying current business state (Rule 3), idempotency (Rule 2), no pointless retries (Rule 4).

---

### 18.8 Cancel a Reminder When It's No Longer Needed

If the customer **pays**, remove the waiting reminder instead of letting it run.

```js
import { cancelOrderReminder } from "../queues/notification.producer.js";

export const handlePaymentSuccess = async (orderId) => {
  await orderRepository.markAsPaid(orderId);

  // Remove the delayed reminder (best effort)
  await cancelOrderReminder(orderId);
};
```

> [!IMPORTANT]
> Cancelling is a **nice extra, not a replacement** for the status check in the worker. The job might already be running when you try to cancel it. The worker's `order.status !== "pending"` check is your **safety net**.

---

### 18.9 Scheduled Jobs (Job Schedulers) + Maintenance Worker

**`src/schedulers/index.js`**

```js
import { maintenanceQueue } from "../queues/index.js";

export const registerSchedulers = async () => {
  // Every day at 9:00 AM
  await maintenanceQueue.upsertJobScheduler(
    "daily-report", // stable scheduler ID
    {
      pattern: "0 9 * * *",
      tz: "UTC", // change to your timezone if needed
    },
    {
      name: "generate-daily-report",
      data: { type: "daily-report" },
    },
  );

  // Every day at 3:00 AM
  await maintenanceQueue.upsertJobScheduler(
    "cleanup-expired-sessions",
    {
      pattern: "0 3 * * *",
      tz: "UTC",
    },
    {
      name: "cleanup-expired-sessions",
      data: { type: "cleanup" },
    },
  );
};
```

**Why this is safe to run on every app start:**

- `upsert` = **update if exists, create if not**.
- The scheduler IDs (`"daily-report"`, `"cleanup-expired-sessions"`) are **stable**, so restarting the app never creates duplicates.
- Scheduling lives in **Redis**, not inside one server's memory (Rule 7).

**`src/workers/maintenance.worker.js`**

```js
import { Worker, UnrecoverableError } from "bullmq";
import { connection } from "../config/redis.js";
import { attachWorkerEvents } from "./worker.utils.js";
import { reportService } from "../services/report.service.js";
import { sessionRepository } from "../repositories/session.repository.js";

export const maintenanceWorker = new Worker(
  "maintenance-queue",
  async (job) => {
    switch (job.name) {
      case "generate-daily-report": {
        await reportService.generateDailyReport();
        return { generated: true };
      }

      case "cleanup-expired-sessions": {
        const deletedCount = await sessionRepository.deleteExpired();
        return { deletedCount };
      }

      default:
        throw new UnrecoverableError(`Unknown job name: ${job.name}`);
    }
  },
  {
    connection,
    concurrency: 1, // maintenance tasks run one at a time
  },
);

attachWorkerEvents(maintenanceWorker, "maintenance-worker");
```

> [!TIP]
> Even if you run 3 servers, the scheduler creates **only one job per schedule**, and **only one worker** picks it up. This solves the "same cleanup runs 3 times" problem from [Section 12](#12-scheduled-cleanup-job).

---

### 18.10 Express Controller (Using the Producer)

**`src/controllers/auth.controller.js`**

```js
import { createUser } from "../services/user.service.js";
import { scheduleWelcomeEmail } from "../queues/email.producer.js";
import { logger } from "../config/logger.js";

export const registerUser = async (req, res, next) => {
  try {
    const user = await createUser(req.body);

    try {
      await scheduleWelcomeEmail({
        userId: user.id,
        email: user.email,
        name: user.name,
      });
    } catch (queueError) {
      // The user is already saved. A queue problem should not
      // make the signup look like it failed. Log it and move on.
      logger.error(
        { userId: user.id, error: queueError.message },
        "Failed to schedule welcome email",
      );
    }

    return res.status(201).json({
      message: "User registered successfully",
    });
  } catch (error) {
    next(error);
  }
};
```

---

### 18.11 Worker Entry Point (with Graceful Shutdown)

**Graceful shutdown** means: when the server is asked to stop (deploy, restart), it **finishes the jobs it is currently running** before closing. Without it, a job could be cut in half.

**`src/worker.js`**

```js
import { connection } from "./config/redis.js";
import { logger } from "./config/logger.js";
import {
  emailQueue,
  notificationQueue,
  maintenanceQueue,
} from "./queues/index.js";
import { emailWorker } from "./workers/email.worker.js";
import { notificationWorker } from "./workers/notification.worker.js";
import { maintenanceWorker } from "./workers/maintenance.worker.js";
import { registerSchedulers } from "./schedulers/index.js";

const workers = [emailWorker, notificationWorker, maintenanceWorker];
const queues = [emailQueue, notificationQueue, maintenanceQueue];

// Safe on every startup because of upsert
await registerSchedulers();

logger.info("Workers started");

let isShuttingDown = false;

const shutdown = async (signal) => {
  if (isShuttingDown) return;
  isShuttingDown = true;

  logger.info({ signal }, "Shutting down gracefully...");

  try {
    // 1. Stop taking new jobs and wait for running jobs to finish
    await Promise.all(workers.map((worker) => worker.close()));

    // 2. Close queues
    await Promise.all(queues.map((queue) => queue.close()));

    // 3. Close Redis connection
    await connection.quit();

    logger.info("Shutdown complete");
    process.exit(0);
  } catch (error) {
    logger.error({ error: error.message }, "Error during shutdown");
    process.exit(1);
  }
};

process.on("SIGTERM", () => shutdown("SIGTERM")); // sent by Docker / Kubernetes / PM2
process.on("SIGINT", () => shutdown("SIGINT")); // Ctrl + C
```

> [!NOTE]
> Your **API process** should also close its queues on `SIGTERM` (call `emailQueue.close()` etc.) so it exits cleanly.

---

### 18.12 What Makes This "Production-Grade"?

| Rule from Section 15               | Where it is in the code                                             |
| ---------------------------------- | ------------------------------------------------------------------- |
| **Rule 1** — Small payloads        | Jobs only carry IDs and small data (18.5)                           |
| **Rule 2** — Idempotent workers    | DB check before sending (18.6, 18.7) + stable `jobId` (18.5)        |
| **Rule 3** — Verify business state | `order.status !== "pending"` check (18.7)                           |
| **Rule 4** — Intentional retries   | `UnrecoverableError` for bad data, normal errors retry (18.6, 18.7) |
| **Rule 5** — Retention             | `removeOnComplete` / `removeOnFail` defaults (18.4)                 |
| **Rule 6** — No `setTimeout`       | `delay` option stored in Redis (18.5)                               |
| **Rule 7** — No local cron         | `upsertJobScheduler` (18.9)                                         |
| **Rule 8** — Meaningful names      | `send-welcome-email`, `order-reminder`, `generate-daily-report`     |
| Server restart safety              | Jobs live in Redis + graceful shutdown (18.11)                      |

### 18.13 Quick Environment Variables Example

```bash
# .env
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
REDIS_PASSWORD=your_strong_password
LOG_LEVEL=info
```

> [!WARNING]
> Never commit your `.env` file to GitHub. Add it to `.gitignore`.

---

## 🧠 One-Line Mental Model

| Concept           | Meaning                                                                                  |
| ----------------- | ---------------------------------------------------------------------------------------- |
| **Delayed Job**   | "Run this **once** later."                                                               |
| **Scheduled Job** | "Keep **creating work** according to a schedule."                                        |
| **Worker**        | "Actually **execute** that work."                                                        |
| **Redis**         | "**Durably hold** the queue/scheduling state so the work isn't tied to one API process." |
