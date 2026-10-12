# BullMQ Day 6 — Production Architecture

> Beginner-friendly revision notes on concurrency, multiple workers, worker scaling, idempotency, duplicate jobs, dead-letter handling, graceful shutdown, production architecture, and common mistakes.

> [!NOTE]
> **How to read these notes**
>
> - All the original lesson content is kept, written in simple English.
> - 🧒 **Simple example** = an everyday example to make the idea easy.
> - 💡 **Extra** = something added to help you in real projects.

---

## Today's Topics

1. Concurrency
2. Multiple Workers
3. Worker Scaling
4. Idempotency
5. Duplicate Jobs
6. Dead-letter style handling
7. Graceful Shutdown
8. Production architecture
9. Common production mistakes

---

## 📑 Table of Contents

1. [Concurrency](#1-concurrency)
2. [Multiple Workers](#2-multiple-workers)
3. [Why Use Multiple Workers?](#3-why-use-multiple-workers)
4. [Worker Scaling](#4-worker-scaling)
5. [Worker vs API Server](#5-worker-vs-api-server)
6. [Idempotency](#6-idempotency)
7. [What Does Idempotency Mean?](#7-what-does-idempotency-mean)
8. [Idempotency in BullMQ](#8-idempotency-in-bullmq)
9. [Real Production Idempotency](#9-real-production-idempotency)
10. [Duplicate Jobs](#10-duplicate-jobs)
11. [Dead-Letter Style Handling](#11-dead-letter-style-handling)
12. [Example DLQ Pattern](#12-example-dlq-pattern)
13. [Retryable vs Permanent Errors](#13-retryable-vs-permanent-errors)
14. [Graceful Shutdown](#14-graceful-shutdown)
15. [Basic Graceful Shutdown](#15-basic-graceful-shutdown)
16. [Production Architecture](#16-production-architecture)
17. [Common Production Mistakes](#17-common-production-mistakes)
18. [Simple Complete Worker (All Together)](#18-simple-complete-worker-all-together)
19. [The Mental Model to Remember](#the-mental-model-to-remember)
20. [Day 6 Checklist](#day-6-checklist)
21. [Beginner-Friendly Production Code, Explained Step by Step](#21-beginner-friendly-production-code-explained-step-by-step)
22. [Quick Revision Summary](#quick-revision-summary)

---

## 1. Concurrency

First, an important distinction.

Suppose the queue has these jobs:

```text
Job 1
Job 2
Job 3
Job 4
Job 5
```

And the worker is:

```js
const worker = new Worker("email-queue", processEmail, {
  connection,
  concurrency: 1,
});
```

Then **only one job is processed at a time**:

```text
Worker
  │
  ├── Job 1
  │
  ├── Job 2
  │
  ├── Job 3
  │
  └── ...
```

Now, with:

```js
concurrency: 3;
```

the worker can handle **3 jobs at the same time**:

```text
              Worker
                │
       ┌────────┼────────┐
       ▼        ▼        ▼
     Job 1    Job 2    Job 3
       │        │        │
       ▼        ▼        ▼
     active   active   active
```

Job 4 **waits** until one slot becomes free.

> [!IMPORTANT]
> `concurrency: 3` does **NOT** mean 3 worker processes.
>
> It means: **one Worker instance can have up to 3 jobs being processed at the same time.**

### 🧒 Simple example

Think of a **shop counter**:

- `concurrency: 1` → one cashier serves **one** customer at a time.
- `concurrency: 3` → one cashier can handle **3** customers at the same time (for example, while one customer is searching for their wallet, the cashier helps the next one).

### Real-world example

Imagine sending emails. One email takes about **2 seconds**.

**Concurrency `1`:**

```text
100 emails
    ×
2 seconds

≈ 200 seconds
```

**Concurrency `10`:**

```text
10 emails at the same time

≈ 20 seconds
```

Much better.

### ⚠️ But more concurrency is not always better

Suppose your email provider allows:

```text
100 requests / second
```

and you configure:

```js
concurrency: 500;
```

You can start hammering the provider. You may get:

```text
429 Too Many Requests
```

or the provider may start throttling you.

> [!WARNING]
> **Concurrency should be based on the capacity of the downstream service** (the email provider, payment API, database, etc.).

### 💡 Extra: Simple rule to choose concurrency

| Type of job                       | Example                       | Good starting concurrency |
| --------------------------------- | ----------------------------- | ------------------------- |
| Mostly **waiting** for a response | Send email, call an API       | `5` to `20`               |
| Heavy **calculation**             | Resize image, build a big PDF | `1` or `2`                |

Start small. Increase slowly while watching for errors.

### 💡 Extra: Add a speed limit

BullMQ has a built-in `limiter`. It limits how many jobs can **start** in a time period.

```js
const worker = new Worker("email-queue", processEmail, {
  connection,
  concurrency: 10,

  // At most 100 jobs every 1 second
  limiter: {
    max: 100,
    duration: 1000,
  },
});
```

> [!NOTE]
> The limiter works for the **whole queue**. Even if you run many workers, together they stay under `100 jobs / second`.

---

## 2. Multiple Workers

Now imagine the queue has:

```text
Queue
 │
 ├── Job 1
 ├── Job 2
 ├── Job 3
 ├── Job 4
 └── Job 5
```

Instead of one worker:

```text
Worker A
```

you run three:

```text
Worker A
Worker B
Worker C
```

All of them listen to the same queue:

```text
"email-queue"
```

Architecture:

```text
                    Redis
                      │
                email-queue
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
      Worker A    Worker B    Worker C
      concurrency 3
```

BullMQ **shares the jobs** between the workers.

So if you have:

```text
3 workers
    ×
concurrency 5
```

you can process around:

```text
15 jobs at the same time
```

(assuming your workload and environment can support it).

> [!TIP]
> **Total jobs at the same time = number of workers × concurrency**

### 🧒 Simple example

Back to the shop: instead of **one** cashier, you open **3 counters**. Each counter (worker) serves customers (jobs) from the **same line** (queue).

---

## 3. Why Use Multiple Workers?

Real-world example: your application needs to send **50,000 emails** after a marketing campaign.

With **one worker**:

```text
Worker A
   ↓
slow
```

With **many workers**:

```text
                 Redis
                   │
                   ▼
             email-queue
                   │
       ┌───────────┼───────────┐
       ▼           ▼           ▼
    Worker 1    Worker 2    Worker 3
       │           │           │
       ▼           ▼           ▼
     Email       Email       Email
```

You can **scale the processing horizontally** (by adding more workers).

### 💡 Extra: See the difference with numbers

Each email takes 2 seconds. Each worker has `concurrency: 10`.

| Workers | Emails per second | Time for 50,000 emails   |
| ------- | ----------------- | ------------------------ |
| 1       | 5                 | about 2 hours 46 minutes |
| 10      | 50                | about 17 minutes         |

> [!NOTE]
> Always check the provider limit too. If the provider allows only 50 emails/second, adding more than 10 workers will not make it faster.

---

## 4. Worker Scaling

This is where production architecture becomes interesting.

Suppose:

```text
Normal traffic
     ↓
  2 workers
```

During a huge campaign:

```text
Traffic spike
     ↓
 10 workers
```

After the spike:

```text
Traffic normal
     ↓
  2 workers
```

This is **horizontal scaling**.

Instead of making one worker extremely powerful:

```text
Worker
CPU: 16 cores
RAM: 32GB
```

you run many smaller workers:

```text
Worker 1
Worker 2
Worker 3
Worker 4
```

This is usually **easier to scale operationally**.

### 💡 Extra: How to run many workers (beginner way)

Put the worker code in its own file and start it as many times as you need.

```bash
# Terminal 1
node src/worker.js

# Terminal 2
node src/worker.js

# Terminal 3
node src/worker.js
```

That's it. You now have **3 workers** on the same queue. BullMQ shares the jobs between them automatically.

If you use Docker Compose, you can do the same with one command:

```bash
docker compose up -d --scale worker=3
```

(This works when your `docker-compose.yml` has a service named `worker` that runs `node src/worker.js`.)

---

## 5. Worker vs API Server

This distinction is **extremely important**.

Don't think that this must always be one process:

```text
Express API
     +
Worker
```

Production architecture is often:

```text
                 Load Balancer
                       │
              ┌────────┼────────┐
              ▼        ▼        ▼
           API #1   API #2   API #3
              │        │        │
              └────────┼────────┘
                       ▼
                     Redis
                       │
                 email-queue
                       │
              ┌────────┼────────┐
              ▼        ▼        ▼
         Worker #1 Worker #2 Worker #3
```

**Why?** They do different work and need to scale differently.

| API servers handle | Workers handle        |
| ------------------ | --------------------- |
| HTTP requests      | Emails                |
| Authentication     | Reports               |
| Validation         | Video processing      |
| DB queries         | Notifications         |
|                    | Other background jobs |

### 💡 Extra: Two separate files

```text
src/
├── server.js     ← starts Express (API). It ADDS jobs to the queue.
└── worker.js     ← starts the BullMQ Worker. It PROCESSES the jobs.
```

```json
{
  "scripts": {
    "start:api": "node src/server.js",
    "start:worker": "node src/worker.js"
  }
}
```

**Benefit:** if a heavy report job uses a lot of CPU, only the **worker** gets slow. Your API stays fast for users.

---

## 6. Idempotency

This is one of the **most important production concepts**.

Imagine:

```text
Order #123
```

needs a payment confirmation email. Your worker receives:

```text
Job A
```

It sends:

```text
📧 Payment successful
```

But then something happens. Maybe the **worker crashes before BullMQ records the job as completed**. The job may be processed **again**.

```text
Job A
   ↓
Email sent
   ↓
Worker crashes
   ↓
Job retried/recovered
   ↓
Email sent again
```

The customer gets:

```text
📧 Payment successful
📧 Payment successful
```

Not good.

> [!NOTE]
> 💡 **Why can this happen?** BullMQ promises that a job runs **at least once**, not exactly once. If a worker dies in the middle of a job, BullMQ gives that job to another worker. So your code must be ready to run the same job twice.

---

## 7. What Does Idempotency Mean?

Simple definition:

> **Running the same operation multiple times should produce the same final business result.**

For example:

```text
Create payment
```

is **dangerous** to repeat blindly.

But:

```text
Set order status = PAID
```

is **idempotent**. Doing it again:

```text
PAID → PAID
```

doesn't create another payment.

### 🧒 Simple example

Think of a **lift button**. Pressing the "Floor 5" button once or 10 times gives the same result: the lift goes to floor 5. That is idempotent.

But a "**Add ₹100**" button is **not** idempotent. Pressing it 10 times adds ₹1000.

### 💡 Extra: Quick comparison

| Operation                 | Safe to repeat? | Why                          |
| ------------------------- | --------------- | ---------------------------- |
| `status = "PAID"`         | ✅ Yes          | Same result every time       |
| `balance = 500`           | ✅ Yes          | Same result every time       |
| `balance = balance + 100` | ❌ No           | Adds 100 every time          |
| Insert a new payment row  | ❌ No           | Creates a new row every time |
| Send an email             | ❌ No           | Sends a new email every time |

For the ❌ operations, add a **check** (for example, a flag in the database) so the second run does nothing.

---

## 8. Idempotency in BullMQ

Suppose:

```js
await emailQueue.add("order-confirmation", {
  orderId: "order_123",
  userId: "user_456",
});
```

We can use a **deterministic job ID** (the same order always gives the same ID):

```js
await emailQueue.add(
  "order-confirmation",
  {
    orderId: "order_123",
    userId: "user_456",
  },
  {
    jobId: `order-confirmation-${orderId}`,
  },
);
```

Now the same logical job can be identified. Conceptually:

```text
order_123
    ↓
order-confirmation-order_123
```

If your producer accidentally tries to add it again, you have a way to avoid treating it as a completely new job.

> [!IMPORTANT]
> A unique `jobId` is **not the same thing** as full business-level idempotency.
>
> Your worker should **still be safe** if the job executes more than once.

### 💡 Extra: Real example — payment webhook

Payment gateways often **send the same webhook twice**. Without protection, one payment can create two jobs.

```js
export const paymentWebhook = async (req, res) => {
  const { orderId, userId } = req.body;

  // Same orderId → same jobId → the second one is ignored
  await emailQueue.add(
    "order-confirmation",
    { orderId, userId },
    { jobId: `order-confirmation-${orderId}` },
  );

  res.sendStatus(200);
};
```

> [!NOTE]
>
> - Use a **hyphen** in custom job IDs (`order-confirmation-123`). Some BullMQ versions do not allow `:`.
> - A custom `jobId` cannot be a plain number like `"123"`. Always add text in front.
> - The duplicate check works **only while the old job is still in Redis**. If the old job was already removed by `removeOnComplete`, the same ID can be added again. That is why the worker must be safe too (next section).

---

## 9. Real Production Idempotency

Don't write this blindly:

```ts
await sendEmail();
await markOrderAsPaid();
```

If the job runs twice, both lines run twice.

### ✅ Use your database as the "memory"

Order table:

```
Order
---------
id
status
confirmationEmailSent   ← the flag that protects us
```

### Worker code (MongoDB / Mongoose)

```ts
async function processOrderConfirmation(job: Job) {
  const { orderId } = job.data;

  const order = await Order.findById(orderId);

  if (!order) {
    throw new UnrecoverableError("Order not found"); // permanent problem
  }

  // ✅ Already done? Skip.
  if (order.confirmationEmailSent) {
    return { skipped: true };
  }

  await sendEmail({
    to: order.customerEmail,
    subject: "Payment successful",
    body: `Your order ${order.id} is confirmed.`,
  });

  await Order.updateOne(
    { _id: orderId },
    { $set: { confirmationEmailSent: true } },
  );

  return { sent: true };
}
```

Now if the same job runs again:

```
confirmationEmailSent === true  →  skip ✅
```

This is **business-level idempotency**.

### 🔒 Even safer: atomic "claim" (prevents race conditions)

If **two workers** pick the same logical job at the same time, both may read `false`. Fix this with one atomic update:

```ts
const result = await Order.updateOne(
  { _id: orderId, confirmationEmailSent: false },
  { $set: { confirmationEmailSent: true } },
);

if (result.modifiedCount === 0) {
  return { skipped: true }; // someone else already did it
}

await sendEmail(/* ... */);
```

> ⚠️ Trade-off: if `sendEmail` fails after the flag is set, the email is lost. In that case, roll the flag back in a `catch`, or use a status like `PENDING → SENT`.

### Another popular approach: idempotency key for external APIs

Many providers (Stripe, payment gateways) accept an **idempotency key**:

```ts
await stripe.paymentIntents.create(
  { amount: 5000, currency: "usd" },
  { idempotencyKey: `payment-${orderId}` }, // same key = same result, no double charge
);
```

---

## 10. Duplicate Jobs

There are actually **two different problems**.

### Problem A — Duplicate enqueue

The **same job gets added twice**.

```text
Producer
   │
   ├── add order-123
   │
   └── add order-123
```

### Problem B — Duplicate processing

The **same job gets processed more than once**.

### They are not the same problem

|                  | Problem A: Duplicate enqueue           | Problem B: Duplicate processing     |
| ---------------- | -------------------------------------- | ----------------------------------- |
| **What happens** | Job is added twice                     | Job runs twice                      |
| **Who fixes it** | BullMQ setting (`jobId`)               | Your application/business logic     |
| **Example fix**  | `jobId: order-confirmation-${orderId}` | `confirmationEmailSent` check in DB |

> [!IMPORTANT]
> BullMQ configuration can help with **duplicate enqueue**.
> Your application/business logic must help with **duplicate processing**.

You need **both**:

```text
Producer  → stable jobId        (stops duplicate enqueue)
Worker    → DB check            (stops duplicate processing)
```

---

## 11. Dead-Letter Style Handling

Suppose the **email provider is down**. The job:

```text
attempt 1 ❌
attempt 2 ❌
attempt 3 ❌
```

Eventually it becomes:

```text
failed
```

Now imagine **thousands** of failed jobs. You don't want to simply forget them.

You may need a **dead-letter queue (DLQ)** or another failure-handling mechanism.

Conceptually:

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
       Retry
          │
          ├── success → Completed
          │
          └── permanent failure
                    │
                    ▼
                   DLQ
```

### A DLQ is useful for

- Manual inspection
- Debugging
- Replaying jobs
- Alerting
- Investigating permanent failures

### 🧒 Simple example

A DLQ is like the **"undelivered letters" box** at a post office. If a letter cannot be delivered after many tries, it is not thrown away. It goes into a special box so someone can check what went wrong.

> [!NOTE]
> 💡 BullMQ does **not** have a built-in DLQ. A DLQ is simply **another queue that you create** and fill yourself when a job fails for good. That's why it is called "dead-letter **style**" handling.
>
> A simpler option: keep failed jobs in the same queue for longer with `removeOnFail: { age: 86400, count: 5000 }` (see the Cleanup Strategy notes).

---

## 12. Example DLQ Pattern

You might have two queues:

```text
email-queue
email-dead-letter-queue
```

After a job permanently fails, your application can record/move the failure information for later investigation. For example:

```js
await deadLetterQueue.add("failed-email", {
  originalJobId: job.id,
  originalJobName: job.name,
  data: job.data,
  failedReason: error.message,
});
```

But **don't blindly put every failure into a DLQ**. For example:

```text
Temporary timeout
```

should normally **retry**. While:

```text
Invalid email address
```

may be a **permanent failure**.

So you need to tell the difference:

```text
Retryable error
       vs
Permanent error
```

(See the next section.)

### 💡 Extra: Simple step-by-step DLQ code

**Step 1 — Create the two queues**

```js
import { Queue } from "bullmq";

export const emailQueue = new Queue("email-queue", {
  connection,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: "exponential", delay: 1000 },
  },
});

// This queue only STORES failed jobs
export const deadLetterQueue = new Queue("email-dead-letter-queue", {
  connection,
});
```

**Step 2 — When a job fails for the last time, copy it to the DLQ**

```js
emailWorker.on("failed", async (job, error) => {
  // Is this the last attempt?
  const isLastAttempt = job.attemptsMade >= job.opts.attempts;

  if (!isLastAttempt) return; // BullMQ will retry, so do nothing

  await deadLetterQueue.add("failed-email", {
    originalJobId: job.id,
    originalJobName: job.name,
    data: job.data,
    failedReason: error.message,
  });

  console.log("Job moved to DLQ:", job.id);
});
```

**Step 3 — Later, when the problem is fixed**

Read the jobs from the DLQ and add them back to the main queue (replay). Because your worker is idempotent (Section 9), replaying is safe.

> [!NOTE]
> Jobs that you mark as permanent errors (next section) stop retrying right away, so they may not reach the maximum attempts. Check the next section's code to see how to handle that case too.

---

## 13. Retryable vs Permanent Errors

This is **very important in real systems**.

### Retryable errors

```text
Network timeout
503 Service Unavailable
429 Too Many Requests
Temporary provider failure
```

**Retry makes sense.**

### Permanent errors

```text
Invalid email
Invalid user ID
Malformed payload
Resource doesn't exist
```

**Retrying 10 times won't fix it.**

Architecture:

```text
             Error
               │
        ┌──────┴──────┐
        ▼             ▼
    Retryable      Permanent
        │             │
        ▼             ▼
      Retry           DLQ
```

This is much better than:

```text
everything → retry 10 times
```

### 🧒 Simple example

- You call a friend and the phone is **busy** → try again in a few minutes (**retryable**).
- You dialed a **wrong number** → calling again 10 times won't help (**permanent**).

### 💡 Extra: Simple code

BullMQ gives you `UnrecoverableError`. When you throw it, the job **fails right away** and is **not retried**.

```js
import { Worker, UnrecoverableError } from "bullmq";

const emailWorker = new Worker(
  "email-queue",
  async (job) => {
    const { email } = job.data;

    // Permanent problem: no email address. Retrying will not help.
    if (!email) {
      throw new UnrecoverableError("Email is missing");
    }

    // Temporary problems (timeout, 503...) are normal errors.
    // BullMQ will retry them automatically.
    await emailProvider.send({ to: email });

    return { sent: true };
  },
  { connection },
);
```

Now update the DLQ code from Section 12 so it also catches permanent errors:

```js
emailWorker.on("failed", async (job, error) => {
  const isPermanent = error instanceof UnrecoverableError;
  const isLastAttempt = job.attemptsMade >= job.opts.attempts;

  // Move to DLQ if retries are finished OR the error is permanent
  if (isPermanent || isLastAttempt) {
    await deadLetterQueue.add("failed-email", {
      originalJobId: job.id,
      originalJobName: job.name,
      data: job.data,
      failedReason: error.message,
    });
  }
});
```

---

## 14. Graceful Shutdown

One of the **most important production topics**.

Imagine your worker is processing:

```text
Job #123
```

and you **deploy a new version**. Kubernetes/Docker sends:

```text
SIGTERM
```

If you immediately kill the process:

```text
Worker 💀
```

you could **interrupt the job**.

What we want:

```text
SIGTERM
   ↓
Stop accepting new work
   ↓
Finish current jobs
   ↓
Close worker
   ↓
Close connections
   ↓
Exit
```

### 🧒 Simple example

A shop is closing for the day. A good shopkeeper does **not** push customers out in the middle of billing. They:

1. Stop letting **new** customers in.
2. **Finish** serving the current customers.
3. Then lock the shop.

That is graceful shutdown.

---

## 15. Basic Graceful Shutdown

BullMQ worker:

```js
const worker = new Worker("email-queue", processEmail, {
  connection,
  concurrency: 5,
});
```

Then:

```js
const shutdown = async (signal) => {
  console.log(`${signal} received`);

  await worker.close();

  console.log("Worker closed");

  process.exit(0);
};

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
```

The important line:

```js
await worker.close();
```

This tells BullMQ:

> Stop taking new jobs and gracefully close the worker.

BullMQ **waits for the jobs that are running right now to finish**, then closes. That is much safer than killing the process.

### 💡 Extra: Also close the queue and Redis

The exact order depends on your architecture, but the principle is:

> **Stop accepting new work → finish current work → close connections → exit.**

```js
const shutdown = async (signal) => {
  console.log(`${signal} received`);

  await worker.close(); // 1. finish running jobs
  await emailQueue.close(); // 2. close the queue
  await connection.quit(); // 3. close Redis last

  console.log("Shutdown complete");
  process.exit(0);
};

process.on("SIGTERM", () => shutdown("SIGTERM")); // sent by Docker / Kubernetes
process.on("SIGINT", () => shutdown("SIGINT")); // Ctrl + C
```

> [!TIP]
> Docker and Kubernetes only wait for a limited time (a "grace period") before killing the process. Set that time to be **longer than your longest job**.

---

## 16. Production Architecture

This is the architecture you should understand by the end of Day 6:

```text
                         CLIENT
                           │
                           ▼
                    ┌──────────────┐
                    │ Load Balancer│
                    └──────┬───────┘
                           │
                ┌──────────┼──────────┐
                ▼          ▼          ▼
             API #1     API #2     API #3
                │          │          │
                └──────────┼──────────┘
                           │
                           ▼
                        Redis
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
          email-queue  report-queue  notification
              │            │            │
        ┌─────┴─────┐      │       ┌────┴────┐
        ▼           ▼      ▼       ▼         ▼
     Worker 1    Worker 2 Worker 3 Worker 4 Worker 5
        │           │      │       │         │
        ▼           ▼      ▼       ▼         ▼
      Email       Email   Reports Notifications
     Provider     Provider
```

And the production concerns around it:

```text
             ┌─────────────────────────┐
             │       BullMQ            │
             │                         │
             │ Concurrency             │
             │ Scaling                 │
             │ Retries                 │
             │ Backoff                 │
             │ Idempotency             │
             │ DLQ                     │
             │ Graceful shutdown       │
             │ Observability           │
             └─────────────────────────┘
```

---

## 17. Common Production Mistakes

### ❌ Mistake 1

```js
concurrency: 1000;
```

without considering the downstream provider.

**Fix:** set concurrency based on what the provider can handle, and use a `limiter` (Section 1).

### ❌ Mistake 2

Thinking:

```text
jobId uniqueness = complete idempotency
```

**It isn't.** `jobId` only stops duplicate **adding**. Duplicate **processing** needs a check in the worker.

### ❌ Mistake 3

Retrying every error.

```text
Invalid email → retry 10 times ❌
```

**Fix:** use `UnrecoverableError` for permanent errors (Section 13).

### ❌ Mistake 4

No graceful shutdown.

```text
SIGTERM
 ↓
process.exit()
```

Dangerous for active jobs.

**Fix:** call `await worker.close()` first (Section 15).

### ❌ Mistake 5

Running the API and heavy workers in the same process **without considering scaling**.

**Fix:** keep `server.js` and `worker.js` separate (Section 5).

### ❌ Mistake 6

No monitoring.

Production should expose things like:

```text
waiting jobs
active jobs
failed jobs
completed jobs
delayed jobs
processing latency
failure rate
```

#### 💡 Extra: Simple monitoring code

Create a small endpoint that shows how many jobs are in each state:

```js
app.get("/health/queues", async (req, res) => {
  const counts = await emailQueue.getJobCounts(
    "waiting",
    "active",
    "delayed",
    "failed",
    "completed",
  );

  res.json(counts);
  // Example: { waiting: 12, active: 5, delayed: 0, failed: 1, completed: 340 }
});
```

Log how long each job took:

```js
emailWorker.on("completed", (job) => {
  console.log({
    jobId: job.id,
    waitedMs: job.processedOn - job.timestamp, // time waiting in the queue
    processingMs: job.finishedOn - job.processedOn, // time being processed
  });
});
```

**What the numbers tell you:**

| If this keeps growing... | It means                                                   |
| ------------------------ | ---------------------------------------------------------- |
| `waiting`                | Workers are too slow → add workers or concurrency          |
| `failed`                 | Something is broken (provider down, bug) → check and alert |
| `waitedMs`               | Users are waiting too long for their emails/reports        |

### ❌ Mistake 7

Putting huge payloads into Redis jobs.

**Bad:**

```js
{
  user: entireUserObject,
  orders: 5000 orders,
  products: 10000 products
}
```

**Better:**

```js
{
  (userId, orderId);
}
```

Then the worker **fetches what it needs** from the database.

### 💡 Extra Mistakes (Bonus)

#### ❌ Mistake 8 — Not listening to the worker `error` event

Redis connection problems are reported through this event. If you don't listen, you won't see them.

```js
worker.on("error", (error) => {
  console.error("Worker error:", error.message);
});
```

#### ❌ Mistake 9 — Blocking Node.js with heavy work

A very heavy synchronous calculation can freeze the worker. BullMQ may think the worker died and give the job to another worker, so the job runs **twice**.

**Fix:** keep heavy work small, or run it in a separate worker process.

#### ❌ Mistake 10 — Wrong Redis memory setting

If Redis is allowed to delete data when its memory is full, it can **delete your queue data**.

**Fix:** set `maxmemory-policy noeviction` on the Redis used by BullMQ.

---

## 18. Simple Complete Worker (All Together)

💡 **Extra section.** This one file puts the main Day 6 ideas together: concurrency, rate limit, idempotency, permanent errors, and DLQ.

**`src/workers/email.worker.js`**

```js
import { Worker, UnrecoverableError } from "bullmq";
import { connection } from "../config/redis.js";
import { deadLetterQueue } from "../queues/index.js";

export const emailWorker = new Worker(
  "email-queue",

  // ---------- 1. The job ----------
  async (job) => {
    const { orderId } = job.data;

    const order = await Order.findById(orderId);

    // Permanent problem → don't retry
    if (!order) {
      throw new UnrecoverableError("Order not found");
    }

    // Idempotency → if already sent, skip
    if (order.confirmationEmailSent) {
      return { skipped: true };
    }

    // Temporary errors here (timeout, 503...) will be retried
    await emailProvider.send({
      to: order.email,
      template: "order-confirmation",
    });

    await Order.updateOne(
      { _id: orderId },
      { $set: { confirmationEmailSent: true } },
    );

    return { sent: true };
  },

  // ---------- 2. Settings ----------
  {
    connection,
    concurrency: 10, // 10 jobs at the same time
    limiter: { max: 100, duration: 1000 }, // max 100 jobs per second
  },
);

// ---------- 3. When a job fails ----------
emailWorker.on("failed", async (job, error) => {
  console.error(`Job ${job.id} failed:`, error.message);

  const isPermanent = error instanceof UnrecoverableError;
  const isLastAttempt = job.attemptsMade >= job.opts.attempts;

  // Retries finished (or permanent error) → save it in the DLQ
  if (isPermanent || isLastAttempt) {
    await deadLetterQueue.add("failed-email", {
      originalJobId: job.id,
      data: job.data,
      failedReason: error.message,
    });
  }
});

// ---------- 4. Redis/connection problems ----------
emailWorker.on("error", (error) => {
  console.error("Worker error:", error.message);
});

// ---------- 5. Graceful shutdown ----------
const shutdown = async () => {
  await emailWorker.close(); // finish running jobs first
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
```

**What each part does:**

| Part                          | Why it matters                                       |
| ----------------------------- | ---------------------------------------------------- |
| `UnrecoverableError`          | Permanent problems are not retried (Section 13)      |
| `confirmationEmailSent` check | Safe if the job runs twice (Section 9)               |
| `concurrency` + `limiter`     | Doesn't overload the provider (Section 1)            |
| `on("failed")` + DLQ          | Failed jobs are saved for investigation (Section 12) |
| `on("error")`                 | Connection problems are visible (Section 17)         |
| `shutdown`                    | Running jobs finish before exit (Section 15)         |

---

## The Mental Model to Remember

Production BullMQ is **not simply**:

```text
Queue → Worker
```

It is:

```text
                   ┌───────────────┐
                   │    Producer   │
                   └───────┬───────┘
                           │
                           ▼
                        Queue
                           │
                    ┌──────┴──────┐
                    │             │
                Retryable      Permanent
                 failure         failure
                    │             │
                    ▼             ▼
                  Retry          DLQ
                    │
                    ▼
                  Worker
                    │
             ┌──────┴──────┐
             ▼             ▼
        Idempotent      External
         operation      service
             │             │
             └──────┬──────┘
                    ▼
               Completed

        + concurrency
        + scaling
        + observability
        + graceful shutdown
```

---

## Day 6 Checklist

Before moving on, make sure you understand:

- [ ] Concurrency (and that it is **not** the number of worker processes)
- [ ] Why more concurrency is not always better
- [ ] Multiple workers on the same queue
- [ ] Jobs at the same time = workers × concurrency
- [ ] Horizontal scaling of workers
- [ ] Why API and workers should be separate processes
- [ ] What idempotency means
- [ ] Duplicate enqueue vs duplicate processing
- [ ] Why a unique `jobId` is **not** full idempotency
- [ ] Business-level idempotency (a flag in the database)
- [ ] What a dead-letter queue is and when to use it
- [ ] Retryable vs permanent errors (`UnrecoverableError`)
- [ ] What graceful shutdown is and why `worker.close()` matters
- [ ] What to monitor (waiting, active, failed, delayed, latency, failure rate)
- [ ] The common production mistakes

---

## 21. Beginner-Friendly Production Code, Explained Step by Step

This is one complete, production-style email system in **one file**, so you can read it from top to bottom.
Below the code, every numbered step is explained in very simple language.

### 🗺️ The big picture first

```
   YOUR APP (API)                 REDIS                    WORKER
 ┌───────────────┐         ┌───────────────┐        ┌────────────────┐
 │ User pays     │         │               │        │ 1. Take a job  │
 │ for an order  │──add──▶ │  email-queue  │──job──▶│ 2. Check DB    │
 │               │         │  (waiting)    │        │ 3. Send email  │
 └───────────────┘         └───────────────┘        │ 4. Mark as sent│
                                  ▲                 └───────┬────────┘
                                  │                         │
                            retry later                ┌────┴─────┐
                                  │                    ▼          ▼
                                  └─── temporary ──  Error?     Success ✅
                                       error            │
                                                        ▼
                                                  permanent error
                                                        │
                                                        ▼
                                                  Dead-Letter Queue 📥
```

### 💻 The code: `email-system.ts`

```ts
import { Queue, Worker, Job, UnrecoverableError } from "bullmq";
import IORedis from "ioredis";
import os from "os";
import { Order } from "./models/order.model"; // your database model
import { emailProvider } from "./services/email-provider"; // your email service

// ======================================================
// STEP 1: Redis connection
// ======================================================
const connection = new IORedis(
  process.env.REDIS_URL ?? "redis://localhost:6379",
  {
    maxRetriesPerRequest: null, // BullMQ workers need this
  },
);

// Settings that can change per worker WITHOUT changing the code
const WORKER_ID = process.env.WORKER_ID ?? `${os.hostname()}-${process.pid}`;
const CONCURRENCY = Number(process.env.WORKER_CONCURRENCY ?? 10);

// ======================================================
// STEP 2: Queues (main queue + dead-letter queue)
// ======================================================
const emailQueue = new Queue("email-queue", {
  connection,
  defaultJobOptions: {
    attempts: 5, // try max 5 times
    backoff: { type: "exponential", delay: 2000 }, // wait 2s, 4s, 8s, 16s...
    removeOnComplete: { age: 3600, count: 1000 }, // clean old successful jobs
    removeOnFail: false, // keep failed jobs for debugging
  },
});

const deadLetterQueue = new Queue("email-dead-letter-queue", { connection });

// ======================================================
// STEP 3: Producer (adds jobs to the queue)
// ======================================================
export async function enqueueOrderConfirmation(orderId: string) {
  await emailQueue.add(
    "order-confirmation",
    { orderId }, // only the ID, not the whole order
    { jobId: `order-confirmation-${orderId}` }, // same order = same jobId
  );
}

// ======================================================
// STEP 4: Processor (the actual work for ONE job)
// ======================================================
async function processEmail(job: Job<{ orderId: string }>) {
  const { orderId } = job.data;
  console.log(`[${WORKER_ID}] ▶ processing job ${job.id}`);

  // 4a. Get fresh data from the database
  const order = await Order.findById(orderId);

  // 4b. Order doesn't exist → retrying is useless
  if (!order) {
    throw new UnrecoverableError(`Order ${orderId} not found`);
  }

  // 4c. IDEMPOTENCY: try to "claim" this order with ONE atomic DB update
  const claim = await Order.updateOne(
    { _id: orderId, confirmationEmailSent: false }, // only if not sent yet
    { $set: { confirmationEmailSent: true } },
  );

  // Nothing changed → another run/worker already did it → skip safely
  if (claim.modifiedCount === 0) {
    return { skipped: true };
  }

  // 4d. Send the email, and decide what kind of error it is
  try {
    await emailProvider.send({
      to: order.customerEmail,
      subject: "Payment successful",
      body: `Your order ${order.id} is confirmed.`,
    });
  } catch (err: any) {
    // Sending failed → undo the claim so the retry is allowed to try again
    await Order.updateOne(
      { _id: orderId },
      { $set: { confirmationEmailSent: false } },
    );

    const status = err.response?.status;

    // Our mistake (bad request, invalid email) → never retry
    if (status && status >= 400 && status < 500 && status !== 429) {
      throw new UnrecoverableError(`Provider rejected email (${status})`);
    }

    // Temporary problem (timeout, 429, 5xx) → BullMQ will retry
    throw err;
  }

  // 4e. Success. The flag is already saved, so we are done.
  console.log(`[${WORKER_ID}] ✔ email sent for order ${orderId}`);
  return { sent: true };
}

// ======================================================
// STEP 5: Worker (picks jobs and runs the processor)
// ======================================================
const worker = new Worker("email-queue", processEmail, {
  connection,
  concurrency: CONCURRENCY, // jobs at the same time, inside THIS worker
  limiter: { max: 100, duration: 1000 }, // max 100 jobs/second, shared by ALL workers
});

console.log(`🚀 Worker ${WORKER_ID} started (concurrency ${CONCURRENCY})`);

// ======================================================
// STEP 6: Events (know what is happening)
// ======================================================
worker.on("completed", (job) => {
  console.log(`✅ Job ${job.id} done`);
});

worker.on("failed", async (job, error) => {
  if (!job) return;

  const maxAttempts = job.opts.attempts ?? 1;
  const isFinalFailure =
    error instanceof UnrecoverableError || job.attemptsMade >= maxAttempts;

  if (!isFinalFailure) {
    console.warn(
      `⚠️ Job ${job.id} failed (${job.attemptsMade}/${maxAttempts}), will retry`,
    );
    return;
  }

  // No more retries → save it in the dead-letter queue
  await deadLetterQueue.add("failed-email", {
    originalJobId: job.id,
    data: job.data,
    failedReason: error.message,
    failedAt: new Date().toISOString(),
  });
  console.error(
    `💀 Job ${job.id} moved to dead-letter queue: ${error.message}`,
  );
});

worker.on("error", (err) => {
  console.error("Worker error:", err); // e.g. Redis connection problems
});

// ======================================================
// STEP 7: Graceful shutdown
// ======================================================
let shuttingDown = false;

async function shutdown(signal: string) {
  if (shuttingDown) return; // don't run twice
  shuttingDown = true;
  console.log(`${signal} received. Shutting down...`);

  // Safety net: if shutdown hangs, force exit after 30 seconds
  setTimeout(() => process.exit(1), 30_000).unref();

  try {
    await worker.close(); // finish running jobs, take no new ones
    await emailQueue.close();
    await deadLetterQueue.close();
    await connection.quit(); // close Redis
    console.log("Shutdown complete ✅");
    process.exit(0);
  } catch (err) {
    console.error("Shutdown failed", err);
    process.exit(1);
  }
}

process.on("SIGTERM", () => shutdown("SIGTERM")); // sent by Docker / Kubernetes
process.on("SIGINT", () => shutdown("SIGINT")); // Ctrl + C
```

> 💡 In a real project, split this into files (`config/redis.ts`, `queues/email.queue.ts`, `workers/email.worker.ts`). The API should only import the **queue** part (Steps 1–3). Only the worker process should run Steps 4–7.

---

### 📖 Step-by-step explanation

#### STEP 1: Redis connection

```ts
const connection = new IORedis(url, { maxRetriesPerRequest: null });
```

- Redis is the **storage** where BullMQ keeps all jobs.
- The queue and the worker both talk to Redis through this connection.
- `maxRetriesPerRequest: null` is required for workers. Without it, BullMQ shows an error. Just remember to always add it.

#### STEP 2: Queues

```ts
const emailQueue = new Queue("email-queue", { connection, defaultJobOptions: {...} });
```

A **queue** is a waiting line for jobs. `defaultJobOptions` are settings applied to **every** job, so you don't repeat them.

| Option                       | Simple meaning                                      |
| ---------------------------- | --------------------------------------------------- |
| `attempts: 5`                | If a job fails, try up to 5 times in total          |
| `backoff: exponential, 2000` | Wait longer after each failure: 2s → 4s → 8s → 16s  |
| `removeOnComplete`           | Delete old successful jobs so Redis doesn't fill up |
| `removeOnFail: false`        | Keep failed jobs so you can investigate them        |

How retries look over time:

```
Attempt 1 ❌ → wait 2s
Attempt 2 ❌ → wait 4s
Attempt 3 ❌ → wait 8s
Attempt 4 ❌ → wait 16s
Attempt 5 ❌ → give up → job is "failed" → goes to the dead-letter queue
```

**Why wait longer each time?** If the email provider is struggling, hitting it again quickly makes it worse. Waiting gives it time to recover.

The **dead-letter queue** is just a second queue. It is the "parking place" for jobs that failed for good, so a human can look at them later.

#### STEP 3: Producer

```ts
await emailQueue.add(
  "order-confirmation",
  { orderId },
  { jobId: `order-confirmation-${orderId}` },
);
```

The producer **adds** jobs. It does not do the work.

- **Job name** (`"order-confirmation"`): a label for the type of job.
- **Job data** (`{ orderId }`): only the **ID**. The worker will fetch the full order from the database. Small data keeps Redis fast and avoids stale information.
- **`jobId`**: makes the ID predictable.

```
add(order_123)  →  jobId = "order-confirmation-order_123"  ✅ created
add(order_123)  →  same jobId                               🚫 ignored (duplicate)
```

This stops **duplicate adding** (for example, a user double-clicks "Pay").

#### STEP 4: Processor

This is the function that does the real work for **one job**. Think of it as a short checklist:

```
Start job
   │
   ▼
Order exists in DB? ──── No ───▶ UnrecoverableError (permanent) 💀
   │ Yes
   ▼
Claim the order (atomic update: sent=false → sent=true)
   │
   ├── Nothing changed (already sent) ─▶ Skip ✅
   │
   ▼ Claimed
Send email
   │
   ├── Success ──────────────────────▶ Done ✅
   │
   └── Error → undo the claim (sent=false)
         │
         ├── 4xx (our fault) ─▶ UnrecoverableError (no retry) 💀
         │
         └── timeout / 429 / 5xx ─▶ normal error (retry later) 🔁
```

Each part in simple words:

- **4a. Fetch from DB:** The job only has an ID, so we load the real order. We always get the latest data.
- **4b. `UnrecoverableError`:** A special BullMQ error that means _"don't retry, it will never work."_ If the order doesn't exist now, it won't exist in 10 seconds either.
- **4c. Idempotency (the claim):** One database update that says _"set `sent = true`, but ONLY if it is still `false`."_ If it changed something, **we won** and may send the email. If it changed nothing, someone else already did it, so we **skip**. (Details in the idempotency section below.)
- **4d. Error classification:**
  - A **4xx** error (like "invalid email") means our request is wrong, so retrying won't help → `UnrecoverableError`.
  - A **timeout, 429 or 5xx** error is temporary → `throw err` normally, and BullMQ retries it automatically using `attempts` and `backoff`.
  - In both cases we first **undo the claim**, because the email was not really sent.
- **4e. Done:** The flag was saved in 4c, so there is nothing more to save.

> ⚠️ Small edge case: if the whole worker crashes _after_ the claim but _before_ the email is sent, the flag says "sent" but no email went out. For most apps this is rare and acceptable. For critical actions, use a status field (`PENDING → SENDING → SENT`) with a timestamp, or the provider's **idempotency key** (see [section 9](#9-real-production-idempotency)).

#### STEP 5: Worker

```ts
const worker = new Worker("email-queue", processEmail, { connection, concurrency: CONCURRENCY, limiter: {...} });
```

The worker **connects the queue to your processor**: _"Take jobs from `email-queue` and run `processEmail` on each."_

```
        email-queue
  ┌───┬───┬───┬───┬───┬───┐
  │ 1 │ 2 │ 3 │...│...│ N │
  └─┬─┴─┬─┴─┬─┴───┴───┴───┘
    │   │   │   (up to CONCURRENCY at the same time)
    ▼   ▼   ▼
   processEmail(job)
```

- **`concurrency: CONCURRENCY`**: how many jobs this one worker runs at the same time. We read it from an environment variable so you can change it **without editing code**.
- **`limiter: { max: 100, duration: 1000 }`**: never start more than 100 jobs per second. This protects your email provider from 429 errors. The limit is stored in Redis, so it is **shared by all workers** on this queue.
- **`WORKER_ID`**: a name for this worker (default: machine name + process id). We print it in logs so you can see **which worker handled which job**.

#### STEP 6: Events

Events let you **see what is happening** (logging and alerts).

| Event       | When it fires                          | What we do                    |
| ----------- | -------------------------------------- | ----------------------------- |
| `completed` | Job finished successfully              | Log it                        |
| `failed`    | Job failed (every failed attempt)      | Decide: retry or dead-letter? |
| `error`     | Worker-level problem (e.g. Redis down) | Log it                        |

The `failed` event fires on **every** failed attempt, not only the last one. So we check:

```
Job failed
   │
   ▼
Is it an UnrecoverableError, OR have we used all attempts?
   │
   ├── No  → just log a warning. BullMQ will retry. 🔁
   │
   └── Yes → save to dead-letter queue 📥
```

In production, replace `console.log` with a real logger (like `pino` or `winston`) and send alerts when the dead-letter queue grows.

#### STEP 7: Graceful shutdown

When you deploy, Docker/Kubernetes sends a `SIGTERM` signal. If you ignore it, the process is killed in the middle of a job.

```
SIGTERM received
      │
      ▼
worker.close()  ──▶ stops taking new jobs, waits for running jobs to finish
      │
      ▼
close queues
      │
      ▼
close Redis connection
      │
      ▼
process.exit(0)  ✅
```

- **`shuttingDown` flag:** if two signals arrive, we don't run shutdown twice.
- **30-second timer:** a safety net. If a job hangs forever, we force-exit instead of waiting forever. `.unref()` means this timer alone won't keep the process alive.
- **`SIGINT`:** what you get when you press `Ctrl + C` locally.
- **Order matters:** first stop the worker, then close queues, and close Redis **last**, because everything else uses it.

---

### 🧠 The 3 key concepts inside this code

The code uses three ideas that work together: **concurrency**, **multiple workers**, and **idempotency**. Here is each one, connected to the exact lines of code.

#### 1️⃣ Concurrency (inside ONE worker)

**Where in the code:** `concurrency: CONCURRENCY` in Step 5.

> Concurrency = how many jobs **one** worker runs at the same time.

```
WORKER_CONCURRENCY=3

            Worker A
               │
      ┌────────┼────────┐
      ▼        ▼        ▼
    Job 1    Job 2    Job 3     ← running now

    Job 4, Job 5, Job 6 ...     ← waiting for a free slot
```

How it works in simple words:

- Sending an email mostly means **waiting for the network**. While Job 1 waits, Job 2 and Job 3 can run too.
- When Job 1 finishes, Job 4 immediately takes its slot.
- Higher concurrency = faster, **but** your database, memory and email provider must handle it.

How to choose a number:

| Situation                           | Suggested start                    |
| ----------------------------------- | ---------------------------------- |
| Calls an external API (emails, SMS) | 5–20                               |
| Heavy CPU work (images, PDFs)       | 1–2 (CPU is the limit)             |
| Provider allows 100 req/sec         | Keep `limiter` at or below 100/sec |

> 💡 Start small, watch your metrics, then increase slowly.

#### 2️⃣ Multiple workers (many processes, one queue)

**Where in the code:** nothing special! You simply **run the same file more than once**. Each copy has its own `WORKER_ID`.

```bash
# Terminal 1
WORKER_ID=worker-A WORKER_CONCURRENCY=5 npx ts-node email-system.ts

# Terminal 2
WORKER_ID=worker-B WORKER_CONCURRENCY=5 npx ts-node email-system.ts

# Terminal 3
WORKER_ID=worker-C WORKER_CONCURRENCY=5 npx ts-node email-system.ts
```

```
                      Redis
                        │
                  email-queue
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
      worker-A      worker-B      worker-C
     (5 at once)   (5 at once)   (5 at once)

   Total at the same time = 3 workers × 5 = 15 jobs
```

Important facts:

- Redis hands each job to **only one** worker, so workers don't fight over the same job.
- If `worker-B` crashes, the others keep working. Its unfinished jobs are picked up again later.
- The `limiter` is **shared**: 3 workers still total **100 jobs/second**, not 300.
- Example log output with 3 workers (your output will differ):

```
[worker-A] ▶ processing job order-confirmation-order_1
[worker-B] ▶ processing job order-confirmation-order_2
[worker-C] ▶ processing job order-confirmation-order_3
[worker-A] ✔ email sent for order order_1
[worker-A] ▶ processing job order-confirmation-order_4
```

Scaling with Docker Compose (same image, more copies):

```bash
docker compose up --scale worker=5 -d
```

Each container gets its own hostname, so `WORKER_ID` is unique automatically.

#### 3️⃣ Idempotency (safe to run twice)

**Where in the code:** the `jobId` in Step 3, and the **claim** (`updateOne` with `confirmationEmailSent: false`) in Step 4.

> Idempotency = running the same job again does NOT cause a second email.

Why do we need it now? Because we have **retries**, **crashes** and **multiple workers**. Any of these can cause the same job to run twice.

There are two layers of protection:

```
LAYER 1: when ADDING a job            LAYER 2: when RUNNING a job
────────────────────────────          ───────────────────────────────
add(order_123)  → created ✅           Run 1: claim sent=false→true ✅ → send email
add(order_123)  → ignored 🚫           Run 2: claim finds sent=true  ❌ → skip

Tool: jobId                           Tool: atomic DB claim
```

Why the claim must be **one atomic update** (and not "read, then write"):

```
❌ WRONG: read, then write (two steps)

Worker A: read  sent=false ─┐
Worker B: read  sent=false ─┼─▶ both think "not sent yet"
Worker A: send email 📧     │
Worker B: send email 📧  ◀──┘   → CUSTOMER GETS 2 EMAILS


✅ RIGHT: one atomic update (check + change in one step)

Worker A: update WHERE sent=false → changed 1 row → I won  → send email 📧
Worker B: update WHERE sent=false → changed 0 rows → skip ✅
```

The database lets only **one** update win. That is what makes it safe even with many workers running at the same time.

What if sending fails? We **undo the claim** (`sent = false`), otherwise the retry would think the email was already sent and skip it:

```
Run 1: claim ✅ → send ❌ (timeout) → undo claim → job retried 🔁
Run 2: claim ✅ → send ✅ → done
```

#### 🔗 How the three work together

```
          50,000 jobs added (jobId stops duplicate adds)
                          │
                          ▼
                    email-queue (Redis)
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
      worker-A        worker-B        worker-C      ← MULTIPLE WORKERS
          │               │               │            (scale out)
     ┌────┼────┐     ┌────┼────┐     ┌────┼────┐
     ▼    ▼    ▼     ▼    ▼    ▼     ▼    ▼    ▼
    job  job  job   job  job  job   job  job  job   ← CONCURRENCY
                                                        (scale up)
          │               │               │
          └───────────────┼───────────────┘
                          ▼
              atomic claim in the database          ← IDEMPOTENCY
          (even if a job runs twice, only ONE
                  email is ever sent)
```

| Concept          | Question it answers                   | Controlled by           |
| ---------------- | ------------------------------------- | ----------------------- |
| Concurrency      | How many jobs at once **per worker**? | `WORKER_CONCURRENCY`    |
| Multiple workers | How many workers in total?            | How many copies you run |
| Idempotency      | Is it safe if a job runs twice?       | `jobId` + atomic claim  |

> 🧮 Total parallel jobs = **number of workers × concurrency**. More parallel jobs means a bigger chance of the same job being touched twice, which is exactly why idempotency is required, not optional.

---

### 🔁 Life of one job (everything together)

```
1. User pays for order_123
        │
        ▼
2. API calls enqueueOrderConfirmation("order_123")
        │   jobId = order-confirmation-order_123  (no duplicates)
        ▼
3. Job waits in Redis ("waiting")
        │
        ▼
4. Worker picks it up ("active")
        │
        ├─ already sent?      → skip → completed ✅
        ├─ email sent OK      → mark sent → completed ✅
        ├─ temporary error    → wait (backoff) → retry 🔁
        └─ permanent error    → failed → dead-letter queue 📥
```

### 🧪 How to try it locally

```bash
# 1. Start Redis
docker run -p 6379:6379 redis

# 2. Start 2 workers (two terminals)
WORKER_ID=worker-A WORKER_CONCURRENCY=3 npx ts-node email-system.ts
WORKER_ID=worker-B WORKER_CONCURRENCY=3 npx ts-node email-system.ts

# 3. Add jobs from another script or your API
#    await enqueueOrderConfirmation("order_123");
#    await enqueueOrderConfirmation("order_123");   // duplicate → ignored
```

Watch the logs: jobs are shared between `worker-A` and `worker-B`, and each order gets only **one** email.

### ✅ What this one file already protects you from

| Problem                          | Protected by                                         |
| -------------------------------- | ---------------------------------------------------- |
| Provider overload (429)          | `concurrency` + shared `limiter`                     |
| Slow processing / traffic spikes | Multiple workers (`WORKER_ID`, `WORKER_CONCURRENCY`) |
| Same job added twice             | `jobId`                                              |
| Same job processed twice         | Atomic claim on `confirmationEmailSent`              |
| Temporary failures               | `attempts` + `backoff`                               |
| Useless retries                  | `UnrecoverableError`                                 |
| Lost failed jobs                 | Dead-letter queue + `removeOnFail: false`            |
| Blind spots                      | `completed` / `failed` / `error` events              |
| Jobs killed during deploy        | Graceful shutdown                                    |
| Heavy Redis memory               | Small payloads + `removeOnComplete`                  |

---

### ⚡ Concurrency + Multiple Workers in this code (simple explanation)

Upar ke code mein **ek hi worker** hai. Production mein hum **wahi file kai baar run** karte hain. Isse speed badhti hai aur ek worker crash ho jaye to bhi kaam chalta rehta hai.

#### 1. Concurrency ko configurable banao

Hard-coded `10` ki jagah environment variable use karo. Isse code badle bina har server ke liye value set kar sakte ho.

```ts
const CONCURRENCY = Number(process.env.WORKER_CONCURRENCY ?? 10);

const worker = new Worker("email-queue", processEmail, {
  connection,
  concurrency: CONCURRENCY, // ek worker = itne jobs ek saath
  limiter: { max: 100, duration: 1000 }, // poori queue ke liye max 100 jobs/second
});
```

- **`concurrency`** = **ek worker** ek saath kitne jobs chalayega.
- **`limiter`** = Redis mein store hota hai, to **saare workers milkar** 100 jobs/second se zyada start nahi karte. Isliye worker badhane par bhi provider overload nahi hota.

#### 2. Har worker ko pehchano (logs ke liye)

```ts
import os from "os";

const WORKER_ID = `${os.hostname()}-${process.pid}`;

worker.on("completed", (job) => {
  console.log(`✅ [${WORKER_ID}] Job ${job.id} done`);
});
```

Logs mein dikhega ki kaun sa worker kaun sa job kar raha hai:

```
✅ [worker-1-101] Job order-confirmation-order_1 done
✅ [worker-2-102] Job order-confirmation-order_2 done
✅ [worker-3-103] Job order-confirmation-order_3 done
```

#### 3. Multiple workers kaise chalayein

**Local (3 terminals):**

```bash
WORKER_CONCURRENCY=5 npx ts-node email-system.ts   # Worker A
WORKER_CONCURRENCY=5 npx ts-node email-system.ts   # Worker B
WORKER_CONCURRENCY=5 npx ts-node email-system.ts   # Worker C
```

**Docker Compose (ek command mein):**

```yaml
services:
  worker:
    build: .
    command: node dist/email-system.js
    environment:
      REDIS_URL: redis://redis:6379
      WORKER_CONCURRENCY: "5"
```

```bash
docker compose up --scale worker=3 -d
```

**Kubernetes:** Deployment mein `replicas: 3` set karo (full YAML section 4 mein hai).

#### 4. Diagram: kaam kaise baantta hai

```
                      Redis
                        │
                  email-queue
        ┌───┬───┬───┬───┬───┬───┬───┬───┐
        │ 1 │ 2 │ 3 │ 4 │ 5 │...│...│ N │
        └─┬─┴─┬─┴─┬─┴───┴───┴───┴───┴───┘
          │   │   │
   ┌──────┘   │   └──────┐
   ▼          ▼          ▼
Worker A   Worker B   Worker C
(5 jobs)   (5 jobs)   (5 jobs)

3 workers × concurrency 5 = ek saath 15 jobs
```

- Har job **sirf ek worker** ko milta hai.
- Agar Worker B crash ho jaye, A aur C kaam karte rehte hain.
- Zyada load aaye to bas workers badha do.

#### 5. Multiple workers ke saath idempotency aur zaroori ho jaati hai

Jab kai workers hote hain, tab ek hi order ka email **do workers ek saath** bhej sakte hain. Jaise, ek job crash hua aur recover hote waqt doosre worker ko mil gaya, jabki pehla worker abhi chal raha tha.

```
Worker A: flag padha = false ──┐
                               ├──▶ dono email bhej dete hain ❌
Worker B: flag padha = false ──┘
```

Iska solution hai **atomic claim**: ek hi database command mein "check + set" karo. Jo pehle jeetega, wahi email bhejega.

```ts
// processEmail ke andar, "4c" aur "4e" ki jagah:

// Claim karne ki koshish: flag false ho to hi true karo (ek atomic step)
const claim = await Order.updateOne(
  { _id: orderId, confirmationEmailSent: false },
  { $set: { confirmationEmailSent: true } },
);

// modifiedCount 0 = kisi aur worker ne pehle hi claim kar liya
if (claim.modifiedCount === 0) {
  return { skipped: true };
}

try {
  await emailProvider.send({
    /* ... */
  });
} catch (err) {
  // Email gaya hi nahi, to claim wapas lo taaki retry ho sake
  await Order.updateOne(
    { _id: orderId },
    { $set: { confirmationEmailSent: false } },
  );
  throw err; // normal error → BullMQ retry karega (4xx ke liye UnrecoverableError)
}
```

```
Worker A: claim try ──▶ jeet gaya ✅ ──▶ email bhejta hai
Worker B: claim try ──▶ modifiedCount 0 ──▶ skip 🚫
```

#### 6. Kitna concurrency aur kitne workers? (simple rule)

```
Total ek saath jobs = workers × concurrency
```

1. Pehle dekho **provider kitna allow karta hai** (jaise 100 requests/second).
2. Us hisaab se `limiter` set karo.
3. Phir kam concurrency se shuru karo (jaise 5 workers × 5 = 25 jobs) aur `waiting` jobs dekho.
4. `waiting` badhta rahe to workers badhao. `429` errors aayen to kam karo.

| Situation                                  | Kya karein                        |
| ------------------------------------------ | --------------------------------- |
| `waiting` jobs badh rahe hain              | Workers badhao                    |
| Provider se `429` aa raha hai              | Concurrency ya `limiter` kam karo |
| Worker ka CPU bhara hai                    | Workers badhao (concurrency nahi) |
| Worker mostly network ka wait kar raha hai | Concurrency badhao                |

## Quick Revision Summary

| Topic                     | One-line answer                                                              |
| ------------------------- | ---------------------------------------------------------------------------- |
| **Concurrency**           | How many jobs **one worker** processes at the same time                      |
| **Multiple workers**      | Many workers on one queue → BullMQ shares the jobs                           |
| **Jobs at the same time** | `workers × concurrency`                                                      |
| **Worker scaling**        | Add more small workers instead of one huge worker                            |
| **Worker vs API**         | Separate processes, because they scale differently                           |
| **Idempotency**           | Running the same job many times gives the same final result                  |
| **Duplicate enqueue**     | Fix with a stable `jobId`                                                    |
| **Duplicate processing**  | Fix with a DB check in the worker                                            |
| **DLQ**                   | A separate queue for jobs that failed for good                               |
| **Retryable errors**      | Timeouts, `503`, `429` → retry with backoff                                  |
| **Permanent errors**      | Invalid data, missing resource → don't retry (`UnrecoverableError`)          |
| **Graceful shutdown**     | On `SIGTERM`: stop new work → finish current jobs → close connections → exit |
| **Monitoring**            | Watch waiting, active, failed, delayed, latency, failure rate                |
| **Payload size**          | Send IDs, not huge objects                                                   |

### Golden rules

> 1. **Concurrency** should match what the downstream service can handle.
> 2. A unique `jobId` stops **duplicate adding**. Your worker logic stops **duplicate processing**.
> 3. **Retry** only errors that can be fixed by trying again.
> 4. **Always** shut down gracefully.
> 5. **Monitor** your queues.
