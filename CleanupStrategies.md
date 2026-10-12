# BullMQ — Cleanup Strategy (Deep Dive)

> Revision notes on how and why to clean up old jobs in BullMQ: `removeOnComplete`, `removeOnFail`, keeping the last N jobs, keeping jobs for X seconds, and building a safe production strategy.

---

## 📑 Table of Contents

**What we'll cover**

1. Why cleanup is needed
2. `removeOnComplete`
3. `removeOnFail`
4. Keep last N jobs
5. Keep jobs for X seconds
6. Failed-job retention strategy
7. Why "delete everything" is dangerous
8. Production strategy for our report system
9. Small implementation task

**Sections in these notes**

1. [Why Do We Need Cleanup?](#1-why-do-we-need-cleanup)
2. [`removeOnComplete`](#2-removeoncomplete)
3. [Should We Always Use `true`?](#3-should-we-always-use-true)
4. [Keep the Last N Completed Jobs](#4-keep-the-last-n-completed-jobs)
5. [Keep Jobs for a Certain Amount of Time](#5-keep-jobs-for-a-certain-amount-of-time)
6. [Failed Jobs Are Different ⚠️](#6-failed-jobs-are-different-️)
7. [A Better Production Strategy](#7-a-better-production-strategy)
8. [Queue ≠ Your Permanent History](#8-queue--your-permanent-history)
9. [Your Daily-Report System](#9-your-daily-report-system)
10. [Cleanup vs Retry](#10-important-distinction-cleanup-vs-retry)
11. [Cleanup Isn't Necessarily Immediate](#11-cleanup-isnt-necessarily-immediate)
12. [What We'll Use in Our Mini Project](#12-what-wed-use-in-our-mini-project)
13. [Quick Revision Summary](#-quick-revision-summary)

---

## 1. Why Do We Need Cleanup?

Suppose your scheduler runs **every 20 seconds**. That means:

| Time     | Number of jobs |
| -------- | -------------- |
| 1 minute | 3 jobs         |
| 1 hour   | 180 jobs       |
| 1 day    | 4,320 jobs     |
| 1 month  | 129,600 jobs   |

Now imagine **every completed job stays in Redis**. Redis slowly fills up:

```text
Redis
├── completed job #1
├── completed job #2
├── completed job #3
├── ...
├── completed job #129,600
└── ...
```

Most of those jobs are **no longer useful**.

### Job lifecycle with cleanup

```text
Job lifecycle
     ↓
  waiting
     ↓
  active
     ↓
 completed
     ↓
┌───────────────┐
│    Cleanup    │
└───────────────┘
     ↓
  deleted
```

---

## 2. `removeOnComplete`

You already used this before:

```js
removeOnComplete: true;
```

**Meaning:** Once the job completes successfully, remove it from the queue's completed-job storage.

```text
Job
 ↓
active
 ↓
completed
 ↓
DELETE
```

### Good for

Jobs where you **don't care** about historical job records.

Examples:

- Resize image
- Send notification
- Generate temporary thumbnail
- Cache refresh

---

## 3. Should We Always Use `true`?

**No.**

Imagine your production system has a **daily financial report**, and someone asks:

> "What happened to yesterday's report?"

If you did this:

```js
removeOnComplete: true;
```

the BullMQ job itself may **already be gone**.

> [!IMPORTANT]
> Cleanup is a **business decision**. It is not simply:
> _"Delete completed jobs because Redis."_

---

## 4. Keep the Last N Completed Jobs

Instead of:

```js
removeOnComplete: true;
```

you can use:

```js
removeOnComplete: 100;
```

**Meaning (conceptually):** Keep the **most recent 100** completed jobs.

```text
Completed jobs

#101 ← deleted
#102 ← deleted
#103 ← deleted
...
#200 ← kept
```

**Useful when** you want some recent history for debugging/monitoring, without keeping everything forever.

---

## 5. Keep Jobs for a Certain Amount of Time

You can also keep jobs based on their **age**.

Conceptually:

```js
removeOnComplete: {
  age: 3600,    // keep up to 1 hour (in seconds)
  count: 1000,  // keep at most 1000 jobs
}
```

This gives you **two boundaries**:

```text
Keep job
   │
   ├── until 1 hour old
   │
   └── maximum 1000 jobs
```

So you are saying:

> Keep completed jobs for a **limited period**, but don't allow **unlimited growth**.

This is much closer to **production thinking**.

---

## 6. Failed Jobs Are Different ⚠️

This is where **beginners often make a mistake**.

Consider:

```js
removeOnFail: true;
```

You might think: _"Failed jobs are useless, delete them."_

But failed jobs can be **extremely valuable for debugging**.

Suppose:

```text
Job #9281
Email provider timeout
```

You may want to inspect:

- `jobId`
- job name
- attempts
- `failedReason`
- timestamp
- `data`

...**before** deciding what to do.

> [!WARNING]
> Blindly doing `removeOnFail: true` can make debugging **much harder**.

---

## 7. A Better Production Strategy

For many systems, a good starting point (conceptually) is:

```js
{
  removeOnComplete: {
    age: 3600,
    count: 1000,
  },

  removeOnFail: {
    age: 86400,
    count: 5000,
  },
}
```

| Job type           | Keep for | Maximum jobs |
| ------------------ | -------- | ------------ |
| ✅ Successful jobs | 1 hour   | 1000         |
| ❌ Failed jobs     | 24 hours | 5000         |

**Failed jobs get longer retention** because they are more useful for investigation.

> [!NOTE]
> These numbers are **not universal**. They depend on:
>
> - Your workload
> - Redis memory
> - Compliance rules
> - Observability (monitoring) needs
> - Whether you store the real business record somewhere else

---

## 8. Queue ≠ Your Permanent History

> [!IMPORTANT]
> **Don't turn BullMQ into your application's audit database.**

| System                   | Role                  | Type of data                  |
| ------------------------ | --------------------- | ----------------------------- |
| **BullMQ**               | Background processing | Temporary operational history |
| **PostgreSQL / MongoDB** | Business records      | Permanent history             |

### Example for a report system

**MongoDB — `Report`**

```text
reportId
type
generatedAt
status
location
fileUrl
```

**BullMQ** only handles:

```text
Generate report job
```

So even if BullMQ deletes the completed job:

```text
BullMQ job deleted
       ↓
Business record still exists
```

✅ **That's the correct architecture.**

---

## 9. Your Daily-Report System

Right now you have:

```text
Scheduler
   ↓
daily-report job
   ↓
Worker
   ↓
generate report
```

Let's think about cleanup. Would we want this?

```js
removeOnComplete: true;
```

**Probably not necessarily.**

**Why?** Because reports may be operationally useful to inspect for a while.

### A better learning configuration

```js
removeOnComplete: {
  age: 3600,
  count: 100,
},

removeOnFail: {
  age: 86400,
  count: 500,
},
```

Then:

```text
Completed
    ↓
Keep recent history
    ↓
Eventually cleanup
```

```text
Failed
    ↓
Keep longer
    ↓
Investigate/retry
    ↓
Eventually cleanup
```

---

## 10. Important Distinction: Cleanup vs Retry

These are **completely different concepts**.

### Retry

```text
Job failed
    ↓
attempts remaining?
    ↓
   YES
    ↓
 backoff
    ↓
  retry
```

### Cleanup

```text
Job completed/failed
        ↓
retention policy
        ↓
eventually remove old job
```

| Concept     | Question it answers                   |
| ----------- | ------------------------------------- |
| **Retry**   | What should happen **after failure**? |
| **Cleanup** | **How long** should we keep the job?  |

> [!CAUTION]
> Don't mix these two concepts.

---

## 11. Cleanup Isn't Necessarily Immediate

Don't think like this:

```text
completed
    ↓
exactly 1 hour later
    ↓
deleted at exactly 10:00:00
```

Retention is handled by BullMQ's cleanup mechanisms. Treat `age` / `count` as **retention boundaries**, **not** as a precise deletion scheduler.

For production systems, think:

> ✅ _"Jobs older than my retention policy should not accumulate."_

rather than:

> ❌ _"This exact job must disappear at this exact second."_

---

## 12. What We'd Use in Our Mini Project

For our learning `reportQueue`, we **intentionally do not use `true`**.

We use retention limits so we can **inspect Redis/BullMQ**:

```js
removeOnComplete: {
  age: 60 * 60,       // 1 hour
  count: 100,
},

removeOnFail: {
  age: 24 * 60 * 60,  // 24 hours
  count: 500,
},
```

That gives us:

| Job type      | Max age  | Max count |
| ------------- | -------- | --------- |
| **Completed** | 1 hour   | 100       |
| **Failed**    | 24 hours | 500       |

---

## 🧠 Quick Revision Summary

| Option                             | What it does                                            | When to use                                                     |
| ---------------------------------- | ------------------------------------------------------- | --------------------------------------------------------------- |
| `removeOnComplete: true`           | Deletes the job as soon as it completes                 | Jobs where history doesn't matter (resize image, cache refresh) |
| `removeOnComplete: 100`            | Keeps only the **last 100** completed jobs              | Some recent history for debugging/monitoring                    |
| `removeOnComplete: { age, count }` | Keeps jobs up to a **time limit** and a **count limit** | Production-style retention                                      |
| `removeOnFail: true`               | Deletes failed jobs immediately                         | ⚠️ Usually a bad idea (hard to debug)                           |
| `removeOnFail: { age, count }`     | Keeps failed jobs **longer**                            | Debugging and investigation                                     |

### Key points to remember

- Cleanup is a **business decision**, not just a Redis memory decision.
- **Failed jobs** should be kept **longer** than successful jobs.
- BullMQ is **temporary operational history**. Real business records belong in your **database**.
- **Retry** = what happens after failure. **Cleanup** = how long we keep the job. They are different.
- `age` and `count` are **retention boundaries**, not exact deletion times.
