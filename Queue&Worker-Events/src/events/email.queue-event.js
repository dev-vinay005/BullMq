
import { QueueEvents } from "bullmq";
import { connection } from "../config/connection.js";

const queueEvent = new QueueEvents("email-queue", { connection });

/*
  JOB LIFECYCLE:

  Producer adds a job
        ↓
  Job enters the queue
        ↓
  Job waits for a worker
        ↓
  WAITING event
*/


// ----------------------------------------------------
// 1. WAITING
// ----------------------------------------------------

/*
  This event happens when a job is waiting in the queue.

  Example:

  Producer
      ↓
  Job 1 added
      ↓
  Queue
      ↓
  Job 1 is WAITING

  The worker has not started processing this job yet.

  Example:
  If 10 jobs are added and the worker can process only
  1 job at a time, the other jobs will stay WAITING.
*/

queueEvent.on("waiting", ({ jobId }) => {

    console.log(
        "⏳ QUEUE EVENT → WAITING",
        {
            jobId,
        }
    );

});


// ----------------------------------------------------
// 2. ACTIVE
// ----------------------------------------------------

/*
  The worker picks a waiting job.

  Job lifecycle:

  WAITING
      ↓
  Worker picks the job
      ↓
  ACTIVE

  This means the job is now being processed by a worker.

  Example:

  Job 1 → WAITING
           ↓
         ACTIVE
           ↓
       Worker processes it
*/

queueEvent.on(
    "active",
    ({ jobId, prev }) => {

        console.log(
            "🔵 QUEUE EVENT → ACTIVE",
            {
                jobId,
                previousState: prev,
            }
        );

    }
);


// ----------------------------------------------------
// 3. PROGRESS
// ----------------------------------------------------

/*
  This event happens when the worker updates the
  progress of a job.

  The worker can do:

      job.updateProgress(...)

  Example:

  Job 1
     ↓
  Processing 10,000 leads
     ↓
  2,000 processed
     ↓
  progress = 20%

  Then:

  5,000 processed
     ↓
  progress = 50%

  Then:

  10,000 processed
     ↓
  progress = 100%

  queueEvent can listen to these progress updates.

  This is very useful for:

  - File imports
  - Lead imports
  - Video processing
  - Report generation
  - Large background tasks
*/

queueEvent.on(
    "progress",
    ({ jobId, data }) => {

        console.log(
            "📊 QUEUE EVENT → PROGRESS",
            {
                jobId,
                progress: data,
            }
        );

    }
);


// ----------------------------------------------------
// 4. COMPLETED
// ----------------------------------------------------

/*
  The worker successfully finishes the job.

  Job lifecycle:

  WAITING
      ↓
  ACTIVE
      ↓
  Worker processes job
      ↓
  SUCCESS
      ↓
  COMPLETED

  Example:

  Job 1
     ↓
  Send email
     ↓
  Email sent successfully
     ↓
  COMPLETED

  This event tells us:

  "This job has successfully finished."
*/

queueEvent.on(
    "completed",
    ({ jobId, returnvalue }) => {

        console.log(
            "✅ QUEUE EVENT → COMPLETED",
            {
                jobId,
                returnvalue,
            }
        );

    }
);


// ----------------------------------------------------
// 5. FAILED
// ----------------------------------------------------

/*
  The worker tried to process the job,
  but something went wrong.

  Job lifecycle:

  WAITING
      ↓
  ACTIVE
      ↓
  Worker processes job
      ↓
  ERROR
      ↓
  FAILED

  Example:

  Job 5
     ↓
  Send email
     ↓
  Email provider timeout
     ↓
  FAILED

  The failedReason tells us why the job failed.

  If retries are configured, the job may be tried again
  before it finally becomes FAILED.
*/

queueEvent.on(
    "failed",
    ({ jobId, failedReason }) => {

        console.log(
            "❌ QUEUE EVENT → FAILED",
            {
                jobId,
                failedReason,
            }
        );

    }
);


// ----------------------------------------------------
// 6. STALLED
// ----------------------------------------------------

/*
  A job becomes STALLED when the worker was processing
  the job but stopped responding properly.

  Simple example:

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

  This is useful for production monitoring.

  It can help us detect:

  - Worker crashes
  - Worker becoming unresponsive
  - Long-running/problematic jobs

  STALLED is different from FAILED.

  FAILED:
      The worker processed the job and an error happened.

  STALLED:
      BullMQ lost track of the worker processing the job.
*/

queueEvent.on(
    "stalled",
    ({ jobId }) => {

        console.log(
            "⚠️ QUEUE EVENT → STALLED",
            {
                jobId,
            }
        );

    }
);

