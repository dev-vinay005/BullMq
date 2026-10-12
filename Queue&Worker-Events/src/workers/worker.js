import { Worker } from "bullmq";
import { connection } from "../config/connection.js";

const sendEmail = async (name, email) => {
    return new Promise((res, rej) => {
        setTimeout(() => {

            if (email === "vinay123@gmail.com") {
                return rej(new Error("Email provider timeout"));
            }

            console.log(`📧 Email sent to ${email}`);
            res({
                name,
                email,
            });

        }, 2000);
    });
};

const worker = new Worker(
    "email-queue",

    async (job) => {
        try {
            console.log(
                `PROCESSOR → Processing job ${job.id}`
            );
            // const startTime = Date.now();
            const { name, email } = job.data;

            await job.updateProgress({
                processed: 50,
                total: 100,
            });

            console.log("Email job started", {
                jobId: job.id,
                jobName: job.name,
                // attempt: job.attemptsMade + 1,
                email,
            });

            const emailResult = await sendEmail(name, email);

            // const duration = Date.now() - startTime;

            // console.log("Email job completed", {
            //     jobId: job.id,
            //     attempt: job.attemptsMade + 1,
            //     // duration: `${duration}ms`,
            //     email,
            // });

            return emailResult;

        } catch (error) {

            // const duration = Date.now() - startTime;

            console.error("Email job failed", {
                jobId: job.id,
                attempt: job.attemptsMade + 1,
                // duration: `${duration}ms`,
                email: job.data.email,
                error: error.message,
            });

            throw error;
        }
    },

    { connection }
);


// Here we are learning about Workers Events

/*
  WORKER EVENT LIFECYCLE

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
  SUCCESS      ERROR
*/


// ====================================================
// 1. ACTIVE
// ====================================================

/*
  This event runs when the worker starts processing a job.

  Simple flow:

      Job is WAITING in queue
              ↓
          Worker picks it
              ↓
            ACTIVE
              ↓
       Worker starts work


  Example:

      Job 1
        ↓
      Worker picks Job 1
        ↓
      Worker starts sending email
        ↓
      ACTIVE event


  Here we can observe:

  - Which job the worker started
  - Job name
  - Job data
  - How many attempts were already made
*/

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


// ====================================================
// 2. COMPLETED
// ====================================================

/*
  This event runs when the worker successfully
  finishes processing a job.

  Simple flow:

      ACTIVE
        ↓
    Worker works
        ↓
      SUCCESS
        ↓
    COMPLETED


  Example:

      Job 1
        ↓
      Worker sends email
        ↓
      Email sent successfully
        ↓
      Worker returns result
        ↓
      COMPLETED event


  We can observe:

  - Which job completed
  - Job data
  - Number of attempts
  - Result returned by the processor


  Example result:

      {
          message: "Email sent",
          email: "user@gmail.com"
      }
*/

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


// ====================================================
// 3. FAILED
// ====================================================

/*
  This event runs when the worker cannot successfully
  finish a job.

  Simple flow:

      ACTIVE
        ↓
    Worker processes
        ↓
       ERROR
        ↓
      FAILED


  Example:

      Job 5
        ↓
      Worker sends email
        ↓
      Email provider timeout
        ↓
      sendEmail() throws error
        ↓
      Worker reports FAILED


  We can observe:

  - Which job failed
  - Job data
  - Number of attempts
  - Error message


  IMPORTANT:

  If retries are configured:

      Attempt 1
         ↓
       FAILED
         ↓
      Retry
         ↓
      Attempt 2
         ↓
       SUCCESS


  In this case, the job does NOT permanently fail
  after the first error.
*/

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


// ====================================================
// 4. ERROR
// ====================================================

/*
  This event is different from "failed".

  "failed" means:

      A particular job failed while being processed.

  "error" means:

      The Worker itself has encountered an error.


  Think about it like this:

      Job error:

          Job 5
             ↓
          Email failed
             ↓
          "failed" event


      Worker error:

          Worker
             ↓
          Worker-level problem
             ↓
          "error" event


  So:

      failed → job processing failed

      error → worker itself had an error


  This event is useful for monitoring the health
  of your worker.
*/

worker.on("error", (error) => {

    console.error("🚨 WORKER EVENT → ERROR");
    console.log("🚨error");

    console.log("================================");
})



// ====================================================
// WORKER CONCURRENCY
// ====================================================

/*
  Concurrency ka matlab:

      Ek worker ek time par kitne jobs
      simultaneously process kar sakta hai.

  Example:

new Worker("email-queue", processor, {
    connection,
    concurrency: 3
});

  Queue mein 10 jobs hain:

      Job 1
      Job 2
      Job 3
      Job 4
      Job 5
      Job 6
      Job 7
      Job 8
      Job 9
      Job 10


  Worker:

          Worker
            │
            ├── Job 1 → ACTIVE
            ├── Job 2 → ACTIVE
            └── Job 3 → ACTIVE

      Maximum 3 jobs ek time par process ho rahe hain.


  Baaki jobs WAITING mein rahengi:

          WAITING
          ├── Job 4
          ├── Job 5
          ├── Job 6
          ├── Job 7
          ├── Job 8
          ├── Job 9
          └── Job 10


  Jab Job 1 complete hoti hai:

          Job 1
            ↓
        COMPLETED
            ↓
        Slot available
            ↓
          Job 4
            ↓
          ACTIVE


  New state:

          Worker
            │
            ├── Job 2 → ACTIVE
            ├── Job 3 → ACTIVE
            └── Job 4 → ACTIVE


  ----------------------------------------------------

  Concurrency + Worker Events:

      concurrency = 3

      Job 1 → ACTIVE
      Job 2 → ACTIVE
      Job 3 → ACTIVE

          ↓

      Worker Events:

      active → Job 1
      active → Job 2
      active → Job 3


      Suppose Job 1 fails:

      Job 1 → FAILED
          ↓
        RETRY
          ↓
      Job 1 → ACTIVE


      Isliye retry ke baad Job 1 ke liye
      "active" event dobara fire ho sakta hai.


  ----------------------------------------------------

  Important:

      concurrency = 3

  ka matlab:

      "Ek worker maximum 3 jobs ko
       simultaneously process kar sakta hai."

  Ye:

      ❌ 3 worker processes nahi hain

      ❌ 3 threads nahi hain

      ✅ Ek worker ke andar 3 concurrent jobs hain.


  Example:

      const worker = new Worker(
          "email-queue",
          processor,
          {
              connection,
              concurrency: 3
          }
      );


  ----------------------------------------------------

  Multiple Workers:

      Worker 1
      concurrency = 3

      Worker 2
      concurrency = 3


      Then:

          Redis Queue
               │
          ┌────┴────┐
          ↓         ↓
       Worker 1  Worker 2
          │         │
       3 jobs     3 jobs
          │         │
          └────┬────┘
               ↓
        ~6 jobs processing


  This is one of the ways we can scale
  background job processing in production.
*/