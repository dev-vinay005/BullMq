import { QueueEvents } from "bullmq";
import { connection } from "../config/connection.js";
const queueEvent = new QueueEvents('email-queue', { connection })

queueEvent.on("waiting", ({ jobId }) => {

    console.log(
        "⏳ QUEUE EVENT → WAITING",
        {
            jobId,
        }
    );

});


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

queueEvent.on(
    "failed",
    (job) => {

        console.log(
            "❌ QUEUE EVENT → FAILED",
            {
                jobId: job.jobId,
                fialedReason: job.failedReason
            }

        );

    }
);