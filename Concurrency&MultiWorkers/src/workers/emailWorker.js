import { Worker } from "bullmq";
import { connection } from "../config/connection.js";

const sendEmail = async () => {
    console.log("Sending email...");

    return new Promise((res, rej) => {
        setTimeout(() => {
            return res("Email send successfully");
        }, 2000)
    })

}

export const emailWorker = new Worker("email-queue",
    async (job) => {

        const startTime = Date.now()


        console.log("Email job started", {
            jobId: job.id,
            jobName: job.name,
        });

        try {
            const data = await sendEmail()
            const duration = Date.now() - startTime
            console.log("Email Sent:", {
                jobId: job.id,
                duration: `${duration}ms`,
            });

            return data

        } catch (err) {
            console.log("Error from the email worker:", err);
            throw err
        }

    }, { connection, concurrency: 3 }) // Set the concurrency to 3, meaning that up to 3 jobs can be processed simultaneously


emailWorker.on("active", (job) => {
    console.log("Email Job Active", {
        jobName: job.name,
        jobId: job.id,
    });

})

emailWorker.on("completed", (job, result) => {
    console.log("Email Job Completed", {
        jobName: job.name,
        jobId: job.id,
        Data: job.data,
        Result: result
    });
})

emailWorker.on("failed", (job, error) => {
    console.log("Email Job Failed", {
        jobName: job.name,
        jobId: job.id,
        failedReason: job.failedReason,
        errorMessage: error.message
    });
})

emailWorker.on("error", (error) => {
    console.error("Email worker error", {
        error: error.message,
    });
});