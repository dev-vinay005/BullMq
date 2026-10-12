import { Worker } from "bullmq";
import { connection } from "../config/connection.js";

const sendEmail = async (name, email) => {
    return new Promise((res, rej) => {
        setTimeout(() => {

            if (email === "vinay@gmail.com") {
                return rej(new Error("Email provider timeout"));
            }

            console.log("Email Sent to:", email);


            res({

                email,
            });

        }, 2000);
    });
};

const worker = new Worker(
    "email-queue",

    async (job) => {
        const { name, email } = job.data;
        const attempt = job.attemptsMade + 1
        console.log("Email job started", {
            jobId: job.id,
            jobName: job.name,
            attempt,
            email,
        });

        try {
            const emailResult = await sendEmail(name, email);
            console.log("Email job completed",
                job.data
            );
            return emailResult;

        } catch (error) {

            console.error("Email job failed", {
                jobId: job.id,
                attempt,
                email,
                error: error.message,
            });

            throw error;
        }
    },

    { connection }
);


worker.on("active", (job) => {
    console.log("Email job is now active", {
        jobId: job.id,
        jobName: job.name,
        email: job.data.email,
    });
});

worker.on("completed", (job, result) => {
    console.log("Email job completed successfully", {
        jobId: job.id,
        jobName: job.name,
        email: job.data.email,
        result,
    });
});

worker.on("failed", (job, error) => {
    console.log("Email job failed loggin the job", {
        // job: job.id,
        // failedReason: job.failedReason,
        error: error.message
    });
})
