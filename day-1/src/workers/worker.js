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
                name,
                email,
            });

        }, 2000);
    });
};

const worker = new Worker(
    "email-queue",

    async (job) => {

        const startTime = Date.now();

        const { name, email } = job.data;

        const attempt = job.attemptsMade + 1

        console.log("Email job started", {
            jobId: job.id,
            jobName: job.name,
            attempt,
            email,
        });

        try {
            if (attempt <= 2) {
                throw new Error("Email provider temporarily unavailable");
            }

            const emailResult = await sendEmail(name, email, job.attemptsMade);

            const duration = Date.now() - startTime;

            console.log("Email job completed", {
                jobId: job.id,
                attempt,
                duration: `${duration}ms`,
                email,
            });

            return emailResult;

        } catch (error) {

            const duration = Date.now() - startTime;

            console.error("Email job failed", {
                jobId: job.id,
                attempt,
                duration: `${duration}ms`,
                email,
                error: error.message,
            });

            throw error;
        }
    },

    { connection }
);


