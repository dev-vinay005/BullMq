import { Queue } from "bullmq";
import { connection } from "../config/connection.js";
const emailQueue = new Queue('email-queue', { connection });

export const addEmailInQueue = async (name, email) => {
    const result = await emailQueue.add(
        "delayed-email",
        {
            name,
            email,
        },
        {
            attempts: 3,
            delay: 5000,
            backoff: {
                type: "exponential",
                delay: 1000,
            },

            // removeOnComplete: true,
            // removeOnFail: true,
        }
    );

    console.log("Email job added", {
        jobId: result.id,
        jobName: result.name,
    });

    return result.id;
};



