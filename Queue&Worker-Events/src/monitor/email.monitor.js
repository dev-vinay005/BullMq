import { emailQueue } from "../queues/emailQueue.js";

export const getEmailQueueStats = async () => {
    const counts =
        await emailQueue.getJobCounts(
            "waiting",
            "active",
            "completed",
            "failed",
            "delayed"
        );

    console.log({
        waiting: counts.waiting ?? 0,
        active: counts.active ?? 0,
        completed: counts.completed ?? 0,
        failed: counts.failed ?? 0,
        delayed: counts.delayed ?? 0,
    });
};

getEmailQueueStats()