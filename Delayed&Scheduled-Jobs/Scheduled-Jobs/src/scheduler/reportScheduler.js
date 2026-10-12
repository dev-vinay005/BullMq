// 20 seconds = one simulated day it means this scheduler will create a job after every 20 sec
// In production, you'd normally use something like:
// Every day at 9:00 AM
import { reportQueue } from "../queues/reportQueue.js";

export const setupReportScheduler = async () => {
    await reportQueue.upsertJobScheduler(
        "daily-report-scheduler",
        {
            every: 20_000,
        },
        {
            name: "daily-report",
            data: {
                reportType: "daily",
            },
        }
    );

    console.log("Daily report scheduler started");
};

setupReportScheduler()
