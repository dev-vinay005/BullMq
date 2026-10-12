import { Worker } from "bullmq";
import { connection } from "../config/connection.js";

const generateReport = async () => {
    console.log("Generating daily Report");

    return new Promise((res, rej) => {
        setTimeout(() => {
            return res({
                totalUsers: 1250,
                totalOrders: 348,
                revenue: 125000,
            })
        }, 2000)
    })

    // await new Promise((res, rej) => {
    //     setTimeout(res, 2000)
    // })

    // return {
    //     totalUsers: 1250,
    //     totalOrders: 348,
    //     revenue: 125000,
    // }    
}

export const reportWorker = new Worker("report-queue",
    async (job) => {

        const startTime = Date.now()


        console.log("Report job started", {
            jobId: job.id,
            jobName: job.name,
            reportType: job.data.reportType,
        });

        try {
            const report = await generateReport()
            const duration = Date.now() - startTime
            console.log("Report Generated:", {
                jobId: job.id,
                report,
                duration: `${duration}ms`,
            });

            return report

        } catch (err) {
            console.log("Error from the Report_queue worker:", err);
            throw err
        }

    }, { connection })


reportWorker.on("active", (job) => {
    console.log("Report Job Active", {
        jobName: job.name,
        jobId: job.id,
    });

})

reportWorker.on("completed", (job, result) => {
    console.log("Report Job Completed", {
        jobName: job.name,
        jobId: job.id,
        Data: job.data,
        Result: result
    });
})

reportWorker.on("failed", (job, error) => {
    console.log("Report Job Failed", {
        jobName: job.name,
        jobId: job.id,
        failedReason: job.failedReason,
        errorMessage: error.message
    });
})

reportWorker.on("error", (error) => {
    console.error("Report worker error", {
        error: error.message,
    });
});