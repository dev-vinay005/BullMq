
import { Queue } from "bullmq";
import { connection } from "../config/connection.js";


export const reportQueue = new Queue("report-queue", { connection })