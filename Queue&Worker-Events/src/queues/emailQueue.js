import { Queue } from "bullmq";
import { connection } from "../config/connection.js";

export const emailQueue = new Queue('email-queue', { connection });
