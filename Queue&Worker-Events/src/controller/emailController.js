import { addEmailInQueue } from "../queues/producer.js";

export const sendEmail = async (req, res) => {
    const { name, email } = req.body;

    const jobId = await addEmailInQueue(name, email);

    res.status(202).json({
        message: "Email job accepted",
        jobId,
    });
};