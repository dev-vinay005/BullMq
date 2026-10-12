import { Queue } from "bullmq";
import { connection } from "../config/connection.js";
const emailQueue = new Queue('email-queue', { connection });

// async function init() {

//     const arr = [
//         {
//             name: "vinay",
//             email: "vinay@gmail.com",
//             subject: "Welcome message",
//             body: "Hey vinay welcome to Platform"
//         },
//         {
//             name: 'karna',
//             email: "karna@gmail.com",
//             subject: "Welcome message",
//             body: "Hey karna welcome to Platform"
//         },
//         {
//             name: "Raman",
//             email: "Raman@gmail.com",
//             subject: "Welcome message",
//             body: "Hey Raman welcome to Platform"
//         },
//         {
//             name: 'Ritik',
//             email: "Ritik@gmail.com",
//             subject: "Welcome message",
//             body: "Hey Ritik welcome to Platform"
//         },
//     ]

//     for (let i = 0; i < arr.length; i++) {
//         setTimeout(async () => {
//             const result = await notificationQueue.add(`email to ${arr[i].name}`, {
//                 name: arr[i].name,
//                 email: arr[i].email,
//                 subject: arr[i].subject,
//                 body: arr[i].body
//             })
//             console.log("Job added");
//         }, i * 2000)
//     }

// }

// init()
export const addEmailInQueue = async (name, email) => {
    const result = await emailQueue.add(
        "welcome-email",
        {
            name,
            email,
        },
        {
            attempts: 3,

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



