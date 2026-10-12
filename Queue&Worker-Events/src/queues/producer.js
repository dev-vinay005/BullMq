import { emailQueue } from "./emailQueue.js";
async function init() {

    const arr = [
        {
            name: "vinay",
            email: "vinay@gmail.com",
            subject: "Welcome message",
            body: "Hey vinay welcome to Platform"
        },
        {
            name: 'karna',
            email: "karna@gmail.com",
            subject: "Welcome message",
            body: "Hey karna welcome to Platform"
        },
        {
            name: "Raman",
            email: "Raman@gmail.com",
            subject: "Welcome message",
            body: "Hey Raman welcome to Platform"
        },
        {
            name: 'Ritik',
            email: "Ritik@gmail.com",
            subject: "Welcome message",
            body: "Hey Ritik welcome to Platform"
        },
        {
            name: "vinay123",
            email: "vinay123@gmail.com",
            subject: "Welcome message",
            body: "Hey vinay welcome to Platform"
        },
        {
            name: 'karna123',
            email: "karna123@gmail.com",
            subject: "Welcome message",
            body: "Hey karna welcome to Platform"
        },
        {
            name: "Raman123",
            email: "Raman123@gmail.com",
            subject: "Welcome message",
            body: "Hey Raman welcome to Platform"
        },
        {
            name: 'Ritik123',
            email: "Ritik123@gmail.com",
            subject: "Welcome message",
            body: "Hey Ritik welcome to Platform"
        },
    ]

    for (let i = 0; i < arr.length; i++) {
        setTimeout(async () => {
            const result = await emailQueue.add(`email-queue`, {
                name: arr[i].name,
                email: arr[i].email,
                subject: arr[i].subject,
                body: arr[i].body
            })
            console.log("Job added");
        }, i * 2000)
    }

}

init()
// export const addEmailInQueue = async (name, email) => {
//     const result = await emailQueue.add(
//         "welcome-email",
//         {
//             name,
//             email,
//         },
//         {
//             attempts: 3,

//             backoff: {
//                 type: "exponential",
//                 delay: 1000,
//             },

//             removeOnComplete: true,
//             removeOnFail: true,
//         }
//     );

//     console.log("Email job added", {
//         jobId: result.id,
//         jobName: result.name,
//     });

//     return result.id;
// };



