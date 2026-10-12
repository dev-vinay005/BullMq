import { emailQueue } from '../queues/emailQueue.js';

const addEmailInQueue = async (req, res) => {
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
            name: "Amit",
            email: "amit@gmail.com",
            subject: "Welcome message",
            body: "Hey Amit welcome to Platform"
        },
        {
            name: "Rahul",
            email: "rahul@gmail.com",
            subject: "Welcome message",
            body: "Hey Rahul welcome to Platform"
        },
        {
            name: "Priya",
            email: "priya@gmail.com",
            subject: "Welcome message",
            body: "Hey Priya welcome to Platform"
        },
        {
            name: "Neha",
            email: "neha@gmail.com",
            subject: "Welcome message",
            body: "Hey Neha welcome to Platform"
        },
        {
            name: "Ankit",
            email: "ankit@gmail.com",
            subject: "Welcome message",
            body: "Hey Ankit welcome to Platform"
        },
        {
            name: "Sneha",
            email: "sneha@gmail.com",
            subject: "Welcome message",
            body: "Hey Sneha welcome to Platform"
        }
    ]
    for (let i = 0; i < arr.length; i++) {

        await emailQueue.add("send-email", {
            name: arr[i].name,
            email: arr[i].email,
            subject: arr[i].subject
        })
        console.log("Job added");
    }

};

addEmailInQueue()