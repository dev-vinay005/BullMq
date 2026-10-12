import express from "express"
import { connectRedis } from "./src/config/redisConnection.js"

const app = express()
const port = 3000

app.use(express.json())
app.use('/send-delayed-email', sendEmail)

app.listen(port, async () => {
    await connectRedis()
    console.log("Server is running on port:", port)
})