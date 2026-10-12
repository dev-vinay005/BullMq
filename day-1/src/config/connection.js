import { config } from "dotenv"
config()

console.log(process.env.REDIS_HOST)
export const connection = {
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    username: process.env.REDIS_USERNAME,
    password: process.env.REDIS_PASSWORD
}