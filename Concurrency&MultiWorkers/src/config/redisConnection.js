import { createClient } from "redis";
import { config } from "dotenv";
config()
export const redisClient = createClient({
    username: process.env.REDIS_USERNAME,
    password: process.env.REDIS_PASSWORD,
    socket: {
        host: process.env.REDIS_HOST,
        port: process.env.REDIS_PORT,
    },
});

redisClient.on("error", (error) => {
    console.error("Redis error:", error);
});

export const connectRedis = async () => {
    if (redisClient.isOpen) {
        return;
    }

    await redisClient.connect();
    console.log("Redis connected");
};

export const disconnectRedis = async () => {
    if (!redisClient.isOpen) {
        return;
    }

    await redisClient.quit();
};

export default redisClient;
