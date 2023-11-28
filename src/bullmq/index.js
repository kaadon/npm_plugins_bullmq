import {Queue, Worker, QueueScheduler} from 'bullmq';
import {redisDB} from "@kaadon.com/database";

export async function job_push({queueName, jobName, jobData}) {
    try {
        let connection = redisDB()
        //逻辑代码
        const queue = new Queue(`bullMQ:${queueName}`, {connection});
        if (Array.isArray(jobData)) {
            for (const item of jobData) {
                await queue.add(jobName, item, {removeOnComplete: true});
            }
        } else {
            await queue.add(jobName, jobData, {removeOnComplete: true});
        }
        await connection.quit()
        return Promise.resolve()
    } catch (e) {
        return Promise.reject(e.message)
    }
}

export function job_worker({
                               queueName,
                               jobName,
                               completed,
                               failed
                           }) {
    let connection = redisDB()
    const myWorker = new Worker(`bullMQ:${queueName}`, async (job) => {
        const job_name = Object.keys(jobName).find(item => item === job.name)
        if (job_name) await jobName[job_name](job.data)
    }, {connection});
    myWorker.on('completed', (job) => completed(job));
    myWorker.on('failed', (job) => failed(job));
}

export const mq_push = async (queueName, jobName, jobData, JobsOptions = {}) => {
    try {
        //逻辑代码
        let connection = redisDB()
        const queue = new Queue(`bullMQ:${queueName}`, {connection});
        let result = []
        if (Array.isArray(jobData)) {
            for (const item of jobData) {
                result.push(await queue.add(jobName, item, JobsOptions));
            }
        } else {
            result.push(await queue.add(jobName, jobData, JobsOptions))
        }
        return Promise.resolve(result)
    } catch (e) {
        return Promise.reject(e)
    }
}

export const mq_helper =async (queueName) => {
    let connection = redisDB()
    const queueScheduler = new QueueScheduler(queueName, { connection: connection });
    await queueScheduler.waitUntilReady();
}

export function mq_worker(queueName, JobObject) {
    if (!Array.isArray(JobObject)) throw new Error("JobObject 格式错误");
    let connection = redisDB()
    const myWorker = new Worker(`bullMQ:${queueName}`, async (job) => {
        let jobFunc = JobObject.find(item => item.name === job.name)
        if (typeof jobFunc?.run === "function") await jobFunc.run(job.data)
    }, {connection});
    myWorker.on('completed', async (job) => {
        let jobFuncCompleted = JobObject.find(item => item.name === job.name)
        if (typeof jobFuncCompleted?.completed === "function") await jobFuncCompleted.completed(job);
    });
    myWorker.on('failed', async (job) => {
        let jobFuncFailed = JobObject.find(item => item.name === job.name)
        if (typeof jobFuncFailed?.failed === "function") await jobFuncFailed.failed(job);
    });
}
