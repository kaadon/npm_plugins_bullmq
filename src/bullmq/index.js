import {Queue, Worker} from 'bullmq';
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
        connection.quit()
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


export function mq_worker(queueName, JobObject) {
    if (!Array.isArray(JobObject)) throw new Error("JobObject 格式错误");
    let connection = redisDB()
    const myWorker = new Worker(`bullMQ:${queueName}`, async (job) => {
        let jobFunc = Object.keys(JobObject).find(item => item.name === job.name)
        if (typeof jobFunc?.run === "function") await jobFunc.run(job.data)
    }, {connection});
    myWorker.on('completed', (job) => {
        let jobFuncCompleted = Object.keys(JobObject).find(item => item.name === job.name)
        if (typeof jobFuncCompleted?.completed === "function") jobFuncCompleted.completed(job);
    });
    myWorker.on('failed', (job) => {
        let jobFuncFailed = Object.keys(JobObject).find(item => item.name === job.name)
        if (typeof jobFuncFailed?.failed === "function") jobFuncFailed.failed(job);
    });
}
