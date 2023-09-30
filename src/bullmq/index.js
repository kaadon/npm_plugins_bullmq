import {Queue, Worker} from 'bullmq';
import {redis} from "@kaadon.com/database";

export async function job_push({queueName, jobName, jobData}) {
    try {
        let connection = redis()
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
    let connection = redis()
    const myWorker = new Worker(`bullMQ:${queueName}`, async (job) => {
        const job_name = Object.keys(jobName).find(item => item === job.name)
        if (job_name) await jobName[job_name](job.data)
    }, {connection});
    myWorker.on('completed', (job)=>completed(job));
    myWorker.on('failed', (job)=>failed(job));
}
