import { createClient } from "redis";
import { Job } from "./job.js";

export class JobQueue {

    constructor() {
        this.client = createClient({
            socket: {
                reconnectStrategy: (retries) => {
                    return Math.min(retries * 500, 5000);
                }
            }
        });
        this.ready = this.connect();
    }

    async connect() {
        await this.client.connect();
    }

    async close() {
        await this.ready;

        if (this.client.isOpen) {
            await this.client.quit();
        }
    }

    async addJob(jobData) {
        await this.ready;

        const job = new Job(jobData);

        await this.client.hSet(
            "jobs",
            job.id,
            JSON.stringify(job)
        );

        if (job.status === "delayed") {
            const executeTime = new Date(job.processAt).getTime();
            await this.client.zAdd(
                "delayed_jobs",
                {
                    score: executeTime,
                    value: job.id
                }
            );
        } else {
            await this.client.zAdd(
                "queued_jobs",
                {
                    score: job.priority,
                    value: job.id
                }
            );
        }

        return job;
    }

    async promoteDelayedJobs() {
        await this.ready;

        const luaScript = `
            local now = ARGV[1]
            local jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now)
            for _, id in ipairs(jobs) do
                local job_data = redis.call('HGET', KEYS[2], id)
                if job_data then
                    redis.call('ZREM', KEYS[1], id)
                    redis.call('ZADD', KEYS[3], 0, id)
                end
            end
            return #jobs
        `;

        return await this.client.eval(luaScript, {
            keys: ["delayed_jobs", "jobs", "queued_jobs"],
            arguments: [Date.now().toString()]
        });
    }

    async getNextJob() {
        await this.ready;

        await this.promoteDelayedJobs();

        const luaScript = `
            local job_id = redis.call('ZPOPMAX', KEYS[1])
            if not job_id or #job_id == 0 then
                return nil
            end
            
            local id = job_id[1]
            local job_data = redis.call('HGET', KEYS[2], id)
            if not job_data then
                return nil
            end

            local now = ARGV[1]
            redis.call('ZADD', KEYS[3], now, id)
            
            return {id, job_data}
        `;

        const result = await this.client.eval(luaScript, {
            keys: ["queued_jobs", "jobs", "processing_jobs"],
            arguments: [Date.now().toString()]
        });

        if (!result) {
            return null;
        }

        const [jobId, jobData] = result;
        const job = JSON.parse(jobData);

        job.status = "processing";
        job.startedAt = new Date();
        job.attempts++;

        await this.client.hSet(
            "jobs",
            job.id,
            JSON.stringify(job)
        );

        return job;
    }

    async heartbeat(jobId) {
        await this.ready;

        const job =
            await this.getJob(jobId);

        if (
            !job ||
            job.status !== "processing"
        ) {
            return false;
        }

        await this.client.zAdd(
            "processing_jobs",
            {
                score: Date.now(),
                value: job.id
            }
        );

        return true;
    }

    async completeJob(
        jobId,
        result = null
    ) {
        await this.ready;

        const job =
            await this.getJob(jobId);

        if (!job) {
            return null;
        }

        job.status = "completed";
        job.result = result;
        job.completedAt = new Date();

        await this.client.hSet(
            "jobs",
            job.id,
            JSON.stringify(job)
        );

        await this.client.zRem(
            "processing_jobs",
            job.id
        );

        return job;
    }

    async failJob(
        jobId,
        error
    ) {
        await this.ready;

        const job =
            await this.getJob(jobId);

        if (!job) {
            return null;
        }

        job.error = error;

        await this.client.zRem(
            "processing_jobs",
            job.id
        );

        if (
            job.attempts <
            job.maxAttempts
        ) {
            job.status = "delayed";
            job.startedAt = null;

            const delayMs = Math.pow(2, job.attempts) * 1000;
            job.processAt = new Date(Date.now() + delayMs);

            await this.client.hSet(
                "jobs",
                job.id,
                JSON.stringify(job)
            );

            await this.client.zAdd(
                "delayed_jobs",
                {
                    score: Date.now() + delayMs,
                    value: job.id
                }
            );

            return job;
        }

        job.status = "failed";
        job.failedAt = new Date();

        await this.client.hSet(
            "jobs",
            job.id,
            JSON.stringify(job)
        );

        await this.client.zAdd(
            "failed_jobs",
            {
                score: Date.now(),
                value: job.id
            }
        );

        return job;
    }

    async getJob(jobId) {
        await this.ready;

        const data =
            await this.client.hGet(
                "jobs",
                jobId
            );

        if (!data) {
            return null;
        }

        return JSON.parse(data);
    }

    async getAllJobs() {
        await this.ready;

        const jobs =
            await this.client.hGetAll("jobs");

        return Object.values(jobs)
            .map(data => JSON.parse(data));
    }

    async getStalledJobs(timeout = 10000) {
        await this.ready;

        const threshold =
            Date.now() - timeout;

        const stalledIds =
            await this.client.zRangeByScore(
                "processing_jobs",
                0,
                threshold
            );

        const stalledJobs = [];

        for (const jobId of stalledIds) {

            const job =
                await this.getJob(jobId);

            if (!job) {
                continue;
            }

            job.status = "queued";
            job.startedAt = null;

            await this.client.hSet(
                "jobs",
                job.id,
                JSON.stringify(job)
            );

            await this.client.zRem(
                "processing_jobs",
                job.id
            );

            await this.client.zAdd(
                "queued_jobs",
                {
                    score: job.priority,
                    value: job.id
                }
            );

            stalledJobs.push(job);
        }

        return stalledJobs;
    }

    async getStats() {
        await this.ready;

        const jobs =
            await this.client.hGetAll("jobs");

        const stats = {
            total: 0,
            queued: 0,
            delayed: 0,
            processing: 0,
            completed: 0,
            failed: 0
        };

        for (
            const data of Object.values(jobs)
        ) {
            const job = JSON.parse(data);

            stats.total++;

            if (
                stats[job.status] !==
                undefined
            ) {
                stats[job.status]++;
            }
        }

        return stats;
    }
}
