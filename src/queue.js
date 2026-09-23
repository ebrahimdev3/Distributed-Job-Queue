import { createClient } from "redis";
import { Job } from "./job.js";

export class JobQueue {
    constructor(redisUrl) {
        this.client = createClient({
            url: redisUrl || process.env.REDIS_URL || "redis://localhost:6379",
            socket: {
                reconnectStrategy: (retries) => Math.min(retries * 500, 5000)
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

        await this.client.hSet("jobs", job.id, JSON.stringify(job));

        if (job.status === "delayed") {
            const executeTime = new Date(job.processAt).getTime();
            await this.client.zAdd("delayed_jobs", { score: executeTime, value: job.id });
            await this.client.hIncrBy("stats", "delayed", 1);
        } else {
            await this.client.zAdd("queued_jobs", { score: job.priority, value: job.id });
            await this.client.hIncrBy("stats", "queued", 1);
        }

        await this.client.hIncrBy("stats", "total", 1);
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
                    redis.call('HINCRBY', KEYS[4], 'delayed', -1)
                    redis.call('HINCRBY', KEYS[4], 'queued', 1)
                end
            end
            return #jobs
        `;

        return await this.client.eval(luaScript, {
            keys: ["delayed_jobs", "jobs", "queued_jobs", "stats"],
            arguments: [Date.now().toString()]
        });
    }

    async getNextJob(workerId) {
        await this.ready;
        await this.promoteDelayedJobs();

        const lockToken = `${workerId}:${Date.now()}:${Math.random().toString(36).substring(2, 9)}`;

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
            local lock_token = ARGV[2]

            redis.call('ZADD', KEYS[3], now, id)
            redis.call('HSET', KEYS[4], id, lock_token)

            redis.call('HINCRBY', KEYS[5], 'queued', -1)
            redis.call('HINCRBY', KEYS[5], 'processing', 1)
            
            return {id, job_data, lock_token}
        `;

        const result = await this.client.eval(luaScript, {
            keys: ["queued_jobs", "jobs", "processing_jobs", "job_locks", "stats"],
            arguments: [Date.now().toString(), lockToken]
        });

        if (!result) return null;

        const [jobId, jobData, assignedToken] = result;
        const job = JSON.parse(jobData);

        job.status = "processing";
        job.startedAt = new Date().toISOString();
        job.attempts++;
        job.lockToken = assignedToken;

        await this.client.hSet("jobs", job.id, JSON.stringify(job));
        return job;
    }

    async heartbeat(jobId, lockToken) {
        await this.ready;

        const luaScript = `
            local current_token = redis.call('HGET', KEYS[1], KEYS[2])
            if current_token == ARGV[1] then
                redis.call('ZADD', KEYS[3], ARGV[2], KEYS[2])
                return 1
            end
            return 0
        `;

        const result = await this.client.eval(luaScript, {
            keys: ["job_locks", jobId, "processing_jobs"],
            arguments: [lockToken, Date.now().toString()]
        });

        return result === 1;
    }

    async completeJob(jobId, lockToken, result = null) {
        await this.ready;

        const luaScript = `
            local current_token = redis.call('HGET', KEYS[1], KEYS[2])
            if current_token ~= ARGV[1] then
                return nil
            end

            redis.call('HDEL', KEYS[1], KEYS[2])
            redis.call('ZREM', KEYS[3], KEYS[2])
            redis.call('HINCRBY', KEYS[4], 'processing', -1)
            redis.call('HINCRBY', KEYS[4], 'completed', 1)
            return 1
        `;

        const authCheck = await this.client.eval(luaScript, {
            keys: ["job_locks", jobId, "processing_jobs", "stats"],
            arguments: [lockToken]
        });

        if (!authCheck) {
            throw new Error(`LockLostError: Worker lost ownership of job ${jobId}. Completion rejected.`);
        }

        const job = await this.getJob(jobId);
        if (!job) return null;

        job.status = "completed";
        job.result = result;
        job.completedAt = new Date().toISOString();

        await this.client.hSet("jobs", job.id, JSON.stringify(job));
        return job;
    }

    async failJob(jobId, lockToken, error) {
        await this.ready;

        const luaScript = `
            local current_token = redis.call('HGET', KEYS[1], KEYS[2])
            if current_token ~= ARGV[1] then
                return nil
            end

            redis.call('HDEL', KEYS[1], KEYS[2])
            redis.call('ZREM', KEYS[3], KEYS[2])
            redis.call('HINCRBY', KEYS[4], 'processing', -1)
            return 1
        `;

        const authCheck = await this.client.eval(luaScript, {
            keys: ["job_locks", jobId, "processing_jobs", "stats"],
            arguments: [lockToken]
        });

        if (!authCheck) {
            throw new Error(`LockLostError: Worker lost ownership of job ${jobId}. Failure report rejected.`);
        }

        const job = await this.getJob(jobId);
        if (!job) return null;

        job.error = error;

        if (job.attempts < job.maxAttempts) {
            job.status = "delayed";
            job.startedAt = null;

            const delayMs = Math.pow(2, job.attempts) * 1000;
            job.processAt = new Date(Date.now() + delayMs).toISOString();

            await this.client.hSet("jobs", job.id, JSON.stringify(job));
            await this.client.zAdd("delayed_jobs", {
                score: Date.now() + delayMs,
                value: job.id
            });
            await this.client.hIncrBy("stats", "delayed", 1);

            return job;
        }

        job.status = "failed";
        job.failedAt = new Date().toISOString();

        await this.client.hSet("jobs", job.id, JSON.stringify(job));
        await this.client.zAdd("dlq_jobs", {
            score: Date.now(),
            value: job.id
        });
        await this.client.hIncrBy("stats", "failed", 1);
        await this.client.hIncrBy("stats", "dlq", 1);

        return job;
    }

    async getJob(jobId) {
        await this.ready;
        const data = await this.client.hGet("jobs", jobId);
        return data ? JSON.parse(data) : null;
    }

    async getAllJobs() {
        await this.ready;
        const jobs = await this.client.hGetAll("jobs");
        return Object.values(jobs).map(data => JSON.parse(data));
    }

    async getDLQJobs() {
        await this.ready;
        const ids = await this.client.zRange("dlq_jobs", 0, -1);
        const jobs = [];
        for (const id of ids) {
            const job = await this.getJob(id);
            if (job) jobs.push(job);
        }
        return jobs;
    }

    async getStalledJobs(timeout = 10000) {
        await this.ready;
        const threshold = Date.now() - timeout;
        const stalledIds = await this.client.zRangeByScore("processing_jobs", 0, threshold);
        const stalledJobs = [];

        for (const jobId of stalledIds) {
            const job = await this.getJob(jobId);
            if (!job) continue;

            job.status = "queued";
            job.startedAt = null;

            await this.client.hDel("job_locks", job.id);
            await this.client.hSet("jobs", job.id, JSON.stringify(job));
            await this.client.zRem("processing_jobs", job.id);
            await this.client.zAdd("queued_jobs", {
                score: job.priority,
                value: job.id
            });

            await this.client.hIncrBy("stats", "processing", -1);
            await this.client.hIncrBy("stats", "queued", 1);

            stalledJobs.push(job);
        }

        return stalledJobs;
    }

    async getStats() {
        await this.ready;
        const stats = await this.client.hGetAll("stats");
        return {
            total: Number(stats.total || 0),
            queued: Number(stats.queued || 0),
            delayed: Number(stats.delayed || 0),
            processing: Number(stats.processing || 0),
            completed: Number(stats.completed || 0),
            failed: Number(stats.failed || 0),
            dlq: Number(stats.dlq || 0)
        };
    }
}
