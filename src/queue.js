import { createClient } from "redis";
import { Job } from "./job.js";

export class JobQueue {

    constructor() {

        this.client = createClient();

        this.ready = this.connect();
    }

    async connect() {

        await this.client.connect();
    }

    async addJob(jobData) {

        await this.ready;

        const job =
            new Job(jobData);

        await this.client.hSet(
            "jobs",
            job.id,
            JSON.stringify(job)
        );

        await this.client.zAdd(
            "queued_jobs",
            {
                score: job.priority,
                value: job.id
            }
        );

        return job;
    }

    async getNextJob() {

        await this.ready;

        const result =
            await this.client.zPopMax(
                "queued_jobs"
            );

        if (!result) {
            return null;
        }

        const job =
            await this.getJob(
                result.value
            );

        if (!job) {
            return null;
        }

        if (job.status !== "queued") {
            return null;
        }

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

        if (
            job.attempts <
            job.maxAttempts
        ) {

            job.status = "queued";
            job.startedAt = null;

            await this.client.hSet(
                "jobs",
                job.id,
                JSON.stringify(job)
            );

            await this.client.zAdd(
                "queued_jobs",
                {
                    score: job.priority,
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
            await this.client.hGetAll(
                "jobs"
            );

        return Object.values(jobs)
            .map(
                job => JSON.parse(job)
            );
    }

    async recoverJob(
        jobId,
        timeout
    ) {

        await this.ready;

        const now =
            Date.now();

        const result =
            await this.client.eval(
                `
                local data =
                    redis.call(
                        "HGET",
                        KEYS[1],
                        ARGV[1]
                    )

                if not data then
                    return 0
                end

                local job =
                    cjson.decode(data)

                if job.status ~= "processing" then
                    return 0
                end

                if not job.startedAt then
                    return 0
                end

                local startedAt =
                    string.sub(
                        job.startedAt,
                        1,
                        19
                    )

                local year =
                    tonumber(
                        string.sub(
                            startedAt,
                            1,
                            4
                        )
                    )

                local month =
                    tonumber(
                        string.sub(
                            startedAt,
                            6,
                            7
                        )
                    )

                local day =
                    tonumber(
                        string.sub(
                            startedAt,
                            9,
                            10
                        )
                    )

                local hour =
                    tonumber(
                        string.sub(
                            startedAt,
                            12,
                            13
                        )
                    )

                local minute =
                    tonumber(
                        string.sub(
                            startedAt,
                            15,
                            16
                        )
                    )

                local second =
                    tonumber(
                        string.sub(
                            startedAt,
                            18,
                            19
                        )
                    )

                local started =
                    os.time({
                        year = year,
                        month = month,
                        day = day,
                        hour = hour,
                        min = minute,
                        sec = second
                    })

                if
                    (
                        ARGV[2] -
                        (
                            started * 1000
                        )
                    )
                    <
                    tonumber(ARGV[3])
                then

                    return 0
                end

                job.status = "queued";
                job.startedAt = cjson.null;

                redis.call(
                    "HSET",
                    KEYS[1],
                    ARGV[1],
                    cjson.encode(job)
                );

                redis.call(
                    "ZADD",
                    KEYS[2],
                    job.priority,
                    ARGV[1]
                );

                return 1
                `,
                {
                    keys: [
                        "jobs",
                        "queued_jobs"
                    ],
                    arguments: [
                        jobId,
                        String(now),
                        String(timeout)
                    ]
                }
            );

        return result === 1;
    }

    async getStalledJobs(
        timeout = 10000
    ) {

        await this.ready;

        const jobs =
            await this.client.hGetAll(
                "jobs"
            );

        const stalledJobs = [];

        for (
            const data of Object.values(jobs)
        ) {

            const job =
                JSON.parse(data);

            if (
                job.status !==
                "processing"
            ) {
                continue;
            }

            if (!job.startedAt) {
                continue;
            }

            const recovered =
                await this.recoverJob(
                    job.id,
                    timeout
                );

            if (recovered) {

                const recoveredJob =
                    await this.getJob(
                        job.id
                    );

                if (recoveredJob) {
                    stalledJobs.push(
                        recoveredJob
                    );
                }
            }
        }

        return stalledJobs;
    }

    async getStats() {

        await this.ready;

        const jobs =
            await this.client.hGetAll(
                "jobs"
            );

        const stats = {
            total: 0,
            queued: 0,
            processing: 0,
            completed: 0,
            failed: 0
        };

        for (
            const data of Object.values(jobs)
        ) {

            const job =
                JSON.parse(data);

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