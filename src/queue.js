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

        const job = new Job(jobData);

        await this.client.hSet(
            "jobs",
            job.id,
            JSON.stringify(job)
        );

        return job;
    }

    async getNextJob() {
        await this.ready;

        const jobs =
            await this.client.hGetAll("jobs");

        const queuedJobs =
            Object.values(jobs)
                .map(job => JSON.parse(job))
                .filter(
                    job => job.status === "queued"
                )
                .sort(
                    (a, b) =>
                        b.priority - a.priority
                );

        if (queuedJobs.length === 0) {
            return null;
        }

        const job = queuedJobs[0];

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

    async completeJob(jobId, result = null) {
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

    async failJob(jobId, error) {
        await this.ready;

        const job =
            await this.getJob(jobId);

        if (!job) {
            return null;
        }

        job.error = error;

        if (job.attempts < job.maxAttempts) {

            job.status = "queued";
            job.startedAt = null;

        } else {

            job.status = "failed";
            job.failedAt = new Date();
        }

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
            await this.client.hGetAll("jobs");

        return Object.values(jobs)
            .map(job => JSON.parse(job));
    }

    async getStats() {
        await this.ready;

        const jobs =
            await this.client.hGetAll("jobs");

        const stats = {
            total: 0,
            queued: 0,
            processing: 0,
            completed: 0,
            failed: 0
        };

        for (const data of Object.values(jobs)) {

            const job = JSON.parse(data);

            stats.total++;

            if (
                stats[job.status] !== undefined
            ) {
                stats[job.status]++;
            }
        }

        return stats;
    }
}