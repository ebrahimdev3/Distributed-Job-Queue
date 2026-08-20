import { Job } from "./job.js";

export class JobQueue {
    constructor() {
        this.jobs = [];
    }

    addJob(jobData) {
        const job = new Job(jobData);

        this.jobs.push(job);

        return job;
    }

    getNextJob() {
        const queuedJobs = this.jobs.filter(
            job => job.status === "queued"
        );

        if (queuedJobs.length === 0) {
            return null;
        }

        queuedJobs.sort(
            (a, b) => b.priority - a.priority
        );

        const job = queuedJobs[0];

        job.status = "processing";
        job.startedAt = new Date();
        job.attempts++;

        return job;
    }

    completeJob(jobId, result = null) {
        const job = this.getJob(jobId);

        if (!job) {
            return null;
        }

        job.status = "completed";
        job.result = result;
        job.completedAt = new Date();

        return job;
    }

    failJob(jobId, error) {
        const job = this.getJob(jobId);

        if (!job) {
            return null;
        }

        job.status = "failed";
        job.error = error;
        job.failedAt = new Date();

        return job;
    }

    getJob(jobId) {
        return this.jobs.find(
            job => job.id === jobId
        );
    }

    getStats() {
        return {
            total: this.jobs.length,

            queued: this.jobs.filter(
                job => job.status === "queued"
            ).length,

            processing: this.jobs.filter(
                job => job.status === "processing"
            ).length,

            completed: this.jobs.filter(
                job => job.status === "completed"
            ).length,

            failed: this.jobs.filter(
                job => job.status === "failed"
            ).length
        };
    }
}