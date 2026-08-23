import { Job } from "./job.js";

export class JobQueue {
    constructor() {
        this.jobs = new Map(); // استخدام Map لسرعة البحث O(1)
    }

    addJob(jobData) {
        const job = new Job(jobData);
        this.jobs.set(job.id, job);
        return job;
    }

    getNextJob() {
        const queuedJobs = Array.from(this.jobs.values())
            .filter(job => job.status === "queued")
            .sort((a, b) => b.priority - a.priority);

        if (queuedJobs.length === 0) return null;

        const job = queuedJobs[0];
        job.status = "processing";
        job.startedAt = new Date();
        job.attempts++;

        return job;
    }

    completeJob(jobId, result = null) {
        const job = this.getJob(jobId);
        if (!job) return null;

        job.status = "completed";
        job.result = result;
        job.completedAt = new Date();

        return job;
    }

    failJob(jobId, error) {
        const job = this.getJob(jobId);
        if (!job) return null;

        job.error = error;

        if (job.attempts < job.maxAttempts) {
            job.status = "queued";
            job.startedAt = null;
            return job;
        }

        job.status = "failed";
        job.failedAt = new Date();
        return job;
    }

    getJob(jobId) {
        return this.jobs.get(jobId) || null;
    }

    getStats() {
        const stats = { total: this.jobs.size, queued: 0, processing: 0, completed: 0, failed: 0 };
        for (const job of this.jobs.values()) {
            if (stats[job.status] !== undefined) {
                stats[job.status]++;
            }
        }
        return stats;
    }
}
