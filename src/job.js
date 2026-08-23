import crypto from "node:crypto";

export class Job {
    constructor({ type, payload = {}, priority = 0, maxAttempts = 3 }) {
        if (!type) throw new Error("Job type is required");

        this.id = crypto.randomUUID();
        this.type = String(type);
        this.payload = payload;
        this.priority = Number(priority) || 0;

        this.status = "queued";
        this.attempts = 0;
        this.maxAttempts = Number(maxAttempts) || 3;

        this.createdAt = new Date();
        this.startedAt = null;
        this.completedAt = null;
        this.failedAt = null;

        this.result = null;
        this.error = null;
    }
}
