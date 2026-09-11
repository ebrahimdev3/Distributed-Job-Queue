import crypto from "node:crypto";

export class Job {
    constructor({
        type,
        payload = {},
        priority = 0,
        delay = 0,
        maxAttempts = 3
    }) {
        this.id = crypto.randomUUID();
        this.type = type;
        this.payload = payload;
        this.priority = Number(priority) || 0;

        this.status = delay > 0 ? "delayed" : "queued";

        this.attempts = 0;
        this.maxAttempts = Number(maxAttempts) || 3;

        this.createdAt = new Date().toISOString();
        this.startedAt = null;
        this.completedAt = null;
        this.failedAt = null;
        
        const now = Date.now();
        this.processAt = delay > 0 ? new Date(now + delay).toISOString() : new Date(now).toISOString();

        this.result = null;
        this.error = null;
    }
}
