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
        this.priority = priority;

        this.status = delay > 0 ? "delayed" : "queued";

        this.attempts = 0;
        this.maxAttempts = maxAttempts;

        this.createdAt = new Date();
        this.startedAt = null;
        this.completedAt = null;
        this.failedAt = null;
        this.processAt = delay > 0 ? new Date(Date.now() + delay) : new Date();

        this.result = null;
        this.error = null;
    }
}
