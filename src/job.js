import crypto from "node:crypto";

export class Job {

    constructor({
        type,
        payload = {},
        priority = 0
    }) {

        this.id = crypto.randomUUID();

        this.type = type;
        this.payload = payload;
        this.priority = priority;

        this.status = "queued";

        this.attempts = 0;
        this.maxAttempts = 3;

        this.createdAt = new Date();
        this.startedAt = null;
        this.completedAt = null;
        this.failedAt = null;

        this.result = null;
        this.error = null;
    }
}