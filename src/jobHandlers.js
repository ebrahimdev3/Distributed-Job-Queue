export class JobHandlers {
    constructor() {
        this.handlers = new Map();

        // تسجيل المعالجات الافتراضية
        this.registerHandler("email", this.email.bind(this));
        this.registerHandler("failing-task", this.failingTask.bind(this));
    }

    registerHandler(type, handler) {
        if (typeof handler !== "function") {
            throw new Error(`Handler for type ${type} must be a function`);
        }
        this.handlers.set(type, handler);
    }

    async execute(job) {
        const handler = this.handlers.get(job.type);

        if (!handler) {
            throw new Error(`Unknown job type: ${job.type}`);
        }

        return handler(job);
    }

    async email(job) {
        await this.sleep(2000);
        return {
            success: true,
            jobId: job.id
        };
    }

    async failingTask(job) {
        await this.sleep(2000);
        throw new Error("Job execution failed");
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}
