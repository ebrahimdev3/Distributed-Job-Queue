export class JobHandlers {

    constructor() {
        this.handlers = new Map();

        this.handlers.set(
            "email",
            this.email.bind(this)
        );

        this.handlers.set(
            "failing-task",
            this.failingTask.bind(this)
        );
    }

    async execute(job) {

        const handler =
            this.handlers.get(job.type);

        if (!handler) {
            throw new Error(
                `Unknown job type: ${job.type}`
            );
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

        throw new Error(
            "Job execution failed"
        );
    }

    sleep(ms) {

        return new Promise(
            resolve => setTimeout(resolve, ms)
        );
    }
}
