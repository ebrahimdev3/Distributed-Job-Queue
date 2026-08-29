import { JobQueue } from "./queue.js";

export class Worker {

    constructor(queue) {

        this.queue = queue;
        this.running = false;
    }

    start() {

        if (this.running) {
            return;
        }

        this.running = true;

        console.log(
            "Worker started"
        );

        this.process();
    }

    stop() {

        this.running = false;

        console.log(
            "Worker stopped"
        );
    }

    async process() {

        while (this.running) {

            await this.recoverStalledJobs();

            const job =
                await this.queue.getNextJob();

            if (!job) {

                await this.sleep(1000);

                continue;
            }

            await this.execute(job);
        }
    }

    async recoverStalledJobs() {

        const jobs =
            await this.queue.getStalledJobs(
                10000
            );

        for (const job of jobs) {

            console.log(
                `Recovered stalled job: ${job.id}`
            );
        }
    }

    async execute(job) {

        console.log(
            `Processing job: ${job.id}`
        );

        try {

            const result =
                await this.runJob(job);

            await this.queue.completeJob(
                job.id,
                result
            );

            console.log(
                `Job completed: ${job.id}`
            );

        } catch (error) {

            await this.queue.failJob(
                job.id,
                error.message
            );

            console.log(
                `Job failed: ${job.id}`
            );
        }
    }

    async runJob(job) {

        console.log(
            `Running ${job.type}...`
        );

        await this.sleep(2000);

        if (
            job.type ===
            "failing-task"
        ) {

            throw new Error(
                "Job execution failed"
            );
        }

        return {
            success: true,
            jobId: job.id
        };
    }

    sleep(ms) {

        return new Promise(
            resolve => {
                setTimeout(
                    resolve,
                    ms
                );
            }
        );
    }
}

const queue =
    new JobQueue();

const worker =
    new Worker(queue);

worker.start();;