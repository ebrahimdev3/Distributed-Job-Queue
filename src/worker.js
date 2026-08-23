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

        console.log("Worker started");

        this.process();
    }

    stop() {

        this.running = false;

        console.log("Worker stopped");
    }

    async process() {

        while (this.running) {

            const job = this.queue.getNextJob();

            if (!job) {
                await this.sleep(1000);
                continue;
            }

            await this.execute(job);
        }
    }

    async execute(job) {

        console.log(
            `Processing job: ${job.id}`
        );

        try {

            const result =
                await this.runJob(job);

            this.queue.completeJob(
                job.id,
                result
            );

            console.log(
                `Job completed: ${job.id}`
            );

        } catch (error) {

            this.queue.failJob(
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

        return {
            success: true,
            jobId: job.id
        };
    }

    sleep(ms) {

        return new Promise(resolve => {
            setTimeout(resolve, ms);
        });
    }
}