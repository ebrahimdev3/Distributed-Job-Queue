import crypto from "node:crypto";
import { JobQueue } from "./queue.js";
import { JobHandlers } from "./jobHandlers.js";
import { Logger } from "./logger.js";

export class Worker {
    constructor(queue) {
        this.queue = queue;
        this.handlers = new JobHandlers();
        this.id = `worker-${crypto.randomUUID()}`;

        this.running = false;
        this.currentJob = null;
        this.heartbeatTimer = null;
        this.stalledCheckTimer = null;
    }

    start() {
        if (this.running) return;

        this.running = true;
        Logger.info(`Worker started`, { workerId: this.id });

        // تشغيل فاحص المهام المتوقفة كل 30 ثانية بشكل منفصل
        this.stalledCheckTimer = setInterval(async () => {
            if (!this.running) return;
            try {
                await this.queue.getStalledJobs();
            } catch (err) {
                Logger.error("Stalled jobs check error", { error: err.message });
            }
        }, 30000);

        this.process();
    }

    async process() {
        while (this.running) {
            try {
                const job = await this.queue.getNextJob();

                if (!job) {
                    await this.sleep(1000);
                    continue;
                }

                this.currentJob = job;
                Logger.info(`Worker claimed job`, { workerId: this.id, jobId: job.id });

                await this.execute(job);
                this.currentJob = null;
            } catch (error) {
                Logger.error("Worker processing error", { error: error.message });
                await this.sleep(2000); // إتاحة وقت لإعادة الاتصال في حال انقطاع الشبكة
            }
        }
    }

    startHeartbeat(job) {
        this.stopHeartbeat();
        this.heartbeatTimer = setInterval(async () => {
            try {
                const alive = await this.queue.heartbeat(job.id);
                if (!alive) this.stopHeartbeat();
            } catch (error) {
                Logger.error(`Heartbeat error`, { jobId: job.id, error: error.message });
            }
        }, 3000);
    }

    stopHeartbeat() {
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }
    }

    async execute(job) {
        Logger.info(`Processing job`, { jobId: job.id });
        this.startHeartbeat(job);

        try {
            const result = await this.handlers.execute(job);
            this.stopHeartbeat();
            await this.queue.completeJob(job.id, result);
            Logger.info(`Job completed`, { jobId: job.id });
        } catch (error) {
            this.stopHeartbeat();
            await this.queue.failJob(job.id, error.message);
            Logger.error(`Job failed`, { jobId: job.id, error: error.message });
        }
    }

    async shutdown() {
        if (!this.running) return;

        Logger.info(`Stopping worker graceful shutdown...`, { workerId: this.id });
        this.running = false;

        if (this.stalledCheckTimer) {
            clearInterval(this.stalledCheckTimer);
        }

        // انتظار إنهاء المهمة الجارية
        while (this.currentJob) {
            await this.sleep(100);
        }

        this.stopHeartbeat();
        await this.queue.close();
        Logger.info(`Worker stopped completely.`, { workerId: this.id });
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

const queue = new JobQueue();
const worker = new Worker(queue);

worker.start();

process.on("SIGINT", () => worker.shutdown());
process.on("SIGTERM", () => worker.shutdown());
