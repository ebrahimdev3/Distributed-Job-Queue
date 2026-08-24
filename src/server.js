import http from "http";

import { JobQueue } from "./queue.js";

import { Worker } from "./worker.js";

const queue = new JobQueue();

const worker = new Worker(queue);

const server = http.createServer(
    async (request, response) => {

        if (
            request.method === "GET" &&
            request.url === "/"
        ) {

            sendJSON(
                response,
                200,
                {
                    message: "Distributed Job Queue",
                    status: "running"
                }
            );

            return;
        }

        if (
            request.method === "GET" &&
            request.url === "/jobs"
        ) {

            sendJSON(
                response,
                200,
                queue.jobs
            );

            return;
        }

        if (
            request.method === "GET" &&
            request.url === "/stats"
        ) {

            sendJSON(
                response,
                200,
                queue.getStats()
            );

            return;
        }

        if (
            request.method === "POST" &&
            request.url === "/jobs"
        ) {

            try {

                const body =
                    await readRequestBody(request);

                const jobData =
                    JSON.parse(body);

                const job =
                    queue.addJob(jobData);

                sendJSON(
                    response,
                    201,
                    job
                );

                return;

            } catch (error) {

                sendJSON(
                    response,
                    400,
                    {
                        error: "Invalid job data"
                    }
                );

                return;
            }
        }

        if (
            request.method === "GET" &&
            request.url === "/jobs/next"
        ) {

            const job =
                queue.getNextJob();

            if (!job) {

                sendJSON(
                    response,
                    204,
                    null
                );

                return;
            }

            sendJSON(
                response,
                200,
                job
            );

            return;
        }

        if (
            request.method === "POST" &&
            request.url.startsWith("/jobs/") &&
            request.url.endsWith("/complete")
        ) {

            const jobId =
                request.url.split("/")[2];

            try {

                const body =
                    await readRequestBody(request);

                const data =
                    JSON.parse(body);

                const job =
                    queue.completeJob(
                        jobId,
                        data.result ?? null
                    );

                if (!job) {

                    sendJSON(
                        response,
                        404,
                        {
                            error: "Job not found"
                        }
                    );

                    return;
                }

                sendJSON(
                    response,
                    200,
                    job
                );

                return;

            } catch (error) {

                sendJSON(
                    response,
                    400,
                    {
                        error: "Invalid request"
                    }
                );

                return;
            }
        }

        sendJSON(
            response,
            404,
            {
                error: "Route not found"
            }
        );
    }
);

function readRequestBody(request) {

    return new Promise(
        (resolve, reject) => {

            let body = "";

            request.on(
                "data",
                chunk => {
                    body += chunk;
                }
            );

            request.on(
                "end",
                () => {
                    resolve(body);
                }
            );

            request.on(
                "error",
                error => {
                    reject(error);
                }
            );
        }
    );
}

function sendJSON(
    response,
    statusCode,
    data
) {

    response.writeHead(
        statusCode,
        {
            "Content-Type": "application/json"
        }
    );

    if (data === null) {
        response.end();
        return;
    }

    response.end(
        JSON.stringify(data)
    );
}

const PORT = 3000;

server.listen(
    PORT,
    () => {
        console.log(
            `Server running on port ${PORT}`
        );

        worker.start();
    }
);