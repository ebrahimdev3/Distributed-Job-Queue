import http from "http";

import { JobQueue } from "./queue.js";

const queue =
    new JobQueue();

const server =
    http.createServer(
        async (
            request,
            response
        ) => {

            if (
                request.method === "GET" &&
                request.url === "/"
            ) {

                sendJSON(
                    response,
                    200,
                    {
                        message:
                            "Distributed Job Queue",
                        status:
                            "running"
                    }
                );

                return;
            }

            if (
                request.method === "GET" &&
                request.url === "/jobs"
            ) {

                const jobs =
                    await queue.getAllJobs();

                sendJSON(
                    response,
                    200,
                    jobs
                );

                return;
            }

            if (
                request.method === "GET" &&
                request.url === "/stats"
            ) {

                const stats =
                    await queue.getStats();

                sendJSON(
                    response,
                    200,
                    stats
                );

                return;
            }

            if (
                request.method === "POST" &&
                request.url === "/jobs"
            ) {

                try {

                    const body =
                        await readRequestBody(
                            request
                        );

                    const jobData =
                        JSON.parse(body);

                    const job =
                        await queue.addJob(
                            jobData
                        );

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
                            error:
                                "Invalid job data"
                        }
                    );

                    return;
                }
            }

            sendJSON(
                response,
                404,
                {
                    error:
                        "Route not found"
                }
            );
        }
    );

function readRequestBody(
    request
) {

    return new Promise(
        (
            resolve,
            reject
        ) => {

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
            "Content-Type":
                "application/json"
        }
    );

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
    }
);