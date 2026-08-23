import http from "node:http";
import { JobQueue } from "./queue.js"; 

const queue = new JobQueue();
const MAX_BODY_SIZE = 1e6;

const server = http.createServer(async (req, res) => {
    const { method, url } = req;

    try {
        if (method === "GET" && url === "/") {
            return sendJSON(res, 200, { message: "Distributed Job Queue", status: "running" });
        }

        if (method === "GET" && url === "/jobs") {
            return sendJSON(res, 200, Array.from(queue.jobs.values()));
        }

        if (method === "GET" && url === "/stats") {
            return sendJSON(res, 200, queue.getStats());
        }

        if (method === "POST" && url === "/jobs") {
            const body = await readRequestBody(req);
            const jobData = JSON.parse(body);
            const job = queue.addJob(jobData);
            return sendJSON(res, 201, job);
        }

        if (method === "GET" && url === "/jobs/next") {
            const job = queue.getNextJob();
            if (!job) return sendJSON(res, 204, null);
            return sendJSON(res, 200, job);
        }

        // إكمال مهمة عبر HTTP
        if (method === "POST" && url.startsWith("/jobs/") && url.endsWith("/complete")) {
            const jobId = url.split("/")[2];
            const body = await readRequestBody(req);
            const { result } = body ? JSON.parse(body) : {};
            const job = queue.completeJob(jobId, result);
            
            if (!job) return sendJSON(res, 404, { error: "Job not found" });
            return sendJSON(res, 200, job);
        }

        sendJSON(res, 404, { error: "Route not found" });
    } catch (error) {
        sendJSON(res, 400, { error: error.message || "Invalid Request" });
    }
});

function readRequestBody(request) {
    return new Promise((resolve, reject) => {
        let body = "";
        let size = 0;

        request.on("data", chunk => {
            size += chunk.length;
            if (size > MAX_BODY_SIZE) {
                request.destroy();
                reject(new Error("Payload too large"));
                return;
            }
            body += chunk;
        });

        request.on("end", () => resolve(body));
        request.on("error", err => reject(err));
    });
}

function sendJSON(response, statusCode, data) {
    response.writeHead(statusCode, { "Content-Type": "application/json" });
    if (data === null) {
        response.end();
        return;
    }
    response.end(JSON.stringify(data));
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));
