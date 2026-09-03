import http from "http";
import { JobQueue } from "./queue.js";
import { Logger } from "./logger.js";

const queue = new JobQueue();

const API_KEY = process.env.API_KEY || "secret-api-key";
const RATE_LIMIT_WINDOW_MS = 60000;
const MAX_REQUESTS_PER_WINDOW = 100;
const requestCounts = new Map();

function applyRateLimit(ip) {
    const now = Date.now();
    const windowData = requestCounts.get(ip) || { count: 0, startTime: now };

    if (now - windowData.startTime > RATE_LIMIT_WINDOW_MS) {
        windowData.count = 1;
        windowData.startTime = now;
    } else {
        windowData.count++;
    }

    requestCounts.set(ip, windowData);
    return windowData.count <= MAX_REQUESTS_PER_WINDOW;
}

function authenticate(request) {
    const authHeader = request.headers["authorization"];
    if (!authHeader) return false;
    const token = authHeader.replace("Bearer ", "").trim();
    return token === API_KEY;
}

function validateJobData(data) {
    if (!data || typeof data !== "object") {
        return "Payload must be an object";
    }

    if (!data.type || typeof data.type !== "string" || data.type.trim() === "") {
        return "Field 'type' is required and must be a non-empty string";
    }

    if (data.priority !== undefined && typeof data.priority !== "number") {
        return "Field 'priority' must be a number";
    }

    if (data.delay !== undefined && (typeof data.delay !== "number" || data.delay < 0)) {
        return "Field 'delay' must be a positive number in milliseconds";
    }

    if (data.payload !== undefined && (typeof data.payload !== "object" || data.payload === null)) {
        return "Field 'payload' must be an object";
    }

    return null;
}

const dashboardHTML = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Job Queue Dashboard</title>
    <style>
        body { font-family: monospace, sans-serif; background: #121212; color: #e0e0e0; margin: 20px; }
        h1 { color: #00ffcc; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; margin-bottom: 20px; }
        .stat-card { background: #1e1e1e; padding: 15px; border-radius: 5px; text-align: center; border: 1px solid #333; }
        .stat-card h3 { margin: 0; font-size: 14px; color: #888; }
        .stat-card p { margin: 5px 0 0 0; font-size: 22px; font-weight: bold; color: #00ffcc; }
        table { width: 100%; border-collapse: collapse; background: #1e1e1e; }
        th, td { border: 1px solid #333; padding: 10px; text-align: left; }
        th { background: #252525; color: #888; }
        .status-queued { color: #ffbb00; }
        .status-delayed { color: #cc66ff; }
        .status-processing { color: #0099ff; }
        .status-completed { color: #00ff66; }
        .status-failed { color: #ff3333; }
    </style>
</head>
<body>
    <h1>Job Queue Dashboard</h1>
    <div class="stats-grid" id="stats"></div>
    <h2>Jobs List</h2>
    <table>
        <thead>
            <tr>
                <th>ID</th>
                <th>Type</th>
                <th>Status</th>
                <th>Priority</th>
                <th>Attempts</th>
                <th>Process At</th>
            </tr>
        </thead>
        <tbody id="jobs-table"></tbody>
    </table>

    <script>
        async function loadData() {
            try {
                const statsRes = await fetch('/stats');
                const stats = await statsRes.json();
                const statsContainer = document.getElementById('stats');
                statsContainer.innerHTML = '';
                for (const [key, val] of Object.entries(stats)) {
                    statsContainer.innerHTML += \`
                        <div class="stat-card">
                            <h3>\${key.toUpperCase()}</h3>
                            <p>\${val}</p>
                        </div>
                    \`;
                }

                const jobsRes = await fetch('/jobs');
                const jobs = await jobsRes.json();
                const tableBody = document.getElementById('jobs-table');
                tableBody.innerHTML = '';
                jobs.forEach(job => {
                    tableBody.innerHTML += \`
                        <tr>
                            <td>\${job.id}</td>
                            <td>\${job.type}</td>
                            <td class="status-\${job.status}">\${job.status}</td>
                            <td>\${job.priority}</td>
                            <td>\${job.attempts}/\${job.maxAttempts}</td>
                            <td>\${new Date(job.processAt).toLocaleString()}</td>
                        </tr>
                    \`;
                });
            } catch (err) {
                console.error("Error fetching dashboard data:", err);
            }
        }

        loadData();
        setInterval(loadData, 3000);
    </script>
</body>
</html>
`;

const server = http.createServer(
    async (request, response) => {
        const clientIp = request.socket.remoteAddress;

        if (!applyRateLimit(clientIp)) {
            Logger.warn("Rate limit exceeded", { ip: clientIp });
            sendJSON(response, 429, { error: "Too many requests" });
            return;
        }

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
            request.url === "/dashboard"
        ) {
            response.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
            response.end(dashboardHTML);
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
            if (!authenticate(request)) {
                Logger.warn("Unauthorized API access attempt", { ip: clientIp });
                sendJSON(response, 401, { error: "Unauthorized access" });
                return;
            }

            try {

                const body =
                    await readRequestBody(request);

                const jobData =
                    JSON.parse(body);

                const validationError = validateJobData(jobData);
                if (validationError) {
                    sendJSON(
                        response,
                        400,
                        {
                            error: validationError
                        }
                    );
                    return;
                }

                const job =
                    await queue.addJob(jobData);

                Logger.info("Job created", { jobId: job.id, type: job.type });

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
                        error: "Invalid JSON format"
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
                await queue.getNextJob();

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

const serverInstance = server.listen(
    PORT,
    () => {
        Logger.info(`Server running on port ${PORT}`);
    }
);

const shutdown = async () => {
    Logger.info("Stopping server and closing connections...");
    serverInstance.close(async () => {
        await queue.close();
        Logger.info("Server closed safely.");
        process.exit(0);
    });
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
