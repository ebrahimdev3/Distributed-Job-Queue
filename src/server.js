import http from "http";
import crypto from "node:crypto";
import { JobQueue } from "./queue.js";
import { Logger } from "./logger.js";

const API_KEY = process.env.API_KEY;
if (!API_KEY) {
    Logger.error("CRITICAL: API_KEY environment variable is missing.");
    process.exit(1);
}

const queue = new JobQueue();

const RATE_LIMIT_WINDOW_SEC = 60;
const MAX_REQUESTS_PER_WINDOW = 100;

async function applyRateLimit(ip) {
    try {
        const key = `rate_limit:${ip}`;
        const requests = await queue.client.incr(key);
        if (requests === 1) {
            await queue.client.expire(key, RATE_LIMIT_WINDOW_SEC);
        }
        return requests <= MAX_REQUESTS_PER_WINDOW;
    } catch (error) {
        Logger.error("Rate limiter failure via Redis", { error: error.message });
        return false;
    }
}

function authenticate(request) {
    const authHeader = request.headers["authorization"];
    if (!authHeader) return false;

    const token = authHeader.replace("Bearer ", "").trim();
    const keyBuffer = Buffer.from(API_KEY);
    const tokenBuffer = Buffer.from(token);

    if (keyBuffer.length !== tokenBuffer.length) {
        return false;
    }

    return crypto.timingSafeEqual(keyBuffer, tokenBuffer);
}

function validateJobData(data) {
    if (!data || typeof data !== "object") return "Payload must be an object";
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
    <title>Job Queue Dashboard</title>
    <style>
        body { font-family: monospace; background: #121212; color: #e0e0e0; margin: 20px; }
        h1 { color: #00ffcc; }
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; margin-bottom: 20px; }
        .stat-card { background: #1e1e1e; padding: 15px; border-radius: 5px; text-align: center; border: 1px solid #333; }
        .stat-card h3 { margin: 0; font-size: 14px; color: #888; }
        .stat-card p { margin: 5px 0 0 0; font-size: 22px; font-weight: bold; color: #00ffcc; }
        table { width: 100%; border-collapse: collapse; background: #1e1e1e; margin-bottom: 30px; }
        th, td { border: 1px solid #333; padding: 10px; text-align: left; }
        th { background: #252525; color: #888; }
        .status-queued { color: #ffbb00; }
        .status-delayed { color: #cc66ff; }
        .status-processing { color: #0099ff; }
        .status-completed { color: #00ff66; }
        .status-failed { color: #ff3333; }
        #auth-section { margin-bottom: 20px; }
        input { background: #252525; border: 1px solid #444; color: #fff; padding: 8px; }
        button { background: #00ffcc; color: #121212; border: none; padding: 8px 12px; cursor: pointer; font-weight: bold; }
    </style>
</head>
<body>
    <h1>Job Queue Dashboard</h1>
    <div id="auth-section">
        <input type="password" id="api-key" placeholder="Enter API Key">
        <button onclick="saveKey()">Connect</button>
    </div>
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

    <h2>Dead Letter Queue (DLQ)</h2>
    <table>
        <thead>
            <tr>
                <th>ID</th>
                <th>Type</th>
                <th>Error</th>
                <th>Attempts</th>
                <th>Failed At</th>
            </tr>
        </thead>
        <tbody id="dlq-table"></tbody>
    </table>

    <script>
        function saveKey() {
            localStorage.setItem('apiKey', document.getElementById('api-key').value);
            loadData();
        }

        async function loadData() {
            const apiKey = localStorage.getItem('apiKey');
            if(!apiKey) return;
            document.getElementById('api-key').value = apiKey;

            const headers = { 'Authorization': 'Bearer ' + apiKey };
            try {
                const statsRes = await fetch('/stats', { headers });
                if(statsRes.status === 401) return alert('Unauthorized: Invalid API Key');
                
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

                const jobsRes = await fetch('/jobs', { headers });
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

                const dlqRes = await fetch('/dlq', { headers });
                const dlqJobs = await dlqRes.json();
                const dlqBody = document.getElementById('dlq-table');
                dlqBody.innerHTML = '';
                dlqJobs.forEach(job => {
                    dlqBody.innerHTML += \`
                        <tr>
                            <td>\${job.id}</td>
                            <td>\${job.type}</td>
                            <td class="status-failed">\${job.error || 'N/A'}</td>
                            <td>\${job.attempts}/\${job.maxAttempts}</td>
                            <td>\${new Date(job.failedAt).toLocaleString()}</td>
                        </tr>
                    \`;
                });

            } catch (err) {
                console.error("Dashboard error:", err);
            }
        }

        if(localStorage.getItem('apiKey')) loadData();
        setInterval(loadData, 3000);
    </script>
</body>
</html>
`;

const server = http.createServer(async (request, response) => {
    const clientIp = request.socket.remoteAddress;

    const allowed = await applyRateLimit(clientIp);
    if (!allowed) {
        Logger.warn("Rate limit exceeded or limiter service unavailable", { ip: clientIp });
        sendJSON(response, 429, { error: "Too many requests or service degraded" });
        return;
    }

    if (request.method === "GET" && request.url === "/") {
        sendJSON(response, 200, { message: "Distributed Job Queue Engine", status: "running" });
        return;
    }

    if (request.method === "GET" && request.url === "/dashboard") {
        response.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
        response.end(dashboardHTML);
        return;
    }

    if (!authenticate(request)) {
        Logger.warn("Unauthorized API access attempt", { ip: clientIp });
        sendJSON(response, 401, { error: "Unauthorized access" });
        return;
    }

    if (request.method === "GET" && request.url === "/jobs") {
        const jobs = await queue.getAllJobs();
        sendJSON(response, 200, jobs);
        return;
    }

    if (request.method === "GET" && request.url === "/dlq") {
        const dlqJobs = await queue.getDLQJobs();
        sendJSON(response, 200, dlqJobs);
        return;
    }

    if (request.method === "GET" && request.url === "/stats") {
        const stats = await queue.getStats();
        sendJSON(response, 200, stats);
        return;
    }

    if (request.method === "POST" && request.url === "/jobs") {
        try {
            const body = await readRequestBody(request);
            const jobData = JSON.parse(body);

            const validationError = validateJobData(jobData);
            if (validationError) {
                sendJSON(response, 400, { error: validationError });
                return;
            }

            const job = await queue.addJob(jobData);
            Logger.info("Job created", { jobId: job.id, type: job.type });
            sendJSON(response, 201, job);
            return;
        } catch {
            sendJSON(response, 400, { error: "Invalid JSON format" });
            return;
        }
    }

    sendJSON(response, 404, { error: "Route not found" });
});

function readRequestBody(request) {
    return new Promise((resolve, reject) => {
        let body = "";
        request.on("data", chunk => { body += chunk; });
        request.on("end", () => resolve(body));
        request.on("error", error => reject(error));
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
const serverInstance = server.listen(PORT, () => {
    Logger.info(`Server running on port ${PORT}`);
});

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
