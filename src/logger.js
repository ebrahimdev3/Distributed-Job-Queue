export class Logger {
    static formatLog(level, message, meta = {}) {
        return JSON.stringify({
            timestamp: new Date().toISOString(),
            level,
            message,
            ...meta
        });
    }

    static info(message, meta) {
        console.log(this.formatLog("INFO", message, meta));
    }

    static error(message, meta) {
        console.error(this.formatLog("ERROR", message, meta));
    }

    static warn(message, meta) {
        console.warn(this.formatLog("WARN", message, meta));
    }
}
