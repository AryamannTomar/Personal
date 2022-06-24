const OS = require('os');
const { createLogger, format, transports } = require('winston');
const { combine, timestamp, label, printf, json } = format;

export const loggerModule = (environment: any) => {
    return createLogger({
        level: 'debug',
        format: combine(
            format.colorize(),
            timestamp({ format: "MMM-DD-YYYY HH:mm:ss" }),
            json(),
        ),

        defaultMeta: { service: 'user-service', hostname: OS.hostname(), timezone: Intl.DateTimeFormat().resolvedOptions().timeZone },
        transports: [
            //
            // - Write all logs with importance level of `error` or less to `error.log`
            // - Write all logs with importance level of `info` or less to `combined.log`
            //
            environment == 'dev' ? new transports.Console() : new transports.File({ filename: 'error.log', level: 'error' }),
            new transports.File({ filename: 'combined.log' }),
        ],
    });
}