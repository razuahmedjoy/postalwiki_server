const { createLogger, format, transports } = require('winston');
const DailyRotateFile = require('winston-daily-rotate-file');
const path = require('path');
const fs = require('fs');

// Ensure logs directory exists
const logsDir = path.join(process.cwd(), 'logs');
if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir, { recursive: true });
}

const socialScrapeLogger = createLogger({
    level: 'debug',
    format: format.combine(
        format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
        format.printf(({ timestamp, level, message }) => `${timestamp} ${level}: ${message}`)
    ),
    transports: [
        new transports.Console({
            level: 'debug',
            format: format.combine(
                format.colorize(),
                format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
                format.printf(({ timestamp, level, message }) => `[SOCIAL_SCRAPE] ${timestamp} ${level}: ${message}`)
            )
        }),
        new DailyRotateFile({
            filename: 'logs/social_scrape/social_scrape-%DATE%.log',
            datePattern: 'YYYY-MM-DD',
            maxSize: '20m',
            maxFiles: '30d', // Keep logs for 30 days
            dirname: 'logs',
            level: 'debug'
        }),
        new DailyRotateFile({
            filename: 'logs/social_scrape/social_scrape-error-%DATE%.log',
            datePattern: 'YYYY-MM-DD',
            maxSize: '20m',
            maxFiles: '30d',
            dirname: 'logs',
            level: 'error'
        })
    ],
});

module.exports = socialScrapeLogger; 