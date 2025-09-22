// config/loggers/companyHouseLogger.js
const winston = require('winston');
const path = require('path');

const logDir = path.join(__dirname, '../../logs/company_house');

// Ensure log directory exists
const fs = require('fs');
if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
}

const companyHouseLogger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.errors({ stack: true }),
        winston.format.json()
    ),
    defaultMeta: { service: 'company-house' },
    transports: [
        // Write all logs with level `error` and below to `error.log`
        new winston.transports.File({
            filename: path.join(logDir, 'error-company-house.log'),
            level: 'error',
            format: winston.format.combine(
                winston.format.timestamp(),
                winston.format.errors({ stack: true }),
                winston.format.json()
            )
        }),
        // Write all logs with level `info` and below to `company-house.log`
        new winston.transports.File({
            filename: path.join(logDir, 'company-house.log'),
            format: winston.format.combine(
                winston.format.timestamp(),
                winston.format.json()
            )
        }),
        // Write all logs with level `debug` and below to `company-house-debug.log`
        new winston.transports.File({
            filename: path.join(logDir, 'company-house-debug.log'),
            level: 'debug',
            format: winston.format.combine(
                winston.format.timestamp(),
                winston.format.errors({ stack: true }),
                winston.format.json()
            )
        })
    ]
});

// If we're not in production then log to the `console` with the format:
// `${info.level}: ${info.message} JSON.stringify({ ...rest }) `
if (process.env.NODE_ENV !== 'production') {
    companyHouseLogger.add(new winston.transports.Console({
        format: winston.format.combine(
            winston.format.colorize(),
            winston.format.simple()
        )
    }));
}

module.exports = companyHouseLogger;