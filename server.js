require('dotenv').config();
const app = require('./app');
const connectDB = require('./config/db');
const logger = require('./config/logger');

const PORT = process.env.PORT || 5000;

// Security headers
app.disable('x-powered-by');

const startServer = async () => {
    try {
        await connectDB();

        const server = app.listen(PORT, () => {
            logger.info(`Server running on port ${PORT}`);
        });

        // Set timeout for server
        server.timeout = 30000; // 30 seconds
        server.keepAliveTimeout = 30000; // 30 seconds

        // Handle server errors
        server.on('error', (error) => {
            logger.error(`Server error: ${error}`);
            process.exit(1);
        });

        // Graceful shutdown
        process.on('SIGTERM', () => {
            logger.info('SIGTERM received. Shutting down gracefully');
            server.close(() => {
                logger.info('Process terminated');
                process.exit(0);
            });
        });

        // Global error handlers
        process.on('unhandledRejection', (reason, promise) => {
            logger.error(`Unhandled Rejection: ${reason}`);
            // Don't exit the process, just log the error
        });

        process.on('uncaughtException', (error) => {
            logger.error(`Uncaught Exception: ${error}`);
            // Don't exit the process, just log the error
        });

    } catch (error) {
        logger.error(`Failed to start server: ${error}`);
        process.exit(1);
    }
};

startServer();
