const express = require('express');
const cors = require('cors');

const authRoutes = require('./routes/authRoutes');
const ssUrlRoutes = require('./routes/ssurlRoutes');

const app = express();

// Body parser - Move this before security middleware
app.use(express.json()); // Limit body size

// CORS configuration - Move this before other middleware
const corsOptions = {
    origin: ['http://localhost:8080', 'http://localhost:8000'],
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With'],
    exposedHeaders: ['Content-Range', 'X-Content-Range'],
    credentials: true,
    preflightContinue: false,
    optionsSuccessStatus: 204
};
app.use(cors(corsOptions));



// api routes
app.use('/api', authRoutes);
app.use('/api/ss-url', ssUrlRoutes);

// Error handling middleware
app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).json({
        status: 'error',
        message: 'Something went wrong!'
    });
});

module.exports = app;
