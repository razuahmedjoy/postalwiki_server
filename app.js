const express = require('express');
const cors = require('cors');

const authRoutes = require('./routes/authRoutes');
const ssUrlRoutes = require('./routes/ssurlRoutes');
const { verifyToken } = require('./middlewares/authmiddleware');
const { authorizeRoles } = require('./middlewares/rolemiddleware');
const { getCollectionStats } = require('./controllers/collectionController');

const app = express();

// Body parser - Move this before security middleware
app.use(express.json({ limit: '50mb' })); // Increased limit for large requests

// CORS configuration - Move this before other middleware
const corsOptions = {
    origin: ['http://localhost:5173', 'https://admin.postalwiki.co.uk', 'https://api.postalwiki.co.uk'],
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With'],
    exposedHeaders: ['Content-Range', 'X-Content-Range'],
    credentials: true,
    preflightContinue: false,
    optionsSuccessStatus: 204
};

// Apply CORS middleware
app.use(cors(corsOptions));

// Handle preflight requests explicitly
app.options('*', cors(corsOptions));

// Add request logging
app.use((req, res, next) => {
    console.log(`${new Date().toISOString()} - ${req.method} ${req.url}`);
    next();
});

// Add timeout handling
app.use((req, res, next) => {
    res.setTimeout(300000, () => { // 5 minutes timeout
        console.error('Request timeout');
        res.status(504).send('Request timeout');
    });
    next();
});

// api routes
app.use('/api', authRoutes);
app.use('/api/stats', getCollectionStats);
app.use('/api/ss-url', verifyToken, authorizeRoles('admin'), ssUrlRoutes);

// ✅ admin-only route example
app.get('/admin', verifyToken, authorizeRoles('admin'), (req, res) => {
    res.json({ message: 'Welcome admin!' });
});

// Error handling middleware
app.use((err, req, res, next) => {
    console.error('Error details:', {
        message: err.message,
        stack: err.stack,
        url: req.url,
        method: req.method,
        body: req.body
    });
    
    if (err.message.includes('CORS')) {
        return res.status(403).json({
            status: 'error',
            message: err.message
        });
    }
    
    res.status(500).json({
        status: 'error',
        message: 'Something went wrong!',
        error: process.env.NODE_ENV === 'development' ? err.message : undefined
    });
});

module.exports = app;
