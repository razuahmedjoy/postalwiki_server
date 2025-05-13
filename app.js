const express = require('express');
const cors = require('cors');

const authRoutes = require('./routes/authRoutes');
const ssUrlRoutes = require('./routes/ssurlRoutes');
const { verifyToken } = require('./middlewares/authmiddleware');
const { authorizeRoles } = require('./middlewares/rolemiddleware');
const { getCollectionStats } = require('./controllers/collectionController');

const app = express();

// Body parser - Move this before security middleware
app.use(express.json()); // Limit body size

// CORS configuration - Move this before other middleware
const corsOptions = {
    origin: function (origin, callback) {
        const allowedOrigins = ['http://localhost:5173', 'https://admin.postalwiki.co.uk', 'https://api.postalwiki.co.uk'];
        // Allow requests with no origin (like mobile apps or curl requests)
        if (!origin) return callback(null, true);
        if (allowedOrigins.indexOf(origin) === -1) {
            return callback(new Error('The CORS policy for this site does not allow access from the specified Origin.'), false);
        }
        return callback(null, true);
    },
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With'],
    exposedHeaders: ['Content-Range', 'X-Content-Range'],
    credentials: true,
    preflightContinue: false,
    optionsSuccessStatus: 204,
    maxAge: 86400 // 24 hours
};

// Apply CORS middleware
app.use(cors(corsOptions));

// Handle preflight requests
app.options('*', cors(corsOptions));

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
    console.error(err.stack);
    if (err.message.includes('CORS')) {
        return res.status(403).json({
            status: 'error',
            message: err.message
        });
    }
    res.status(500).json({
        status: 'error',
        message: 'Something went wrong!'
    });
});

module.exports = app;
