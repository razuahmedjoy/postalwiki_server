const mongoose = require('mongoose');
const logger = require('./logger.js');

const connectDB = async () => {
    try {
        // Log connection details (without sensitive info)
        const uri = process.env.MONGO_URI;
        const dbName = uri.split('/').pop().split('?')[0];
        const authSource = uri.includes('authSource=') 
            ? uri.split('authSource=')[1].split('&')[0] 
            : 'admin';
        
        logger.info(`Connecting to MongoDB database: ${dbName}`);
        logger.info(`Authentication database: ${authSource}`);

        await mongoose.connect(process.env.MONGO_URI);
        
        // Log successful connection details
        logger.info('MongoDB connected successfully');
        logger.info(`Connected to database: ${mongoose.connection.db.databaseName}`);
        logger.info(`Connection host: ${mongoose.connection.host}`);
        
        // Test write permissions
        try {
            const testCollection = mongoose.connection.db.collection('permission_test');
            await testCollection.insertOne({ test: 'test' });
            await testCollection.deleteOne({ test: 'test' });
            logger.info('Write permissions test successful');
        } catch (error) {
            logger.error('Write permissions test failed:', error.message);
        }

    } catch (err) {
        logger.error(`MongoDB connection error: ${err.message}`);
        logger.error('Connection error details:', JSON.stringify(err));
        process.exit(1);
    }
};

module.exports = connectDB;
