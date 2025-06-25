const mongoose = require('mongoose');
const SocialScrape = require('../models/SocialScrape');
const connectDB = require('../config/db');
const logger = require('../config/logger');
const winston = require('winston');
const DailyRotateFile = require('winston-daily-rotate-file');
require('dotenv').config();

const MAX_CHAR_LIMIT = 400;
const BATCH_SIZE = 1000; // Process in batches to avoid memory issues

// Create a dedicated logger for this script
const scriptLogger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
        winston.format.printf(({ timestamp, level, message }) => `${timestamp} ${level}: ${message}`)
    ),
    transports: [
        new winston.transports.Console(),
        new DailyRotateFile({
            filename: 'logs/trim-fields-%DATE%.log',
            datePattern: 'YYYY-MM-DD',
            maxSize: '20m',
            maxFiles: '14d',
            dirname: 'logs'
        })
    ],
});

async function trimField(fieldValue, maxLength) {
    if (!fieldValue || typeof fieldValue !== 'string') {
        return fieldValue;
    }
    
    if (fieldValue.length <= maxLength) {
        return fieldValue;
    }
    
    return fieldValue.substring(0, maxLength);
}

async function processBatch(batch) {
    const updates = [];
    const updatedUrls = [];
    
    for (const record of batch) {
        const originalTitle = record.title;
        const originalKeywords = record.keywords;
        const originalMetaDescription = record.meta_description;
        
        const trimmedTitle = await trimField(originalTitle, MAX_CHAR_LIMIT);
        const trimmedKeywords = await trimField(originalKeywords, MAX_CHAR_LIMIT);
        const trimmedMetaDescription = await trimField(originalMetaDescription, MAX_CHAR_LIMIT);
        
        // Check if any field needs trimming
        if (trimmedTitle !== originalTitle || 
            trimmedKeywords !== originalKeywords || 
            trimmedMetaDescription !== originalMetaDescription) {
            
            updates.push({
                updateOne: {
                    filter: { _id: record._id },
                    update: {
                        $set: {
                            title: trimmedTitle,
                            keywords: trimmedKeywords,
                            meta_description: trimmedMetaDescription
                        }
                    }
                }
            });
            
            updatedUrls.push(record.url);
        }
    }
    
    if (updates.length > 0) {
        try {
            await SocialScrape.bulkWrite(updates);
            scriptLogger.info(`Updated ${updates.length} records in this batch`);
            
            // Log each updated URL
            for (const url of updatedUrls) {
                scriptLogger.info(`Updated record with URL: ${url}`);
            }
            
            return updates.length;
        } catch (error) {
            scriptLogger.error(`Error updating batch: ${error.message}`);
            throw error;
        }
    }
    
    return 0;
}

async function trimFields() {
    try {
        scriptLogger.info('Starting field trimming script...');
        scriptLogger.info(`Target fields: title, keywords, meta_description`);
        scriptLogger.info(`Maximum character limit: ${MAX_CHAR_LIMIT}`);
        scriptLogger.info(`Batch size: ${BATCH_SIZE}`);
        
        // Connect to database
        await connectDB();
        scriptLogger.info('Connected to MongoDB successfully');
        
        // Get total count
        const totalCount = await SocialScrape.countDocuments();
        scriptLogger.info(`Total records in collection: ${totalCount.toLocaleString()}`);
        
        let processedCount = 0;
        let updatedCount = 0;
        let batchNumber = 0;
        
        // Process in batches
        while (processedCount < totalCount) {
            batchNumber++;
            scriptLogger.info(`Processing batch ${batchNumber} (${processedCount + 1} to ${Math.min(processedCount + BATCH_SIZE, totalCount)})`);
            
            const batch = await SocialScrape.find({})
                .select('_id url title keywords meta_description')
                .skip(processedCount)
                .limit(BATCH_SIZE)
                .lean();
            
            if (batch.length === 0) {
                break;
            }
            
            const batchUpdates = await processBatch(batch);
            updatedCount += batchUpdates;
            processedCount += batch.length;
            
            scriptLogger.info(`Batch ${batchNumber} completed. Processed: ${processedCount}/${totalCount} (${((processedCount/totalCount)*100).toFixed(2)}%)`);
            
            // Add a small delay to prevent overwhelming the database
            await new Promise(resolve => setTimeout(resolve, 100));
        }
        
        scriptLogger.info('=== SCRIPT COMPLETED ===');
        scriptLogger.info(`Total records processed: ${processedCount.toLocaleString()}`);
        scriptLogger.info(`Total records updated: ${updatedCount.toLocaleString()}`);
        scriptLogger.info(`Percentage updated: ${((updatedCount/processedCount)*100).toFixed(2)}%`);
        
    } catch (error) {
        scriptLogger.error(`Script failed: ${error.message}`);
        scriptLogger.error(error.stack);
        process.exit(1);
    } finally {
        // Close database connection
        if (mongoose.connection.readyState === 1) {
            await mongoose.connection.close();
            scriptLogger.info('Database connection closed');
        }
        process.exit(0);
    }
}

// Run the script
trimFields(); 