// services/SocialScrapeService.js
const csv = require('csv-parse');
const fs = require('fs');
const path = require('path');
const { Transform } = require('stream');
const { EventEmitter } = require('events');
const SocialScrape = require('../models/SocialScrape');
const logger = require('../config/logger');

// Reduced batch size and parallel processing for 4GB RAM, 2-core VPS
const BATCH_SIZE = 1000; // Reduced from 50000 to 1000 for better reliability
const PARALLEL_BATCHES = 2; // Reduced to match CPU cores
const IMPORT_DIR = path.join(__dirname, '../imports/social_scrape');
const eventEmitter = new EventEmitter();

// Progress tracking
const progressTracker = {
    currentFile: null,
    processed: 0,
    total: 0,
    upserted: 0,
    modified: 0,
    errors: [],
    isComplete: false
};

// Utility Functions
const ensureImportDirectory = async () => {
    try {
        await fs.promises.mkdir(IMPORT_DIR, { recursive: true });
    } catch (error) {
        logger.error('Error creating import directory:', error);
        throw new Error('Failed to create import directory');
    }
};

const moveCompletedFile = async (filePath) => {
    try {
        const filename = path.basename(filePath);
        const today = new Date().toISOString().split('T')[0];
        const completedDir = path.join(IMPORT_DIR, `completed_${today}`);
        
        // Create completed directory if it doesn't exist
        await fs.promises.mkdir(completedDir, { recursive: true });
        
        // Move the file
        const newPath = path.join(completedDir, filename);
        await fs.promises.rename(filePath, newPath);
        
        logger.info(`Moved file ${filename} to completed directory`);
    } catch (error) {
        logger.error(`Failed to move file: ${error.message}`);
        throw error;
    }
};

// Ensure indexes exist for better performance
const ensureIndexes = async () => {
    try {
        await SocialScrape.collection.createIndex({ url: 1, date: 1 }, { unique: true });
        logger.info('Indexes created successfully');
    } catch (error) {
        logger.error('Error creating indexes:', error);
    }
};

// Process batches in parallel with memory management
const processBatchesInParallel = async (batches, filename, processed) => {
    try {
        // Process batches sequentially to avoid memory pressure
        let results = { upserted: 0, modified: 0 };
        
        for (const batch of batches) {
            const result = await insertBatch(batch, filename, processed, null);
            results.upserted += result.upserted;
            results.modified += result.modified;
            
            // Add a small delay between batches to allow memory cleanup
            await new Promise(resolve => setTimeout(resolve, 100));
        }
        
        return results;
    } catch (error) {
        logger.error('Error processing batches:', error);
        throw error;
    }
};

const insertBatch = async (batch, filename, processed, total) => {
    try {
        const operations = batch.map(doc => ({
            updateOne: {
                filter: { url: doc.url, date: doc.date },
                update: { $set: doc },
                upsert: true
            }
        }));

        logger.info(`Attempting to insert batch of ${batch.length} records`);

        // Modified MongoDB settings for better reliability
        const result = await SocialScrape.bulkWrite(operations, { 
            ordered: true,
            writeConcern: { w: 1 }, // Changed to 1 to ensure write acknowledgment
            bypassDocumentValidation: true
        });

        logger.info(`Batch insert result - Upserted: ${result.upsertedCount}, Modified: ${result.modifiedCount}`);

        progressTracker.upserted += result.upsertedCount;
        progressTracker.modified += result.modifiedCount;
        progressTracker.processed = processed;

        return {
            success: true,
            upserted: result.upsertedCount,
            modified: result.modifiedCount
        };
    } catch (error) {
        logger.error(`Error in insertBatch: ${error.message}`);
        logger.error(`Error details: ${JSON.stringify(error)}`);
        progressTracker.errors.push({
            filename,
            error: error.message
        });
        throw error;
    }
};

const processRecord = (record) => {
    try {
        const trimUrl = (url) => {
            if (!url) return '';
            return url
                .replace(/^(https?:\/\/)/i, '')
                .replace(/^www\./i, '')
                .replace(/^([^/]+).*?$/, '$1');
        };

        // Validate if the string is a valid domain name
        const isValidDomain = (domain) => {
            return /^[a-zA-Z0-9-]+(\.[a-zA-Z0-9-]+)+$/.test(domain);
        };

        const cleanSocialUrl = (url) => {
            if (!url) return '';
            // Remove everything after ? in URLs
            return url.replace(/^(https?:\/\/)/i, '')
            .replace(/^www\./i, '').split('?')[0];
        };

        const cleanText = (text) => {
            if (!text) return '';
            // Remove control characters and extra spaces
            return text.replace(/[\x00-\x1F\x7F-\x9F]/g, '')
                .replace(/\s+/g, ' ')
                .trim();
        };

        // Get the URL from the first column
        const url = Object.values(record)[0];
        
        // Skip if URL is not a valid domain
        if (!isValidDomain(url)) {
            logger.debug(`Skipping invalid domain: ${url}`);
            return null;
        }

        // Skip records with error or no data
        if (record.RESULT === 'Fetch error or no data found' || record.RESULT === 'not required') {
            // If we only have URL and no other data, skip this record
            const hasOtherData = Object.entries(record).some(([key, value]) => 
                key !== 'RESULT' && value && value.trim() !== ''
            );
            if (!hasOtherData) {
                logger.debug(`Skipping record with no data for URL: ${url}`);
                return null;
            }
        }

        // Process the record based on CODE
        const processedRecord = {
            url: trimUrl(url),
            date: new Date(record.DATE?.split('/').reverse().join('-')), // Convert DD/MM/YYYY to YYYY-MM-DD, handle undefined
        };

        switch (record.CODE) {
            case '[RD]':
                processedRecord.redirectUrl = cleanSocialUrl(record.RESULT);
                break;
            case '[TI]':
                processedRecord.title = cleanText(record.RESULT);
                break;
            case '[KW]':
                processedRecord.keywords = cleanText(record.RESULT);
                break;
            case '[SC]':
            case '[ER]':
                processedRecord.statusCode = record.RESULT;
                break;
            case '[PC]':
                processedRecord.postcode = cleanText(record.RESULT);
                break;
            case '[EM]':
                processedRecord.email = cleanText(record.RESULT);
                break;
            case '[TW]':
                processedRecord.twitter = cleanSocialUrl(record.RESULT);
                break;
            case '[FB]':
                processedRecord.facebook = cleanSocialUrl(record.RESULT);
                break;
            case '[LK]':
                processedRecord.linkedin = cleanSocialUrl(record.RESULT);
                break;
            case '[PT]':
                processedRecord.pinterest = cleanSocialUrl(record.RESULT);
                break;
            case '[YT]':
                processedRecord.youtube = cleanSocialUrl(record.RESULT);
                break;
            case '[IS]':
                processedRecord.instagram = cleanSocialUrl(record.RESULT);
                break;
            case '[RD]':
                processedRecord.redirect_url = cleanSocialUrl(record.RESULT);
                break;
            case '[MD]':
                processedRecord.meta_description = cleanText(record.RESULT);
                break;
        }

        logger.debug(`Processed record for URL: ${processedRecord.url}`);
        return processedRecord;
    } catch (error) {
        logger.error(`Error processing record: ${error.message}`);
        logger.error(`Record data: ${JSON.stringify(record)}`);
        return null;
    }
};

const processFile = async (filePath) => {
    const filename = path.basename(filePath);
    let processed = 0;
    let batches = [];
    let currentBatch = [];

    // Reset progress for new file
    progressTracker.currentFile = filename;
    progressTracker.processed = 0;
    progressTracker.total = 0;
    progressTracker.upserted = 0;
    progressTracker.modified = 0;
    progressTracker.errors = [];
    progressTracker.isComplete = false;

    // Ensure indexes exist
    await ensureIndexes();

    return new Promise((resolve, reject) => {
        const parser = csv.parse({
            columns: true,
            skip_empty_lines: true,
            highWaterMark: 1024 * 1024 // 1MB chunks
        });

        parser.on('readable', async () => {
            let record;
            while ((record = parser.read()) !== null) {
                try {
                    const processedRecord = processRecord(record);
                    if (processedRecord) {
                        currentBatch.push(processedRecord);
                        processed++;
                        progressTracker.processed = processed;

                        if (currentBatch.length >= BATCH_SIZE) {
                            batches.push([...currentBatch]);
                            currentBatch = [];

                            // Process batches when we have enough
                            if (batches.length >= PARALLEL_BATCHES) {
                                const results = await processBatchesInParallel(batches, filename, processed);
                                batches = [];
                                
                                // Add a small delay to allow memory cleanup
                                await new Promise(resolve => setTimeout(resolve, 100));
                            }
                        }
                    }
                } catch (error) {
                    progressTracker.errors.push({
                        filename,
                        error: error.message
                    });
                }
            }
        });

        parser.on('end', async () => {
            try {
                // Process remaining records
                if (currentBatch.length > 0) {
                    batches.push(currentBatch);
                }
                if (batches.length > 0) {
                    await processBatchesInParallel(batches, filename, processed);
                }
                await moveCompletedFile(filePath);
                progressTracker.isComplete = true;
                resolve({ filename, processed });
            } catch (error) {
                reject(error);
            }
        });

        parser.on('error', (error) => {
            progressTracker.errors.push({
                filename,
                error: error.message
            });
            reject(error);
        });

        // Use streams with smaller chunks for better memory management
        fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 }) // 1MB chunks
            .pipe(parser);
    });
};

const getImportFiles = async () => {
    try {
        await ensureImportDirectory();
        logger.info('Reading import directory:', IMPORT_DIR);
        const files = await fs.promises.readdir(IMPORT_DIR);
        return files.filter(file => file.endsWith('.csv'));
    } catch (error) {
        logger.error('Error reading import directory:', error);
        return [];
    }
};

const getCollectionStats = async () => {
    return await SocialScrape.countDocuments();
};

const SocialScrapeService = {
    getImportFiles,
    getCollectionStats,
    processFile,
    getProgress: () => ({ ...progressTracker })
};

module.exports = {
    SocialScrapeService,
    IMPORT_DIR,
    eventEmitter
};