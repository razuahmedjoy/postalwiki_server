const ScreenshotUrl = require("../models/ScreenshotUrl");
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const logger = require("../config/logger");

const ssUrlLogFolderName = 'ssUrlLogs';
const logFolderPath = path.join(__dirname, '..', 'logs', ssUrlLogFolderName);

// Utility function to ensure log directories exist
const ensureLogDirectories = () => {
    const directories = ['missing_urls', 'duplicate_urls'];
    directories.forEach(dir => {
        const dirPath = path.join(logFolderPath, dir);
        if (!fs.existsSync(dirPath)) {
            fs.mkdirSync(dirPath, { recursive: true });
        }
    });
};

// Utility function to write logs in batch
const writeBatchLogs = (logType, urls) => {
    if (!urls.length) return;
    
    const logFileName = `${logType}_${new Date().toLocaleDateString('en-US', { 
        day: 'numeric', 
        month: 'numeric', 
        year: 'numeric' 
    }).replace(/\//g, '.')}.log`;
    
    const logPath = path.join(logFolderPath, logType, logFileName);
    const logContent = urls.join('\n') + '\n';
    
    try {
        fs.appendFileSync(logPath, logContent);
    } catch (error) {
        logger.error(`Error writing to ${logType} log file: ${error.message}`);
    }
};

const importSSUrl = async (req, res) => {
    try {
        const { chunk, bucketName } = req.body;
        const startTime = Date.now();

        if (!chunk || !Array.isArray(chunk) || !bucketName) {
            return res.status(400).json({
                status: 0,
                success: 0,
                errors: 0,
                totalcount: 0,
                notfound: 0,
                errormessages: 'Invalid request data',
                resultdebug: ''
            });
        }

        logger.info(`Starting import of ${chunk.length} records`);

        // Initialize counters and tracking arrays
        const stats = {
            totalCount: chunk.length,
            successCount: 0,
            errorsCount: 0,
            notFoundCount: 0,
            duplicateCount: 0,
            errorMessages: [],
            resultDebug: []
        };

        // Filter out invalid entries first
        const validEntries = chunk.filter(csvLine => {
            if (!csvLine.url || !csvLine.image) {
                stats.errorMessages.push('Line without URL or Image found.');
                stats.errorsCount++;
                return false;
            }

            const url = csvLine.url.trim();
            const image = csvLine.image.trim();

            if (!url || !image) {
                stats.errorMessages.push(`URL or Image empty: URL: ${url} | Image: ${image}`);
                stats.errorsCount++;
                return false;
            }

            return true;
        });

        logger.info(`Found ${validEntries.length} valid entries out of ${chunk.length} total entries`);

        // Process in smaller batches to manage memory
        const BATCH_SIZE = 20; // Process 20 records at a time
        const entriesToInsert = [];
        const missingUrls = [];

        for (let i = 0; i < validEntries.length; i += BATCH_SIZE) {
            const batch = validEntries.slice(i, i + BATCH_SIZE);
            logger.info(`Processing batch ${i/BATCH_SIZE + 1} of ${Math.ceil(validEntries.length/BATCH_SIZE)}`);

            // Process each entry in the batch
            for (const csvLine of batch) {
                const url = csvLine.url.trim();
                const image = csvLine.image.trim();
                const imageUrl = `https://h1m7.c11.e2-4.dev/${bucketName}/${image}`;

                try {
                    const response = await axios.head(imageUrl, {
                        timeout: 5000,
                        maxRedirects: 2,
                        validateStatus: function (status) {
                            return status >= 200 && status < 500; // Accept all responses to handle them properly
                        }
                    });

                    if (response.status === 404) {
                        stats.notFoundCount++;
                        stats.resultDebug.push(`Image not found at URL: ${bucketName}/${image}`);
                        missingUrls.push(url);
                        continue;
                    }

                    const imgExists = response.status === 200 && 
                                    response.headers['content-type'] === 'image/webp';

                    if (!imgExists) {
                        stats.notFoundCount++;
                        stats.resultDebug.push(`Image not a WebP image: ${bucketName}/${image}`);
                        missingUrls.push(url);
                        continue;
                    }

                    entriesToInsert.push({
                        url,
                        image: `${bucketName}/${image}`
                    });

                    // Insert in smaller chunks to reduce memory usage
                    if (entriesToInsert.length >= 50) {
                        try {
                            const result = await ScreenshotUrl.insertMany(entriesToInsert, { 
                                ordered: false,
                                writeConcern: { w: 0 }
                            });
                            stats.successCount += result.length;
                            entriesToInsert.length = 0; // Clear the array
                            logger.info(`Successfully inserted ${result.length} records`);
                        } catch (error) {
                            if (error.name === 'BulkWriteError') {
                                const insertedCount = error.insertedDocs?.length || 0;
                                stats.successCount += insertedCount;
                                error.writeErrors?.forEach(writeError => {
                                    if (writeError.code === 11000) { // Duplicate key error
                                        stats.duplicateCount++;
                                    }
                                    stats.errorMessages.push(writeError.errmsg || writeError.message);
                                });
                                logger.warn(`Bulk write error: ${insertedCount} inserted, ${error.writeErrors?.length || 0} errors`);
                            } else {
                                stats.errorMessages.push(`Database error: ${error.message}`);
                                logger.error(`Database error: ${error.message}`);
                            }
                        }
                    }

                } catch (error) {
                    if (error.response?.status === 404) {
                        stats.notFoundCount++;
                        stats.resultDebug.push(`Image not found at URL: ${bucketName}/${image}`);
                        missingUrls.push(url);
                    } else {
                        stats.errorMessages.push(`Error checking image ${image}: ${error.message}`);
                        stats.errorsCount++;
                        logger.error(`Error checking image ${image}: ${error.message}`);
                    }
                }

                // Add a small delay between requests
                await new Promise(resolve => setTimeout(resolve, 100));
            }

            // Log progress
            logger.info(`Processed ${Math.min(i + BATCH_SIZE, validEntries.length)}/${validEntries.length} records`);
        }

        // Insert any remaining entries
        if (entriesToInsert.length > 0) {
            try {
                const result = await ScreenshotUrl.insertMany(entriesToInsert, { 
                    ordered: false,
                    writeConcern: { w: 0 }
                });
                stats.successCount += result.length;
                logger.info(`Inserted final ${result.length} records`);
            } catch (error) {
                if (error.name === 'BulkWriteError') {
                    const insertedCount = error.insertedDocs?.length || 0;
                    stats.successCount += insertedCount;
                    error.writeErrors?.forEach(writeError => {
                        if (writeError.code === 11000) {
                            stats.duplicateCount++;
                        }
                        stats.errorMessages.push(writeError.errmsg || writeError.message);
                    });
                } else {
                    stats.errorMessages.push(`Database error: ${error.message}`);
                }
            }
        }

        // Calculate final error count
        stats.errorsCount = stats.totalCount - stats.successCount - stats.notFoundCount;

        const endTime = Date.now();
        const processingTime = (endTime - startTime) / 1000; // Convert to seconds

        logger.info(`Import completed in ${processingTime} seconds. Stats:`, stats);

        return res.json({
            status: stats.successCount > 0 ? 1 : (stats.errorMessages.some(msg => msg.includes('duplicate key')) ? 2 : 0),
            success: stats.successCount,
            errors: stats.errorsCount,
            totalcount: stats.totalCount,
            notfound: stats.notFoundCount,
            duplicates: stats.duplicateCount,
            errormessages: stats.errorMessages.join('<br>'),
            resultdebug: stats.resultDebug.join('<br>'),
            processingTime: `${processingTime} seconds`
        });

    } catch (error) {
        logger.error(`Server error in importSSUrl: ${error.message}`);
        return res.status(500).json({
            status: 0,
            success: 0,
            errors: 1,
            totalcount: 1,
            notfound: 0,
            errormessages: `Server error: ${error.message}`,
            resultdebug: ''
        });
    }
}

const totalCount = async (req, res) => {
    const count = await ScreenshotUrl.countDocuments();
 
    res.json({
        collectionName: 'screenshotUrls',
        totalCount: count,
    });
}


const dropAll = async (req, res) => {
    try {
        await ScreenshotUrl.deleteMany({});
        res.status(200).json({ message: 'All records deleted successfully' });
    } catch (error) {
        res.status(500).json({ message: 'Error deleting records', error: error.message });
    }
}


module.exports = {
    importSSUrl,
    dropAll,
    totalCount
}




