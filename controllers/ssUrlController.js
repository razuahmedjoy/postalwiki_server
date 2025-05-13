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

        // Process image checks in parallel with a concurrency limit
        const concurrencyLimit = 10; // Adjust based on your server capacity
        const entriesToInsert = [];
        const missingUrls = [];

        // Process entries in batches
        for (let i = 0; i < validEntries.length; i += concurrencyLimit) {
            const batch = validEntries.slice(i, i + concurrencyLimit);
            const batchPromises = batch.map(async (csvLine) => {
                const url = csvLine.url.trim();
                const image = csvLine.image.trim();
                const imageUrl = `https://h1m7.c11.e2-4.dev/${bucketName}/${image}`;

                try {
                    const response = await axios.head(imageUrl);
                    const imgExists = response.status === 200 && 
                                    response.headers['content-type'] === 'image/webp';

                    if (!imgExists) {
                        stats.notFoundCount++;
                        stats.resultDebug.push(`Image not a WebP image: ${bucketName}/${image}`);
                        missingUrls.push(url);
                        return null;
                    }

                    return {
                        url,
                        image: `${bucketName}/${image}`
                    };
                } catch (error) {
                    if (error.response?.status === 404) {
                        stats.notFoundCount++;
                        stats.resultDebug.push(`Image not found at URL: ${bucketName}/${image}`);
                        missingUrls.push(url);
                    } else {
                        stats.errorMessages.push(`Error checking image ${image}: ${error.message}`);
                        stats.errorsCount++;
                    }
                    return null;
                }
            });

            const batchResults = await Promise.all(batchPromises);
            entriesToInsert.push(...batchResults.filter(Boolean));
        }

        // Insert valid entries into MongoDB in batches
        if (entriesToInsert.length > 0) {
            try {
                const result = await ScreenshotUrl.insertMany(entriesToInsert, { 
                    ordered: false,
                    writeConcern: { w: 0 } // Fire and forget for better performance
                });
                
                stats.successCount = result.length;
                logger.info(`Inserted ${result.length} documents`);
                logger.info(`Missing ${missingUrls.length} documents`);

            } catch (error) {
                if (error.name === 'BulkWriteError') {
                    stats.successCount = error.insertedDocs?.length || 0;
                    error.writeErrors?.forEach(writeError => {
                        stats.errorMessages.push(writeError.errmsg || writeError.message);
                    });
                    stats.errorsCount = stats.totalCount - stats.successCount - stats.notFoundCount;
                } else {
                    stats.errorMessages.push(`Database error: ${error.message}`);
                    stats.errorsCount = stats.totalCount - stats.notFoundCount;
                }
            }
        } else {
            stats.errorsCount = stats.totalCount - stats.notFoundCount;
        }

        return res.json({
            status: stats.successCount > 0 ? 1 : (stats.errorMessages.some(msg => msg.includes('duplicate key')) ? 2 : 0),
            success: stats.successCount,
            errors: stats.errorsCount,
            totalcount: stats.totalCount,
            notfound: stats.notFoundCount,
            duplicates: stats.duplicateCount,
            errormessages: stats.errorMessages.join('<br>'),
            resultdebug: stats.resultDebug.join('<br>')
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




