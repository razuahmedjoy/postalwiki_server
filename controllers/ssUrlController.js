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
            totalCount: 0,
            successCount: 0,
            errorsCount: 0,
            notFoundCount: 0,
            duplicateCount: 0,
            errorMessages: '',
            resultDebug: ''
        };

        const entriesToInsert = [];
        const missingUrls = [];
        const duplicateUrls = [];
        const existingImages = new Set();

        // Ensure log directories exist
        // ensureLogDirectories();

        // Process each entry in the chunk
        for (const csvLine of chunk) {
            stats.totalCount++;

            if (!csvLine.url || !csvLine.image) {
                stats.errorMessages += 'Line without URL or Image found.<br>';
                stats.errorsCount++;
                continue;
            }

            const url = csvLine.url.trim();
            const image = csvLine.image.trim();

            if (!url || !image) {
                stats.errorMessages += `URL or Image empty:<br>URL: ${url} | Image: ${image}<br>`;
                stats.errorsCount++;
                continue;
            }

            // Check for duplicates
            if (existingImages.has(image)) {
                duplicateUrls.push(image);
                stats.duplicateCount++;
                continue;
            }
            existingImages.add(image);

            // Check if the image exists using axios
            try {
                const imageUrl = `https://h1m7.c11.e2-4.dev/${bucketName}/${image}`;
                const response = await axios.head(imageUrl);
                
                const imgExists = response.status === 200 && 
                                response.headers['content-type'] === 'image/webp';

                if (!imgExists) {
                    stats.notFoundCount++;
                    stats.resultDebug += `Image not a WebP image: ${bucketName}/${image}<br>`;
                    missingUrls.push(url);
                    continue;
                }

                entriesToInsert.push({
                    url,
                    image: `${bucketName}/${image}`
                });

            } catch (error) {
                if (error.response && error.response.status === 404) {
                    stats.notFoundCount++;
                    stats.resultDebug += `Image not found at URL: ${bucketName}/${image}<br>`;
                    missingUrls.push(url);
                } else {
                    stats.errorMessages += `Error checking image ${image}: ${error.message}<br>`;
                    stats.errorsCount++;
                }
            }
        }

        // Batch write logs
        // writeBatchLogs('missing_urls', missingUrls);
        // writeBatchLogs('duplicate_urls', duplicateUrls);

        // Insert valid entries into MongoDB
        if (entriesToInsert.length > 0) {
            try {
                const result = await ScreenshotUrl.insertMany(entriesToInsert, { ordered: false });
                stats.successCount = result.length;

            } catch (error) {
                if (error.name === 'BulkWriteError') {
                    stats.successCount = error.insertedDocs?.length || 0;
                    for (const writeError of error.writeErrors || []) {
                        stats.errorMessages += `${writeError.errmsg || writeError.message}<br>`;
                    }
                    stats.errorsCount = stats.totalCount - stats.successCount - stats.notFoundCount;
                } else {
                    stats.errorMessages += `Database error: ${error.message}<br>`;
                    stats.errorsCount = stats.totalCount - stats.notFoundCount;
                }
            }
        } else {
            stats.errorsCount = stats.totalCount - stats.notFoundCount;
        }

        return res.json({
            status: stats.successCount > 0 ? 1 : (stats.errorMessages.includes('duplicate key') ? 2 : 0),
            success: stats.successCount,
            errors: stats.errorsCount,
            totalcount: stats.totalCount,
            notfound: stats.notFoundCount,
            duplicates: stats.duplicateCount,
            errormessages: stats.errorMessages,
            resultdebug: stats.resultDebug
        });

    } catch (error) {
        
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




