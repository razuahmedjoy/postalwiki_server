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

        // Set a longer timeout for this route
        req.setTimeout(30000); // 30 seconds timeout

        let totalCount = 0;
        let successCount = 0;
        let errorsCount = 0;
        let notFoundCount = 0;
        let errorMessages = [];
        let resultDebug = [];

        const entriesToInsert = [];
        const processedUrls = new Set(); // Track processed URLs to avoid duplicates

        // Process entries in smaller batches
        const BATCH_SIZE = 10;
        for (let i = 0; i < chunk.length; i += BATCH_SIZE) {
            const batch = chunk.slice(i, i + BATCH_SIZE);
            
            // Process batch concurrently with Promise.all
            await Promise.all(batch.map(async (csvLine) => {
                totalCount++;

                if (!csvLine.url || !csvLine.image) {
                    errorMessages.push(`Line without URL or Image found.`);
                    errorsCount++;
                    return;
                }

                const url = csvLine.url.trim();
                const image = csvLine.image.trim();

                if (!url || !image) {
                    errorMessages.push(`URL or Image empty: URL: ${url} | Image: ${image}`);
                    errorsCount++;
                    return;
                }

                // Skip if URL was already processed
                if (processedUrls.has(url)) {
                    return;
                }
                processedUrls.add(url);

                try {
                    const imageUrl = `https://h1m7.c11.e2-4.dev/${bucketName}/${image}`;
                    const response = await axios.head(imageUrl, {
                        timeout: 5000,
                        maxRedirects: 2,
                        validateStatus: function (status) {
                            return status >= 200 && status < 500;
                        }
                    });

                    if (response.status === 404) {
                        notFoundCount++;
                        resultDebug.push(`Image not found at URL: ${bucketName}/${image}`);
                        return;
                    }

                    const imgExists = response.status === 200 &&
                        response.headers['content-type'] === 'image/webp';

                    if (!imgExists) {
                        notFoundCount++;
                        resultDebug.push(`Image not a WebP image: ${bucketName}/${image}`);
                        return;
                    }

                    entriesToInsert.push({
                        url,
                        image: `${bucketName}/${image}`
                    });

                } catch (error) {
                    notFoundCount++;
                    errorMessages.push(`Error checking image ${image}: ${error.message}`);
                }
            }));

            // Insert batch if we have entries
            if (entriesToInsert.length > 0) {
                try {
                    const result = await ScreenshotUrl.insertMany(entriesToInsert, { 
                        ordered: false,
                        writeConcern: { w: 0 }
                    });
                    successCount += result.length;
                    entriesToInsert.length = 0; // Clear the array
                } catch (error) {
                    if (error.name === 'BulkWriteError') {
                        successCount += error.insertedDocs?.length || 0;
                        error.writeErrors?.forEach(writeError => {
                            errorMessages.push(writeError.errmsg || writeError.message);
                        });
                    } else {
                        errorMessages.push(`Database error: ${error.message}`);
                    }
                }
            }

            // Add a small delay between batches
            await new Promise(resolve => setTimeout(resolve, 100));
        }

        // Calculate final error count
        errorsCount = totalCount - successCount - notFoundCount;

        return res.json({
            status: successCount > 0 ? 1 : (errorMessages.some(msg => msg.includes('duplicate key')) ? 2 : 0),
            success: successCount,
            errors: errorsCount,
            totalcount: totalCount,
            notfound: notFoundCount,
            errormessages: errorMessages.join('<br>'),
            resultdebug: resultDebug.join('<br>')
        });

    } catch (error) {
        console.error('Server error:', error);
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




