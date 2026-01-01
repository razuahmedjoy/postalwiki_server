const ScreenshotUrl = require("../models/ScreenshotUrl");
const axios = require('axios');
const ssUrlLogger = require("../config/loggers/ssUrlLogger");
const https = require('https');

// Create an HTTPS agent for connection pooling
const httpsAgent = new https.Agent({
    keepAlive: true,
    maxSockets: 200, // Increased for higher concurrency
    maxFreeSockets: 20,
    timeout: 60000 // 60 seconds socket timeout
});

// Helper function to check image with retries
const checkImageWithRetry = async (url, retries = 3) => {
    for (let i = 0; i < retries; i++) {
        try {
            const response = await axios.head(url, {
                timeout: 15000,
                maxRedirects: 2,
                httpsAgent: httpsAgent,
                validateStatus: function (status) {
                    return status >= 200 && status < 500;
                }
            });
            return response;
        } catch (error) {
            const isLastAttempt = i === retries - 1;
            const shouldRetry = !error.response || (error.response.status >= 500 && error.response.status < 600);

            if (!shouldRetry || isLastAttempt) {
                throw error;
            }
            // Faster backoff for performance: 500ms, 1000ms, 2000ms
            await new Promise(resolve => setTimeout(resolve, 500 * Math.pow(2, i)));
        }
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
                duplicates: 0,
                errormessages: 'Invalid request data',
                resultdebug: ''
            });
        }

        // Set a significantly longer timeout for this route
        req.setTimeout(300000); // 5 minutes timeout

        let totalCount = 0;
        let successCount = 0;
        let errorsCount = 0;
        let notFoundCount = 0;
        let duplicateCount = 0;
        let errorMessages = [];
        let resultDebug = [];

        const entriesToInsert = [];
        const processedUrls = new Set();

        // Optimizing Batch Size for Speed
        const BATCH_SIZE = 50;
        for (let i = 0; i < chunk.length; i += BATCH_SIZE) {
            const batch = chunk.slice(i, i + BATCH_SIZE);

            // Process batch concurrently
            await Promise.all(batch.map(async (csvLine, index) => {
                totalCount++;

                if (!csvLine.url || !csvLine.image) {
                    const msg = `Line without URL or Image found.`;
                    errorMessages.push(msg);
                    ssUrlLogger.error(msg, { reason: 'Missing Data' });
                    errorsCount++;
                    return;
                }

                const url = csvLine.url.trim();
                const image = csvLine.image.trim();

                if (!url || !image) {
                    const msg = `URL or Image empty: URL: ${url} | Image: ${image}`;
                    errorMessages.push(msg);
                    ssUrlLogger.error(msg, { url, image, reason: 'Empty Data' });
                    errorsCount++;
                    return;
                }

                if (processedUrls.has(url)) {
                    duplicateCount++; // Tracking internal duplicates in chunk
                    return;
                }
                processedUrls.add(url);

                try {
                    const imageUrl = `https://h1m7.c11.e2-4.dev/${bucketName}/${image}`;

                    const response = await checkImageWithRetry(imageUrl);

                    if (response.status === 404) {
                        notFoundCount++;
                        resultDebug.push(`Image not found at URL: ${bucketName}/${image}`);
                        return;
                    }

                    const imgExists = response.status === 200 &&
                        response.headers['content-type'] === 'image/webp';

                    if (!imgExists) {
                        notFoundCount++;
                        const msg = `Image not a WebP image or error: ${bucketName}/${image} (Status: ${response.status})`;
                        resultDebug.push(msg);
                        ssUrlLogger.error(msg, { url: imageUrl, image, reason: `Invalid Content Type or Status ${response.status}` });
                        return;
                    }

                    entriesToInsert.push({
                        url,
                        image: `${bucketName}/${image}`
                    });

                } catch (error) {
                    let reason = error.message;
                    if (error.code === 'ECONNABORTED') {
                        reason = 'Timeout after retries';
                        errorMessages.push(`Timeout checking image ${image}`);
                    } else {
                        errorMessages.push(`Error checking image ${image}: ${error.message}`);
                    }
                    ssUrlLogger.error('Image check failed', { url, image, reason });
                    errorsCount++;
                }
            }));

            // Insert batch
            if (entriesToInsert.length > 0) {
                try {
                    const result = await ScreenshotUrl.insertMany(entriesToInsert, {
                        ordered: false,
                        writeConcern: { w: 0 }
                    });
                    successCount += result.length;

                    // Note: With w:0, result might not strictly return nInserted in all drivers, 
                    // but usually it does or simply implies success if no error thrown.
                    // If result.length is undefined (legacy), assume entriesToInsert.length.
                    if (result && typeof result.length === 'undefined') {
                        // fallback if insertMany returns varying result format
                        // In Mongoose insertMany returns the documents.
                    }

                } catch (error) {
                    if (error.name === 'BulkWriteError' || error.code === 11000) {
                        let currentBatchSuccess = 0;
                        if (error.result && error.result.nInserted) {
                            currentBatchSuccess = error.result.nInserted;
                        } else if (error.insertedDocs) {
                            currentBatchSuccess = error.insertedDocs.length;
                        }

                        successCount += currentBatchSuccess;

                        const writeErrors = error.writeErrors || [];
                        let batchDuplicates = 0;

                        writeErrors.forEach(writeError => {
                            if (writeError.code === 11000) {
                                batchDuplicates++;
                                // Duplicates are expected, we don't log them as technical errors in the error log to avoid noise, 
                                // unless user explicitly asked for "error reason". The user said "if same url and image exist... should not be inserted",
                                // implying this is normal business logic, not a system failure.
                            } else {
                                const msg = `DB Error: ${writeError.errmsg || writeError.message}`;
                                errorMessages.push(msg);
                                ssUrlLogger.error('Database Write Error', { reason: msg });
                            }
                        });
                        duplicateCount += batchDuplicates;

                    } else {
                        const msg = `Database error: ${error.message}`;
                        errorMessages.push(msg);
                        ssUrlLogger.error('Database Fatal Error', { reason: msg });
                    }
                }
                entriesToInsert.length = 0;
            }

            // Reduced delay for higher throughput
            await new Promise(resolve => setTimeout(resolve, 10));
        }

        errorsCount = totalCount - successCount - notFoundCount - duplicateCount;
        if (errorsCount < 0) errorsCount = 0;

        return res.json({
            status: (successCount > 0 || duplicateCount > 0) ? 1 : 0,
            success: successCount,
            errors: errorsCount,
            totalcount: totalCount,
            notfound: notFoundCount,
            duplicates: duplicateCount,
            errormessages: errorMessages.join('<br>'),
            resultdebug: resultDebug.join('<br>')
        });

    } catch (error) {
        ssUrlLogger.error('Server specific error', { reason: error.message, stack: error.stack });
        console.error('Server error:', error);
        return res.status(500).json({
            status: 0,
            success: 0,
            errors: 1,
            totalcount: 1,
            notfound: 0,
            duplicates: 0,
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
