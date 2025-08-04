// services/BotsolService.js
const csv = require('csv-parse');
const fs = require('fs');
const path = require('path');
const Botsol = require('../models/Botsol');
const areaCodes = require('../utils/areaCodes');
const botsolLogger = require('../config/loggers/botsolLogger');

// Configuration
const BATCH_SIZE = 2000;
const PARALLEL_BATCHES = 2;
const IMPORT_DIR = path.join(__dirname, '../imports/botsol');

// Progress tracker
const importProgressTracker = {
    currentFile: null,
    processed: 0,
    total: 0,
    upserted: 0,
    modified: 0,
    errors: [],
    isComplete: false,
    isRunning: false
};

// Utility Functions
const ensureImportDirectory = async () => {
    try {
        await fs.promises.mkdir(IMPORT_DIR, { recursive: true });
    } catch (error) {
        console.error('Error creating import directory:', error);
        throw new Error('Failed to create import directory');
    }
};

const resetImportProgress = () => {
    importProgressTracker.currentFile = null;
    importProgressTracker.processed = 0;
    importProgressTracker.total = 0;
    importProgressTracker.upserted = 0;
    importProgressTracker.modified = 0;
    importProgressTracker.errors = [];
    importProgressTracker.isComplete = false;
    importProgressTracker.isRunning = false;
    console.log('Reset import progress tracker');
};

const setImportRunning = (running) => {
    importProgressTracker.isRunning = running;
    console.log(`Set import running status to: ${running}`);
};

const moveCompletedFile = async (filePath) => {
    try {
        const filename = path.basename(filePath);
        // const today = new Date().toISOString().split('T')[0];
        // const completedDir = path.join(IMPORT_DIR, `completed_${today}`);

        // await fs.promises.mkdir(completedDir, { recursive: true });
        // const newPath = path.join(completedDir, filename);
        // await fs.promises.rename(filePath, newPath);

        console.log(`Moved file ${filename} to completed directory`);
    } catch (error) {
        console.error(`Failed to move file: ${error.message}`);
        throw error;
    }
};

// Domain validation utility
const isValidDomain = (domain) => {
    if (!domain || typeof domain !== 'string') return false;
    
    // Basic domain validation
    const domainRegex = /^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/;
    return domainRegex.test(domain) && domain.length > 0 && domain.length <= 253;
};



// Phone utilities
const cleanPhoneNumber = (phone) => {
    if (!phone || typeof phone !== 'string') return null;

    let cleaned = phone.trim();

    // Handle scientific notation
    if (cleaned.match(/[Ee][+-]?\d+/)) {
        try {
            const number = parseFloat(cleaned);
            if (!isNaN(number)) {
                cleaned = Math.floor(number).toString();
            }
        } catch (error) {
            console.warn(`Failed to parse scientific notation: ${cleaned}`);
        }
    }

    // Clean the phone number
    cleaned = cleaned.replace(/\s+/g, '')
        .replace(/-/g, '')
        .replace('+', '')
        .replace(/\./g, '')
        .replace(/[\(\)\[\]]/g, '')
        .replace(/[^0-9]/g, '');

    if (cleaned.startsWith('44')) {
        if (cleaned.length == 12) {
            cleaned = cleaned.slice(2);
            cleaned = '0' + cleaned;
        }
        if (cleaned.length == 13) {
            cleaned = cleaned.slice(2);
        }
    }
    if (cleaned.length == 10 && !cleaned.startsWith('44') && !cleaned.startsWith('0')) {
        cleaned = '0' + cleaned;
    }

    return cleaned;
};

const getAreaCode = (phone) => {
    const areaCode = areaCodes.find(area => phone.startsWith(area.code));
    return areaCode ? areaCode.areaName : null;
};

const isValidPhoneNumber = (phone, url) => {
    if (!phone || typeof phone !== 'string') return false;
    const cleanedPhone = cleanPhoneNumber(phone);
    return cleanedPhone ? cleanedPhone : false;
};

// Process record
const processRecord = (record) => {
    try {
        const trimUrl = (url) => {
            if (!url) return '';
            return url
                .replace(/^(https?:\/\/)/i, '')
                .replace(/^www\./i, '')
                .replace(/^([^/]+).*?$/, '$1');
        };

        const cleanSocialUrl = (url) => {
            if (!url) return '';
            return url.replace(/^(https?:\/\/)/i, '')
                .replace(/^www\./i, '').split('?')[0];
        };

        const cleanText = (text) => {
            if (!text) return '';
            return text.replace(/[\x00-\x1F\x7F-\x9F]/g, '')
                .replace(/\s+/g, ' ')
                .trim()
                .substring(0, 400);
        };

        // Get URL from Website or URL column
        let url = record?.Website;
        // if url is found, trim it to only include the domain name 
        if (url) {
            url = trimUrl(url);
        }
        botsolLogger.info(`Processing URL: ${url}`);
        if (!isValidDomain(url)) {
            console.debug(`Skipping invalid domain: ${url}`);
            return null;
        }

        const processedRecord = {
            url: trimUrl(url),
            date: new Date(),
            company_name: cleanText(record.Name),
            address: cleanText(record.Full_Address),
            email: cleanText(record.Email),
            facebook: cleanSocialUrl(record.Facebook),
            twitter: cleanSocialUrl(record.Twitter),
            instagram: cleanSocialUrl(record.Instagram),
            meta_description: cleanText(record.Description),
            phone: []
        };

        // Process phone number
        if (record.Phone) {
            const validPhone = isValidPhoneNumber(record.Phone, processedRecord.url);
            if (validPhone) {
                const areaCode = getAreaCode(validPhone);
                if (areaCode && validPhone.length === 11) {
                    processedRecord.phone.push({
                        number: validPhone,
                        areaName: areaCode
                    });
                }
            }
        }

        // Extract postcode from address
        if (processedRecord.address) {
            const postcodeMatch = processedRecord.address.match(/[A-Z]{1,2}[0-9][A-Z0-9]? ?[0-9][A-Z]{2}/i);
            if (postcodeMatch) {
                processedRecord.postcode = postcodeMatch[0].toUpperCase();
            }
        }

        return processedRecord;
    } catch (error) {
        console.error(`Error processing record: ${error.message}`);
        return null;
    }
};

// Batch processing
const insertBatch = async (batch, filename, processed) => {
    try {
        const operations = batch.map(doc => ({
            updateOne: {
                filter: { url: doc.url, date: doc.date },
                update: { $set: doc },
                upsert: true
            }
        }));

        const result = await Botsol.bulkWrite(operations, {
            ordered: false,
            writeConcern: { w: 1 },
            bypassDocumentValidation: true
        });

        importProgressTracker.upserted += result.upsertedCount;
        importProgressTracker.modified += result.modifiedCount;
        importProgressTracker.processed = processed;

        return {
            success: true,
            upserted: result.upsertedCount,
            modified: result.modifiedCount
        };
    } catch (error) {
        console.error(`Error in insertBatch: ${error.message}`);
        importProgressTracker.errors.push({
            filename,
            error: error.message
        });
        throw error;
    }
};

const processBatchesInParallel = async (batches, filename, processed) => {
    try {
        let results = { upserted: 0, modified: 0 };

        for (const batch of batches) {
            const result = await insertBatch(batch, filename, processed, null);
            results.upserted += result.upserted;
            results.modified += result.modified;
            botsolLogger.info(`Processed ${results.upserted} upserts and ${results.modified} modifications`);
            await new Promise(resolve => setTimeout(resolve, 100));
        }

        return results;
    } catch (error) {
        console.error('Error processing batches:', error);
        throw error;
    }
};

// Main file processing
const processFile = async (filePath) => {
    const filename = path.basename(filePath);
    let processed = 0;
    let batches = [];
    let currentBatch = [];
    let skippedLines = 0;

    // Reset progress for new file
    importProgressTracker.currentFile = filename;
    importProgressTracker.processed = 0;
    importProgressTracker.total = 0;
    importProgressTracker.upserted = 0;
    importProgressTracker.modified = 0;
    importProgressTracker.errors = [];
    importProgressTracker.isComplete = false;

    // Try to get file creation date
    let fileDate = new Date();
    try {
        const stats = await fs.promises.stat(filePath);
        fileDate = stats.birthtime;
    } catch (error) {
        console.warn(`Could not get file creation date for ${filename}, using current date`);
    }

    return new Promise((resolve, reject) => {
        const parser = csv.parse({
            columns: true,
            skip_empty_lines: true,
            relax_column_count: true,
            relax_quotes: true,
            highWaterMark: 1024 * 1024,
            skip_records_with_error: true // Skip problematic records
        });

        parser.on('readable', async () => {
            let record;
            while ((record = parser.read()) !== null) {
                try {
            
                    const processedRecord = processRecord(record);
                    if (processedRecord) {
                        // Use file creation date if available
                        if (fileDate) {
                            processedRecord.date = fileDate;
                        }
                        
                        currentBatch.push(processedRecord);
                        processed++;
                        importProgressTracker.processed = processed;

                        if (currentBatch.length >= BATCH_SIZE) {
                            batches.push([...currentBatch]);
                            currentBatch = [];
                            if (batches.length >= PARALLEL_BATCHES) {
                                const results = await processBatchesInParallel(batches, filename, processed);
                                batches = [];
                                await new Promise(resolve => setTimeout(resolve, 100));
                            }
                        }
                    }
                } catch (error) {
                    skippedLines++;
                    console.error(`Skipping malformed line: ${error.message}`);
                    importProgressTracker.errors.push({
                        filename,
                        error: `Skipped malformed line: ${error.message}`
                    });
                }
            }
        });

        parser.on('end', async () => {
            try {
                if (currentBatch.length > 0) {
                    batches.push([...currentBatch]);
                }
                if (batches.length > 0) {
                    await processBatchesInParallel(batches, filename, processed);
                }

                if (skippedLines > 0) {
                    console.log(`Completed processing ${filename}. Processed: ${processed}, Skipped: ${skippedLines} malformed lines`);
                    importProgressTracker.errors.push({
                        filename,
                        error: `Skipped ${skippedLines} malformed lines during processing`
                    });
                }

                await moveCompletedFile(filePath);
                importProgressTracker.isComplete = true;
                resolve({ filename, processed });
            } catch (error) {
                reject(error);
            }
        });

        parser.on('error', (error) => {
            const errorMessage = `CSV parsing error (continuing with valid lines): ${error.message}`;
            console.warn(`Error in ${filename}: ${errorMessage}`);
            skippedLines++;
            importProgressTracker.errors.push({
                filename,
                error: errorMessage
            });
        });

        fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 })
            .pipe(parser);
    });
};

const getImportFiles = async () => {
    try {
        await ensureImportDirectory();
        console.log('Reading import directory:', IMPORT_DIR);

        const files = await fs.promises.readdir(IMPORT_DIR);
        return files.filter(file => file.endsWith('.csv'));

    } catch (error) {
        console.error('Error reading import directory:', error);
        return [];
    }
};

const getCollectionStats = async () => {
    return await Botsol.countDocuments();
};

const BotsolService = {
    getImportFiles,
    getCollectionStats,
    processFile,
    getImportProgress: () => ({ ...importProgressTracker }),
    resetImportProgress,
    setImportRunning
};

module.exports = {
    BotsolService,
    IMPORT_DIR
};
