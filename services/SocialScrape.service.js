// services/SocialScrapeService.js
const csv = require('csv-parse');
const fs = require('fs');
const path = require('path');
const { EventEmitter } = require('events');
const SocialScrape = require('../models/SocialScrape');
const socialScrapeLogger = require('../config/socialScrapeLogger');
const { isValidDomain } = require('../utils/helpers');
const { archiveFile } = require('../utils/fileUtils');

// Reduced batch size and parallel processing for 4GB RAM, 2-core VPS
const BATCH_SIZE = 1000; // Reduced from 50000 to 1000 for better reliability
const PARALLEL_BATCHES = 2; // Reduced to match CPU cores
const IMPORT_DIR = path.join(__dirname, '../imports/social_scrape');
const BLACKLIST_DIR = path.join(__dirname, '../imports/social_scrape_blacklisted');
const PHONE_DIR = path.join(__dirname, '../imports/social_scrape_phone');

// Create separate event emitters for each process
const importEventEmitter = new EventEmitter();
const blacklistEventEmitter = new EventEmitter();
const phoneEventEmitter = new EventEmitter();

// Separate progress trackers for each process
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

// Store for blacklist progress trackers
const blacklistProgressStore = new Map();

// Store for phone progress trackers
const phoneProgressStore = new Map();

// Utility Functions
const ensureImportDirectory = async () => {
    try {
        await fs.promises.mkdir(IMPORT_DIR, { recursive: true });
    } catch (error) {
        socialScrapeLogger.error('Error creating import directory:', error);
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
    socialScrapeLogger.info('Reset import progress tracker');
};

const setImportRunning = (running) => {
    importProgressTracker.isRunning = running;
    socialScrapeLogger.info(`Set import running status to: ${running}`);
};

const moveCompletedFile = async (filePath) => {
    try {
        const filename = path.basename(filePath);
        const today = new Date().toISOString().split('T')[0];
        const completedDir = path.join(IMPORT_DIR, `completed_${today}`);

        // Create completed directory if it doesn't exist
        await fs.promises.mkdir(completedDir, { recursive: true });

        // Move the file with retry logic for EBUSY errors
        const newPath = path.join(completedDir, filename);
        await fs.promises.rename(filePath, newPath);

        socialScrapeLogger.info(`Moved file ${filename} to completed directory`);
    } catch (error) {
        socialScrapeLogger.error(`Failed to move file: ${error.message}`);
        throw error;
    }
};

// Ensure indexes exist for better performance
const ensureIndexes = async () => {
    try {
        // Create compound unique index on URL + date
        await SocialScrape.collection.createIndex({ url: 1, date: 1 }, { unique: true, background: true });
        socialScrapeLogger.info('Indexes created successfully');
    } catch (error) {
        socialScrapeLogger.error('Error creating indexes:', error);
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
        socialScrapeLogger.error('Error processing batches:', error);
        throw error;
    }
};

const insertBatch = async (batch, filename, processed, total) => {
    try {
        // Group records by URL + date combination to handle duplicates properly
        const urlDateGroups = new Map();

        for (const doc of batch) {
            const key = `${doc.url}_${doc.date.toISOString().split('T')[0]}`; // Use date without time for grouping
            if (!urlDateGroups.has(key)) {
                urlDateGroups.set(key, []);
            }
            urlDateGroups.get(key).push(doc);
        }

        // Create operations for each unique URL + date combination
        const operations = [];
        for (const [key, docs] of urlDateGroups) {
            // Merge all records for the same URL + date combination
            const mergedDoc = mergeRecordsForSameUrlDate(docs);

            operations.push({
                updateOne: {
                    filter: { url: mergedDoc.url, date: mergedDoc.date },
                    update: { $set: mergedDoc },
                    upsert: true
                }
            });
        }

        socialScrapeLogger.info(`Attempting to insert batch of ${operations.length} unique URL+date combinations (from ${batch.length} total records)`);

        // Modified MongoDB settings for better reliability
        const result = await SocialScrape.bulkWrite(operations, {
            ordered: false, // Changed to false to continue on individual errors
            writeConcern: { w: 1 }, // Changed to 1 to ensure write acknowledgment
            bypassDocumentValidation: true
        });

        socialScrapeLogger.info(`Batch insert result - Upserted: ${result.upsertedCount}, Modified: ${result.modifiedCount}`);

        importProgressTracker.upserted += result.upsertedCount;
        importProgressTracker.modified += result.modifiedCount;
        importProgressTracker.processed = processed;

        return {
            success: true,
            upserted: result.upsertedCount,
            modified: result.modifiedCount
        };
    } catch (error) {
        // Handle duplicate key errors gracefully
        if (error.code === 11000) {
            socialScrapeLogger.warn(`Duplicate key error in batch (continuing): ${error.message}`);

            // Try to insert records one by one to handle duplicates
            let upserted = 0;
            let modified = 0;

            for (const [key, docs] of urlDateGroups) {
                try {
                    const mergedDoc = mergeRecordsForSameUrlDate(docs);

                    const result = await SocialScrape.updateOne(
                        { url: mergedDoc.url, date: mergedDoc.date },
                        { $set: mergedDoc },
                        { upsert: true }
                    );

                    if (result.upsertedCount > 0) upserted++;
                    if (result.modifiedCount > 0) modified++;

                } catch (individualError) {
                    socialScrapeLogger.warn(`Failed to insert URL+date combination ${key}: ${individualError.message}`);
                    importProgressTracker.errors.push({
                        filename,
                        error: `Failed to insert URL+date combination ${key}: ${individualError.message}`
                    });
                }
            }

            importProgressTracker.upserted += upserted;
            importProgressTracker.modified += modified;
            importProgressTracker.processed = processed;

            return {
                success: true,
                upserted: upserted,
                modified: modified
            };
        }

        socialScrapeLogger.error(`Error in insertBatch: ${error.message}`);
        socialScrapeLogger.error(`Error details: ${JSON.stringify(error)}`);
        importProgressTracker.errors.push({
            filename,
            error: error.message
        });
        throw error;
    }
};

// Helper function to merge multiple records for the same URL + date combination
const mergeRecordsForSameUrlDate = (docs) => {
    if (docs.length === 1) {
        return docs[0];
    }

    // Merge multiple records for the same URL + date
    const mergedDoc = {
        url: docs[0].url,
        date: docs[0].date,
        title: '',
        twitter: '',
        facebook: '',
        instagram: '',
        linkedin: '',
        youtube: '',
        pinterest: '',
        email: '',
        phone: [],
        postcode: '',
        statusCode: '',
        redirect_url: '',
        meta_description: ''
    };

    // Merge all fields from all records, taking the first non-empty value
    for (const doc of docs) {
        if (doc.title && !mergedDoc.title) mergedDoc.title = doc.title;
        if (doc.twitter && !mergedDoc.twitter) mergedDoc.twitter = doc.twitter;
        if (doc.facebook && !mergedDoc.facebook) mergedDoc.facebook = doc.facebook;
        if (doc.instagram && !mergedDoc.instagram) mergedDoc.instagram = doc.instagram;
        if (doc.linkedin && !mergedDoc.linkedin) mergedDoc.linkedin = doc.linkedin;
        if (doc.youtube && !mergedDoc.youtube) mergedDoc.youtube = doc.youtube;
        if (doc.pinterest && !mergedDoc.pinterest) mergedDoc.pinterest = doc.pinterest;
        if (doc.email && !mergedDoc.email) mergedDoc.email = doc.email;
        if (doc.postcode && !mergedDoc.postcode) mergedDoc.postcode = doc.postcode;
        if (doc.statusCode && !mergedDoc.statusCode) mergedDoc.statusCode = doc.statusCode;
        if (doc.redirect_url && !mergedDoc.redirect_url) mergedDoc.redirect_url = doc.redirect_url;
        if (doc.meta_description && !mergedDoc.meta_description) mergedDoc.meta_description = doc.meta_description;

        // Merge phone arrays
        if (doc.phone && Array.isArray(doc.phone)) {
            mergedDoc.phone = [...new Set([...mergedDoc.phone, ...doc.phone])];
        }
    }

    return mergedDoc;
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
                .trim()
                .substring(0, 400);
        };

        // Get the URL from the first column
        const url = Object.values(record)[0];

        // Skip if URL is not a valid domain
        if (!isValidDomain(url)) {
            socialScrapeLogger.debug(`Skipping invalid domain: ${url}`);
            return null;
        }

        // Skip records with error or no data
        if (record.RESULT === 'Fetch error or no data found' || record.RESULT === 'not required') {
            // If we only have URL and no other data, skip this record
            const hasOtherData = Object.entries(record).some(([key, value]) =>
                key !== 'RESULT' && value && value.trim() !== ''
            );
            if (!hasOtherData) {
                socialScrapeLogger.debug(`Skipping record with no data for URL: ${url}`);
                return null;
            }
        }

        // Process the record based on CODE
        const processedRecord = {
            url: trimUrl(url),
            date: new Date(record.DATE?.split('/').reverse().join('-')),
        };

        switch (record.CODE) {
            case '[TI]':
                processedRecord.title = cleanText(record.RESULT);
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

        // socialScrapeLogger.debug(`Processed record for URL: ${processedRecord.url}`);
        return processedRecord;
    } catch (error) {
        socialScrapeLogger.error(`Error processing record: ${error.message}`);
        socialScrapeLogger.error(`Record data: ${JSON.stringify(record)}`);
        return null;
    }
};

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

    // Ensure indexes exist
    // await ensureIndexes();

    return new Promise((resolve, reject) => {
        const parser = csv.parse({
            columns: true,
            skip_empty_lines: true,
            relax_column_count: true,
            relax_quotes: true, // Be more flexible with quotes
            // skip_records_with_error: true, // Skip records with parsing errors
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
                        importProgressTracker.processed = processed;

                        // Process in batches when we have enough records
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
                    skippedLines++;
                    // console the exact line that is causing the error with the line number and data could be read from the parser.read()
                    const line = parser.read();
                    // log these using logger.warn
                    socialScrapeLogger.error(`Skipping malformed line: ${error.message} at line: ${line}`);



                    // Add to errors but don't stop the process
                    importProgressTracker.errors.push({
                        filename,
                        error: `Skipped malformed line: ${error.message}`
                    });
                }
            }
        });

        parser.on('end', async () => {
            try {
                // Process remaining records
                if (currentBatch.length > 0) {
                    batches.push([...currentBatch]);
                }
                if (batches.length > 0) {
                    await processBatchesInParallel(batches, filename, processed);
                }

                // Log summary of skipped lines
                if (skippedLines > 0) {
                    socialScrapeLogger.info(`Completed processing ${filename}. Processed: ${processed}, Skipped: ${skippedLines} malformed lines`);
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
            // For CSV parsing errors, log but don't stop the entire process
            const errorMessage = `CSV parsing error (continuing with valid lines): ${error.message}`;
            socialScrapeLogger.warn(`Error in ${filename}: ${errorMessage}`);

            skippedLines++;
            importProgressTracker.errors.push({
                filename,
                error: errorMessage
            });

            // Don't reject the promise, let it continue processing
            // The parser will skip the problematic line and continue
        });

        // Use streams with smaller chunks for better memory management
        fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 }) // 1MB chunks
            .pipe(parser);
    });
};

const getImportFiles = async (isBlackList = false) => {
    try {
        await ensureImportDirectory();
        if (isBlackList) {
            socialScrapeLogger.info('Reading blacklist directory:', BLACKLIST_DIR);
        }
        else {
            socialScrapeLogger.info('Reading import directory:', IMPORT_DIR);
        }

        if (isBlackList) {
            const files = await fs.promises.readdir(BLACKLIST_DIR);
            return files.filter(file => file.endsWith('.csv'));

        }
        else {
            const files = await fs.promises.readdir(IMPORT_DIR);
            return files.filter(file => file.endsWith('.csv'));
        }

    } catch (error) {
        socialScrapeLogger.error('Error reading import directory:', error);
        return [];
    }
};

const getBlacklistFiles = async () => {
    try {
        await ensureImportDirectory();
        const files = await fs.promises.readdir(BLACKLIST_DIR);
        return files.filter(file => file.endsWith('.csv'));
    } catch (error) {
        socialScrapeLogger.error('Error reading blacklist directory:', error);
        throw new Error('Failed to read blacklist directory');
    }
};


const processBlacklistFile = async (filePath, urlColumn, processId) => {
    try {
        // Get or create progress tracker for this process
        let progressTracker = blacklistProgressStore.get(processId);
        if (!progressTracker) {
            progressTracker = {
                currentFile: null,
                processed: 0,
                total: 0,
                upserted: 0,
                modified: 0,
                errors: [],
                isComplete: false
            };
            blacklistProgressStore.set(processId, progressTracker);
        }

        // Reset progress for new file
        progressTracker.currentFile = path.basename(filePath);
        progressTracker.processed = 0;
        progressTracker.total = 0;
        progressTracker.upserted = 0;
        progressTracker.modified = 0;
        progressTracker.errors = [];
        progressTracker.isComplete = false;

        // Create logs directory if it doesn't exist
        const logsDir = path.join(process.cwd(), 'logs', 'social_scrape');
        await fs.promises.mkdir(logsDir, { recursive: true });
        const logFile = path.join(logsDir, 'blacklisted_logs.log');

        const fileContent = await fs.promises.readFile(filePath, 'utf-8');
        const records = fileContent.split('\n').filter(line => line.trim());
        progressTracker.total = records.length;

        // Log start of processing
        await fs.promises.appendFile(logFile, `\n[${new Date().toISOString()}] Starting processing of file: ${path.basename(filePath)}\n`);

        for (const record of records) {
            try {
                const columns = record.split(',');
                if (columns.length < urlColumn) {
                    const errorMsg = `Invalid record format: ${record}`;
                    progressTracker.errors.push(errorMsg);
                    await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] ${errorMsg}\n`);
                    continue;
                }

                let url = columns[urlColumn - 1].trim();
                if (!url) {
                    const errorMsg = `Empty URL in record: ${record}`;
                    progressTracker.errors.push(errorMsg);
                    await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] ${errorMsg}\n`);
                    continue;
                }

                // Clean the URL
                url = url
                    .replace(/^(https?:\/\/)/i, '')
                    .replace(/^www\./i, '')
                    .replace(/^([^/]+).*?$/, '$1')
                    .toLowerCase();

                if (!isValidDomain(url)) {
                    const errorMsg = `Invalid domain format: ${url}`;
                    progressTracker.errors.push(errorMsg);
                    await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] ${errorMsg}\n`);
                    continue;
                }

                // Create a new document with default values
                const defaultDoc = {
                    url,
                    date: new Date()
                };

                const result = await SocialScrape.findOneAndUpdate(
                    { url },
                    {
                        $set: {
                            is_blacklisted: true
                        },
                        $setOnInsert: defaultDoc
                    },
                    {
                        upsert: true,
                        new: true,
                        setDefaultsOnInsert: true
                    }
                );

                if (result.isNew) {
                    progressTracker.upserted++;
                    await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] Inserted new record for URL: ${url}\n`);
                } else if (result.isModified('is_blacklisted')) {
                    progressTracker.modified++;
                    await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] Updated blacklist status for URL: ${url}\n`);
                }

                progressTracker.processed++;
                blacklistEventEmitter.emit('progress', { processId, ...progressTracker });

            } catch (error) {
                const errorMsg = `Error processing record: ${error.message}`;
                progressTracker.errors.push(errorMsg);
                await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] ${errorMsg}\n`);
            }
        }

        // Archive the file after processing
        const archiveDir = path.join(process.cwd(), 'imports', 'social_scrape_blacklisted', 'completed_' + new Date().toISOString().split('T')[0]);
        await archiveFile(filePath, {
            archiveDir,
            useTimestamp: true,
            timestampFormat: 'ISO',
            prefix: 'blacklist'
        });

        // Log completion
        await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] Processing completed. Processed: ${progressTracker.processed}, Upserted: ${progressTracker.upserted}, Modified: ${progressTracker.modified}, Errors: ${progressTracker.errors.length}\n`);

        progressTracker.isComplete = true;
        blacklistEventEmitter.emit('progress', { processId, ...progressTracker });

    } catch (error) {
        const errorMsg = `Error processing blacklist file: ${error.message}`;
        socialScrapeLogger.error(errorMsg);
        throw error;
    }
};

const getCollectionStats = async () => {
    return await SocialScrape.countDocuments();
};

const getBlacklistProgress = (processId) => {
    const progress = blacklistProgressStore.get(processId);
    return progress ? { ...progress } : null;
};

// Phone number validation utility
const isValidPhoneNumber = (phone) => {
    if (!phone || typeof phone !== 'string') return false;

    // Clean the phone number first
    const cleanedPhone = cleanPhoneNumber(phone);
    if (!cleanedPhone) return false;

    let digitsOnly;
    if (cleanedPhone.includes(']')) {
        // Extract number part after the closing bracket
        const parts = cleanedPhone.split('] ');
        if (parts.length === 2) {
            digitsOnly = parts[1].replace(/\D/g, '');
        } else {
            digitsOnly = cleanedPhone.replace(/\D/g, '');
        }
    } else {
        // No formatting, just remove non-digits
        digitsOnly = cleanedPhone.replace(/\D/g, '');
    }

    // Check if it's exactly 10 or 11 digits
    if (digitsOnly.length !== 10 && digitsOnly.length !== 11) {
        socialScrapeLogger.debug(`Phone validation failed for "${phone}" - cleaned to "${cleanedPhone}" with ${digitsOnly.length} digits (expected 10 or 11)`);
        return false;
    }

    socialScrapeLogger.debug(`Phone validation passed for "${phone}" - cleaned to "${cleanedPhone}" with ${digitsOnly.length} digits`);
    return true;
};

// Clean phone number for storage
const cleanPhoneNumber = (phone, url = '') => {
    if (!phone || typeof phone !== 'string') return null;

    let cleaned = phone.trim();
    socialScrapeLogger.debug(`Cleaning phone number: "${phone}" for URL: "${url}"`);

    // Remove spaces
    cleaned = cleaned.replace(/\s+/g, '');

    // Remove dashes
    cleaned = cleaned.replace(/-/g, '');

    // Remove dots (decimal points)
    cleaned = cleaned.replace(/\./g, '');

    // Remove brackets (both round and square brackets)
    cleaned = cleaned.replace(/[\(\)\[\]]/g, '');

    socialScrapeLogger.debug(`After basic cleaning: "${cleaned}"`);

    // Check for valid country codes and format accordingly
    const formattedPhone = formatPhoneWithCountryCode(cleaned, url);

    if (!formattedPhone) {
        socialScrapeLogger.debug(`formatPhoneWithCountryCode returned null for "${cleaned}"`);
        return null;
    }

    socialScrapeLogger.debug(`Final formatted phone: "${formattedPhone}"`);
    return formattedPhone;
};

// Format phone number with country code validation
const formatPhoneWithCountryCode = (phone, url = '') => {
    // Import country phone codes
    const countryPhoneCodes = require('../utils/phone_country_code');

    // Check if URL is UK domain for special handling
    const isUKDomain = url && (url.endsWith('.co.uk') || url.endsWith('.uk'));

    socialScrapeLogger.debug(`Formatting phone: "${phone}" for URL: "${url}" (UK domain: ${isUKDomain})`);

    // Check if it starts with + (international format)
    if (phone.startsWith('+')) {
        socialScrapeLogger.debug(`Phone starts with +, checking country codes`);
        // Find matching country code
        for (const country of countryPhoneCodes) {
            const countryCode = '+' + country.phone;

            if (phone.startsWith(countryCode)) {
                const numberPart = phone.substring(countryCode.length);
                const digitsOnly = numberPart.replace(/\D/g, '');

                socialScrapeLogger.debug(`Found country match: ${country.label} (${countryCode}), number part: "${numberPart}", digits: "${digitsOnly}" (length: ${digitsOnly.length})`);

                // Check if length is valid for this country
                const isValidLength = checkPhoneLength(digitsOnly, country);

                socialScrapeLogger.debug(`Length validation for ${country.label}: ${isValidLength} (expected: ${country.phoneLength})`);

                if (isValidLength) {
                    // Format with proper spacing
                    const result = `[${countryCode}] ${digitsOnly}`;
                    socialScrapeLogger.debug(`Valid length, returning: "${result}"`);
                    return result;
                } else {
                    // Try to add leading zero if length is short
                    const expectedLength = getExpectedLength(country);
                    if (digitsOnly.length === expectedLength - 1) {
                        const result = `[${countryCode}] 0${digitsOnly}`;
                        socialScrapeLogger.debug(`Added leading zero, returning: "${result}"`);
                        return result;
                    } else {
                        socialScrapeLogger.debug(`Length mismatch: got ${digitsOnly.length}, expected ${expectedLength}, cannot add leading zero`);
                    }
                }
            }
        }
        socialScrapeLogger.debug(`No country code match found for "${phone}"`);
    }

    // Check without + prefix
    socialScrapeLogger.debug(`Checking without + prefix`);
    for (const country of countryPhoneCodes) {
        const countryCode = country.phone;

        if (phone.startsWith(countryCode)) {
            const numberPart = phone.substring(countryCode.length);
            const digitsOnly = numberPart.replace(/\D/g, '');

            socialScrapeLogger.debug(`Found country match (no +): ${country.label} (${countryCode}), number part: "${numberPart}", digits: "${digitsOnly}" (length: ${digitsOnly.length})`);

            // Check if length is valid for this country
            const isValidLength = checkPhoneLength(digitsOnly, country);

            socialScrapeLogger.debug(`Length validation for ${country.label}: ${isValidLength} (expected: ${country.phoneLength})`);

            if (isValidLength) {
                // Format with proper spacing
                const result = `[+${countryCode}] ${digitsOnly}`;
                socialScrapeLogger.debug(`Valid length, returning: "${result}"`);
                return result;
            } else {
                // Try to add leading zero if length is short
                const expectedLength = getExpectedLength(country);
                if (digitsOnly.length === expectedLength - 1) {
                    const result = `[+${countryCode}] 0${digitsOnly}`;
                    socialScrapeLogger.debug(`Added leading zero, returning: "${result}"`);
                    return result;
                } else {
                    socialScrapeLogger.debug(`Length mismatch: got ${digitsOnly.length}, expected ${expectedLength}, cannot add leading zero`);
                }
            }
        }
    }

    // If no valid country code found, keep as-is if length is 10-11 digits
    const digitsOnly = phone.replace(/\D/g, '');
    socialScrapeLogger.debug(`No country code match, checking if digits only (${digitsOnly.length}) is 10-11 digits`);
    if (digitsOnly.length === 10 || digitsOnly.length === 11) {
        socialScrapeLogger.debug(`Valid length for digits only, returning: "${digitsOnly}"`);
        return digitsOnly;
    }

    socialScrapeLogger.debug(`No valid format found for "${phone}"`);
    return null;
};

// Check if phone number length is valid for a country
const checkPhoneLength = (digits, country) => {
    if (country.phoneLength) {
        if (Array.isArray(country.phoneLength)) {
            return country.phoneLength.includes(digits.length);
        } else {
            return digits.length === country.phoneLength;
        }
    }

    // Fallback for countries with min/max
    if (country.min && country.max) {
        return digits.length >= country.min && digits.length <= country.max;
    }

    return false;
};

// Get expected length for a country
const getExpectedLength = (country) => {
    if (country.phoneLength) {
        if (Array.isArray(country.phoneLength)) {
            return country.phoneLength[0]; // Use first length as default
        } else {
            return country.phoneLength;
        }
    }

    if (country.min) {
        return country.min;
    }

    return 10; // Default fallback
};

// Convert various country codes to UK format (legacy function - keeping for backward compatibility)
const convertCountryCodeToUK = (phone) => {
    // This function is now deprecated in favor of the new country code validation
    // Keeping it for backward compatibility but it should not be used in new logic
    return phone;
};

const getPhoneFiles = async () => {
    try {
        await ensureImportDirectory();
        const files = await fs.promises.readdir(PHONE_DIR);
        return files.filter(file => file.endsWith('.csv'));
    } catch (error) {
        socialScrapeLogger.error('Error reading phone directory:', error);
        throw new Error('Failed to read phone directory');
    }
};

const processPhoneFile = async (filePath, processId) => {
    try {
        // Get or create progress tracker for this process
        let progressTracker = phoneProgressStore.get(processId);
        if (!progressTracker) {
            progressTracker = {
                currentFile: null,
                processed: 0,
                total: 0,
                updated: 0,
                created: 0,
                errors: [],
                isComplete: false,
                totalFiles: 0,
                completedFiles: 0
            };
            phoneProgressStore.set(processId, progressTracker);
            socialScrapeLogger.warn(`Created new progress tracker for ${processId} in service (should have been initialized in controller)`);
        } else {
            socialScrapeLogger.info(`Using existing progress tracker for ${processId}: totalFiles=${progressTracker.totalFiles}`);
        }

        // Update current file (don't reset other progress)
        progressTracker.currentFile = path.basename(filePath);
        // Don't reset processed, updated, created, errors, or total - accumulate them
        progressTracker.isComplete = false;

        // Create logs directory if it doesn't exist
        const logsDir = path.join(process.cwd(), 'logs', 'social_scrape');
        await fs.promises.mkdir(logsDir, { recursive: true });
        const logFile = path.join(logsDir, 'phone_logs.log');

        // Log start of processing
        await fs.promises.appendFile(logFile, `\n[${new Date().toISOString()}] Starting phone processing of file: ${path.basename(filePath)}\n`);

        return new Promise((resolve, reject) => {
            // Add timeout to prevent hanging
            const timeout = setTimeout(() => {
                const errorMsg = 'Phone processing timeout - process took too long';
                progressTracker.errors.push(errorMsg);
                progressTracker.completedFiles++;
                socialScrapeLogger.info(`Marked file ${path.basename(filePath)} as completed (timeout). Total completed: ${progressTracker.completedFiles}/${progressTracker.totalFiles}`);
                phoneEventEmitter.emit('progress', { processId, ...progressTracker });
                reject(new Error(errorMsg));
            }, 300000); // 5 minutes timeout

            const parser = csv.parse({
                columns: false, // No headers
                skip_empty_lines: true,
                relax_column_count: true, // Allow flexible column count
                relax_quotes: true, // Be more flexible with quotes
                highWaterMark: 1024 * 1024 // 1MB chunks
            });

            let urlPhoneMap = new Map(); // To group phones by URL
            let urlCountMap = new Map(); // To count occurrences of each URL
            let lineNumber = 0;
            let totalLines = 0;

            // First pass: count total lines for progress calculation
            const countLines = () => {
                return new Promise((resolve) => {
                    const lineCount = fs.readFileSync(filePath, 'utf-8').split('\n').filter(line => line.trim()).length;
                    resolve(lineCount);
                });
            };

            countLines().then((lineCount) => {
                totalLines = lineCount;
                progressTracker.total += lineCount;
                phoneEventEmitter.emit('progress', { processId, ...progressTracker });
            });

            parser.on('readable', async () => {
                let record;
                while ((record = parser.read()) !== null) {
                    lineNumber++;
                    try {
                        // Skip records with insufficient data
                        if (!record || record.length < 3) {
                            const errorMsg = `Line ${lineNumber}: Invalid record format - expected at least 3 columns, got ${record ? record.length : 0}`;
                            progressTracker.errors.push(errorMsg);
                            continue;
                        }

                        const url = record[0]?.trim();
                        const code = record[1]?.trim();
                        const phoneData = record[2]?.trim();
                        // Note: record[3] is the date, which we don't need for phone processing

                        if (!url || !code || !phoneData) {
                            const errorMsg = `Line ${lineNumber}: Missing required data - URL: ${!!url}, Code: ${!!code}, Phone: ${!!phoneData}`;
                            progressTracker.errors.push(errorMsg);
                            continue;
                        }

                        // Only process records with [PN] code
                        if (code !== '[PN]') {
                            continue;
                        }

                        // Clean and validate URL
                        const cleanUrl = url
                            .replace(/^(https?:\/\/)/i, '')
                            .replace(/^www\./i, '')
                            .replace(/^([^/]+).*?$/, '$1')
                            .toLowerCase();

                        if (!isValidDomain(cleanUrl)) {
                            const errorMsg = `Line ${lineNumber}: Invalid domain format: ${cleanUrl}`;
                            progressTracker.errors.push(errorMsg);
                            continue;
                        }

                        // Count URL occurrences
                        urlCountMap.set(cleanUrl, (urlCountMap.get(cleanUrl) || 0) + 1);

                        // Validate phone number
                        if (!isValidPhoneNumber(phoneData)) {
                            const errorMsg = `Line ${lineNumber}: Invalid phone number: ${phoneData} for URL: ${cleanUrl}`;
                            progressTracker.errors.push(errorMsg);
                            continue;
                        }

                        // Clean phone number
                        const cleanPhone = cleanPhoneNumber(phoneData, cleanUrl);
                        if (!cleanPhone) {
                            const errorMsg = `Line ${lineNumber}: Failed to clean phone number: ${phoneData} for URL: ${cleanUrl}`;
                            progressTracker.errors.push(errorMsg);
                            continue;
                        }

                        // Group phones by URL
                        if (!urlPhoneMap.has(cleanUrl)) {
                            urlPhoneMap.set(cleanUrl, new Set());
                        }
                        urlPhoneMap.get(cleanUrl).add(cleanPhone);

                        progressTracker.processed++;
                        phoneEventEmitter.emit('progress', { processId, ...progressTracker });

                        // Process in batches
                        if (urlPhoneMap.size >= BATCH_SIZE) {
                            await processPhoneBatch(urlPhoneMap, progressTracker, logFile);
                            urlPhoneMap.clear();
                        }

                    } catch (error) {
                        const errorMsg = `Line ${lineNumber}: Error processing record: ${error.message}`;
                        progressTracker.errors.push(errorMsg);
                        socialScrapeLogger.error(`Phone processing error on line ${lineNumber}:`, error);
                    }
                }
            });

            parser.on('end', async () => {
                try {
                    clearTimeout(timeout); // Clear timeout on successful completion

                    // Filter out URLs with more than 3 rows
                    const filteredUrlPhoneMap = new Map();
                    for (const [url, phoneSet] of urlPhoneMap) {
                        const urlCount = urlCountMap.get(url) || 0;
                        if (urlCount <= 3) {
                            filteredUrlPhoneMap.set(url, phoneSet);
                        } else {
                            const errorMsg = `Skipped URL ${url} - has ${urlCount} rows (more than 3)`;
                            progressTracker.errors.push(errorMsg);
                            await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] ${errorMsg}\n`);
                        }
                    }

                    // Process remaining records
                    if (filteredUrlPhoneMap.size > 0) {
                        await processPhoneBatch(filteredUrlPhoneMap, progressTracker, logFile);
                    }

                    // Archive the file after processing
                    const archiveDir = path.join(process.cwd(), 'imports', 'social_scrape_phone', 'completed_' + new Date().toISOString().split('T')[0]);
                    await archiveFile(filePath, {
                        archiveDir,
                        useTimestamp: true,
                        timestampFormat: 'ISO',
                        prefix: 'phone'
                    });

                    // Log completion
                    await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] Phone processing completed. Processed: ${progressTracker.processed}, Updated: ${progressTracker.updated}, Created: ${progressTracker.created}, Errors: ${progressTracker.errors.length}\n`);

                    // Mark file as completed
                    progressTracker.completedFiles++;
                    socialScrapeLogger.info(`Marked file ${path.basename(filePath)} as completed. Total completed: ${progressTracker.completedFiles}/${progressTracker.totalFiles}`);

                    // Check if all files are completed
                    if (progressTracker.completedFiles >= progressTracker.totalFiles) {
                        progressTracker.isComplete = true;
                        progressTracker.currentFile = null; // Clear current file when complete
                        socialScrapeLogger.info(`All files completed for process ${processId}. Setting isComplete = true`);

                        // Schedule cleanup of this progress tracker after 1 hour
                        setTimeout(() => {
                            if (phoneProgressStore.has(processId)) {
                                phoneProgressStore.delete(processId);
                                socialScrapeLogger.info(`Cleaned up completed progress tracker for process: ${processId}`);
                            }
                        }, 60 * 60 * 1000); // 1 hour
                    }

                    // Update progress tracker with timestamp
                    updateProgressTracker(processId, progressTracker);
                    phoneEventEmitter.emit('progress', { processId, ...progressTracker });
                    resolve({ filename: path.basename(filePath), processed: progressTracker.processed });
                } catch (error) {
                    clearTimeout(timeout);
                    // Mark file as completed even on error
                    progressTracker.completedFiles++;
                    socialScrapeLogger.info(`Marked file ${path.basename(filePath)} as completed (CSV error). Total completed: ${progressTracker.completedFiles}/${progressTracker.totalFiles}`);

                    // Check if all files are completed
                    if (progressTracker.completedFiles >= progressTracker.totalFiles) {
                        progressTracker.isComplete = true;
                        progressTracker.currentFile = null;
                        socialScrapeLogger.info(`All files completed for process ${processId} (with error). Setting isComplete = true`);

                        // Schedule cleanup of this progress tracker after 1 hour
                        setTimeout(() => {
                            if (phoneProgressStore.has(processId)) {
                                phoneProgressStore.delete(processId);
                                socialScrapeLogger.info(`Cleaned up completed progress tracker for process: ${processId}`);
                            }
                        }, 60 * 60 * 1000); // 1 hour
                    }

                    // Update progress tracker with timestamp
                    updateProgressTracker(processId, progressTracker);
                    phoneEventEmitter.emit('progress', { processId, ...progressTracker });
                    reject(error);
                }
            });

            parser.on('error', (error) => {
                clearTimeout(timeout);
                const errorMsg = `CSV parsing error: ${error.message}`;
                progressTracker.errors.push(errorMsg);
                socialScrapeLogger.error('CSV parsing error:', error);

                // Mark file as completed even on error
                progressTracker.completedFiles++;
                socialScrapeLogger.info(`Marked file ${path.basename(filePath)} as completed (CSV error). Total completed: ${progressTracker.completedFiles}/${progressTracker.totalFiles}`);

                // Check if all files are completed
                if (progressTracker.completedFiles >= progressTracker.totalFiles) {
                    progressTracker.isComplete = true;
                    progressTracker.currentFile = null;
                    socialScrapeLogger.info(`All files completed for process ${processId} (with error). Setting isComplete = true`);

                    // Schedule cleanup of this progress tracker after 1 hour
                    setTimeout(() => {
                        if (phoneProgressStore.has(processId)) {
                            phoneProgressStore.delete(processId);
                            socialScrapeLogger.info(`Cleaned up completed progress tracker for process: ${processId}`);
                        }
                    }, 60 * 60 * 1000); // 1 hour
                }

                // Update progress tracker with timestamp
                updateProgressTracker(processId, progressTracker);
                phoneEventEmitter.emit('progress', { processId, ...progressTracker });

                reject(error);
            });

            // Use streams with smaller chunks for better memory management
            fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 }) // 1MB chunks
                .pipe(parser);
        });

    } catch (error) {
        const errorMsg = `Error processing phone file: ${error.message}`;
        socialScrapeLogger.error(errorMsg);
        throw error;
    }
};

const processPhoneBatch = async (urlPhoneMap, progressTracker, logFile) => {
    try {
        for (const [urlKey, phoneSet] of urlPhoneMap) {
            try {
                let phones = Array.from(phoneSet);

                // Limit to maximum 3 phone numbers per URL
                if (phones.length > 3) {
                    const originalCount = phones.length;
                    phones = phones.slice(0, 3);
                    const skippedMsg = `Limited phone numbers for URL ${urlKey} from ${originalCount} to 3`;
                    progressTracker.errors.push(skippedMsg);
                    await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] ${skippedMsg}\n`);
                }

                // Find ALL existing records with this URL
                const existingRecords = await SocialScrape.find({ url: urlKey });

                socialScrapeLogger.debug(`Processing URL: ${urlKey}, found ${existingRecords.length} existing records`);

                if (existingRecords.length > 0) {
                    // Update all existing records with this URL
                    for (const existingRecord of existingRecords) {
                        const existingPhones = existingRecord.phone || [];
                        const newPhones = [...new Set([...existingPhones, ...phones])];

                        // Limit to maximum 3 phone numbers total
                        const finalPhones = newPhones.slice(0, 3);

                        socialScrapeLogger.debug(`Updating record ${existingRecord._id} for URL ${urlKey}: existing phones [${existingPhones.join(', ')}], new phones [${phones.join(', ')}], final phones [${finalPhones.join(', ')}]`);

                        await SocialScrape.updateOne(
                            { _id: existingRecord._id },
                            { $set: { phone: finalPhones } }
                        );
                    }

                    progressTracker.updated++;
                    await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] Updated phone numbers for URL: ${urlKey} (${existingRecords.length} records), phones: ${phones.join(', ')}\n`);
                } else {
                    // Create new record only if no existing records found
                    const newRecord = {
                        url: urlKey,
                        date: new Date(),
                        phone: phones
                    };

                    socialScrapeLogger.debug(`Creating new record for URL ${urlKey} with phones [${phones.join(', ')}]`);

                    await SocialScrape.create(newRecord);

                    progressTracker.created++;
                    await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] Created new record for URL: ${urlKey}, phones: ${phones.join(', ')}\n`);
                }

            } catch (error) {
                const errorMsg = `Error processing URL ${urlKey}: ${error.message}`;
                progressTracker.errors.push(errorMsg);
                socialScrapeLogger.error(`Error processing URL ${urlKey}:`, error);
                await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] ${errorMsg}\n`);
            }
        }
    } catch (error) {
        socialScrapeLogger.error('Error processing phone batch:', error);
        throw error;
    }
};

const getPhoneProgress = (processId) => {
    const progress = phoneProgressStore.get(processId);
    return progress ? { ...progress } : null;
};

// Clean up old progress trackers to prevent memory leaks
const cleanupOldProgressTrackers = () => {
    const now = Date.now();
    const maxAge = 24 * 60 * 60 * 1000; // 24 hours

    for (const [processId, progress] of phoneProgressStore.entries()) {
        // If process is complete and older than 24 hours, remove it
        if (progress.isComplete && progress.lastUpdated && (now - progress.lastUpdated) > maxAge) {
            phoneProgressStore.delete(processId);
            socialScrapeLogger.info(`Cleaned up old progress tracker for process: ${processId}`);
        }
    }
};

// Update progress tracker with timestamp
const updateProgressTracker = (processId, updates) => {
    const progress = phoneProgressStore.get(processId);
    if (progress) {
        Object.assign(progress, updates, { lastUpdated: Date.now() });
        phoneProgressStore.set(processId, progress);
    }
};

// Utility function to find URLs with multiple records (same URL, different dates)
const findDuplicateUrls = async () => {
    try {
        // Find URLs that have multiple records with different dates
        const duplicates = await SocialScrape.aggregate([
            {
                $group: {
                    _id: "$url",
                    count: { $sum: 1 },
                    records: { $push: { _id: "$_id", date: "$date", phone: "$phone" } }
                }
            },
            {
                $match: {
                    count: { $gt: 1 }
                }
            },
            {
                $sort: { count: -1 }
            }
        ]);

        socialScrapeLogger.info(`Found ${duplicates.length} URLs with multiple records (different dates)`);

        for (const duplicate of duplicates.slice(0, 10)) { // Show first 10
            socialScrapeLogger.info(`URL: ${duplicate._id}, Count: ${duplicate.count}, Records: ${duplicate.records.map(r => r._id).join(', ')}`);
        }

        return duplicates;
    } catch (error) {
        socialScrapeLogger.error('Error finding URLs with multiple records:', error);
        return [];
    }
};

const SocialScrapeService = {
    getImportFiles,
    getBlacklistFiles,
    getPhoneFiles,
    getCollectionStats,
    processFile,
    processBlacklistFile,
    processPhoneFile,
    getImportProgress: () => ({ ...importProgressTracker }),
    resetImportProgress,
    setImportRunning,
    getBlacklistProgress,
    getPhoneProgress,
    findDuplicateUrls,
    cleanupOldProgressTrackers,
    updateProgressTracker
};

// Start periodic cleanup of old progress trackers (every 6 hours)
setInterval(() => {
    cleanupOldProgressTrackers();
}, 6 * 60 * 60 * 1000); // 6 hours

module.exports = {
    SocialScrapeService,
    IMPORT_DIR,
    BLACKLIST_DIR,
    PHONE_DIR,
    importEventEmitter,
    blacklistEventEmitter,
    phoneEventEmitter,
    phoneProgressStore
};