// services/AddressMaster.service.js
const { parse } = require('csv-parse');
const fs = require('fs');
const path = require('path');
const AddressMaster = require('../models/AddressMaster');
const addressMasterLogger = require('../config/loggers/addressMasterLogger');
const { archiveFile } = require('../utils/fileUtils');

// Configuration
const BATCH_SIZE = 2000;
const PARALLEL_BATCHES = 2;
const IMPORT_DIR = path.join(__dirname, '../imports/address_master');

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
        addressMasterLogger.error('Error creating import directory:', error);
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
    addressMasterLogger.info('Reset import progress tracker');
};

const setImportRunning = (running) => {
    importProgressTracker.isRunning = running;
    addressMasterLogger.info(`Set import running status to: ${running}`);
};

const moveCompletedFile = async (filePath) => {
    try {
        const filename = path.basename(filePath);
        const today = new Date().toISOString().split('T')[0];
        const completedDir = path.join(IMPORT_DIR, `completed_${today}`);

        await fs.promises.mkdir(completedDir, { recursive: true });
        const newPath = path.join(completedDir, filename);
        await fs.promises.rename(filePath, newPath);

        addressMasterLogger.info(`Moved file ${filename} to completed directory`);
    } catch (error) {
        addressMasterLogger.error(`Error moving file: ${error.message}`);
        throw error;
    }
};

// Generate random date for backward compatibility
const generateRandomDate = (format = 'd/m/Y') => {
    const startDate = new Date();
    startDate.setFullYear(startDate.getFullYear() - 2);
    
    const endDate = new Date();
    endDate.setFullYear(endDate.getFullYear() - 1);
    
    const randomTimestamp = Math.floor(Math.random() * (endDate.getTime() - startDate.getTime())) + startDate.getTime();
    const randomDate = new Date(randomTimestamp);
    
    const day = String(randomDate.getDate()).padStart(2, '0');
    const month = String(randomDate.getMonth() + 1).padStart(2, '0');
    const year = randomDate.getFullYear();
    
    return `${day}/${month}/${year}`;
};

// Clean text data
const cleanText = (text) => {
    if (!text) return '';
    return text.toString().trim().replace(/\\/g, '/');
};

// Get district from postcode (simplified version)
const getDistrictByPostcode = (postcode) => {
    if (!postcode) return null;
    
    // Extract district from postcode (first part before space)
    const parts = postcode.trim().split(' ');
    if (parts.length >= 2) {
        return parts[0];
    }
    
    // If no space, take first 2-4 characters
    const match = postcode.match(/^([A-Z]{1,2}\d{1,2}[A-Z]?)/i);
    return match ? match[1] : postcode.substring(0, 4);
};

// Process a single CSV record into AddressMaster format
const processRecord = (record, index) => {
    try {
        // Expecting CSV format: F1 (postcode), F2, F3, F4... (address fields)
        const postcode = cleanText(record[0] || record.F1);
        
        // Basic validation
        if (!postcode) {
            addressMasterLogger.warn(`Skipping record ${index}: Missing postcode`);
            return null;
        }

        // Skip header row
        if (postcode.toLowerCase() === 'postcode' || postcode.toLowerCase() === 'f1') {
            return null;
        }

        // Get district from postcode
        const district = getDistrictByPostcode(postcode);
        if (!district) {
            addressMasterLogger.warn(`Skipping record ${index}: Could not determine district for postcode ${postcode}`);
            return null;
        }

        // Process address fields (everything after postcode)
        const addressFields = [];
        for (let i = 1; i < record.length; i++) {
            const field = cleanText(record[i]);
            if (field && field !== district) {
                addressFields.push(field);
            }
        }

        // Validate we have address fields
        if (addressFields.length === 0) {
            addressMasterLogger.warn(`Skipping record ${index}: No valid address fields`);
            return null;
        }

        const processedRecord = {
            postcode: postcode.toUpperCase(),
            district: district,
            address: addressFields,
            dateCreated: generateRandomDate()
        };

        return processedRecord;
    } catch (error) {
        addressMasterLogger.error(`Error processing record ${index}: ${error.message}`);
        return null;
    }
};

// Process CSV file
const processFile = async (filePath) => {
    return new Promise((resolve, reject) => {
        const filename = path.basename(filePath);
        const records = [];
        let processedCount = 0;
        let skippedLines = 0;
        
        addressMasterLogger.info(`Starting to process file: ${filename}`);
        importProgressTracker.currentFile = filename;

        const parser = parse({
            columns: false,
            skip_empty_lines: true,
            trim: true,
            from_line: 2, // Skip header row
            relax_column_count: true
        });

        parser.on('readable', function () {
            let record;
            while ((record = parser.read()) !== null) {
                const processedRecord = processRecord(record, processedCount);
                if (processedRecord) {
                    records.push(processedRecord);
                } else {
                    skippedLines++;
                }
                processedCount++;
            }
        });

        parser.on('error', function (error) {
            const errorMessage = `CSV parsing error (continuing with valid lines): ${error.message}`;
            addressMasterLogger.warn(`Error in ${filename}: ${errorMessage}`);

            skippedLines++;
            importProgressTracker.errors.push({
                filename,
                error: errorMessage
            });
        });

        parser.on('end', async function () {
            try {
                addressMasterLogger.info(`Parsed ${records.length} valid records from ${filename} (skipped ${skippedLines} invalid lines)`);
                // Set total so frontend can compute progress percentage
                importProgressTracker.total = records.length;
                
                if (records.length === 0) {
                    addressMasterLogger.warn(`No valid records found in ${filename}`);
                    await moveCompletedFile(filePath);
                    resolve({ processed: 0, upserted: 0, modified: 0 });
                    return;
                }

                // Process records in batches
                let totalUpserted = 0;
                let totalModified = 0;

                // Convert records array into batches queue
                const batches = [];
                for (let i = 0; i < records.length; i += BATCH_SIZE) {
                    batches.push(records.slice(i, i + BATCH_SIZE));
                }

                // Simple bounded concurrency worker
                let active = 0;
                let idx = 0;
                const results = { upserted: 0, modified: 0 };

                const runNext = async () => {
                    if (idx >= batches.length) return;
                    const batch = batches[idx++];
                    active++;
                    const start = Date.now();
                    try {
                        const bulkOps = batch.map(doc => ({
                            updateOne: {
                                filter: { 
                                    postcode: doc.postcode,
                                    address: doc.address
                                },
                                update: { $set: doc },
                                upsert: true
                            }
                        }));

                        // Use unordered bulkWrite with relaxed writeConcern for throughput
                        const result = await AddressMaster.bulkWrite(bulkOps, {
                            ordered: false,
                            writeConcern: { w: 1 },
                            bypassDocumentValidation: true
                        });

                        const elapsed = Date.now() - start;
                        const up = result.upsertedCount || 0;
                        const mod = result.modifiedCount || 0;

                        results.upserted += up;
                        results.modified += mod;

                        importProgressTracker.processed += batch.length;
                        importProgressTracker.upserted += up;
                        importProgressTracker.modified += mod;

                        addressMasterLogger.info(`Batch processed (size=${batch.length}) upserted=${up} modified=${mod} time=${elapsed}ms`);
                    } catch (err) {
                        addressMasterLogger.error(`Error processing batch in ${filename}: ${err.message}`);
                        importProgressTracker.errors.push({ filename, error: `Batch processing error: ${err.message}` });
                        // On duplicate key errors, try individual upserts
                        if (err && err.code === 11000) {
                            addressMasterLogger.warn('Duplicate key error encountered, retrying batch item-by-item');
                            for (const doc of batch) {
                                try {
                                    const r = await AddressMaster.updateOne({ 
                                        postcode: doc.postcode,
                                        address: doc.address
                                    }, { $set: doc }, { upsert: true });
                                    if (r.upsertedCount && r.upsertedCount > 0) results.upserted++;
                                    if (r.modifiedCount && r.modifiedCount > 0) results.modified++;
                                } catch (uerr) {
                                    addressMasterLogger.warn(`Failed individual upsert for ${doc.postcode}: ${uerr.message}`);
                                    importProgressTracker.errors.push({ filename, error: `Failed upsert ${doc.postcode}: ${uerr.message}` });
                                }
                            }
                        }
                    } finally {
                        active--;
                        // Start next batch if any
                        if (idx < batches.length) {
                            // If we have spare capacity, start another
                            if (active < PARALLEL_BATCHES) runNext();
                        }
                    }
                };

                // Start initial workers
                const starters = Math.min(PARALLEL_BATCHES, batches.length);
                const starterPromises = [];
                for (let s = 0; s < starters; s++) {
                    starterPromises.push(runNext());
                }

                // Wait until all batches processed
                while (idx < batches.length || active > 0) {
                    // small delay to avoid busy loop
                    // eslint-disable-next-line no-await-in-loop
                    await new Promise(res => setTimeout(res, 100));
                }

                totalUpserted += results.upserted;
                totalModified += results.modified;

                await moveCompletedFile(filePath);
                
                addressMasterLogger.info(`Completed processing ${filename}: ${totalUpserted} upserted, ${totalModified} modified`);
                resolve({ processed: records.length, upserted: totalUpserted, modified: totalModified });

            } catch (error) {
                addressMasterLogger.error(`Error saving records from ${filename}: ${error.message}`);
                importProgressTracker.errors.push({
                    filename,
                    error: `Database error: ${error.message}`
                });
                reject(error);
            }
        });

        // Use streams with smaller chunks for better memory management
        fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 }) // 1MB chunks
            .pipe(parser);
    });
};

const getImportFiles = async () => {
    try {
        await ensureImportDirectory();
        addressMasterLogger.info('Reading import directory:', IMPORT_DIR);

        const files = await fs.promises.readdir(IMPORT_DIR);
        return files.filter(file => file.endsWith('.csv'));

    } catch (error) {
        addressMasterLogger.error('Error reading import directory:', error);
        return [];
    }
};

const getCollectionStats = async () => {
    return await AddressMaster.countDocuments();
};

const AddressMasterService = {
    processFile,
    getImportFiles,
    getCollectionStats,
    getImportProgress: () => ({ ...importProgressTracker }),
    resetImportProgress,
    setImportRunning
};

module.exports = {
    AddressMasterService,
    IMPORT_DIR
};