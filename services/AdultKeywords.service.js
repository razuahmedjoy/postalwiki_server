// services/AdultKeywords.service.js
const csv = require('csv-parse');
const fs = require('fs');
const path = require('path');
const SocialScrape = require('../models/SocialScrape');
const AdultKeywordsReference = require('../models/AdultKeywordsReference');
const { adultKeywords_exact_match, adultKeywords_contains } = require('../utils/adult_keywords');
const adultKeywordsLogger = require('../config/loggers/adultKeywordsLogger');
const { isValidDomain } = require('../utils/helpers');

const MATCH_DIR = path.join(__dirname, '../update/social_scrape/match_adult_keywords');
const BATCH_SIZE = 2000; // Increased from 100 to 2000 for better performance
const PARALLEL_BATCHES = 2; // Process 2 batches in parallel
const MAX_MEMORY_USAGE = 0.8; // Stop processing if memory usage exceeds 80%

// Progress tracker for adult keywords matching
const matchingProgressTracker = {
    currentFile: null,
    processed: 0,
    total: 0,
    exactMatches: 0,
    containsMatches: 0,
    updatedRecords: 0,
    createdReferences: 0,
    errors: [],
    isComplete: false,
    isRunning: false
};

// Utility Functions
const ensureMatchDirectory = async () => {
    try {
        await fs.promises.mkdir(MATCH_DIR, { recursive: true });
    } catch (error) {
        adultKeywordsLogger.error('Error creating match directory:', error);
        throw new Error('Failed to create match directory');
    }
};

// Memory monitoring utility
const checkMemoryUsage = () => {
    const memUsage = process.memoryUsage();
    const heapUsedMB = memUsage.heapUsed / 1024 / 1024;
    const heapTotalMB = memUsage.heapTotal / 1024 / 1024;
    const memoryUsagePercent = memUsage.heapUsed / memUsage.heapTotal;
    
    adultKeywordsLogger.debug('Memory usage check', {
        heapUsed: `${Math.round(heapUsedMB)}MB`,
        heapTotal: `${Math.round(heapTotalMB)}MB`,
        memoryUsagePercent: `${(memoryUsagePercent * 100).toFixed(2)}%`
    });
    
    if (memoryUsagePercent > MAX_MEMORY_USAGE) {
        adultKeywordsLogger.warn('High memory usage detected', {
            heapUsed: `${Math.round(heapUsedMB)}MB`,
            heapTotal: `${Math.round(heapTotalMB)}MB`,
            memoryUsagePercent: `${(memoryUsagePercent * 100).toFixed(2)}%`,
            threshold: `${(MAX_MEMORY_USAGE * 100).toFixed(2)}%`
        });
        return false;
    }
    
    return true;
};

// Force garbage collection if available
const forceGarbageCollection = () => {
    if (global.gc) {
        global.gc();
        adultKeywordsLogger.debug('Forced garbage collection');
    }
};

const resetMatchingProgress = () => {
    matchingProgressTracker.currentFile = null;
    matchingProgressTracker.processed = 0;
    matchingProgressTracker.total = 0;
    matchingProgressTracker.exactMatches = 0;
    matchingProgressTracker.containsMatches = 0;
    matchingProgressTracker.updatedRecords = 0;
    matchingProgressTracker.createdReferences = 0;
    matchingProgressTracker.errors = [];
    matchingProgressTracker.isComplete = false;
    matchingProgressTracker.isRunning = false;
    adultKeywordsLogger.info('Reset adult keywords matching progress tracker');
};

const setMatchingRunning = (running) => {
    matchingProgressTracker.isRunning = running;
    adultKeywordsLogger.info(`Set adult keywords matching running status to: ${running}`);
};

const getMatchingFiles = async () => {
    try {
        await ensureMatchDirectory();
        const files = await fs.promises.readdir(MATCH_DIR);
        return files.filter(file => file.endsWith('.csv'));
    } catch (error) {
        adultKeywordsLogger.error('Error reading match directory:', error);
        throw error;
    }
};

const moveCompletedFile = async (filePath) => {
    try {
        const filename = path.basename(filePath);
        const today = new Date().toISOString().split('T')[0];
        const completedDir = path.join(MATCH_DIR, `completed_${today}`);

        // Create completed directory if it doesn't exist
        await fs.promises.mkdir(completedDir, { recursive: true });

        // Move the file
        const newPath = path.join(completedDir, filename);
        await fs.promises.rename(filePath, newPath);

        adultKeywordsLogger.info(`Moved file ${filename} to completed directory`);
    } catch (error) {
        adultKeywordsLogger.error(`Failed to move file: ${error.message}`);
        throw error;
    }
};

// Process a single record based on CODE
const processRecord = (record) => {
    try {
        const trimUrl = (url) => {
            if (!url) return '';
            return url
                .replace(/^(https?:\/\/)/i, '')
                .replace(/^www\./i, '')
                .replace(/^([^/]+).*?$/, '$1');
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
            adultKeywordsLogger.debug(`Skipping invalid domain: ${url}`);
            return null;
        }

        // Skip records with error or no data
        if (record.RESULT === 'Fetch error or no data found' || record.RESULT === 'not required') {
            // If we only have URL and no other data, skip this record
            const hasOtherData = Object.entries(record).some(([key, value]) =>
                key !== 'RESULT' && value && value.trim() !== ''
            );
            if (!hasOtherData) {
                adultKeywordsLogger.debug(`Skipping record with no data for URL: ${url}`);
                return null;
            }
        }

        // Process the record based on CODE - ignore date column
        const processedRecord = {
            url: trimUrl(url),
            date: new Date(), // Use current date for all records
            title: '',
            meta_description: '',
            keywords: ''
        };

        switch (record.CODE) {
            case '[TI]':
                processedRecord.title = cleanText(record.RESULT);
                break;
            case '[MD]':
                processedRecord.meta_description = cleanText(record.RESULT);
                break;
            case '[KW]':
                processedRecord.keywords = cleanText(record.RESULT);
                break;
        }

        return processedRecord;
    } catch (error) {
        adultKeywordsLogger.error(`Error processing record: ${error.message}`);
        return null;
    }
};

// Merge multiple records for the same URL + date
const mergeRecordsForSameUrlDate = (docs, filename) => {
    if (docs.length === 1) {
        return { ...docs[0], csv_source: filename };
    }

    // Merge multiple records for the same URL + date
    const mergedDoc = {
        url: docs[0].url,
        date: docs[0].date, // All records now have the same current date
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
        meta_description: '',
        keywords: '',
        csv_source: filename
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
        if (doc.keywords && !mergedDoc.keywords) mergedDoc.keywords = doc.keywords;

        // Merge phone arrays
        if (doc.phone && Array.isArray(doc.phone)) {
            mergedDoc.phone = [...new Set([...mergedDoc.phone, ...doc.phone])];
        }
    }

    return mergedDoc;
};

// Check if text contains exact adult keywords
const checkExactMatch = (text) => {
    if (!text) return null;
    const lowerText = text.toLowerCase();
    
    for (const keyword of adultKeywords_exact_match) {
        const lowerKeyword = keyword.toLowerCase();
        
        // Check for exact phrase match (word boundaries)
        if (lowerText.includes(lowerKeyword)) {
            const regex = new RegExp(`\\b${lowerKeyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
            if (regex.test(text)) {
                return keyword;
            }
        }
    }
    return null;
};

// Check if text contains any adult keywords from contains list
const checkContainsMatch = (text) => {
    if (!text) return [];
    const lowerText = text.toLowerCase();
    const matches = [];
    
    for (const keyword of adultKeywords_contains) {
        const lowerKeyword = keyword.toLowerCase();
        
        if (lowerText.includes(lowerKeyword)) {
            const regex = new RegExp(`\\b${lowerKeyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
            if (regex.test(text)) {
                matches.push(keyword);
            }
        }
    }
    return matches;
};

// Process batch of records efficiently
const processBatch = async (records, filename) => {
    try {
        // Check memory usage before processing batch
        if (!checkMemoryUsage()) {
            adultKeywordsLogger.warn('High memory usage detected before processing batch, forcing garbage collection');
            forceGarbageCollection();
            
            if (!checkMemoryUsage()) {
                adultKeywordsLogger.warn('Memory usage still high, waiting before processing batch');
                await new Promise(resolve => setTimeout(resolve, 2000));
            }
        }

        // First, process individual records
        const processedRecords = records.map(record => processRecord(record)).filter(record => record !== null);
        
        // Clear original records to free memory
        records.length = 0;
        
        // Group records by URL and date
        const urlDateMap = new Map();
        for (const record of processedRecords) {
            const dateKey = `${record.url}_${record.date.toISOString().split('T')[0]}`;
            
            if (!urlDateMap.has(dateKey)) {
                urlDateMap.set(dateKey, []);
            }
            urlDateMap.get(dateKey).push(record);
        }
        
        // Clear processed records to free memory
        processedRecords.length = 0;
        
        // Merge records for same URL + date
        const mergedRecords = [];
        for (const [key, docs] of urlDateMap) {
            const mergedDoc = mergeRecordsForSameUrlDate(docs, filename);
            mergedRecords.push(mergedDoc);
        }
        
        // Clear URL date map to free memory
        urlDateMap.clear();

        // Bulk lookup existing social scrape records
        const urls = mergedRecords.map(record => record.url);
        const existingSocialScrapeRecords = await SocialScrape.find({ url: { $in: urls } }).lean();
        const socialScrapeMap = new Map();
        existingSocialScrapeRecords.forEach(record => {
            socialScrapeMap.set(record.url, record);
        });

        // Process records in parallel with bulk operations
        const exactMatchUpdates = [];
        const containsMatchReferences = [];
        const processedUrls = new Set();

        for (const record of mergedRecords) {
            if (processedUrls.has(record.url)) continue;
            processedUrls.add(record.url);

            const { url, title, meta_description, keywords } = record;
            
            if (!socialScrapeMap.has(url)) {
                continue; // Skip if not in social scrape database
            }

            // Check for exact matches first
            const titleExact = checkExactMatch(title);
            const descExact = checkExactMatch(meta_description);
            const keywordsExact = checkExactMatch(keywords);

            if (titleExact || descExact || keywordsExact) {
                // Exact match found - prepare bulk update
                exactMatchUpdates.push({
                    updateOne: {
                        filter: { url: url },
                        update: {
                            $set: {
                                title: "Possible 18+ content – text / image removed",
                                meta_description: "Possible 18+ content – text / image removed",
                                is_adult_content: true
                            }
                        }
                    }
                });
                
                matchingProgressTracker.exactMatches++;
                matchingProgressTracker.updatedRecords++;
            } else {
                // Check for contains matches
                const titleContains = checkContainsMatch(title);
                const descContains = checkContainsMatch(meta_description);
                const keywordsContains = checkContainsMatch(keywords);

                if (titleContains.length > 0 || descContains.length > 0 || keywordsContains.length > 0) {
                    const allMatches = [...new Set([...titleContains, ...descContains, ...keywordsContains])];
                    
                    // Prepare reference data
                    const referenceData = {
                        url,
                        matched_keywords: allMatches,
                        match_type: 'contains',
                        csv_source: filename
                    };

                    if (titleContains.length > 0) referenceData.title = title;
                    if (descContains.length > 0) referenceData.meta_description = meta_description;
                    if (keywordsContains.length > 0) referenceData.keywords = keywords;

                    containsMatchReferences.push(referenceData);
                    matchingProgressTracker.containsMatches++;
                }
            }
            
            matchingProgressTracker.processed++;
        }

        // Execute bulk operations
        let updatedCount = 0;
        let referenceCount = 0;

        // Bulk update social scrape records
        if (exactMatchUpdates.length > 0) {
            try {
                const updateResult = await SocialScrape.bulkWrite(exactMatchUpdates, {
                    ordered: false,
                    writeConcern: { w: 1 }
                });
                updatedCount = updateResult.modifiedCount;
                adultKeywordsLogger.info(`Bulk updated ${updatedCount} social scrape records for exact matches`);
            } catch (error) {
                adultKeywordsLogger.error('Error in bulk update of social scrape records:', error);
                // Fallback to individual updates
                for (const update of exactMatchUpdates) {
                    try {
                        await SocialScrape.updateOne(update.updateOne.filter, update.updateOne.update);
                        updatedCount++;
                    } catch (individualError) {
                        adultKeywordsLogger.error(`Individual update failed for URL: ${update.updateOne.filter.url}`, individualError);
                    }
                }
            }
        }

        // Bulk insert/update adult keywords references
        if (containsMatchReferences.length > 0) {
            try {
                const referenceOperations = containsMatchReferences.map(ref => ({
                    updateOne: {
                        filter: { url: ref.url },
                        update: { 
                            $set: { 
                                ...ref,
                                updated_at: new Date()
                            }
                        },
                        upsert: true
                    }
                }));

                const referenceResult = await AdultKeywordsReference.bulkWrite(referenceOperations, {
                    ordered: false,
                    writeConcern: { w: 1 }
                });
                referenceCount = referenceResult.upsertedCount + referenceResult.modifiedCount;
                matchingProgressTracker.createdReferences += referenceResult.upsertedCount;
                adultKeywordsLogger.info(`Bulk processed ${referenceCount} adult keywords references`);
            } catch (error) {
                adultKeywordsLogger.error('Error in bulk processing of adult keywords references:', error);
                // Fallback to individual operations
                for (const ref of containsMatchReferences) {
                    try {
                        await AdultKeywordsReference.updateOne(
                            { url: ref.url },
                            { $set: { ...ref, updated_at: new Date() } },
                            { upsert: true }
                        );
                        referenceCount++;
                        matchingProgressTracker.createdReferences++;
                    } catch (individualError) {
                        adultKeywordsLogger.error(`Individual reference operation failed for URL: ${ref.url}`, individualError);
                    }
                }
            }
        }

        // Clear arrays to free memory
        exactMatchUpdates.length = 0;
        containsMatchReferences.length = 0;
        mergedRecords.length = 0;

        adultKeywordsLogger.info(`Processed batch: ${records.length} records, ${updatedCount} updated, ${referenceCount} references processed`);

        // Force garbage collection after batch
        forceGarbageCollection();
        
    } catch (error) {
        adultKeywordsLogger.error(`Error processing batch from ${filename}:`, error);
        matchingProgressTracker.errors.push({
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
};

// Process batches in parallel with memory management
const processBatchesInParallel = async (batches, filename, processed) => {
    try {
        // Process batches sequentially to avoid memory pressure
        let results = { exactMatches: 0, containsMatches: 0, updatedRecords: 0, createdReferences: 0 };

        for (const batch of batches) {
            await processBatch(batch, filename);
            
            // Add a small delay between batches to allow memory cleanup
            await new Promise(resolve => setTimeout(resolve, 100));
        }

        return results;
    } catch (error) {
        adultKeywordsLogger.error('Error processing batches:', error);
        throw error;
    }
};

// Process CSV file using streaming for memory efficiency
const processFile = async (filePath) => {
    try {
        const filename = path.basename(filePath);
        adultKeywordsLogger.info(`Processing file for adult keywords matching: ${filename}`);
        
        matchingProgressTracker.currentFile = filename;
        
        let processed = 0;
        let batches = [];
        let currentBatch = [];
        let skippedLines = 0;

        // Reset progress for new file
        matchingProgressTracker.processed = 0;
        matchingProgressTracker.total = 0;
        matchingProgressTracker.exactMatches = 0;
        matchingProgressTracker.containsMatches = 0;
        matchingProgressTracker.updatedRecords = 0;
        matchingProgressTracker.createdReferences = 0;
        matchingProgressTracker.errors = [];

        return new Promise((resolve, reject) => {
            const parser = csv.parse({
                columns: true,
                skip_empty_lines: true,
                relax_column_count: true,
                relax_quotes: true,
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
                            matchingProgressTracker.processed = processed;

                            // Process in batches when we have enough records
                            if (currentBatch.length >= BATCH_SIZE) {
                                batches.push([...currentBatch]);
                                currentBatch = [];
                                
                                // Process batches when we have enough
                                if (batches.length >= PARALLEL_BATCHES) {
                                    await processBatchesInParallel(batches, filename, processed);
                                    batches = [];

                                    // Add a small delay to allow memory cleanup
                                    await new Promise(resolve => setTimeout(resolve, 100));
                                }
                            }
                        }
                    } catch (error) {
                        skippedLines++;
                        adultKeywordsLogger.warn(`Skipping malformed line: ${error.message}`);
                        matchingProgressTracker.errors.push({
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

                    // Log summary
                    if (skippedLines > 0) {
                        adultKeywordsLogger.info(`Completed processing ${filename}. Processed: ${processed}, Skipped: ${skippedLines} malformed lines`);
                    }

                    await moveCompletedFile(filePath);
                    adultKeywordsLogger.info(`Completed processing file: ${filename}`);
                    resolve({ filename, processed });
                } catch (error) {
                    reject(error);
                }
            });

            parser.on('error', (error) => {
                const errorMessage = `CSV parsing error (continuing with valid lines): ${error.message}`;
                adultKeywordsLogger.warn(`Error in ${filename}: ${errorMessage}`);
                skippedLines++;
                matchingProgressTracker.errors.push({
                    filename,
                    error: errorMessage
                });
            });

            // Use streams with smaller chunks for better memory management
            fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 }) // 1MB chunks
                .pipe(parser);
        });
        
    } catch (error) {
        adultKeywordsLogger.error(`Error processing file ${filePath}:`, error);
        throw error;
    }
};

// Start adult keywords matching process
const startMatching = async () => {
    try {
        // Check if matching is already running
        if (matchingProgressTracker.isRunning) {
            throw new Error('Adult keywords matching is already running. Please wait for it to complete.');
        }

        const files = await getMatchingFiles();

        if (files.length === 0) {
            throw new Error('No CSV files found in match_adult_keywords directory');
        }

        // Reset progress before starting new matching
        resetMatchingProgress();

        // Set matching as running
        setMatchingRunning(true);

        // Log the start of the matching process
        adultKeywordsLogger.info('Starting adult keywords matching process', {
            action: 'process_started',
            filesCount: files.length,
            files: files,
            batchSize: BATCH_SIZE
        });

        // Start processing files asynchronously
        processFiles(files).catch(error => {
            console.error('Error processing files for adult keywords matching:', error);
            // Set matching as not running on error
            setMatchingRunning(false);
        });

        return {
            success: true,
            message: 'Adult keywords matching started',
            files: files
        };
    } catch (error) {
        adultKeywordsLogger.error('Error starting adult keywords matching', {
            action: 'start_failed',
            error: error.message
        });
        throw error;
    }
};

// Stop adult keywords matching process
const stopMatching = async () => {
    try {
        if (!matchingProgressTracker.isRunning) {
            return {
                success: false,
                message: 'Adult keywords matching is not currently running'
            };
        }

        // Set matching as not running
        setMatchingRunning(false);
        matchingProgressTracker.isComplete = true;

        adultKeywordsLogger.info('Adult keywords matching stopped by user');

        return {
            success: true,
            message: 'Adult keywords matching stopped successfully'
        };
    } catch (error) {
        adultKeywordsLogger.error('Error stopping adult keywords matching:', error);
        throw error;
    }
};

// Process multiple files for adult keywords matching
const processFiles = async (files) => {
    try {
        adultKeywordsLogger.info(`Starting to process ${files.length} files for adult keywords matching`);

        for (let i = 0; i < files.length; i++) {
            const file = files[i];
            try {
                adultKeywordsLogger.info(`Processing file ${i + 1}/${files.length}: ${file}`);

                const filePath = path.join(MATCH_DIR, file);
                await processFile(filePath);

                adultKeywordsLogger.info(`Completed processing file ${i + 1}/${files.length}: ${file}`);
                
                // Check if matching was stopped
                if (!matchingProgressTracker.isRunning) {
                    adultKeywordsLogger.info('Adult keywords matching was stopped, stopping file processing');
                    break;
                }
            } catch (error) {
                adultKeywordsLogger.error(`Error processing file ${i + 1}/${files.length} (${file}):`, error);
                matchingProgressTracker.errors.push({
                    error: error.message,
                    file: file,
                    timestamp: new Date().toISOString()
                });
                
                // Continue with next file
                continue;
            }
        }

        // Mark overall matching as complete
        matchingProgressTracker.isComplete = true;
        matchingProgressTracker.currentFile = null;
        setMatchingRunning(false);
        
        // Log completion with summary statistics
        adultKeywordsLogger.info('Completed adult keywords matching process', {
            action: 'process_completed',
            filesProcessed: files.length,
            totalRecords: matchingProgressTracker.total,
            exactMatches: matchingProgressTracker.exactMatches,
            containsMatches: matchingProgressTracker.containsMatches,
            updatedRecords: matchingProgressTracker.updatedRecords,
            createdReferences: matchingProgressTracker.createdReferences,
            errors: matchingProgressTracker.errors.length
        });
        
        adultKeywordsLogger.info(`Completed processing all ${files.length} files for adult keywords matching`);
        
    } catch (error) {
        adultKeywordsLogger.error('Error in processFiles for adult keywords matching:', error);
        matchingProgressTracker.errors.push({
            error: error.message,
            timestamp: new Date().toISOString()
        });
        setMatchingRunning(false);
        throw error;
    }
};

// Get matching progress
const getMatchingProgress = () => {
    return { ...matchingProgressTracker };
};

// Get statistics
const getStats = async () => {
    try {
        const totalReferences = await AdultKeywordsReference.countDocuments();
        const unprocessedReferences = await AdultKeywordsReference.countDocuments({ processed: false });
        const exactMatches = await AdultKeywordsReference.countDocuments({ match_type: 'exact' });
        const containsMatches = await AdultKeywordsReference.countDocuments({ match_type: 'contains' });
        
        return {
            totalReferences,
            unprocessedReferences,
            exactMatches,
            containsMatches,
            currentProgress: matchingProgressTracker
        };
    } catch (error) {
        adultKeywordsLogger.error('Error getting adult keywords stats:', error);
        throw error;
    }
};

// Get all references
const getReferences = async () => {
    try {
        return await AdultKeywordsReference.find().sort({ created_at: -1 });
    } catch (error) {
        adultKeywordsLogger.error('Error getting adult keywords references:', error);
        throw error;
    }
};

// Get paginated references
const getPaginatedReferences = async (page = 1, limit = 50, matchType = null, processed = null) => {
    try {
        const skip = (page - 1) * limit;
        const filter = {};
        
        if (matchType) {
            filter.match_type = matchType;
        }
        
        if (processed !== null) {
            filter.processed = processed;
        }
        
        const references = await AdultKeywordsReference.find(filter)
            .sort({ created_at: -1 })
            .skip(skip)
            .limit(limit);
            
        const total = await AdultKeywordsReference.countDocuments(filter);
        
        return {
            references,
            pagination: {
                page,
                limit,
                total,
                pages: Math.ceil(total / limit)
            }
        };
    } catch (error) {
        adultKeywordsLogger.error('Error getting paginated adult keywords references:', error);
        throw error;
    }
};

// Bulk process references
const bulkProcessReferences = async (recordIds, isAdultContent) => {
    try {
        adultKeywordsLogger.info('Starting bulk process for references', {
            action: 'bulk_process_started',
            recordCount: recordIds.length,
            isAdultContent: isAdultContent
        });

        let processed = 0;
        let updated = 0;

        // Get all references by IDs
        const references = await AdultKeywordsReference.find({ _id: { $in: recordIds } });
        
        if (references.length === 0) {
            throw new Error('No references found with the provided IDs');
        }

        // Process each reference
        for (const reference of references) {
            try {
                if (isAdultContent) {
                    // Mark as adult content - update social scrape record
                    const updateResult = await SocialScrape.updateMany(
                        { url: reference.url },
                        {
                            $set: {
                                title: "Possible 18+ content – text / image removed",
                                meta_description: "Possible 18+ content – text / image removed",
                                is_adult_content: true
                            }
                        }
                    );

                    if (updateResult.modifiedCount > 0) {
                        updated++;
                        adultKeywordsLogger.info('Updated social scrape record for adult content', {
                            url: reference.url,
                            action: 'social_scrape_updated',
                            isAdultContent: true
                        });
                    }
                }

                // Mark reference as processed
                await AdultKeywordsReference.updateOne(
                    { _id: reference._id },
                    { 
                        $set: { 
                            processed: true,
                            processed_at: new Date(),
                            updated_at: new Date()
                        }
                    }
                );

                processed++;
                adultKeywordsLogger.info('Marked reference as processed', {
                    url: reference.url,
                    action: 'reference_processed',
                    isAdultContent: isAdultContent
                });

            } catch (error) {
                adultKeywordsLogger.error(`Error processing reference ${reference._id}:`, error);
                // Continue with other records
            }
        }

        adultKeywordsLogger.info('Completed bulk process for references', {
            action: 'bulk_process_completed',
            totalRecords: recordIds.length,
            processed: processed,
            updated: updated,
            isAdultContent: isAdultContent
        });

        return {
            message: `Successfully processed ${processed} references${isAdultContent ? ` and updated ${updated} social scrape records` : ''}`,
            processed: processed,
            updated: updated
        };

    } catch (error) {
        adultKeywordsLogger.error('Error in bulk process references:', error);
        throw error;
    }
};

module.exports = {
    AdultKeywordsService: {
        startMatching,
        stopMatching,
        getMatchingProgress,
        getStats,
        getReferences,
        getPaginatedReferences,
        bulkProcessReferences
    },
    matchingProgressTracker,
    resetMatchingProgress,
    setMatchingRunning
}; 