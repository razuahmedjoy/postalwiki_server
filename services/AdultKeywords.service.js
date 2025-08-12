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
const BATCH_SIZE = 1000; // Process records in batches

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

// Process a single record based on CODE (same as social scrape import)
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

        // Process the record based on CODE
        const processedRecord = {
            url: trimUrl(url),
            date: new Date(record.DATE?.split('/').reverse().join('-')),
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

// Merge multiple records for the same URL + date (same as social scrape import)
const mergeRecordsForSameUrlDate = (docs, filename) => {
    if (docs.length === 1) {
        return { ...docs[0], csv_source: filename };
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
        // This ensures we match the exact phrase, not just partial matches
        if (lowerText.includes(lowerKeyword)) {
            // Additional check: ensure it's not part of a larger word
            // Look for word boundaries or exact matches
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
        
        // Check if the keyword is contained in the text
        if (lowerText.includes(lowerKeyword)) {
            // Additional check: ensure it's not part of a larger word
            // Look for word boundaries or exact matches
            const regex = new RegExp(`\\b${lowerKeyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
            if (regex.test(text)) {
                matches.push(keyword);
            }
        }
    }
    return matches;
};

// Process merged record for adult keywords matching
const processMergedRecord = async (mergedRecord) => {
    try {
        const { url, title, meta_description, keywords } = mergedRecord;
        
        if (!url) {
            return { type: 'error', message: 'URL is required' };
        }

        // First, check if the record exists in social scrape database
        const existingSocialScrapeRecord = await SocialScrape.findOne({ url: url });
        
        if (!existingSocialScrapeRecord) {
     
            return { type: 'no_social_scrape_record', url };
        }

        // Check for exact matches first (only if record exists in social scrape)
        const titleExact = checkExactMatch(title);
        const descExact = checkExactMatch(meta_description);
        const keywordsExact = checkExactMatch(keywords);

        if (titleExact || descExact || keywordsExact) {
            // Exact match found - update social scrape record
            const exactKeyword = titleExact || descExact || keywordsExact;
            
            // Log the exact match before updating
            adultKeywordsLogger.info('Exact match found - updating social scrape record', {
                url,
                keyword: exactKeyword,
                matchType: 'exact',
                action: 'update_social_scrape',
                source: titleExact ? 'title' : descExact ? 'meta_description' : 'keywords'
            });
            
            const result = await updateSocialScrapeRecord(url, exactKeyword);

            return { 
                type: 'exact', 
                keyword: exactKeyword, 
                url, 
                updated: result.updated,
                record: result.record 
            };
        }

        // Only check for contains matches if no exact matches were found
        const titleContains = checkContainsMatch(title);
        const descContains = checkContainsMatch(meta_description);
        const keywordsContains = checkContainsMatch(keywords);

        if (titleContains.length > 0 || descContains.length > 0 || keywordsContains.length > 0) {
            // Contains match found - create reference (only for existing social scrape records)
            const allMatches = [...new Set([...titleContains, ...descContains, ...keywordsContains])];
            
            // Log the contains match before creating reference
            adultKeywordsLogger.info('Contains match found - creating reference', {
                url,
                keywords: allMatches,
                matchType: 'contains',
                action: 'create_reference',
                source: {
                    title: titleContains,
                    meta_description: descContains,
                    keywords: keywordsContains
                }
            });
            
            const result = await createAdultKeywordsReference(url, title, meta_description, keywords, allMatches, mergedRecord.csv_source);
            return { 
                type: 'contains', 
                keywords: allMatches, 
                url, 
                referenceCreated: result.created 
            };
        }

        return { type: 'no_match', url };

    } catch (error) {
        adultKeywordsLogger.error(`Error processing merged record for URL ${mergedRecord.url}:`, error);
        return { type: 'error', message: error.message, url: mergedRecord.url };
    }
};

// Update social scrape record with adult content warning
const updateSocialScrapeRecord = async (url, matchedKeyword) => {
    try {
        // Safety check: ensure URL exists in social scrape database
        const existingRecord = await SocialScrape.findOne({ url: url });
        if (!existingRecord) {
            // adultKeywordsLogger.warn(`Attempted to update non-existent social scrape record for URL: ${url}`);
            return { updated: false, record: null, error: 'URL not found in social scrape database' };
        }

        const updateResult = await SocialScrape.updateMany(
            { url: url },
            {
                $set: {
                    title: "Possible 18+ content – text / image removed",
                    meta_description: "Possible 18+ content – text / image removed",
                    is_adult_content: true
                }
            }
        );
  

        if (updateResult.modifiedCount > 0) {
            // adultKeywordsLogger.info('Successfully updated social scrape record', {
            //     url,
            //     keyword: matchedKeyword,
            //     matchType: 'exact',
            //     action: 'record_updated',
            //     modifiedCount: updateResult.modifiedCount
            // });
            return { updated: true, record: updateResult };
        } else {
            adultKeywordsLogger.warn('No social scrape record found for URL', {
                url,
                keyword: matchedKeyword,
                matchType: 'exact',
                action: 'no_record_found'
            });
            return { updated: false, record: null };
        }
    } catch (error) {
        adultKeywordsLogger.error(`Error updating social scrape record for URL ${url}:`, error);
        throw error;
    }
};

// Create adult keywords reference for user verification
const createAdultKeywordsReference = async (url, title, meta_description, keywords, matchedKeywords, csvSource) => {
    try {
        // Additional safety check: ensure URL exists in social scrape database
        const existingSocialScrapeRecord = await SocialScrape.findOne({ url: url, $or: [{ is_adult_content: false }, { is_adult_content: { $exists: false } }] });
        if (!existingSocialScrapeRecord) {
            // adultKeywordsLogger.warn(`Attempted to create reference for URL not in social scrape database: ${url}`);
            return { created: false, updated: false, error: 'URL not found in social scrape database' };
        }

        // Determine which properties had contains matches
        const titleContains = checkContainsMatch(title);
        const descContains = checkContainsMatch(meta_description);
        const keywordsContains = checkContainsMatch(keywords);

        // Only store properties that had matches
        const referenceData = {
            url,
            matched_keywords: matchedKeywords,
            match_type: 'contains',
            csv_source: csvSource
        };

        // Add title only if it had contains matches
        if (titleContains.length > 0) {
            referenceData.title = title;
        }

        // Add meta_description only if it had contains matches
        if (descContains.length > 0) {
            referenceData.meta_description = meta_description;
        }

        // Add keywords only if it had contains matches
        if (keywordsContains.length > 0) {
            referenceData.keywords = keywords;
        }

        // Check if reference already exists
        const existingReference = await AdultKeywordsReference.findOne({ url });
        
        if (existingReference) {
            // Update existing reference with new matched keywords and properties
            const updatedKeywords = [...new Set([...existingReference.matched_keywords, ...matchedKeywords])];
            
            // Merge properties - keep existing ones and add new ones that had matches
            const updateData = { 
                matched_keywords: updatedKeywords,
                updated_at: new Date()
            };

            // Update title if we have a new title match
            if (titleContains.length > 0) {
                updateData.title = title;
            }

            // Update meta_description if we have a new description match
            if (descContains.length > 0) {
                updateData.meta_description = meta_description;
            }

            // Update keywords if we have a new keywords match
            if (keywordsContains.length > 0) {
                updateData.keywords = keywords;
            }

            await AdultKeywordsReference.updateOne(
                { url },
                { $set: updateData }
            );
            
            // Log which properties had matches
            const matchedProperties = [];
            if (titleContains.length > 0) matchedProperties.push('title');
            if (descContains.length > 0) matchedProperties.push('meta_description');
            if (keywordsContains.length > 0) matchedProperties.push('keywords');
            
            // adultKeywordsLogger.info('Updated existing adult keywords reference', {
            //     url,
            //     keywords: matchedKeywords,
            //     matchType: 'contains',
            //     action: 'reference_updated',
            //     totalKeywords: updatedKeywords.length,
            //     matchedProperties: matchedProperties
            // });
            
            return { created: false, updated: true };
        } else {
            // Create new reference with only the properties that had matches
            const reference = new AdultKeywordsReference(referenceData);
            await reference.save();
            
            // Log which properties had matches
            const matchedProperties = [];
            if (titleContains.length > 0) matchedProperties.push('title');
            if (descContains.length > 0) matchedProperties.push('meta_description');
            if (keywordsContains.length > 0) matchedProperties.push('keywords');
            
            // adultKeywordsLogger.info('Created new adult keywords reference', {
            //     url,
            //     keywords: matchedKeywords,
            //     matchType: 'contains',
            //     action: 'reference_created',
            //     csvSource,
            //     matchedProperties: matchedProperties
            // });
            
            return { created: true, updated: false };
        }
    } catch (error) {
        adultKeywordsLogger.error(`Error creating adult keywords reference for URL ${url}:`, error);
        throw error;
    }
};

// Process CSV file for adult keywords matching
const processFile = async (filePath) => {
    try {
        const filename = path.basename(filePath);
        adultKeywordsLogger.info(`Processing file for adult keywords matching: ${filename}`);
        
        matchingProgressTracker.currentFile = filename;
        
        // Count total lines in file
        const fileContent = await fs.promises.readFile(filePath, 'utf8');
        const lines = fileContent.split('\n').filter(line => line.trim());
        matchingProgressTracker.total = lines.length - 1; // Subtract header
        
        const records = [];
        let batchCount = 0;
        
        return new Promise((resolve, reject) => {
            const parser = csv.parse({
                columns: true,
                skip_empty_lines: true,
                trim: true
            });

            parser.on('data', (record) => {
                records.push(record);
                
                if (records.length >= BATCH_SIZE) {
                    processBatch(records, filename);
                    records.length = 0; // Clear array
                    batchCount++;
                }
            });

            parser.on('end', async () => {
                try {
                    // Process remaining records
                    if (records.length > 0) {
                        await processBatch(records, filename);
                    }
                    
                    // Move completed file
                    await moveCompletedFile(filePath);
                    
                    adultKeywordsLogger.info(`Completed processing file: ${filename}`);
                    resolve();
                } catch (error) {
                    reject(error);
                }
            });

            parser.on('error', (error) => {
                reject(error);
            });

            // Start parsing
            parser.write(fileContent);
            parser.end();
        });
        
    } catch (error) {
        adultKeywordsLogger.error(`Error processing file ${filePath}:`, error);
        throw error;
    }
};

// Process batch of records
const processBatch = async (records, filename) => {
    try {
        // First, process individual records
        const processedRecords = records.map(record => processRecord(record)).filter(record => record !== null);
        
        // Group records by URL and date
        const urlDateMap = new Map();
        for (const record of processedRecords) {
            const key = `${record.url}_${record.date.toISOString().split('T')[0]}`;
            if (!urlDateMap.has(key)) {
                urlDateMap.set(key, []);
            }
            urlDateMap.get(key).push(record);
        }
        
        // Merge records for same URL + date
        const mergedRecords = [];
        for (const [key, docs] of urlDateMap) {
            const mergedDoc = mergeRecordsForSameUrlDate(docs, filename);
            mergedRecords.push(mergedDoc);
        }
        
        // Now process merged records for adult keywords matching
        const promises = mergedRecords.map(record => processMergedRecord(record));
        const results = await Promise.allSettled(promises);
        
        for (const result of results) {
            if (result.status === 'fulfilled') {
                const data = result.value;
                
                if (data.type === 'exact') {
                    matchingProgressTracker.exactMatches++;
                    if (data.updated) {
                        matchingProgressTracker.updatedRecords++;
                    }
                } else if (data.type === 'contains') {
                    matchingProgressTracker.containsMatches++;
                    if (data.referenceCreated) {
                        matchingProgressTracker.createdReferences++;
                    }
                } else if (data.type === 'no_social_scrape_record') {
                    // Record doesn't exist in social scrape database, skip
                    adultKeywordsLogger.debug(`Skipping URL ${data.url} - not found in social scrape database`);
                }
                
                matchingProgressTracker.processed++;
            } else {
                matchingProgressTracker.errors.push({
                    error: result.reason.message,
                    timestamp: new Date().toISOString()
                });
            }
        }
        
        adultKeywordsLogger.info(`Processed batch of ${mergedRecords.length} merged records from ${filename}`);
        
    } catch (error) {
        adultKeywordsLogger.error(`Error processing batch from ${filename}:`, error);
        matchingProgressTracker.errors.push({
            error: error.message,
            timestamp: new Date().toISOString()
        });
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