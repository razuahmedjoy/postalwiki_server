// services/SocialScrapeService.js
const csv = require('csv-parse');
const fs = require('fs');
const path = require('path');
const { EventEmitter } = require('events');
const SocialScrape = require('../models/SocialScrape');
const logger = require('../config/logger');
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
    isComplete: false
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
        logger.error('Error creating import directory:', error);
        throw new Error('Failed to create import directory');
    }
};

const moveCompletedFile = async (filePath) => {
    try {
        const filename = path.basename(filePath);
        const today = new Date().toISOString().split('T')[0];
        const completedDir = path.join(IMPORT_DIR, `completed_${today}`);
        
        // Create completed directory if it doesn't exist
        await fs.promises.mkdir(completedDir, { recursive: true });
        
        // Move the file
        const newPath = path.join(completedDir, filename);
        await fs.promises.rename(filePath, newPath);
        
        logger.info(`Moved file ${filename} to completed directory`);
    } catch (error) {
        logger.error(`Failed to move file: ${error.message}`);
        throw error;
    }
};

// Ensure indexes exist for better performance
const ensureIndexes = async () => {
    try {
        await SocialScrape.collection.createIndex({ url: 1, date: 1 }, { unique: true });
        logger.info('Indexes created successfully');
    } catch (error) {
        logger.error('Error creating indexes:', error);
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
        logger.error('Error processing batches:', error);
        throw error;
    }
};

const insertBatch = async (batch, filename, processed, total) => {
    try {
        const operations = batch.map(doc => ({
            updateOne: {
                filter: { url: doc.url, date: doc.date },
                update: { $set: doc },
                upsert: true
            }
        }));

        logger.info(`Attempting to insert batch of ${batch.length} records`);

        // Modified MongoDB settings for better reliability
        const result = await SocialScrape.bulkWrite(operations, { 
            ordered: true,
            writeConcern: { w: 1 }, // Changed to 1 to ensure write acknowledgment
            bypassDocumentValidation: true
        });

        logger.info(`Batch insert result - Upserted: ${result.upsertedCount}, Modified: ${result.modifiedCount}`);

        importProgressTracker.upserted += result.upsertedCount;
        importProgressTracker.modified += result.modifiedCount;
        importProgressTracker.processed = processed;

        return {
            success: true,
            upserted: result.upsertedCount,
            modified: result.modifiedCount
        };
    } catch (error) {
        logger.error(`Error in insertBatch: ${error.message}`);
        logger.error(`Error details: ${JSON.stringify(error)}`);
        importProgressTracker.errors.push({
            filename,
            error: error.message
        });
        throw error;
    }
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
            logger.debug(`Skipping invalid domain: ${url}`);
            return null;
        }

        // Skip records with error or no data
        if (record.RESULT === 'Fetch error or no data found' || record.RESULT === 'not required') {
            // If we only have URL and no other data, skip this record
            const hasOtherData = Object.entries(record).some(([key, value]) => 
                key !== 'RESULT' && value && value.trim() !== ''
            );
            if (!hasOtherData) {
                logger.debug(`Skipping record with no data for URL: ${url}`);
                return null;
            }
        }

        // Process the record based on CODE
        const processedRecord = {
            url: trimUrl(url),
            date: new Date(record.DATE?.split('/').reverse().join('-')), // Convert DD/MM/YYYY to YYYY-MM-DD, handle undefined
        };

        switch (record.CODE) {
            case '[TI]':
                processedRecord.title = cleanText(record.RESULT);
                break;
            case '[KW]':
                processedRecord.keywords = cleanText(record.RESULT);
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

        logger.debug(`Processed record for URL: ${processedRecord.url}`);
        return processedRecord;
    } catch (error) {
        logger.error(`Error processing record: ${error.message}`);
        logger.error(`Record data: ${JSON.stringify(record)}`);
        return null;
    }
};

const processFile = async (filePath, isBlackList = false) => {
    const filename = path.basename(filePath);
    let processed = 0;
    let batches = [];
    let currentBatch = [];

    // Reset progress for new file
    importProgressTracker.currentFile = filename;
    importProgressTracker.processed = 0;
    importProgressTracker.total = 0;
    importProgressTracker.upserted = 0;
    importProgressTracker.modified = 0;
    importProgressTracker.errors = [];
    importProgressTracker.isComplete = false;

    // Ensure indexes exist
    await ensureIndexes();

    return new Promise((resolve, reject) => {
        const parser = csv.parse({
            columns: true,
            skip_empty_lines: true,
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
                    importProgressTracker.errors.push({
                        filename,
                        error: error.message
                    });
                }
            }
        });

        parser.on('end', async () => {
            try {
                // Process remaining records
                if (currentBatch.length > 0) {
                    batches.push(currentBatch);
                }
                if (batches.length > 0) {
                    await processBatchesInParallel(batches, filename, processed);
                }
                await moveCompletedFile(filePath);
                importProgressTracker.isComplete = true;
                resolve({ filename, processed });
            } catch (error) {
                reject(error);
            }
        });

        parser.on('error', (error) => {
            importProgressTracker.errors.push({
                filename,
                error: error.message
            });
            reject(error);
        });

        // Use streams with smaller chunks for better memory management
        fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 }) // 1MB chunks
            .pipe(parser);
    });
};

const getImportFiles = async (isBlackList = false) => {
    try {
        await ensureImportDirectory();
        if(isBlackList){
            logger.info('Reading blacklist directory:', BLACKLIST_DIR);
        }
        else{
            logger.info('Reading import directory:', IMPORT_DIR);
        }
        
        if(isBlackList){
            const files = await fs.promises.readdir(BLACKLIST_DIR);
            return files.filter(file => file.endsWith('.csv'));
        
        }
        else{
            const files = await fs.promises.readdir(IMPORT_DIR);
            return files.filter(file => file.endsWith('.csv'));
        }

    } catch (error) {
        logger.error('Error reading import directory:', error);
        return [];
    }
};

const getBlacklistFiles = async () => {
    try {
        await ensureImportDirectory();
        const files = await fs.promises.readdir(BLACKLIST_DIR);
        return files.filter(file => file.endsWith('.csv'));
    } catch (error) {
        logger.error('Error reading blacklist directory:', error);
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
        const archiveDir = path.join(process.cwd(), 'imports', 'social_scrape_blacklisted', 'completed_'+new Date().toISOString().split('T')[0]);
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
        logger.error(errorMsg);
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
    
    // Remove all non-digit characters for length check
    const digitsOnly = cleanedPhone.replace(/\D/g, '');
    
    // Check if it's exactly 10 or 11 digits
    if (digitsOnly.length !== 10 && digitsOnly.length !== 11) return false;
    
    // Additional validation patterns for UK numbers
    const ukPatterns = [
        /^(\+44|44)?[1-9]\d{1,4}\d{6,10}$/, // UK mobile/landline
        /^(\+44|44)?7\d{9}$/, // UK mobile
        /^(\+44|44)?1\d{1,4}\d{6,10}$/, // UK landline
        /^0[1-9]\d{1,4}\d{6,10}$/, // UK landline with 0
        /^07\d{9}$/ // UK mobile with 0
    ];
    
    return ukPatterns.some(pattern => pattern.test(digitsOnly));
};

// Clean phone number for storage
const cleanPhoneNumber = (phone) => {
    if (!phone || typeof phone !== 'string') return null;
    
    let cleaned = phone.trim();
    
    // Remove spaces
    cleaned = cleaned.replace(/\s+/g, '');
    
    // Remove dashes
    cleaned = cleaned.replace(/-/g, '');
    
    // Remove dots (decimal points)
    cleaned = cleaned.replace(/\./g, '');
    
    // Remove brackets (both round and square brackets)
    cleaned = cleaned.replace(/[\(\)\[\]]/g, '');
    
    // Handle country codes - convert to UK format
    cleaned = convertCountryCodeToUK(cleaned);
    
    // If it starts with 1- (US format), convert to UK format
    if (cleaned.startsWith('1-')) {
        cleaned = '0' + cleaned.substring(2);
    }
    
    // If it starts with 1 and is followed by a digit, convert to UK format
    if (cleaned.match(/^1\d{10}$/)) {
        cleaned = '0' + cleaned.substring(1);
    }
    
    // If it's a 10-digit number starting with 1, add 0
    if (cleaned.match(/^1\d{9}$/)) {
        cleaned = '0' + cleaned;
    }
    
    // If it starts with +44, convert to UK format
    if (cleaned.startsWith('+44')) {
        cleaned = '0' + cleaned.substring(3);
    }
    
    // If it starts with 44, convert to UK format
    if (cleaned.startsWith('44')) {
        cleaned = '0' + cleaned.substring(2);
    }
    
    // Final validation - should be 10 or 11 digits
    const digitsOnly = cleaned.replace(/\D/g, '');
    if (digitsOnly.length !== 10 && digitsOnly.length !== 11) {
        return null;
    }
    
    // Return in international format for storage
    if (cleaned.startsWith('0')) {
        return '+44' + cleaned.substring(1);
    }
    
    return cleaned;
};

// Convert various country codes to UK format
const convertCountryCodeToUK = (phone) => {
    // List of country codes to convert to UK format
    const countryCodeMap = {
        '+1809': '+44', '+1829': '+44', '+1849': '+44', '+1787': '+44', '+1939': '+44',
        '+441624': '+44', '+441534': '+44', '+441481': '+44', '+1876': '+44', '+1869': '+44',
        '+1868': '+44', '+1784': '+44', '+1767': '+44', '+1758': '+44', '+1721': '+44',
        '+1684': '+44', '+1671': '+44', '+1670': '+44', '+1664': '+44', '+1649': '+44',
        '+1473': '+44', '+1441': '+44', '+1345': '+44', '+1340': '+44', '+1284': '+44',
        '+1268': '+44', '+1264': '+44', '+1246': '+44', '+1242': '+44', '+998': '+44',
        '+996': '+44', '+995': '+44', '+994': '+44', '+993': '+44', '+992': '+44',
        '+977': '+44', '+976': '+44', '+975': '+44', '+974': '+44', '+973': '+44',
        '+972': '+44', '+971': '+44', '+970': '+44', '+968': '+44', '+967': '+44',
        '+966': '+44', '+965': '+44', '+964': '+44', '+963': '+44', '+962': '+44',
        '+961': '+44', '+960': '+44', '+886': '+44', '+880': '+44', '+856': '+44',
        '+855': '+44', '+853': '+44', '+852': '+44', '+850': '+44', '+692': '+44',
        '+691': '+44', '+690': '+44', '+689': '+44', '+688': '+44', '+687': '+44',
        '+686': '+44', '+685': '+44', '+683': '+44', '+682': '+44', '+681': '+44',
        '+680': '+44', '+679': '+44', '+678': '+44', '+677': '+44', '+676': '+44',
        '+675': '+44', '+674': '+44', '+673': '+44', '+672': '+44', '+670': '+44',
        '+599': '+44', '+598': '+44', '+597': '+44', '+595': '+44', '+593': '+44',
        '+592': '+44', '+591': '+44', '+590': '+44', '+509': '+44', '+508': '+44',
        '+507': '+44', '+506': '+44', '+505': '+44', '+504': '+44', '+503': '+44',
        '+502': '+44', '+501': '+44', '+500': '+44', '+423': '+44', '+421': '+44',
        '+420': '+44', '+389': '+44', '+387': '+44', '+386': '+44', '+385': '+44',
        '+383': '+44', '+382': '+44', '+381': '+44', '+380': '+44', '+379': '+44',
        '+378': '+44', '+377': '+44', '+376': '+44', '+375': '+44', '+374': '+44',
        '+373': '+44', '+372': '+44', '+371': '+44', '+370': '+44', '+359': '+44',
        '+358': '+44', '+357': '+44', '+356': '+44', '+355': '+44', '+354': '+44',
        '+353': '+44', '+352': '+44', '+351': '+44', '+350': '+44', '+299': '+44',
        '+298': '+44', '+297': '+44', '+291': '+44', '+290': '+44', '+269': '+44',
        '+268': '+44', '+267': '+44', '+266': '+44', '+265': '+44', '+264': '+44',
        '+263': '+44', '+262': '+44', '+261': '+44', '+260': '+44', '+258': '+44',
        '+257': '+44', '+256': '+44', '+255': '+44', '+254': '+44', '+253': '+44',
        '+252': '+44', '+251': '+44', '+250': '+44', '+249': '+44', '+248': '+44',
        '+246': '+44', '+245': '+44', '+244': '+44', '+243': '+44', '+242': '+44',
        '+241': '+44', '+240': '+44', '+239': '+44', '+238': '+44', '+237': '+44',
        '+236': '+44', '+235': '+44', '+234': '+44', '+233': '+44', '+232': '+44',
        '+231': '+44', '+230': '+44', '+229': '+44', '+228': '+44', '+227': '+44',
        '+226': '+44', '+225': '+44', '+224': '+44', '+223': '+44', '+222': '+44',
        '+221': '+44', '+220': '+44', '+218': '+44', '+216': '+44', '+213': '+44',
        '+212': '+44', '+211': '+44', '+98': '+44', '+95': '+44', '+94': '+44',
        '+93': '+44', '+92': '+44', '+91': '+44', '+90': '+44', '+86': '+44',
        '+84': '+44', '+82': '+44', '+81': '+44', '+66': '+44', '+65': '+44',
        '+64': '+44', '+63': '+44', '+62': '+44', '+61': '+44', '+60': '+44',
        '+58': '+44', '+57': '+44', '+56': '+44', '+55': '+44', '+54': '+44',
        '+53': '+44', '+52': '+44', '+51': '+44', '+49': '+44', '+48': '+44',
        '+47': '+44', '+46': '+44', '+45': '+44', '+43': '+44', '+41': '+44',
        '+40': '+44', '+39': '+44', '+36': '+44', '+34': '+44', '+33': '+44',
        '+32': '+44', '+31': '+44', '+30': '+44', '+27': '+44', '+20': '+44',
        '+7': '+44', '+1': '+44'
    };
    
    // Check for country codes and convert them
    for (const [countryCode, ukCode] of Object.entries(countryCodeMap)) {
        if (phone.startsWith(countryCode)) {
            return phone.replace(countryCode, ukCode);
        }
    }
    
    return phone;
};

const getPhoneFiles = async () => {
    try {
        await ensureImportDirectory();
        const files = await fs.promises.readdir(PHONE_DIR);
        return files.filter(file => file.endsWith('.csv'));
    } catch (error) {
        logger.error('Error reading phone directory:', error);
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
                isComplete: false
            };
            phoneProgressStore.set(processId, progressTracker);
        }

        // Reset progress for new file
        progressTracker.currentFile = path.basename(filePath);
        progressTracker.processed = 0;
        progressTracker.total = 0;
        progressTracker.updated = 0;
        progressTracker.created = 0;
        progressTracker.errors = [];
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
                progressTracker.isComplete = true;
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
                        const cleanPhone = cleanPhoneNumber(phoneData);
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
                        logger.error(`Phone processing error on line ${lineNumber}:`, error);
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

                    progressTracker.isComplete = true;
                    phoneEventEmitter.emit('progress', { processId, ...progressTracker });
                    resolve({ filename: path.basename(filePath), processed: progressTracker.processed });
                } catch (error) {
                    clearTimeout(timeout);
                    reject(error);
                }
            });

            parser.on('error', (error) => {
                clearTimeout(timeout);
                const errorMsg = `CSV parsing error: ${error.message}`;
                progressTracker.errors.push(errorMsg);
                logger.error('CSV parsing error:', error);
                
                // Mark process as complete with error
                progressTracker.isComplete = true;
                phoneEventEmitter.emit('progress', { processId, ...progressTracker });
                
                reject(error);
            });

            // Use streams with smaller chunks for better memory management
            fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 }) // 1MB chunks
                .pipe(parser);
        });

    } catch (error) {
        const errorMsg = `Error processing phone file: ${error.message}`;
        logger.error(errorMsg);
        throw error;
    }
};

const processPhoneBatch = async (urlPhoneMap, progressTracker, logFile) => {
    try {
        for (const [url, phoneSet] of urlPhoneMap) {
            try {
                let phones = Array.from(phoneSet);
                
                // Limit to maximum 3 phone numbers per URL
                if (phones.length > 3) {
                    const originalCount = phones.length;
                    phones = phones.slice(0, 3);
                    const skippedMsg = `Limited phone numbers for URL ${url} from ${originalCount} to 3`;
                    progressTracker.errors.push(skippedMsg);
                    await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] ${skippedMsg}\n`);
                }
                
                // Find existing record
                const existingRecord = await SocialScrape.findOne({ url });
                
                if (existingRecord) {
                    // Update existing record
                    const existingPhones = existingRecord.phone || [];
                    const newPhones = [...new Set([...existingPhones, ...phones])];
                    
                    // Limit to maximum 3 phone numbers total
                    const finalPhones = newPhones.slice(0, 3);
                    
                    await SocialScrape.updateOne(
                        { url },
                        { $set: { phone: finalPhones } }
                    );
                    
                    progressTracker.updated++;
                    await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] Updated phone numbers for URL: ${url}, phones: ${finalPhones.join(', ')}\n`);
                } else {
                    // Create new record
                    const newRecord = {
                        url,
                        date: new Date(),
                        phone: phones
                    };
                    
                    await SocialScrape.create(newRecord);
                    
                    progressTracker.created++;
                    await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] Created new record for URL: ${url}, phones: ${phones.join(', ')}\n`);
                }
                
            } catch (error) {
                const errorMsg = `Error processing URL ${url}: ${error.message}`;
                progressTracker.errors.push(errorMsg);
                await fs.promises.appendFile(logFile, `[${new Date().toISOString()}] ${errorMsg}\n`);
            }
        }
    } catch (error) {
        logger.error('Error processing phone batch:', error);
        throw error;
    }
};

const getPhoneProgress = (processId) => {
    const progress = phoneProgressStore.get(processId);
    return progress ? { ...progress } : null;
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
    getBlacklistProgress,
    getPhoneProgress
};

module.exports = {
    SocialScrapeService,
    IMPORT_DIR,
    BLACKLIST_DIR,
    PHONE_DIR,
    importEventEmitter,
    blacklistEventEmitter,
    phoneEventEmitter
};