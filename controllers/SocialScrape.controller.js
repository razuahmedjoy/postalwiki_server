// controllers/SocialScrapeController.js
const { SocialScrapeService, IMPORT_DIR, BLACKLIST_DIR, PHONE_DIR } = require('../services/SocialScrape.service');
const path = require('path');
const SocialScrape = require('../models/SocialScrape');
const { importEventEmitter, blacklistEventEmitter, phoneEventEmitter } = require('../services/SocialScrape.service');
const logger = require('../config/logger');
const { v4: uuidv4 } = require('uuid');


const startImport = async (req, res) => {
    try {
        let blacListImport = false;
        const { isBlackList } = req.body

        if (isBlackList) {
            blacListImport = true;
        }

        const files = await SocialScrapeService.getImportFiles(blacListImport);

        if (files.length === 0) {
            return res.status(404).json({ message: 'No CSV files found to import' });
        }

        // Start processing files asynchronously
        processFiles(files, blacListImport).catch(error => {
            console.error('Error processing files:', error);
        });

        res.json({
            success: true,
            message: 'Import started',
            files: files
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
};

const processFiles = async (files, blacListImport=false) => {
    for (const file of files) {
        if(blacListImport){
            const filePath = path.join(BLACKLIST_DIR, file);
            await SocialScrapeService.processFile(filePath);
        }
        else{
            const filePath = path.join(IMPORT_DIR, file);
            await SocialScrapeService.processFile(filePath);
        }
    }
};

const getStats = async (req, res) => {
    try {
        // Use estimatedDocumentCount instead of countDocuments for better performance
        const stats = await SocialScrape.estimatedDocumentCount();
        res.json({ success: true, stats });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
};

const getImportProgress = async (req, res) => {
    try {
        const progress = SocialScrapeService.getImportProgress();
        res.json({
            success: true,
            data: progress
        });
    } catch (error) {
        logger.error('Error getting import progress:', error);
        res.status(500).json({ success: false, error: 'Failed to get import progress' });
    }
};

const getBlacklistProgress = async (req, res) => {
    try {
        const { processId } = req.query;
        if (!processId) {
            return res.status(400).json({ error: 'Process ID is required' });
        }

        const progress = SocialScrapeService.getBlacklistProgress(processId);
        if (!progress) {
            return res.status(404).json({ error: 'Process not found' });
        }

        res.json(progress);
    } catch (error) {
        logger.error('Error getting blacklist progress:', error);
        res.status(500).json({ error: 'Failed to get blacklist progress' });
    }
};

const getPaginatedSocialScrapes = async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 100;
        const searchUrl = req.query.searchUrl || '';
        const skip = (page - 1) * limit;

        // Build search query
        const query = {};
        if (searchUrl) {
            // Use case-insensitive exact match instead of regex
            query.url = { $eq: searchUrl.toLowerCase() };
        }

        // Use lean() for better performance as we don't need Mongoose documents
        // Only select required fields to reduce data transfer
        const socialScrapes = await SocialScrape.find(query)
            .select('url date title twitter facebook instagram linkedin youtube pinterest email phone postcode keywords statusCode redirect_url meta_description is_blacklisted')
            .sort({ date: -1 })
            .skip(skip)
            .limit(limit)
            .lean();

        // Optimize count query by using hint to force index usage
        const total = searchUrl
            ? await SocialScrape.countDocuments(query).hint({ url: 1 })
            : await SocialScrape.estimatedDocumentCount();

        res.json({
            success: true,
            data: socialScrapes,
            pagination: {
                total,
                page,
                limit,
                totalPages: Math.ceil(total / limit)
            }
        });
    } catch (error) {
        console.error('Error fetching social scrapes:', error);
        res.status(500).json({ success: false, error: 'Internal server error' });
    }
};

const processBlacklistFiles = async (files, urlColumn, processId) => {
    for (const file of files) {
        const filePath = path.join(BLACKLIST_DIR, file);
        await SocialScrapeService.processBlacklistFile(filePath, urlColumn, processId);
    }
};

const updateBlacklist = async (req, res) => {
    try {
        const { urlColumn = 1 } = req.body; // Default to first column if not specified
        
        const files = await SocialScrapeService.getBlacklistFiles();
        
        if (files.length === 0) {
            return res.status(404).json({ message: 'No CSV files found in blacklist directory' });
        }

        // Generate a unique process ID
        const processId = uuidv4();

        // Start processing files asynchronously
        processBlacklistFiles(files, urlColumn, processId).catch(error => {
            logger.error('Error processing blacklist files:', error);
            const progress = SocialScrapeService.getBlacklistProgress(processId);
            if (progress) {
                progress.errors.push(error.message);
                progress.isComplete = true;
                blacklistEventEmitter.emit('progress', { processId, ...progress });
            }
        });

        res.json({
            success: true,
            message: 'Blacklist update started',
            processId,
            files: files
        });
    } catch (error) {
        logger.error('Error starting blacklist update:', error);
        res.status(500).json({ success: false, error: error.message });
    }
};

const getProgress = async (req, res) => {
    try {
        const progress = SocialScrapeService.getProgress();
        res.json(progress);
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
};

const updatePhoneNumber = async (req, res) => {
    try {
        const files = await SocialScrapeService.getPhoneFiles();
        
        if (files.length === 0) {
            return res.status(404).json({ message: 'No CSV files found in phone directory' });
        }

        // Generate a unique process ID
        const processId = uuidv4();

        // Start processing files asynchronously
        processPhoneFiles(files, processId).catch(error => {
            logger.error('Error processing phone files:', error);
            const progress = SocialScrapeService.getPhoneProgress(processId);
            if (progress) {
                progress.errors.push(error.message);
                progress.isComplete = true;
                phoneEventEmitter.emit('progress', { processId, ...progress });
            }
        });

        res.json({
            success: true,
            message: 'Phone number update started',
            processId,
            files: files
        });
    } catch (error) {
        logger.error('Error starting phone number update:', error);
        res.status(500).json({ success: false, error: error.message });
    }
};

const processPhoneFiles = async (files, processId) => {
    try {
        logger.info(`Starting phone processing for ${files.length} files with process ID: ${processId}`);
        
        for (const file of files) {
            try {
                logger.info(`Processing phone file: ${file}`);
                const filePath = path.join(PHONE_DIR, file);
                await SocialScrapeService.processPhoneFile(filePath, processId);
                logger.info(`Completed processing phone file: ${file}`);
            } catch (error) {
                logger.error(`Error processing phone file ${file}:`, error);
                
                // Update progress with error
                const progress = SocialScrapeService.getPhoneProgress(processId);
                if (progress) {
                    progress.errors.push(`Failed to process file ${file}: ${error.message}`);
                    progress.isComplete = true;
                    phoneEventEmitter.emit('progress', { processId, ...progress });
                }
                
                // Continue with next file instead of stopping completely
                continue;
            }
        }
        
        logger.info(`Completed phone processing for all files with process ID: ${processId}`);
    } catch (error) {
        logger.error('Error in processPhoneFiles:', error);
        throw error;
    }
};

const getPhoneProgress = async (req, res) => {
    try {
        const { processId } = req.query;
        if (!processId) {
            return res.status(400).json({ error: 'Process ID is required' });
        }

        logger.info(`Getting phone progress for process ID: ${processId}`);
        
        const progress = SocialScrapeService.getPhoneProgress(processId);
        if (!progress) {
            logger.warn(`No progress found for process ID: ${processId}`);
            return res.status(404).json({ error: 'Process not found' });
        }

        logger.info(`Phone progress for ${processId}:`, {
            currentFile: progress.currentFile,
            processed: progress.processed,
            updated: progress.updated,
            created: progress.created,
            errors: progress.errors.length,
            isComplete: progress.isComplete
        });

        res.json(progress);
    } catch (error) {
        logger.error('Error getting phone progress:', error);
        res.status(500).json({ error: 'Failed to get phone progress' });
    }
};

const SocialScrapeController = {
    startImport,
    getStats,
    getImportProgress,
    getBlacklistProgress,
    getPhoneProgress,
    getPaginatedSocialScrapes,
    updateBlacklist,
    getProgress,
    updatePhoneNumber
};

module.exports = {
    SocialScrapeController
};