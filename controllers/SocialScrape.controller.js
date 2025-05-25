// controllers/SocialScrapeController.js
const { SocialScrapeService, IMPORT_DIR } = require('../services/SocialScrape.service');
const path = require('path');
const SocialScrape = require('../models/SocialScrape');

const startImport = async (req, res) => {
    try {
        const files = await SocialScrapeService.getImportFiles();

        if (files.length === 0) {
            return res.status(404).json({ message: 'No CSV files found to import' });
        }

        // Start processing files asynchronously
        processFiles(files).catch(error => {
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

const processFiles = async (files) => {
    for (const file of files) {
        const filePath = path.join(IMPORT_DIR, file);
        await SocialScrapeService.processFile(filePath);
    }
};

const getStats = async (req, res) => {
    try {
        const stats = await SocialScrapeService.getCollectionStats();
        res.json({ success: true, stats });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
};

const getImportProgress = (req, res) => {
    try {
        const progress = SocialScrapeService.getProgress();
        res.json({
            success: true,
            data: progress
        });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
};

const getPaginatedSocialScrapes = async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 10;
        const searchUrl = req.query.searchUrl || '';
        const skip = (page - 1) * limit;

        // Build search query
        const query = {};
        if (searchUrl) {
            query.url = { $regex: searchUrl, $options: 'i' };
        }

        // Get total count for pagination
        const total = await SocialScrape.countDocuments(query);

        // Get paginated data
        const socialScrapes = await SocialScrape.find(query)
            .sort({ date: -1 })
            .skip(skip)
            .limit(limit);

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

const SocialScrapeController = {
    startImport,
    getStats,
    getImportProgress,
    getPaginatedSocialScrapes
};

module.exports = {
    SocialScrapeController
};