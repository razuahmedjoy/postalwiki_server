// controllers/SocialScrapeController.js
const { SocialScrapeService, IMPORT_DIR } = require('../services/SocialScrape.service');
const path = require('path');

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

const SocialScrapeController = {
    startImport,
    getStats,
    getImportProgress
};

module.exports = {
    SocialScrapeController
};