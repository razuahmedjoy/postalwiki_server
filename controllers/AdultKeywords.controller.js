// controllers/AdultKeywords.controller.js
const { AdultKeywordsService } = require('../services/AdultKeywords.service');
const socialScrapeLogger = require('../config/socialScrapeLogger');

const startMatching = async (req, res) => {
    try {
        const result = await AdultKeywordsService.startMatching();
        
        res.json({
            success: true,
            message: result.message,
            files: result.files
        });
    } catch (error) {
        socialScrapeLogger.error('Error in startMatching controller:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
};

const stopMatching = async (req, res) => {
    try {
        const result = await AdultKeywordsService.stopMatching();
        
        res.json({
            success: result.success,
            message: result.message
        });
    } catch (error) {
        socialScrapeLogger.error('Error in stopMatching controller:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
};

const getMatchingProgress = async (req, res) => {
    try {
        const progress = AdultKeywordsService.getMatchingProgress();
        
        res.json({
            success: true,
            progress: progress
        });
    } catch (error) {
        socialScrapeLogger.error('Error in getMatchingProgress controller:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
};

const getStats = async (req, res) => {
    try {
        const stats = await AdultKeywordsService.getStats();
        
        res.json({
            success: true,
            stats: stats
        });
    } catch (error) {
        socialScrapeLogger.error('Error in getStats controller:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
};

const getReferences = async (req, res) => {
    try {
        const references = await AdultKeywordsService.getReferences();
        
        res.json({
            success: true,
            references: references
        });
    } catch (error) {
        socialScrapeLogger.error('Error in getReferences controller:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
};

const getPaginatedReferences = async (req, res) => {
    try {
        const { page = 1, limit = 50, matchType, processed } = req.query;
        
        const result = await AdultKeywordsService.getPaginatedReferences(
            parseInt(page), 
            parseInt(limit), 
            matchType, 
            processed !== undefined ? processed === 'true' : null
        );
        
        res.json({
            success: true,
            data: result
        });
    } catch (error) {
        socialScrapeLogger.error('Error in getPaginatedReferences controller:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
};

module.exports = {
    AdultKeywordsController: {
        startMatching,
        stopMatching,
        getMatchingProgress,
        getStats,
        getReferences,
        getPaginatedReferences
    }
}; 