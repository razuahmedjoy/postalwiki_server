const express = require('express');
const { SocialScrapeController } = require('../controllers/SocialScrape.controller');
const router = express.Router();
const { importEventEmitter, blacklistEventEmitter } = require('../services/SocialScrape.service');

// BASE URL: /api/social-scrape

// Public routes (no auth required)
router.get('/import-progress', SocialScrapeController.getImportProgress);
router.get('/blacklist-progress', SocialScrapeController.getBlacklistProgress);

// Protected routes (auth required)
router.post('/import', SocialScrapeController.startImport);
router.post('/update-blacklist', SocialScrapeController.updateBlacklist);
router.get('/stats', SocialScrapeController.getStats);
router.get('/paginated', SocialScrapeController.getPaginatedSocialScrapes);

module.exports = router;