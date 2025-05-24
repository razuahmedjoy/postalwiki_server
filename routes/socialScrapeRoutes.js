const express = require('express');
const { SocialScrapeController } = require('../controllers/SocialScrape.controller');
const router = express.Router();

// BASE URL: /api/social-scrape

// Public routes (no auth required)
router.get('/progress', SocialScrapeController.getImportProgress);

// Protected routes (auth required)
router.post('/import', SocialScrapeController.startImport);
router.get('/stats', SocialScrapeController.getStats);

module.exports = router;