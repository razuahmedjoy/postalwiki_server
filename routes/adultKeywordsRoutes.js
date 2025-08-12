const express = require('express');
const { AdultKeywordsController } = require('../controllers/AdultKeywords.controller');
const router = express.Router();

// BASE URL: /api/adult-keywords

// Public routes (no auth required)
router.get('/matching-progress', AdultKeywordsController.getMatchingProgress);

// Protected routes (auth required)
router.post('/start-matching', AdultKeywordsController.startMatching);
router.post('/stop-matching', AdultKeywordsController.stopMatching);

router.get('/stats', AdultKeywordsController.getStats);
router.get('/references', AdultKeywordsController.getReferences);
router.get('/references/paginated', AdultKeywordsController.getPaginatedReferences);

// Bulk action endpoints
router.post('/references/bulk-process', AdultKeywordsController.bulkProcessReferences);

module.exports = router; 