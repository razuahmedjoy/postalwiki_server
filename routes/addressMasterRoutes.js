const express = require('express');
const router = express.Router();
const { AddressMasterController } = require('../controllers/AddressMaster.controller');

// BASE URL: /api/address-master

// Public routes (no auth required) - though these are protected at app level
router.get('/import-progress', AddressMasterController.getImportProgress);
router.get('/stats', AddressMasterController.getStats);
router.get('/search', AddressMasterController.searchAddresses);
router.get('/postcode/:postcode', AddressMasterController.getAddressByPostcode);
router.get('/data', AddressMasterController.getPaginatedData);

// Protected routes (auth required) - protected at app level
router.post('/import', AddressMasterController.startImport);
router.post('/stop-import', AddressMasterController.stopImport);
router.delete('/delete-all', AddressMasterController.deleteAllData);

module.exports = router;