const express = require('express');
const { RMAddressController } = require('../controllers/RMAddress.controller');

const router = express.Router();

// BASE URL: /api/rm-address
router.get('/import-progress', RMAddressController.getImportProgress);
router.get('/stats', RMAddressController.getStats);
router.get('/paginated', RMAddressController.getPaginatedAddresses);

router.post('/import', RMAddressController.startImport);
router.post('/stop-import', RMAddressController.stopImport);

module.exports = router;
