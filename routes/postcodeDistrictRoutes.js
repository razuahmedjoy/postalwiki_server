const express = require('express');
const router = express.Router();
const postcodeController = require('../controllers/postcodeDistrictController');
const multer = require('multer');
const path = require('path');
const fs = require('fs');

// Configure Multer for temp storage
const uploadDir = path.join(__dirname, '../imports/temp');
if (!fs.existsSync(uploadDir)) {
    fs.mkdirSync(uploadDir, { recursive: true });
}

const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, uploadDir);
    },
    filename: function (req, file, cb) {
        cb(null, Date.now() + '-' + file.originalname);
    }
});

const upload = multer({
    storage: storage,
    limits: { fileSize: 100 * 1024 * 1024 }, // 100MB limit
    fileFilter: (req, file, cb) => {
        // Simple CSV check
        if (file.mimetype === 'text/csv' || file.mimetype === 'application/vnd.ms-excel' || file.originalname.endsWith('.csv')) {
            cb(null, true);
        } else {
            cb(new Error('Only CSV files are allowed!'), false);
        }
    }
});

// Import Routes
router.post('/import/start', postcodeController.startImportJob);
router.post('/import/upload/:jobId', upload.single('file'), postcodeController.uploadAndProcess);
router.get('/import/status/:jobId', postcodeController.getImportStatus);

// CRUD Routes
router.post('/search', postcodeController.searchPostcodes);
router.post('/create', postcodeController.createEntry);
router.put('/:id', postcodeController.updateEntry);
router.delete('/:id', postcodeController.deleteEntry);

module.exports = router;
