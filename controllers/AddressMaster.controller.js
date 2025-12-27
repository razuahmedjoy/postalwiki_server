// controllers/AddressMaster.controller.js
const { AddressMasterService, IMPORT_DIR } = require('../services/AddressMaster.service');
const path = require('path');
const AddressMaster = require('../models/AddressMaster');
const addressMasterLogger = require('../config/loggers/addressMasterLogger');

const startImport = async (req, res) => {
    try {
        // Check if import is already running
        const currentProgress = AddressMasterService.getImportProgress();
        if (currentProgress.isRunning && !currentProgress.isComplete) {
            return res.status(409).json({
                success: false,
                message: 'Import is already running. Please wait for it to complete.'
            });
        }

        const files = await AddressMasterService.getImportFiles();
        
        // Limit the number of files to process in a single run to avoid overload
        const MAX_FILES_PER_RUN = 1000;
        let filesToProcess = files;
        if (files.length > MAX_FILES_PER_RUN) {
            addressMasterLogger.warn(`Found ${files.length} files, limiting to first ${MAX_FILES_PER_RUN} files for this run`);
            filesToProcess = files.slice(0, MAX_FILES_PER_RUN);
        }

        if (files.length === 0) {
            return res.status(404).json({ 
                success: false,
                message: 'No CSV files found to import' 
            });
        }

        // Reset import progress before starting new import
        AddressMasterService.resetImportProgress();

        // Set import as running
        AddressMasterService.setImportRunning(true);

        // Start processing files asynchronously
        processFiles(filesToProcess).catch(error => {
            addressMasterLogger.error('Error processing files:', error);
            // Set import as not running on error
            AddressMasterService.setImportRunning(false);
        });

        res.json({
            success: true,
            message: 'Import started',
            files: filesToProcess,
            totalFound: files.length,
            limitedTo: filesToProcess.length
        });
    } catch (error) {
        addressMasterLogger.error('Error starting import:', error);
        res.status(500).json({ success: false, error: error.message });
    }
};

const processFiles = async (filesToProcess) => {
    try {
        addressMasterLogger.info(`Starting to process ${filesToProcess.length} files`);
        
        for (const filename of filesToProcess) {
            const filePath = path.join(IMPORT_DIR, filename);
            
            try {
                await AddressMasterService.processFile(filePath);
            } catch (error) {
                addressMasterLogger.error(`Error processing file ${filename}:`, error);
                const progress = AddressMasterService.getImportProgress();
                progress.errors.push({
                    filename: filename,
                    error: error.message
                });
            }
        }

        // Mark import as complete
        AddressMasterService.setImportRunning(false);
        addressMasterLogger.info('Import process completed');

    } catch (error) {
        addressMasterLogger.error('Error in processFiles:', error);
        AddressMasterService.setImportRunning(false);
    }
};

const getImportProgress = async (req, res) => {
    try {
        const progress = AddressMasterService.getImportProgress();
        res.json({
            success: true,
            data: progress
        });
    } catch (error) {
        addressMasterLogger.error('Error getting import progress:', error);
        res.status(500).json({ success: false, error: error.message });
    }
};

const getStats = async (req, res) => {
    try {
        const stats = await AddressMasterService.getCollectionStats();
        res.json({
            success: true,
            stats: stats
        });
    } catch (error) {
        addressMasterLogger.error('Error getting collection stats:', error);
        res.status(500).json({ success: false, error: error.message });
    }
};

const getPaginatedData = async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = Math.min(parseInt(req.query.limit) || 100, 500);
        const skip = (page - 1) * limit;

        // Build query filters
        const query = {};
        
        if (req.query.searchPostcode) {
            query.postcode = { $regex: req.query.searchPostcode, $options: 'i' };
        }

        if (req.query.searchDistrict) {
            query.district = { $regex: req.query.searchDistrict, $options: 'i' };
        }

        if (req.query.searchAddress) {
            query.$text = { $search: req.query.searchAddress };
        }

        // Use cursor-based pagination for better performance on large datasets
        let cursor = null;
        if (req.query.cursor && req.query.useCursor === 'true') {
            cursor = { _id: { $gt: req.query.cursor } };
            Object.assign(query, cursor);
        }

        const totalCount = await AddressMaster.countDocuments(query);
        
        let addressMasterQuery = AddressMaster.find(query)
            .limit(limit)
            .lean();

        // Add text score for search relevance
        if (req.query.searchAddress) {
            addressMasterQuery = addressMasterQuery
                .select({ 
                    postcode: 1, 
                    district: 1, 
                    address: 1,
                    dateCreated: 1,
                    createdAt: 1,
                    updatedAt: 1,
                    score: { $meta: 'textScore' }
                })
                .sort({ score: { $meta: 'textScore' } });
        } else {
            addressMasterQuery = addressMasterQuery.sort({ _id: 1 });
            if (!cursor) {
                addressMasterQuery = addressMasterQuery.skip(skip);
            }
        }

        const addressMaster = await addressMasterQuery.exec();

        // Get next cursor for pagination
        const nextCursor = addressMaster.length > 0 ? addressMaster[addressMaster.length - 1]._id : null;
        const hasMore = addressMaster.length === limit;

        res.json({
            success: true,
            data: addressMaster,
            pagination: {
                page,
                limit,
                total: totalCount,
                hasMore,
                nextCursor
            }
        });

    } catch (error) {
        addressMasterLogger.error('Error getting paginated data:', error);
        res.status(500).json({ success: false, error: error.message });
    }
};

const getAddressByPostcode = async (req, res) => {
    try {
        const { postcode } = req.params;
        
        if (!postcode) {
            return res.status(400).json({ 
                success: false, 
                error: 'Postcode is required' 
            });
        }

        const addressMaster = await AddressMaster.findOne({ 
            postcode: postcode.toUpperCase() 
        }).lean();

        if (!addressMaster) {
            return res.status(404).json({ 
                success: false, 
                error: 'Address not found' 
            });
        }

        res.json({
            success: true,
            data: {
                ...addressMaster,
                fullAddress: addressMaster.address ? addressMaster.address.join(', ') : ''
            }
        });

    } catch (error) {
        addressMasterLogger.error('Error getting address by postcode:', error);
        res.status(500).json({ success: false, error: error.message });
    }
};

const searchAddresses = async (req, res) => {
    try {
        const { query, limit = 10 } = req.query;
        
        if (!query) {
            return res.status(400).json({ 
                success: false, 
                error: 'Search query is required' 
            });
        }

        const addresses = await AddressMaster.find(
            { $text: { $search: query } },
            { 
                postcode: 1, 
                district: 1, 
                address: 1,
                dateCreated: 1,
                score: { $meta: 'textScore' }
            }
        )
        .sort({ score: { $meta: 'textScore' } })
        .limit(parseInt(limit))
        .lean();

        res.json({
            success: true,
            data: addresses,
            count: addresses.length
        });

    } catch (error) {
        addressMasterLogger.error('Error searching addresses:', error);
        res.status(500).json({ success: false, error: error.message });
    }
};

const stopImport = async (req, res) => {
    try {
        const currentProgress = AddressMasterService.getImportProgress();
        
        if (!currentProgress.isRunning || currentProgress.isComplete) {
            return res.status(400).json({
                success: false,
                message: 'No import is currently running.'
            });
        }

        // Set import as not running and complete
        AddressMasterService.setImportRunning(false);
        
        addressMasterLogger.info('Import stopped by user request');
        
        res.json({
            success: true,
            message: 'Import stopped successfully'
        });

    } catch (error) {
        addressMasterLogger.error('Error stopping import:', error);
        res.status(500).json({ success: false, error: error.message });
    }
};

const deleteAllData = async (req, res) => {
    try {
        addressMasterLogger.info('Starting to delete all AddressMaster data');
        
        // Get count before deletion for response
        const count = await AddressMaster.countDocuments({});
        
        // Delete all documents in the collection
        const result = await AddressMaster.deleteMany({});
        
        addressMasterLogger.info(`Successfully deleted ${result.deletedCount} AddressMaster records`);
        
        res.json({
            success: true,
            message: `Successfully deleted all AddressMaster data`,
            deletedCount: result.deletedCount,
            totalCount: count
        });

    } catch (error) {
        addressMasterLogger.error('Error deleting all AddressMaster data:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message,
            message: 'Failed to delete AddressMaster data'
        });
    }
};

const AddressMasterController = {
    startImport,
    getImportProgress,
    getStats,
    getPaginatedData,
    getAddressByPostcode,
    searchAddresses,
    stopImport,
    deleteAllData
};

module.exports = {
    AddressMasterController
};