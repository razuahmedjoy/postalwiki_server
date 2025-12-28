const PostcodeDistrict = require('../models/PostcodeDistrict');
const PostcodeImportJob = require('../models/PostcodeImportJob');
const postcodeLogger = require('../config/loggers/postcodeDistrictLogger');
const fs = require('fs');
const { parse } = require('csv-parse');

// Start Import Job
exports.startImportJob = async (req, res) => {
    try {
        const job = await PostcodeImportJob.create({
            status: 'pending'
        });
        postcodeLogger.info(`Started new import job: ${job._id}`);
        res.status(200).json({ jobId: job._id });
    } catch (error) {
        postcodeLogger.error(`Error starting import job: ${error.message}`);
        res.status(500).json({ message: 'Failed to start import job' });
    }
};

// Upload and Process File (Background)
exports.uploadAndProcess = async (req, res) => {
    const { jobId } = req.params;

    if (!req.file) {
        return res.status(400).json({ message: 'No file uploaded' });
    }

    try {
        // Return 202 immediately
        res.status(202).json({ message: 'File accepted for processing' });

        // Identify Job
        const job = await PostcodeImportJob.findById(jobId);
        if (!job) {
            postcodeLogger.warn(`Job ${jobId} not found, deleting file.`);
            fs.unlinkSync(req.file.path);
            return;
        }

        job.status = 'processing';
        await job.save();
        postcodeLogger.info(`Processing file for job ${jobId}: ${req.file.originalname}`);

        // Start Processing
        processCsvFile(req.file.path, job);

    } catch (error) {
        postcodeLogger.error(`Error in upload handler: ${error.message}`);
    }
};

async function processCsvFile(filePath, job) {
    const batchSize = 5000;
    let batch = [];
    let processedCount = 0;

    postcodeLogger.info(`Starting CSV parse for job ${job._id}`);

    try {
        // Create Read Stream
        const parser = fs.createReadStream(filePath)
            .pipe(parse({
                columns: ['district', 'postcode'], // Explicit columns for headerless file
                skip_empty_lines: true,
                trim: true,
                relax_column_count: true
            }));

        for await (const row of parser) {
            // Check if it's a header row inadvertently
            if (row.district && row.district.toLowerCase() === 'district' &&
                row.postcode && row.postcode.toLowerCase().includes('postcode')) {
                postcodeLogger.info(`Skipping header row for job ${job._id}`);
                continue;
            }

            const postcode = row.postcode;
            const district = row.district;

            if (postcode && district) {
                batch.push({
                    postcode: postcode.trim(),
                    district: district.trim()
                });
            }

            if (batch.length >= batchSize) {
                await insertBatch(batch, job);
                processedCount += batch.length;
                job.totalProcessed = processedCount;
                await job.save();
                batch = [];
                postcodeLogger.debug(`Job ${job._id}: Processed ${processedCount} rows`);
            }
        }

        // Insert remaining
        if (batch.length > 0) {
            await insertBatch(batch, job);
            processedCount += batch.length;
            job.totalProcessed = processedCount;
            await job.save();
        }

        // Finish
        job.status = 'completed';
        await job.save();
        postcodeLogger.info(`Job ${job._id} completed. Total processed: ${processedCount}, Inserted: ${job.insertedCount}, Errors: ${job.errors}`);

    } catch (error) {
        postcodeLogger.error(`Error processing CSV for job ${job._id}: ${error.message}`);
        job.status = 'failed';
        job.errorLogs.push(error.message);

        // Ensure not saving NaN which causes validation error like the user saw
        if (isNaN(job.insertedCount)) {
            postcodeLogger.warn(`Fixed NaN insertedCount before saving failure status.`);
            job.insertedCount = 0;
        }
        if (isNaN(job.errors)) job.errors = 0;

        await job.save();
    } finally {
        // Cleanup
        try {
            if (fs.existsSync(filePath)) {
                fs.unlinkSync(filePath);
            }
        } catch (e) { postcodeLogger.error('Error deleting temp file', e); }
    }
}

async function insertBatch(batch, job) {
    try {
        const result = await PostcodeDistrict.insertMany(batch, { ordered: false });
        job.insertedCount += result.length;
    } catch (error) {
        // Detailed logging for debugging production issues
        postcodeLogger.debug(`Batch insertion exception. Code: ${error.code}, Name: ${error.name}.`);

        if (error.code === 11000 || error.name === 'BulkWriteError' || error.name === 'MongoBulkWriteError') {
            // Handle duplicates / partial success

            // 1. Calculate inserted count safely
            // Mongoose creates error.insertedDocs for unordered mode
            const insertedDocsCount = error.insertedDocs ? error.insertedDocs.length : 0;
            // Native driver might provide result.nInserted
            const resultInserted = (error.result && typeof error.result.nInserted === 'number') ? error.result.nInserted : 0;

            // Use whichever is available, default to 0
            // Logic: If result.nInserted is available, it's usually reliable. If not, insertedDocs length.
            const nInserted = Math.max(insertedDocsCount, resultInserted);

            job.insertedCount += nInserted;

            if (isNaN(job.insertedCount)) {
                postcodeLogger.error(`Critical: Job insertedCount became NaN. Batch size: ${batch.length}, nInserted derived: ${nInserted}`);
                job.insertedCount = 0;
            }

            // 2. Handle Errors
            if (error.writeErrors) {
                job.errors += error.writeErrors.length;

                // Log sample for user
                if (error.writeErrors.length > 0 && job.errorLogs.length < 50) {
                    const firstErr = error.writeErrors[0];
                    const opCode = firstErr.err && firstErr.err.op ? firstErr.err.op.postcode : 'Batch Error';
                    job.errorLogs.push(`Duplicate/Error: ${opCode}`);
                }

                // Detailed server log
                if (error.writeErrors.length > 0) {
                    postcodeLogger.info(`Batch had ${error.writeErrors.length} duplicates. Successfully inserted: ${nInserted}`);
                }
            } else {
                job.errors += (batch.length - nInserted);
                postcodeLogger.warn(`Batch error 11000 but no writeErrors found. Full error: ${JSON.stringify(error)}`);
            }
        } else {
            // General unexpected error
            job.errors += batch.length;
            job.errorLogs.push(error.message || 'Unknown batch error');
            postcodeLogger.error(`Batch insert critical failure job ${job._id}: ${error.message}`, { stack: error.stack });
        }
    }
}

// Get Job Status
exports.getImportStatus = async (req, res) => {
    try {
        const job = await PostcodeImportJob.findById(req.params.jobId);
        if (!job) return res.status(404).json({ message: 'Job not found' });
        res.status(200).json(job);
    } catch (error) {
        postcodeLogger.error(`Error getting status: ${error.message}`);
        res.status(500).json({ message: 'Error fetching status' });
    }
};

// Search with Pagination
exports.searchPostcodes = async (req, res) => {
    const { postcode, district, page = 1, limit = 500 } = req.body;

    try {
        const query = {};
        if (postcode) query.postcode = { $regex: postcode, $options: 'i' };
        if (district) query.district = { $regex: district, $options: 'i' };

        const pageNum = parseInt(page);
        const limitNum = parseInt(limit);
        const skip = (pageNum - 1) * limitNum;

        // Count logic: if empty query, use fast estimate
        let count;
        if (Object.keys(query).length === 0) {
            count = await PostcodeDistrict.estimatedDocumentCount();
        } else {
            count = await PostcodeDistrict.countDocuments(query);
        }

        const data = await PostcodeDistrict.find(query)
            .sort({ _id: 1 })
            .skip(skip)
            .limit(limitNum)
            .lean(); // Optimization

        res.status(200).json({
            success: true,
            count: data.length,
            total: count,
            page: pageNum,
            totalPages: Math.ceil(count / limitNum),
            data
        });
    } catch (error) {
        postcodeLogger.error(`Search failed: ${error.message}`);
        res.status(500).json({ message: 'Search failed', error: error.message });
    }
};

// Create Single
exports.createEntry = async (req, res) => {
    try {
        const { postcode, district } = req.body;
        const entry = await PostcodeDistrict.create({ postcode, district });
        postcodeLogger.info(`Created entry: ${postcode}`);
        res.status(201).json({ success: true, data: entry });
    } catch (error) {
        if (error.code === 11000) {
            return res.status(400).json({ message: 'Postcode already exists' });
        }
        postcodeLogger.error(`Creation failed: ${error.message}`);
        res.status(500).json({ message: 'Creation failed' });
    }
};

// Update
exports.updateEntry = async (req, res) => {
    try {
        const { id } = req.params;
        const { district } = req.body;
        await PostcodeDistrict.updateOne({ _id: id }, { $set: { district } });
        postcodeLogger.info(`Updated entry ${id}`);
        res.status(200).json({ success: true });
    } catch (error) {
        postcodeLogger.error(`Update failed: ${error.message}`);
        res.status(500).json({ message: 'Update failed' });
    }
};

// Delete
exports.deleteEntry = async (req, res) => {
    try {
        const { id } = req.params;
        await PostcodeDistrict.deleteOne({ _id: id });
        postcodeLogger.info(`Deleted entry ${id}`);
        res.status(200).json({ success: true });
    } catch (error) {
        postcodeLogger.error(`Deletion failed: ${error.message}`);
        res.status(500).json({ message: 'Deletion failed' });
    }
};
