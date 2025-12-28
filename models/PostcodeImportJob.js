const mongoose = require('mongoose');

const PostcodeImportJobSchema = new mongoose.Schema({
    status: {
        type: String,
        enum: ['pending', 'processing', 'completed', 'failed'],
        default: 'pending'
    },
    totalProcessed: {
        type: Number,
        default: 0
    },
    insertedCount: {
        type: Number,
        default: 0
    },
    errors: {
        type: Number,
        default: 0
    },
    errorLogs: [{
        type: String
    }],
    createdAt: {
        type: Date,
        default: Date.now,
        expires: 86400 // Auto-delete after 24 hours
    }
}, {
    collection: 'postcode_import_jobs',
    timestamps: true
});

const PostcodeImportJob = mongoose.model('PostcodeImportJob', PostcodeImportJobSchema);

module.exports = PostcodeImportJob;
