const mongoose = require('mongoose');

const addressMasterSchema = new mongoose.Schema({
    postcode: {
        type: String,
        required: true,
        trim: true
    },
    district: {
        type: String,
        required: true,
        trim: true
    },
    address: [{
        type: String,
        trim: true
    }],
    correctionVersion: {
        type: String,
        default: null
    },
    exceptionVersion: {
        type: String,
        default: null
    },
    dateCreated: {
        type: String,
        required: true
    }
}, {
    timestamps: true
});

// Indexes for efficient queries
addressMasterSchema.index({ postcode: 1 });
addressMasterSchema.index({ district: 1 });
addressMasterSchema.index({ postcode: 1, district: 1 });
addressMasterSchema.index({ postcode: 1, address: 1 }, { unique: true });
addressMasterSchema.index({ address: 'text' });

// Pre-save middleware for data cleaning
addressMasterSchema.pre('save', function(next) {
    if (this.postcode) {
        this.postcode = this.postcode.trim().toUpperCase();
    }
    if (this.district) {
        this.district = this.district.trim();
    }
    if (this.address && Array.isArray(this.address)) {
        this.address = this.address.map(addr => addr.trim()).filter(addr => addr.length > 0);
    }
    next();
});

// Virtual for full address string
addressMasterSchema.virtual('fullAddress').get(function() {
    if (!this.address || this.address.length === 0) return '';
    return this.address.join(', ');
});

// Ensure virtual fields are serialized
addressMasterSchema.set('toJSON', { virtuals: true });

module.exports = mongoose.model('AddressMaster', addressMasterSchema);