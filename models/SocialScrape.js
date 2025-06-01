// models/SocialScrape.js
const mongoose = require('mongoose');

const socialScrapeSchema = new mongoose.Schema({
  url: { type: String, required: true },
  date: { type: Date, required: true },
  title: String,
  twitter: String,
  postcode: String,
  email: String,
  phone: String,
  facebook: String,
  youtube: String,
  instagram: String,
  linkedin: String,
  pinterest: String,
  keywords: String,
  statusCode: String,
  redirect_url: String,
  meta_description: String,
  is_blacklisted: { type: Boolean, default: false },
  
}, { timestamps: true, collection: 'socialscrapes', strict: false });

// Compound index for unique url + date combination
socialScrapeSchema.index({ url: 1, date: 1 }, { unique: true, background: true });
// Add a dedicated index for URL search
socialScrapeSchema.index({ url: 1 }, { background: true });



// Add error handling for duplicate key errors
socialScrapeSchema.post('save', function(error, doc, next) {
  if (error.name === 'MongoError' && error.code === 11000) {
    next(new Error('Duplicate key error'));
  } else {
    next(error);
  }
});

const SocialScrape = mongoose.model('SocialScrape', socialScrapeSchema);

module.exports = SocialScrape;