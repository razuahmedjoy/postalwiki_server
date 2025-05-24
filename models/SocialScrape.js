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
}, { timestamps: true });

// Compound index for unique url + date combination
socialScrapeSchema.index({ url: 1, date: 1 }, { unique: true });

module.exports = mongoose.model('SocialScrape', socialScrapeSchema);