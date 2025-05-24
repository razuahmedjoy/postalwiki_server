const mongoose = require('mongoose');



// Define the schema for the ScreenshotUrl collection
const screenshotUrlSchema = new mongoose.Schema({
    url: { type: String, required: true },
    image: { type: String, required: true, unique: true }
}, {
    versionKey: false
});

screenshotUrlSchema.index({ image: 1 }, { name: 'image_1' });

module.exports = mongoose.model('screenshot_url', screenshotUrlSchema);