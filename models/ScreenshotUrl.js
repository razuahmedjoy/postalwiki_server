const mongoose = require('mongoose');



// Define the schema for the ScreenshotUrl collection
const screenshotUrlSchema = new mongoose.Schema({
    url: { type: String, required: true },
    image: { type: String, required: true, unique: true }
});


module.exports = mongoose.model('screenshot_url', screenshotUrlSchema);