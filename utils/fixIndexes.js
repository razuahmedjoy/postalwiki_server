const mongoose = require('mongoose');
const SocialScrape = require('../models/SocialScrape');

const fixIndexes = async () => {
    try {
        console.log('Connecting to MongoDB...');
        await mongoose.connect(process.env.MONGODB_URI || 'mongodb://localhost:27017/web_postalwiki');
        
        console.log('Checking existing indexes...');
        const indexes = await SocialScrape.collection.indexes();
        
        console.log('Current indexes:');
        indexes.forEach((index, i) => {
            console.log(`${i}: ${JSON.stringify(index.key)} - unique: ${index.unique || false}`);
        });
        
        // Find and drop the problematic compound unique index
        const problematicIndex = indexes.find(index => 
            index.key && 
            index.key.url === 1 && 
            index.key.date === -1 && 
            index.unique === true
        );
        
        if (problematicIndex) {
            console.log(`Found problematic index: ${JSON.stringify(problematicIndex.key)}`);
            console.log('Dropping problematic compound unique index...');
            await SocialScrape.collection.dropIndex(problematicIndex.name);
            console.log('Successfully dropped problematic index');
        } else {
            console.log('No problematic compound unique index found');
        }
        
        // Ensure the correct indexes exist
        console.log('Creating correct indexes...');
        
        // Drop any existing compound index
        try {
            await SocialScrape.collection.dropIndex('url_1_date_1');
            console.log('Dropped old compound index');
        } catch (e) {
            console.log('No old compound index to drop');
        }
        
        // Create the correct indexes
        await SocialScrape.collection.createIndex({ date: -1 }, { background: true });
        console.log('Created date index');
        
        await SocialScrape.collection.createIndex({ url: 'text' }, { background: true });
        console.log('Created text index');
        
        console.log('Index fix completed successfully');
        
    } catch (error) {
        console.error('Error fixing indexes:', error);
    } finally {
        await mongoose.disconnect();
        console.log('Disconnected from MongoDB');
    }
};

// Run the fix if this script is executed directly
if (require.main === module) {
    fixIndexes();
}

module.exports = { fixIndexes }; 