# Address Master Import System

This system provides CSV import functionality for address master data, following the same architecture pattern as other features in the postalwiki_server.

## Features

- CSV file import with automatic processing
- Postcode and district validation
- Address field cleaning and standardization
- Batch processing for memory efficiency
- Progress tracking with real-time updates
- Error handling and comprehensive logging
- File archiving after processing
- Search and pagination capabilities
- RESTful API endpoints

## CSV Format

The system expects CSV files with the following format (based on the db_export implementation):

- Column 0: `F1` → Postcode (required)
- Column 1+: Address fields (at least one required)

Example CSV format:
```csv
F1,F2,F3,F4,F5
SW1A 1AA,123 High Street,Apartment 1,Building A,
W1K 6PZ,456 Oxford Street,Suite 200,,
```

*Note: The column mapping matches the PHP implementation in db_export*

## File Structure

```
server/
├── models/
│   └── AddressMaster.js              # Address Master model with indexes
├── services/
│   └── AddressMaster.service.js      # Import service logic
├── controllers/
│   └── AddressMaster.controller.js   # API controller
├── routes/
│   └── addressMasterRoutes.js        # API routes
├── config/
│   └── loggers/
│       └── addressMasterLogger.js    # Dedicated logger
└── imports/
    └── address_master/               # CSV files directory
        └── completed_YYYY-MM-DD/    # Processed files
```

## API Endpoints

### Start Import
```
POST /api/address-master/import
```
Starts processing CSV files in the import directory.

**Headers:**
- `Authorization: Bearer <token>`
- `Content-Type: application/json`

**Response:**
```json
{
  "success": true,
  "message": "Import started",
  "files": ["address_master_data.csv"],
  "totalFound": 1,
  "limitedTo": 1
}
```

### Get Import Progress
```
GET /api/address-master/import-progress
```
Returns current import progress and status.

**Response:**
```json
{
  "success": true,
  "data": {
    "currentFile": "address_master_data.csv",
    "processed": 1500,
    "total": 0,
    "upserted": 1200,
    "modified": 300,
    "errors": [],
    "isComplete": false,
    "isRunning": true
  }
}
```

### Stop Import
```
POST /api/address-master/stop-import
```
Stops the currently running import process.

**Response:**
```json
{
  "success": true,
  "message": "Import stopped successfully"
}
```

### Get Collection Stats
```
GET /api/address-master/stats
```
Returns the total number of address master records.

**Response:**
```json
{
  "success": true,
  "stats": 2345678
}
```

### Get Paginated Data
```
GET /api/address-master/data?page=1&limit=100&searchPostcode=SW1&searchDistrict=Westminster&searchAddress=street
```
Returns paginated address master data with optional filters.

**Query Parameters:**
- `page` (number): Page number (default: 1)
- `limit` (number): Items per page (default: 100, max: 500)
- `searchPostcode` (string): Filter by postcode
- `searchDistrict` (string): Filter by district
- `searchAddress` (string): Full-text search in address fields
- `cursor` (string): Cursor for cursor-based pagination
- `useCursor` (boolean): Enable cursor-based pagination

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "_id": "66e123456789abcdef123456",
      "postcode": "SW1A 1AA",
      "district": "SW1A",
      "address": ["123 High Street", "Apartment 1"],
      "dateCreated": "15/03/2023",
      "fullAddress": "123 High Street, Apartment 1",
      "createdAt": "2025-09-20T10:30:00.000Z",
      "updatedAt": "2025-09-20T10:30:00.000Z"
    }
  ],
  "pagination": {
    "page": 1,
    "limit": 100,
    "total": 4567890,
    "hasMore": true,
    "nextCursor": "66e123456789abcdef123456"
  }
}
```

### Search Addresses
```
GET /api/address-master/search?query=high street&limit=10
```
Performs text search across address fields.

**Response:**
```json
{
  "success": true,
  "data": [
    {
      "postcode": "SW1A 1AA",
      "district": "SW1A",
      "address": ["123 High Street", "Apartment 1"],
      "dateCreated": "15/03/2023",
      "score": 1.5
    }
  ],
  "count": 1
}
```

### Get Address by Postcode
```
GET /api/address-master/postcode/:postcode
```
Returns detailed information for a specific postcode.

**Response:**
```json
{
  "success": true,
  "data": {
    "postcode": "SW1A 1AA",
    "district": "SW1A",
    "address": ["123 High Street", "Apartment 1"],
    "dateCreated": "15/03/2023",
    "fullAddress": "123 High Street, Apartment 1",
    "createdAt": "2025-09-20T10:30:00.000Z",
    "updatedAt": "2025-09-20T10:30:00.000Z"
  }
}
```

## Usage

1. Place CSV files in the `server/imports/address_master/` directory
2. Start the import process via API
3. Monitor progress using the progress endpoint
4. Processed files are automatically moved to `completed_YYYY-MM-DD/` subdirectories

## Data Processing

The system automatically:
- Validates required fields (postcode and address fields)
- Cleans and trims text data
- Extracts district from postcode
- Converts backslashes to forward slashes
- Removes duplicate district entries from address fields
- Handles duplicate addresses using upsert logic
- Adds import timestamp

## Error Handling

- CSV parsing errors are logged but don't stop the process
- Invalid records are logged and skipped
- Malformed records are logged and skipped
- Duplicate postcode/address combinations are handled with upsert logic

## Performance

- Batch size: 2000 records per batch
- Parallel processing: 2 batches at a time
- Memory efficient streaming for large files
- Indexed database queries for fast retrieval
- Cursor-based pagination for large datasets

## Model Schema

```javascript
{
  postcode: String,              // Required, trimmed, uppercase, indexed
  district: String,              // Required, trimmed, indexed
  address: [String],             // Array of address fields, trimmed
  dateCreated: String,           // Date in d/m/Y format
  correctionVersion: String,     // Optional correction version
  exceptionVersion: String,      // Optional exception version
  is_blacklisted: Boolean,       // Default: false
  createdAt: Date,              // Auto-generated
  updatedAt: Date               // Auto-generated
}
```

## Indexes

- Index on `postcode`
- Index on `district`
- Compound index on `postcode + district`
- Unique index on `postcode + address`
- Text index on `address` for search
- Index on `createdAt` for sorting

## Logging

All import activities are logged to:
- `logs/address_master/combined.log` - All log levels
- `logs/address_master/error.log` - Error level only

Log format includes timestamps, service identification, and structured data.

## Authentication

All endpoints require:
- Valid JWT token in Authorization header
- Admin role for import/management operations

## Frontend Integration

The frontend provides:
- Import interface with progress tracking
- Data table with search and pagination
- Real-time import status updates
- Error display and handling

## Testing

Test the import functionality by:
1. Creating sample CSV files with proper format
2. Uploading them to the import directory
3. Starting import via API
4. Monitoring progress and logs
5. Verifying data in the search interface

## Notes

- The system is designed to handle large CSV files efficiently
- File processing is asynchronous to avoid blocking the API
- Progress tracking allows monitoring of long-running imports
- The data structure matches the db_export PHP implementation for compatibility
- Address fields are stored as arrays for flexible querying
- District extraction from postcode follows UK postcode standards