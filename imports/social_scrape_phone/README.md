# Phone Number Processing

This directory contains CSV files for phone number processing in the Social Scrape system.

## CSV Format

The CSV files should have the following format (no headers):

```
url,code,phone_number,date
```

### Columns:
1. **url** - The website URL (e.g., example.com)
2. **code** - Must be `[PN]` to indicate phone number data
3. **phone_number** - The phone number to be processed
4. **date** - The date (ignored for phone processing)

### Example:
```
example.com,[PN],+44123456789,10-06-2025
test.co.uk,[PN],07123456789,10-06-2025
demo.org,[PN],02012345678,10-06-2025
```

## Phone Number Validation Rules

### Length Requirements
- Phone numbers must be exactly **10 or 11 digits** after cleaning
- Numbers with fewer or more digits are rejected

### Country Code Validation
The system validates phone numbers against a comprehensive list of country codes and their expected lengths. Each country has specific phone number length requirements.

### Cleaning Process
1. **Remove spaces** - All spaces are removed
2. **Remove dashes** - All dashes (-) are removed
3. **Remove dots** - All decimal points (.) are removed
4. **Remove brackets** - All brackets ( ) [ ] are removed
5. **Country code validation** - Check against valid country codes
6. **Length validation** - Verify number length matches country requirements
7. **Formatting** - Apply proper formatting with country code brackets

### Phone Number Formatting
Phone numbers are formatted as: `[+COUNTRY_CODE] PHONE_NUMBER`

#### Examples of Formatting:
- `+44535931969` → `[+44] 0535931969` (UK, added leading 0)
- `+42059547892` → `[+420] 059547892` (Czech Republic, added leading 0)
- `+35123456789` → `[+351] 0123456789` (Portugal, added leading 0)
- `+240-222-039796` → `[+240] 222039796` (Equatorial Guinea)
- `07508770171` → `[+44] 7508770171` (UK number without country code)

### Special Handling for UK Domains
- URLs ending in `.co.uk` or `.uk` are given special consideration for UK phone numbers
- UK numbers are formatted as `[+44] PHONE_NUMBER`

### Invalid Numbers (Will be Rejected):
- Numbers with decimal points: `0.161748457891`
- Numbers that are too long: `221748455043592`
- Numbers that are too short: `123456789`
- Numbers that don't match any country's requirements
- Numbers with invalid country codes

## URL Processing Rules

### Row Limit
- URLs with **more than 3 rows** are completely skipped
- All phone numbers for such URLs are rejected
- This helps eliminate spam and invalid data

### Duplicate Handling
- Multiple phone numbers for the same URL are grouped together
- Duplicate phone numbers are automatically removed
- Each URL gets a unique array of phone numbers

## Processing Behavior

1. **Existing Records**: If a URL exists in the database, phone numbers are added to the existing `phone` array
2. **New Records**: If a URL doesn't exist, a new record is created with the phone numbers
3. **Multiple Phones**: Multiple phone numbers for the same URL are grouped together
4. **Duplicate Prevention**: Duplicate phone numbers are automatically removed
5. **URL Filtering**: URLs with more than 3 rows are completely skipped
6. **Phone Number Limit**: Each URL is limited to a maximum of **3 phone numbers**
   - If a URL has more than 3 phone numbers after processing, only the first 3 are kept
   - This applies to both new records and updates to existing records
   - Additional phone numbers are logged as skipped

## Logging

Processing logs are saved to `logs/social_scrape/phone_logs.log` with detailed information about:
- Records processed
- Records updated
- New records created
- Errors encountered
- URLs skipped due to row count
- Invalid phone numbers rejected

## Testing

A test file `test_phone_formatting.csv` is included with various phone number formats to verify the validation and formatting rules:

```
example.co.uk,[PN],+44535931969,10-06-2025
test.co.uk,[PN],+42059547892,10-06-2025
demo.org,[PN],+35123456789,10-06-2025
uk-site.co.uk,[PN],+240-222-039796,10-06-2025
local-site.co.uk,[PN],07508770171,10-06-2025
invalid-site.com,[PN],0.161748457891,10-06-2025
```

Expected results:
- `+44535931969` → `[+44] 0535931969` (UK, added leading 0)
- `+42059547892` → `[+420] 059547892` (Czech Republic, added leading 0)
- `+35123456789` → `[+351] 0123456789` (Portugal, added leading 0)
- `+240-222-039796` → `[+240] 222039796` (Equatorial Guinea)
- `07508770171` → `[+44] 7508770171` (UK number)
- `0.161748457891` → Rejected (contains decimal point)

## File Management

After processing, CSV files are automatically moved to a `completed_YYYY-MM-DD` subdirectory with a timestamp prefix. 