# Configuration Files

This directory contains the production and test configuration files for the Email Pricing Bot.

## Files

### Production Configuration
- **`core_config_production.json`** - Production core settings
- **`supplier_config_production.json`** - Production supplier configurations (starts with DEXTAR)
- **`brand_config_production.json`** - Production brand settings with real Google Drive folder IDs

### Test Configuration
- **`core_config_test.json`** - Test core settings
- **`supplier_config_test.json`** - Test supplier configurations (identical to production)
- **`brand_config_test.json`** - Test brand settings with placeholder folder IDs

## Key Configuration Details

### DEXTAR Supplier
- **Email Domain**: `dextarworldtrade.com`
- **Default Expiry**: 20 days
- **Discount**: None
- **Brands**: OPEL, FCA, MOPAR, PSA, HONDA, NISSAN, TOYOTA

### Email Settings
- **Monitor**: `pricing@ucalexports.com`
- **Summary Recipient**: `robin.ashford@ucalexports.com`
- **Ignore Domain**: `ucalexports.com`
- **Ignore Emails**: 
  - `noreply@ucalexports.com`
  - `automated@ucalexports.com`

### Execution Schedule
- **Time**: 18:00 IST (6:00 PM India Standard Time)
- **Frequency**: Daily
- **Config Check**: Every hour

### System Defaults
- **Default Expiry Duration**: 30 days (system-wide fallback)

## Typical DEXTAR Email Format

```
Subject: Toyota Price File for October 2025

Body:
Attached is the Toyota Price File for October 2025.

Prices Expire October 23, 2025
```

## Setup Instructions

### For Production

1. **Update GCP Project Details** in `core_config_production.json`:
   ```json
   "project_id": "your-actual-project-id",
   "bucket_name": "your-actual-bucket-name"
   ```

2. **Upload to GCS**:
   ```bash
   gsutil cp core_config_production.json gs://your-bucket/config/core_config.json
   gsutil cp supplier_config_production.json gs://your-bucket/config/supplier_config.json
   gsutil cp brand_config_production.json gs://your-bucket/config/brand_config.json
   ```

3. **Verify Google Drive Folder IDs** in `brand_config_production.json` are correct

### For Test

1. **Create Test Google Drive Folders** for each brand

2. **Update Folder IDs** in `brand_config_test.json`:
   - Replace all `TEST_FOLDER_ID_*` placeholders with actual test folder IDs

3. **Update GCP Project Details** in `core_config_test.json`

4. **Upload to GCS** (use different bucket or prefix):
   ```bash
   gsutil cp core_config_test.json gs://your-bucket/config-test/core_config.json
   gsutil cp supplier_config_test.json gs://your-bucket/config-test/supplier_config.json
   gsutil cp brand_config_test.json gs://your-bucket/config-test/brand_config.json
   ```

## Adding More Suppliers

To add additional suppliers to the configuration:

1. Edit the appropriate `supplier_config_*.json` file
2. Add a new supplier object with:
   - `supplier`: Supplier name (uppercase, underscores for spaces)
   - `email_domain`: Email domain to match
   - `default_expiry_days`: Optional default expiry
   - `discount_percent`: Optional discount percentage
   - `config`: Array of brand configurations
3. Upload updated file to GCS
4. No code deployment needed - changes take effect on next config reload

## Brand Aliases

Brand aliases allow flexible matching of brand names in filenames and subject lines:

- **MOPAR**: mopar, chrysler, dodge, jeep, ram
- **TOYOTA**: toyota, lexus, scion
- **NISSAN**: nissan, infiniti, datsun
- **VAG**: vag, volkswagen, vw, audi, skoda, seat
- **PSA**: psa, peugeot, citroen
- **OPEL**: opel, vauxhall
- **MERCEDES**: mercedes, mercedes-benz, benz

All matching is case-insensitive.

## Ignore Email List

Emails from these addresses will be ignored:
- Any email from `@ucalexports.com` domain
- `noreply@ucalexports.com`
- `automated@ucalexports.com`

To add more ignored emails, update the `ignore_emails` array in the core config.

## State Files

- **Production**: `state/last_processed_production.json`
- **Test**: `state/last_processed_test.json`

These track the last processed email timestamp to avoid reprocessing.

## Environment Indicator

The test configuration includes an `"environment": "test"` field to help identify which environment is running.

## Notes

- All configuration files are in JSON format
- Configuration is reloaded every hour (no redeployment needed for changes)
- Execution only happens once per day at the scheduled time
- Both production and test use the same mailbox but different output folders
