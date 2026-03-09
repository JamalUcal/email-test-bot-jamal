# Supplier Configuration Management

This directory contains supplier configuration files for the email pricing bot.

## Structure

- **`supplier_config.json`** - The main configuration file used in production (merged from individual files)
- **`suppliers/`** - Directory containing individual supplier configuration files
- **`split_suppliers.py`** - Script to split `supplier_config.json` into individual files
- **`merge_suppliers.py`** - Script to merge individual files back into `supplier_config.json`

## Workflow

### Editing Supplier Configurations

1. **Work on individual supplier files** in the `suppliers/` directory
   - Each supplier has its own JSON file (e.g., `al_babtain.json`, `allegiance_automotive.json`)
   - This makes it easier to edit and review changes for specific suppliers

2. **After making changes**, merge them back into the main config:
   ```bash
   python3 merge_suppliers.py
   ```

3. **Deploy the updated config** to production:
   ```bash
   cd ../..
   ./deploy/update-and-deploy.sh -config
   ```

### Adding a New Supplier

1. Create a new JSON file in `suppliers/` directory with the supplier name (lowercase, underscores for spaces)
2. Follow the structure of existing supplier files
3. Run `merge_suppliers.py` to update `supplier_config.json`

### Splitting the Config (if needed)

If you need to regenerate individual files from `supplier_config.json`:
```bash
python3 split_suppliers.py
```

**Note:** This will overwrite all files in the `suppliers/` directory.

## Supplier File Format

Each supplier file should follow this structure:

```json
{
  "supplier": "SUPPLIER_NAME",
  "email_domain": "example.com",
  "default_expiry_days": 20,
  "metadata": {
    "location": "LOCATION",
    "currency": "USD",
    "decimalFormat": "decimal"
  },
  "config": [
    {
      "brand": "BRAND_NAME"
      // location, currency, decimalFormat inherited from metadata
      // Can override at brand level if needed
    }
  ]
}
```

**Note:** The `columns` field is deprecated - the system now uses intelligent header detection.

## Tips

- Always merge after editing individual files before deploying
- The merge script processes files in alphabetical order
- Keep supplier names consistent (uppercase with underscores)
- Test configuration changes in the test environment first
