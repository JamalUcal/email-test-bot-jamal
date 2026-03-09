#!/usr/bin/env python3
"""
Migrate configuration files to use supplier-level defaults.

This script:
1. Converts "european" to "comma" in all decimalFormat fields
2. Identifies common values for location, currency, decimalFormat across brands
3. Moves common values to supplier metadata section
4. Removes duplicate values from brand configs
5. Keeps brand-level overrides when they differ from supplier defaults

Processes both:
- config/scraper/scraper_config.json (web scraping)
- config/supplier/supplier_config.json (email processing)
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from collections import Counter


def convert_european_to_comma(config: List[Dict[str, Any]]) -> Tuple[int, List[str]]:
    """
    Convert all "european" decimalFormat values to "comma".
    
    Args:
        config: List of supplier configurations
        
    Returns:
        Tuple of (count of conversions, list of supplier names affected)
    """
    conversion_count = 0
    affected_suppliers: List[str] = []
    
    for supplier in config:
        supplier_name = supplier.get('supplier', 'Unknown')
        supplier_affected = False
        
        # Check metadata
        if 'metadata' in supplier and 'decimalFormat' in supplier['metadata']:
            if supplier['metadata']['decimalFormat'].lower() == 'european':
                supplier['metadata']['decimalFormat'] = 'comma'
                conversion_count += 1
                supplier_affected = True
        
        # Check brand configs
        for brand_config in supplier.get('config', []):
            if 'decimalFormat' in brand_config:
                if brand_config['decimalFormat'].lower() == 'european':
                    brand_config['decimalFormat'] = 'comma'
                    conversion_count += 1
                    supplier_affected = True
        
        if supplier_affected:
            affected_suppliers.append(supplier_name)
    
    return conversion_count, affected_suppliers


def find_common_values(supplier: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Find common values for location, currency, decimalFormat across all brands.
    
    Args:
        supplier: Supplier configuration
        
    Returns:
        Dict with common values (or None if values differ across brands)
    """
    brand_configs = supplier.get('config', [])
    if not brand_configs:
        return {'location': None, 'currency': None, 'decimalFormat': None}
    
    # Collect all values for each field
    locations: List[str] = []
    currencies: List[str] = []
    decimal_formats: List[str] = []
    
    for brand_config in brand_configs:
        if 'location' in brand_config:
            locations.append(brand_config['location'])
        if 'currency' in brand_config:
            currencies.append(brand_config['currency'])
        if 'decimalFormat' in brand_config:
            decimal_formats.append(brand_config['decimalFormat'])
    
    # Find most common value (if all are the same, it will be unanimous)
    common_location = None
    common_currency = None
    common_decimal_format = None
    
    if locations and len(set(locations)) == 1:
        # All locations are the same
        common_location = locations[0]
    
    if currencies and len(set(currencies)) == 1:
        # All currencies are the same
        common_currency = currencies[0]
    
    if decimal_formats and len(set(decimal_formats)) == 1:
        # All decimal formats are the same
        common_decimal_format = decimal_formats[0]
    
    return {
        'location': common_location,
        'currency': common_currency,
        'decimalFormat': common_decimal_format
    }


def migrate_supplier_to_defaults(supplier: Dict[str, Any]) -> Dict[str, Any]:
    """
    Migrate a supplier config to use metadata defaults.
    
    Args:
        supplier: Supplier configuration
        
    Returns:
        Migrated supplier configuration
    """
    supplier_name = supplier.get('supplier', 'Unknown')
    
    # Find common values
    common_values = find_common_values(supplier)
    
    # Ensure metadata section exists
    if 'metadata' not in supplier:
        supplier['metadata'] = {}
    
    # Track what we're moving to metadata
    moved_fields: List[str] = []
    
    # Move common values to metadata (if not already there)
    for field, value in common_values.items():
        if value is not None:
            # Only move to metadata if ALL brands have this value
            if field not in supplier['metadata']:
                supplier['metadata'][field] = value
                moved_fields.append(field)
            elif supplier['metadata'][field] != value:
                # Metadata has different value - keep both (brand overrides)
                continue
    
    # Remove duplicate values from brand configs
    for brand_config in supplier.get('config', []):
        for field in ['location', 'currency', 'decimalFormat']:
            if field in supplier['metadata']:
                # If brand has same value as metadata, remove it
                if field in brand_config and brand_config[field] == supplier['metadata'][field]:
                    del brand_config[field]
    
    if moved_fields:
        print(f"  ✓ {supplier_name}: Moved to metadata: {', '.join(moved_fields)}")
    
    return supplier


def migrate_config_file(file_path: Path, dry_run: bool = False) -> Dict[str, Any]:
    """
    Migrate a configuration file.
    
    Args:
        file_path: Path to configuration file
        dry_run: If True, don't write changes
        
    Returns:
        Migration statistics
    """
    print(f"\nProcessing: {file_path}")
    
    # Read config
    with open(file_path, 'r') as f:
        config = json.load(f)
    
    # Convert "european" to "comma"
    conversion_count, affected_suppliers = convert_european_to_comma(config)
    
    if conversion_count > 0:
        print(f"  ✓ Converted {conversion_count} 'european' values to 'comma'")
        print(f"    Affected suppliers: {', '.join(affected_suppliers)}")
    else:
        print(f"  ✓ No 'european' values found")
    
    # Migrate each supplier to use defaults
    suppliers_migrated = 0
    for supplier in config:
        original_config = json.dumps(supplier.get('config', []), sort_keys=True)
        migrate_supplier_to_defaults(supplier)
        new_config = json.dumps(supplier.get('config', []), sort_keys=True)
        
        if original_config != new_config:
            suppliers_migrated += 1
    
    print(f"  ✓ Migrated {suppliers_migrated} suppliers to use metadata defaults")
    
    # Write back
    if not dry_run:
        with open(file_path, 'w') as f:
            json.dump(config, f, indent=2)
        print(f"  ✓ Written to {file_path}")
    else:
        print(f"  ⚠ DRY RUN - No changes written")
    
    return {
        'file': str(file_path),
        'conversions': conversion_count,
        'affected_suppliers': affected_suppliers,
        'suppliers_migrated': suppliers_migrated
    }


def main():
    """Main migration function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate config files to use supplier-level defaults')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be changed without writing')
    args = parser.parse_args()
    
    # Get project root
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    
    # Config files to migrate
    scraper_config = project_root / 'config' / 'scraper' / 'scraper_config.json'
    supplier_config = project_root / 'config' / 'supplier' / 'supplier_config.json'
    
    # Check files exist
    if not scraper_config.exists():
        print(f"ERROR: {scraper_config} not found")
        sys.exit(1)
    
    if not supplier_config.exists():
        print(f"ERROR: {supplier_config} not found")
        sys.exit(1)
    
    print("=" * 80)
    print("CONFIG MIGRATION: Supplier-Level Defaults")
    print("=" * 80)
    
    if args.dry_run:
        print("\n⚠ DRY RUN MODE - No changes will be written\n")
    
    # Migrate both files
    stats = []
    stats.append(migrate_config_file(scraper_config, args.dry_run))
    stats.append(migrate_config_file(supplier_config, args.dry_run))
    
    # Summary
    print("\n" + "=" * 80)
    print("MIGRATION SUMMARY")
    print("=" * 80)
    
    total_conversions = sum(s['conversions'] for s in stats)
    total_migrated = sum(s['suppliers_migrated'] for s in stats)
    
    print(f"\nTotal 'european' → 'comma' conversions: {total_conversions}")
    print(f"Total suppliers migrated to use defaults: {total_migrated}")
    
    if args.dry_run:
        print("\n⚠ DRY RUN - Run without --dry-run to apply changes")
    else:
        print("\n✓ Migration complete!")
        print("\nNext steps:")
        print("1. Review the changes in git diff")
        print("2. Test with: python scripts/run_scraper_local.py --test")
        print("3. Deploy: ./deploy/update-and-deploy.sh -config")


if __name__ == '__main__':
    main()

