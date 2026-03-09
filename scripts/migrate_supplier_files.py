#!/usr/bin/env python3
"""
Migrate individual supplier files in config/supplier/suppliers/ directory.

This script applies the same migration as migrate_config_defaults.py but to
the individual supplier JSON files that are the source of truth.
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from collections import Counter


def convert_european_to_comma_supplier(supplier: Dict[str, Any]) -> Tuple[int, bool]:
    """
    Convert all "european" decimalFormat values to "comma" in a supplier config.
    
    Args:
        supplier: Supplier configuration
        
    Returns:
        Tuple of (count of conversions, whether supplier was affected)
    """
    conversion_count = 0
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
    
    return conversion_count, supplier_affected


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


def migrate_supplier_file(file_path: Path, dry_run: bool = False) -> Dict[str, Any]:
    """
    Migrate a single supplier file.
    
    Args:
        file_path: Path to supplier JSON file
        dry_run: If True, don't write changes
        
    Returns:
        Migration statistics
    """
    # Read supplier config
    with open(file_path, 'r') as f:
        supplier = json.load(f)
    
    supplier_name = supplier.get('supplier', file_path.stem.upper())
    
    # Convert "european" to "comma"
    conversion_count, supplier_affected = convert_european_to_comma_supplier(supplier)
    
    # Migrate to use defaults
    original_config = json.dumps(supplier.get('config', []), sort_keys=True)
    migrate_supplier_to_defaults(supplier)
    new_config = json.dumps(supplier.get('config', []), sort_keys=True)
    
    was_migrated = original_config != new_config or conversion_count > 0
    
    # Write back
    if not dry_run and was_migrated:
        with open(file_path, 'w') as f:
            json.dump(supplier, f, indent=2)
            f.write('\n')  # Add trailing newline
    
    return {
        'file': str(file_path),
        'supplier': supplier_name,
        'conversions': conversion_count,
        'was_migrated': was_migrated
    }


def main():
    """Main migration function."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Migrate individual supplier files to use supplier-level defaults')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be changed without writing')
    args = parser.parse_args()
    
    # Get suppliers directory
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    suppliers_dir = project_root / 'config' / 'supplier' / 'suppliers'
    
    if not suppliers_dir.exists():
        print(f"ERROR: {suppliers_dir} not found")
        sys.exit(1)
    
    print("=" * 80)
    print("SUPPLIER FILES MIGRATION: Supplier-Level Defaults")
    print("=" * 80)
    
    if args.dry_run:
        print("\n⚠ DRY RUN MODE - No changes will be written\n")
    
    # Get all supplier JSON files
    json_files = sorted(suppliers_dir.glob("*.json"))
    
    if not json_files:
        print(f"ERROR: No JSON files found in {suppliers_dir}")
        sys.exit(1)
    
    print(f"\nProcessing {len(json_files)} supplier files...\n")
    
    # Migrate each file
    stats: List[Dict[str, Any]] = []
    for json_file in json_files:
        result = migrate_supplier_file(json_file, args.dry_run)
        stats.append(result)
        
        if result['conversions'] > 0:
            print(f"  ✓ {result['supplier']}: Converted {result['conversions']} 'european' values to 'comma'")
    
    # Summary
    print("\n" + "=" * 80)
    print("MIGRATION SUMMARY")
    print("=" * 80)
    
    total_conversions = sum(s['conversions'] for s in stats)
    total_migrated = sum(1 for s in stats if s['was_migrated'])
    
    print(f"\nTotal files processed: {len(json_files)}")
    print(f"Total 'european' → 'comma' conversions: {total_conversions}")
    print(f"Total files migrated to use defaults: {total_migrated}")
    
    if args.dry_run:
        print("\n⚠ DRY RUN - Run without --dry-run to apply changes")
    else:
        print("\n✓ Migration complete!")
        print("\nNext steps:")
        print("1. Rebuild supplier_config.json: python config/supplier/merge_suppliers.py")
        print("2. Review the changes in git diff")
        print("3. Test with: python scripts/run_email_local.py --test")
        print("4. Deploy: ./deploy/update-and-deploy.sh -config")


if __name__ == '__main__':
    main()

