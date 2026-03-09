"""
Flatten email supplier config files.

This script flattens the structure of individual supplier files in config/supplier/suppliers/:
1. Moves metadata.location → top-level location
2. Moves metadata.currency → top-level currency
3. Moves metadata.decimalFormat → top-level decimalFormat
4. Removes now-empty metadata object
"""

import json
from pathlib import Path
from typing import Dict, Any

def flatten_supplier_file(file_path: Path) -> bool:
    """
    Flatten a single supplier config file.
    
    Args:
        file_path: Path to supplier JSON file
        
    Returns:
        True if file was modified, False otherwise
    """
    with open(file_path, 'r') as f:
        supplier = json.load(f)
    
    supplier_name = supplier.get('supplier', file_path.stem)
    changes = []
    
    # Flatten metadata to top-level
    if 'metadata' in supplier:
        metadata = supplier['metadata']
        
        # Move location, currency, decimalFormat to top-level
        fields_to_move = ['location', 'currency', 'decimalFormat']
        for field in fields_to_move:
            if field in metadata:
                supplier[field] = metadata[field]
                del metadata[field]
                changes.append(f"Moved metadata.{field} to top-level")
        
        # Remove metadata object if now empty
        if not metadata:
            del supplier['metadata']
            changes.append("Removed empty metadata object")
    
    if changes:
        print(f"  {supplier_name}:")
        for change in changes:
            print(f"    - {change}")
        
        # Write back
        with open(file_path, 'w') as f:
            json.dump(supplier, f, indent=2)
        
        return True
    
    return False


def main() -> None:
    """Main entry point."""
    suppliers_dir = Path("config/supplier/suppliers")
    
    if not suppliers_dir.exists():
        print(f"ERROR: {suppliers_dir} not found")
        return
    
    print(f"Flattening supplier config files in {suppliers_dir}\n")
    
    # Get all JSON files
    json_files = list(suppliers_dir.glob("*.json"))
    
    if not json_files:
        print("No JSON files found")
        return
    
    modified_count = 0
    
    for file_path in sorted(json_files):
        if flatten_supplier_file(file_path):
            modified_count += 1
    
    print(f"\n✓ Migration complete: {modified_count}/{len(json_files)} files modified")
    print("\nNext step: Run merge_suppliers.py to rebuild supplier_config.json")
    print("  python config/supplier/merge_suppliers.py")


if __name__ == '__main__':
    main()

