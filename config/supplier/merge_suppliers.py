#!/usr/bin/env python3
"""
Merge individual supplier files from suppliers/ directory into supplier_config.json.
This script reads all .json files in the suppliers/ directory and combines them
into a single supplier_config.json file.
"""

import json
import os
from pathlib import Path

def merge_suppliers():
    # Get the suppliers directory
    suppliers_dir = Path(__file__).parent / "suppliers"
    
    if not suppliers_dir.exists():
        print(f"Error: {suppliers_dir} does not exist")
        return
    
    # Read all supplier JSON files
    suppliers = []
    json_files = sorted(suppliers_dir.glob("*.json"))
    
    if not json_files:
        print(f"Error: No JSON files found in {suppliers_dir}")
        return
    
    for filepath in json_files:
        try:
            with open(filepath, 'r') as f:
                supplier_data = json.load(f)
                suppliers.append(supplier_data)
                print(f"✓ Loaded {filepath.name}")
        except json.JSONDecodeError as e:
            print(f"✗ Error reading {filepath.name}: {e}")
            continue
    
    # Write merged config
    output_file = Path(__file__).parent / "supplier_config.json"
    with open(output_file, 'w') as f:
        json.dump(suppliers, f, indent=2)
    
    print(f"\n✓ Merged {len(suppliers)} suppliers into supplier_config.json")
    print(f"  Output: {output_file}")

if __name__ == "__main__":
    merge_suppliers()
