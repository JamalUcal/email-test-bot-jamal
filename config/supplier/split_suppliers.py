#!/usr/bin/env python3
"""
Split supplier_config.json into individual supplier files.
Each supplier gets its own JSON file in the suppliers/ directory.
"""

import json
import os
from pathlib import Path

def split_suppliers():
    # Read the main supplier config
    config_file = Path(__file__).parent / "supplier_config.json"
    
    with open(config_file, 'r') as f:
        suppliers = json.load(f)
    
    # Create suppliers directory if it doesn't exist
    suppliers_dir = Path(__file__).parent / "suppliers"
    suppliers_dir.mkdir(exist_ok=True)
    
    # Write each supplier to its own file
    for supplier_data in suppliers:
        supplier_name = supplier_data.get("supplier", "unknown")
        filename = f"{supplier_name.lower()}.json"
        filepath = suppliers_dir / filename
        
        with open(filepath, 'w') as f:
            json.dump(supplier_data, f, indent=2)
        
        print(f"✓ Created {filename}")
    
    print(f"\n✓ Split {len(suppliers)} suppliers into individual files")
    print(f"  Location: {suppliers_dir}")

if __name__ == "__main__":
    split_suppliers()
