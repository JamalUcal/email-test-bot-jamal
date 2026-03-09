"""
Log parser utility for extracting upload summaries from scraper logs.

Parses JSON log files to extract:
- Errors found during execution
- Successful uploads with Google Drive links, organized by supplier and brand

Uses brand_config.json to resolve abbreviations to full canonical brand names.
"""

import json
import re
import sys
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Any


def load_brand_configs(config_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Load brand configurations from JSON file.
    
    Args:
        config_path: Path to brand_config.json (uses default if None)
        
    Returns:
        List of brand configuration dictionaries
    """
    if config_path is None:
        # Default to config/brand/brand_config.json relative to repo root
        repo_root = Path(__file__).parent.parent.parent
        config_path = str(repo_root / "config" / "brand" / "brand_config.json")
    
    try:
        with open(config_path, 'r') as f:
            configs = json.load(f)
        return configs
    except FileNotFoundError:
        print(f"Warning: Brand config file not found: {config_path}")
        return []
    except json.JSONDecodeError as e:
        print(f"Warning: Failed to parse brand config JSON: {e}")
        return []
    except Exception as e:
        print(f"Warning: Failed to load brand configs: {e}")
        return []


def resolve_brand_name(brand_code: str, brand_configs: List[Dict]) -> str:
    """
    Resolve brand code/abbreviation to full canonical brand name.
    
    Args:
        brand_code: Brand code or abbreviation (e.g., "HY", "BM", "KI")
        brand_configs: List of brand configurations
        
    Returns:
        Full canonical brand name (e.g., "HYUNDAI", "BMW", "KIA")
    """
    if not brand_code:
        return brand_code
    
    # Normalize for comparison
    brand_code_lower = brand_code.lower().strip()
    
    # Try to find matching brand config
    for config in brand_configs:
        brand_name = config.get('brand', '')
        
        # Check exact brand name match (case-insensitive)
        if brand_name.lower() == brand_code_lower:
            return brand_name
        
        # Check aliases
        aliases = config.get('aliases', [])
        if isinstance(aliases, list):
            for alias in aliases:
                if isinstance(alias, str) and alias.lower() == brand_code_lower:
                    return brand_name
    
    # If no match found, return uppercase version of input
    return brand_code.upper()


def parse_log_file(log_path: str) -> Tuple[Dict[str, Dict[str, List[str]]], List[str]]:
    """
    Parse log file to extract uploads and errors.
    
    Args:
        log_path: Path to log file
        
    Returns:
        Tuple of (supplier_brands dict, errors list)
        supplier_brands: {supplier: {brand: [links]}}
        errors: List of error messages
    """
    # Load brand configs for name resolution
    brand_configs = load_brand_configs()
    
    # Data structures
    supplier_brands = defaultdict(lambda: defaultdict(list))
    errors = []
    
    # Track context
    current_supplier = None
    
    # Read and parse log file
    with open(log_path, 'r') as f:
        lines = f.readlines()
    
    # Parse each line
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
        
        try:
            data = json.loads(line)
            message = data.get('message', '')
            severity = data.get('severity', '')
            
            # Track current supplier from RESULTS
            if 'RESULTS:' in message:
                current_supplier = message.split('RESULTS:')[1].strip()
                continue
            
            # Check for "Drive:" link - look at previous line for filename
            if 'Drive:' in message and 'https://drive.google.com' in message:
                match = re.search(r'https://drive\.google\.com/file/d/[^\s"\']+', message)
                if match:
                    web_link = match.group(0)
                    
                    # Look at previous line for filename
                    filename = None
                    for j in range(max(0, i-5), i):
                        prev_line = lines[j].strip()
                        if not prev_line:
                            continue
                        try:
                            prev_data = json.loads(prev_line)
                            prev_msg = prev_data.get('message', '')
                            # Look for filename pattern in previous message
                            if ('.csv' in prev_msg or '.xlsx' in prev_msg) and 'Drive:' not in prev_msg:
                                # Extract filename - handle patterns like:
                                # "  ✓ FCA_PART5_NEOPARTA_EUR_LITHUANIA_OCT08_2025.csv"
                                patterns = [
                                    r'([A-Z][A-Z0-9_]+)_(NEOPARTA|CONNEX|TECHNOPARTS|AUTOCAR|APF|MATEROM)_([A-Z0-9_]+)\.(csv|xlsx)',
                                    r'([A-Z][A-Z0-9_]+)_([A-Z]+)_([A-Z0-9_]+)_([A-Z0-9_]+)_([A-Z0-9_]+)\.(csv|xlsx)',
                                ]
                                for pattern in patterns:
                                    m = re.search(pattern, prev_msg)
                                    if m:
                                        filename = m.group(0)
                                        break
                                if filename:
                                    break
                        except:
                            pass
                    
                    if filename:
                        # Extract brand and supplier from filename
                        parts = filename.split('_')
                        if len(parts) >= 2:
                            # Find supplier name in parts
                            suppliers = ['NEOPARTA', 'CONNEX', 'TECHNOPARTS', 'AUTOCAR', 'APF', 'MATEROM']
                            supplier_idx = -1
                            for idx, part in enumerate(parts):
                                if part in suppliers:
                                    supplier_idx = idx
                                    break
                            
                            if supplier_idx > 0:
                                supplier = parts[supplier_idx]
                                brand_code = '_'.join(parts[:supplier_idx])
                                
                                # Resolve to full brand name
                                full_brand_name = resolve_brand_name(brand_code, brand_configs)
                                
                                supplier_name = current_supplier or supplier
                                supplier_brands[supplier_name][full_brand_name].append(web_link)
            
            # Check for successful uploads with web_link
            if 'File uploaded successfully' in message and 'web_link' in data:
                filename = data.get('filename', '')
                web_link = data.get('web_link', '')
                
                if filename and web_link:
                    parts = filename.split('_')
                    if len(parts) >= 2:
                        brand_code = parts[0]
                        supplier = parts[1]
                        
                        # Resolve to full brand name
                        full_brand_name = resolve_brand_name(brand_code, brand_configs)
                        
                        supplier_name = current_supplier or supplier
                        supplier_brands[supplier_name][full_brand_name].append(web_link)
            
            # Check for "Added downloaded file" which has brand info
            if 'Added downloaded file for' in message and 'brand' in data:
                supplier_from_msg = message.split('Added downloaded file for')[1].strip().split()[0]
                brand_code = data.get('brand', '')
                drive_file_id = data.get('drive_file_id', '')
                if brand_code and drive_file_id:
                    web_link = f"https://drive.google.com/file/d/{drive_file_id}/view?usp=drivesdk"
                    
                    # Resolve to full brand name
                    full_brand_name = resolve_brand_name(brand_code, brand_configs)
                    
                    supplier_name = current_supplier or supplier_from_msg
                    supplier_brands[supplier_name][full_brand_name].append(web_link)
            
            # Check for errors
            if severity == 'ERROR':
                error_msg = message
                if error_msg and 'Drive upload failed' not in error_msg:
                    errors.append(error_msg)
        
        except json.JSONDecodeError:
            # Not a JSON line, skip
            pass
        except Exception as e:
            pass
    
    # Convert defaultdict to regular dict for return type compliance
    return dict(supplier_brands), errors


def generate_summary(supplier_brands: Dict[str, Dict[str, List[str]]], errors: List[str], output_path: str) -> None:
    """
    Generate summary file from parsed data.
    
    Args:
        supplier_brands: {supplier: {brand: [links]}}
        errors: List of error messages
        output_path: Path to output file
    """
    output_lines = []
    
    # Errors section
    output_lines.append("ERRORS FOUND")
    output_lines.append("=" * 80)
    if errors:
        # Get unique errors
        unique_errors = list(set(errors))
        for error in unique_errors[:30]:  # Limit to first 30 unique errors
            output_lines.append(f"- {error}")
        if len(unique_errors) > 30:
            output_lines.append(f"\n... and {len(unique_errors) - 30} more unique errors")
    else:
        output_lines.append("No errors found")
    output_lines.append("")
    output_lines.append("")
    
    # Successful uploads section
    output_lines.append("SUCCESSFUL UPLOADS")
    output_lines.append("=" * 80)
    output_lines.append("")
    
    # Sort suppliers alphabetically
    for supplier in sorted(supplier_brands.keys()):
        output_lines.append(supplier)
        output_lines.append("=" * 80)
        
        # Sort brands alphabetically
        for brand in sorted(supplier_brands[supplier].keys()):
            # Get unique links for this brand
            links = list(set(supplier_brands[supplier][brand]))
            for link in links:
                output_lines.append(f"{brand} - {link}")
        
        output_lines.append("")
    
    # Write output file
    with open(output_path, 'w') as f:
        f.write('\n'.join(output_lines))


def main():
    """Main entry point for command-line usage."""
    if len(sys.argv) < 2:
        print("Usage: python -m utils.log_parser <log_file> [output_file]")
        sys.exit(1)
    
    log_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else 'upload_summary.txt'
    
    if not Path(log_path).exists():
        print(f"Error: Log file not found: {log_path}")
        sys.exit(1)
    
    print(f"Parsing log file: {log_path}")
    supplier_brands, errors = parse_log_file(log_path)
    
    print(f"Generating summary: {output_path}")
    generate_summary(supplier_brands, errors, output_path)
    
    print(f"\nSummary created: {output_path}")
    print(f"Suppliers: {len(supplier_brands)}")
    print(f"Total unique errors: {len(set(errors))}")
    for supplier in sorted(supplier_brands.keys()):
        total_files = sum(len(links) for links in supplier_brands[supplier].values())
        print(f"  {supplier}: {total_files} files across {len(supplier_brands[supplier])} brands")


if __name__ == '__main__':
    main()
