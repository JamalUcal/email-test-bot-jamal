#!/usr/bin/env python3
"""
Analyze log file for specific error types identified by user.
"""

import json
import re
from collections import defaultdict
from pathlib import Path

def analyze_log(log_path: str):
    """Analyze log for specific error types."""
    
    error_types = {
        'connex_unknown': [],
        'header_detection_errors': [],
        'test_config_issues': [],
        'vw_1_processing': [],
        'technoparts_errors': []
    }
    
    test_config_loaded = False
    fca_folder_id = None
    subaru_folder_id = None
    
    with open(log_path, 'r') as f:
        lines = f.readlines()
    
    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
        
        try:
            data = json.loads(line)
            message = data.get('message', '')
            severity = data.get('severity', '')
            
            # Check for test config loading
            if 'Loading brand config from: brand_config_test.json' in message:
                test_config_loaded = True
                error_types['test_config_issues'].append({
                    'line': i+1,
                    'message': 'Test config loaded successfully',
                    'timestamp': data.get('timestamp', '')
                })
            
            # Check for FCA/SUBARU folder IDs in test config
            if 'sample_brands_with_folders' in data:
                folders = data.get('sample_brands_with_folders', [])
                for brand, folder_id in folders:
                    if brand == 'FCA':
                        fca_folder_id = folder_id
                    if brand == 'SUBARU':
                        subaru_folder_id = folder_id
            
            # Check for CONNEX files with UNKNOWN
            if 'CONNEX' in message and '_UNKNOWN_EUR' in message:
                error_types['connex_unknown'].append({
                    'line': i+1,
                    'message': message,
                    'timestamp': data.get('timestamp', '')
                })
            
            # Check for header detection errors
            if 'Could not detect headers' in message or 'Missing required columns' in message:
                error_types['header_detection_errors'].append({
                    'line': i+1,
                    'message': message,
                    'timestamp': data.get('timestamp', ''),
                    'severity': severity
                })
            
            # Check for VW_1.csv processing
            if 'VW_1.csv' in message:
                error_types['vw_1_processing'].append({
                    'line': i+1,
                    'message': message,
                    'timestamp': data.get('timestamp', '')
                })
            
            # Check for TECHNOPARTS errors
            if 'TECHNOPARTS' in message and ('ERROR' in severity or 'Could not detect' in message):
                error_types['technoparts_errors'].append({
                    'line': i+1,
                    'message': message,
                    'timestamp': data.get('timestamp', '')
                })
            
            # Check for Drive folder 404 errors
            if 'Google Drive folder not found' in message or '404' in message:
                if 'FCA' in message or 'SUBARU' in message:
                    error_types['test_config_issues'].append({
                        'line': i+1,
                        'message': message,
                        'timestamp': data.get('timestamp', ''),
                        'severity': severity
                    })
        
        except json.JSONDecodeError:
            continue
    
    # Generate report
    print("=" * 80)
    print("ERROR ANALYSIS REPORT")
    print("=" * 80)
    print()
    
    # Issue 1: CONNEX UNKNOWN filenames
    print("ISSUE 1: CONNEX Filenames with UNKNOWN")
    print("-" * 80)
    if error_types['connex_unknown']:
        print(f"❌ FOUND {len(error_types['connex_unknown'])} instances")
        # Check if final output filenames use correct dates
        connex_final_files = [m for m in error_types['connex_unknown'] if 'output_file' in m.get('message', '')]
        if connex_final_files:
            print("   However, final output files show correct dates:")
            for item in connex_final_files[:3]:
                if 'JAN02' in item['message'] or 'JAN03' in item['message']:
                    print(f"   ✅ {item['message']}")
        else:
            print("   Sample instances:")
            for item in error_types['connex_unknown'][:3]:
                print(f"   - {item['message']}")
    else:
        print("✅ NO ISSUES FOUND - All CONNEX files use correct dates")
    print()
    
    # Issue 2: Header detection errors
    print("ISSUE 2: Header Detection Errors")
    print("-" * 80)
    if error_types['header_detection_errors']:
        print(f"❌ FOUND {len(error_types['header_detection_errors'])} header detection errors")
        print("   Sample errors:")
        for item in error_types['header_detection_errors'][:5]:
            msg = item['message']
            # Check if error message includes found columns
            if 'Found columns' in msg or 'Successfully matched' in msg:
                print(f"   ✅ Enhanced error message: {msg[:150]}...")
            else:
                print(f"   ❌ Old format error: {msg[:150]}...")
    else:
        print("✅ NO ISSUES FOUND - No header detection errors")
    print()
    
    # Issue 3: Test config brand folders
    print("ISSUE 3: Test Config Brand Folders")
    print("-" * 80)
    if test_config_loaded:
        print("✅ Test config loaded successfully")
        if fca_folder_id:
            expected_fca = "1WRlx8NUXJgPjvydEi_1tf0hfTUjULdeA"
            if fca_folder_id == expected_fca:
                print(f"✅ FCA folder ID correct: {fca_folder_id}")
            else:
                print(f"❌ FCA folder ID mismatch: got {fca_folder_id}, expected {expected_fca}")
        if subaru_folder_id:
            expected_subaru = "1DXTxyMa_t1aT2-p01pE1Jrusj4uY9chJ"
            if subaru_folder_id == expected_subaru:
                print(f"✅ SUBARU folder ID correct: {subaru_folder_id}")
            else:
                print(f"❌ SUBARU folder ID mismatch: got {subaru_folder_id}, expected {expected_subaru}")
        
        if error_types['test_config_issues']:
            print(f"❌ Found {len([e for e in error_types['test_config_issues'] if '404' in e.get('message', '')])} Drive folder 404 errors")
            for item in error_types['test_config_issues']:
                if '404' in item.get('message', ''):
                    print(f"   - {item['message']}")
        else:
            print("✅ No Drive folder 404 errors found")
    else:
        print("❌ Test config not loaded (should be loaded with --use-test-config)")
    print()
    
    # Issue 4: VW_1.csv processing
    print("ISSUE 4: CONNEX VW_1.csv File Processing")
    print("-" * 80)
    if error_types['vw_1_processing']:
        print(f"✅ FOUND {len(error_types['vw_1_processing'])} VW_1.csv references")
        # Check if it's being processed
        processed = [m for m in error_types['vw_1_processing'] if 'NEW FILE' in m.get('message', '') or 'download' in m.get('message', '').lower()]
        if processed:
            print("   ✅ VW_1.csv is being processed:")
            for item in processed[:2]:
                print(f"   - {item['message']}")
        else:
            print("   ⚠️  VW_1.csv found but processing status unclear")
    else:
        print("⚠️  No VW_1.csv references found in log")
    print()
    
    # Issue 5: TECHNOPARTS errors
    print("ISSUE 5: TECHNOPARTS Header Detection")
    print("-" * 80)
    if error_types['technoparts_errors']:
        print(f"❌ FOUND {len(error_types['technoparts_errors'])} TECHNOPARTS errors")
        for item in error_types['technoparts_errors'][:5]:
            print(f"   - {item['message']}")
    else:
        print("✅ NO ISSUES FOUND - No TECHNOPARTS errors")
    print()
    
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    total_issues = sum(len(v) for k, v in error_types.items() if k != 'vw_1_processing')
    if total_issues == 0:
        print("✅ All identified issues appear to be resolved!")
    else:
        print(f"⚠️  Found {total_issues} potential issues to review")

if __name__ == '__main__':
    import sys
    log_path = sys.argv[1] if len(sys.argv) > 1 else 'full run.log'
    analyze_log(log_path)


