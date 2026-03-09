#!/usr/bin/env python3
"""
Test AUTOCAR with Chromium browser (exactly like Cloud Run).

This forces the use of Chromium instead of Firefox to see if
that's the difference between local success and Cloud Run failure.
"""

import asyncio
import sys
import os
import tempfile
import shutil
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from scrapers.browser_manager import BrowserManager
from scrapers.supplier_scrapers.autocar_scraper import AutocarScraper
from utils.logger import setup_logger, get_logger
import json

logger = setup_logger(__name__)


async def test_chromium_browser():
    """Test AUTOCAR with Chromium (Cloud Run environment simulation)."""
    
    print("=" * 80)
    print("🧪 TESTING AUTOCAR WITH CHROMIUM (Exact Cloud Run Browser)")
    print("=" * 80)
    print()
    
    temp_download_dir = tempfile.mkdtemp(prefix='test_chromium_downloads_')
    temp_screenshot_dir = tempfile.mkdtemp(prefix='test_chromium_screenshots_')
    
    print(f"📁 Download dir: {temp_download_dir}")
    print(f"📸 Screenshot dir: {temp_screenshot_dir}")
    print()
    
    try:
        # Load configs
        print("📋 Loading configuration from local files...")
        
        config_dir = Path(__file__).parent.parent / 'config'
        
        with open(config_dir / 'scraper' / 'scraper_config.json', 'r') as f:
            scrapers = json.load(f)
        
        with open(config_dir / 'brand' / 'brand_config.json', 'r') as f:
            brands = json.load(f)
        
        autocar_config = None
        for scraper_config in scrapers:
            if scraper_config.get('supplier') == 'AUTOCAR':
                autocar_config = scraper_config
                break
        
        if not autocar_config:
            print("❌ AUTOCAR configuration not found")
            return False
        
        print("✓ Configuration loaded")
        print()
        
        # Pre-populate brand config cache
        from scrapers.brand_matcher import set_brand_configs_cache
        set_brand_configs_cache(brands)
        print(f"✓ Brand config cache populated with {len(brands)} brands")
        print()
        
        # Check credentials
        username_env = autocar_config['authentication']['username_env']
        password_env = autocar_config['authentication']['password_env']
        username = os.getenv(username_env)
        password = os.getenv(password_env)
        
        if not username or not password:
            print(f"❌ Credentials not found in environment")
            return False
        
        print(f"✓ Credentials found: {username[:3]}***")
        print()
        
        # FORCE CLOUD RUN ENVIRONMENT (Chromium)
        print("🌐 FORCING Cloud Run environment (Chromium browser)...")
        os.environ['K_SERVICE'] = 'test-chromium'  # Trick browser_manager into using Chromium
        
        browser_manager = BrowserManager(
            headless=True,  # Change to False to watch
            download_dir=temp_download_dir,
            screenshot_dir=temp_screenshot_dir
        )
        
        print("🤖 Creating AUTOCAR scraper...")
        scraper = AutocarScraper(
            config=autocar_config,
            browser_manager=browser_manager,
            start_index=0,
            state_manager=None
        )
        
        print()
        print("=" * 80)
        print("🚀 STARTING SCRAPER WITH CHROMIUM")
        print("=" * 80)
        print()
        
        # Run scraper
        result = await scraper.scrape()
        
        print()
        print("=" * 80)
        print("📊 RESULTS")
        print("=" * 80)
        print()
        
        if result.success:
            print(f"✅ SUCCESS with Chromium: Downloaded {len(result.files)} files")
            print()
            print("🎯 CONCLUSION: Chromium works fine!")
            print("   The issue on Cloud Run must be something else:")
            print("   - IP-based restrictions")
            print("   - Network/DNS issues in asia-south1 region")
            print("   - Rate limiting")
            print("   - Cloud Run specific environment issues")
            return True
        else:
            print(f"❌ FAILED with Chromium: {len(result.errors)} errors")
            print()
            for i, error in enumerate(result.errors, 1):
                print(f"   Error {i}: {error}")
            print()
            print("🎯 CONCLUSION: Chromium is the problem!")
            print("   Firefox works, Chromium fails.")
            print("   Options:")
            print("   1. Use Firefox on Cloud Run (need to install)")
            print("   2. Fix Chromium compatibility issues")
            print("   3. Use API instead of browser automation")
            return False
        
    except Exception as e:
        print()
        print("=" * 80)
        print(f"💥 EXCEPTION: {type(e).__name__}")
        print("=" * 80)
        print()
        print(f"Error: {str(e)}")
        print()
        
        import traceback
        print("Traceback:")
        print(traceback.format_exc())
        
        return False
        
    finally:
        # Cleanup
        print()
        print("🧹 Cleaning up...")
        try:
            # Remove the fake K_SERVICE env var
            if 'K_SERVICE' in os.environ:
                del os.environ['K_SERVICE']
            
            shutil.rmtree(temp_download_dir, ignore_errors=True)
            shutil.rmtree(temp_screenshot_dir, ignore_errors=True)
            print("✓ Cleanup complete")
        except Exception as e:
            print(f"⚠ Cleanup warning: {e}")
        
        print()
        print("=" * 80)
        print("🏁 CHROMIUM TEST COMPLETE")
        print("=" * 80)


def main():
    """Main entry point."""
    print()
    print("╔" + "═" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  🧪 CHROMIUM TEST - Testing Cloud Run's Browser".ljust(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "═" * 78 + "╝")
    print()
    
    success = asyncio.run(test_chromium_browser())
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

