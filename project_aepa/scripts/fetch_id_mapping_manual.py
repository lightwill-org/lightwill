#!/usr/bin/env python3
"""
Fetch FilesetLists ID mapping from Judicial Yuan Open Data Platform
(Manual browser mode - connects to your already-open browser)

Usage:
1. Start Chrome with remote debugging:
   /Applications/Google\ Chrome.app/Contents/MacOS/Google\ Chrome --remote-debugging-port=9222

2. Login and navigate to the judgment dataset page

3. Run this script:
   python scripts/fetch_id_mapping_manual.py
"""

import json
import time
import re
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options


OUTPUT_FILE = Path(__file__).parent.parent / "data" / "id_mapping.json"


def connect_to_browser():
    """Connect to already-running Chrome with remote debugging enabled"""
    print("\n" + "=" * 60)
    print("Connecting to your Chrome browser...")
    print("=" * 60)

    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")

    try:
        driver = webdriver.Chrome(options=chrome_options)
        print("✓ Connected!")
        return driver
    except Exception as e:
        print(f"\n✗ Connection failed: {e}")
        print("\nMake sure Chrome is running with:")
        print(
            "  /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222"
        )
        return None


def extract_month_id_mapping(driver):
    """Extract month and ID mapping from current page"""
    mappings = {}

    # Wait a bit for page to be ready
    time.sleep(1)

    try:
        # Find all RAR download links
        rar_links = driver.find_elements(By.CSS_SELECTOR, "a.badge.badge-RAR-brd-r")

        print(f"  Found {len(rar_links)} RAR links")

        for link in rar_links:
            try:
                href = link.get_attribute("href")

                if not href or "FilesetLists" not in href:
                    continue

                # Extract ID from URL
                id_match = re.search(r"FilesetLists/(\d+)/", href)
                if not id_match:
                    continue

                file_id = int(id_match.group(1))

                # Find the corresponding title by going up the DOM tree
                parent = link
                title_text = None

                for _ in range(10):  # Max 10 levels up
                    try:
                        parent = parent.find_element(By.XPATH, "..")

                        # Look for title element
                        try:
                            title_elem = parent.find_element(
                                By.CSS_SELECTOR, "li.title"
                            )
                            title_text = title_elem.text.strip()
                            break
                        except Exception:
                            pass
                    except Exception:
                        break

                if not title_text:
                    continue

                # Extract month (YYYYMM format) from title
                match = re.search(r"(\d{6})", title_text)
                if not match:
                    continue

                month_str = match.group(1)
                mappings[month_str] = file_id
                print(f"    {month_str} → {file_id}")

            except Exception:
                continue

    except Exception as e:
        print(f"  ✗ Error: {e}")

    return mappings


def scrape_all_pages(driver):
    """Scrape all pages of judgment datasets"""
    all_mappings = {}

    print("\n" + "=" * 60)
    print("Starting to scrape ID mappings...")
    print("=" * 60)
    print("\nTip: You can manually navigate between pages,")
    print("     or let the script auto-navigate (press Enter)")
    print("=" * 60 + "\n")

    # Get base URL from current page
    current_url = driver.current_url
    base_url = (
        current_url.split("&page=")[0]
        if "&page=" in current_url
        else current_url.split("?page=")[0]
        if "?page=" in current_url
        else current_url
    )

    page = 1

    while True:
        print(f"\n📄 Page {page}...")

        # Extract mappings from current page
        page_mappings = extract_month_id_mapping(driver)

        if page_mappings:
            all_mappings.update(page_mappings)
            print(f"  Total so far: {len(all_mappings)} mappings")
        else:
            print("  No mappings found")

        # Ask user what to do
        print("\nOptions:")
        print("  [Enter] = Auto-navigate to next page")
        print("  [m] = I'll manually navigate (press Enter when ready)")
        print("  [q] = Quit and save")

        choice = input("Choice: ").strip().lower()

        if choice == "q":
            print("\nStopping...")
            break
        elif choice == "m":
            input("\nNavigate to next page manually, then press Enter...")
        else:
            # Auto-navigate to next page
            page += 1
            separator = "&" if "?" in base_url else "?"
            url = f"{base_url}{separator}page={page}"

            print(f"  Navigating to: {url}")
            driver.get(url)
            time.sleep(2)

            # Check if page loaded successfully
            try:
                rar_links = driver.find_elements(
                    By.CSS_SELECTOR, "a.badge.badge-RAR-brd-r"
                )
                if len(rar_links) == 0:
                    print("  No more data found. Reached end.")
                    break
            except Exception:
                print("  Page load failed. Stopping.")
                break

    return all_mappings


def main():
    """Main function"""
    print("\n" + "=" * 60)
    print("Judicial Yuan ID Mapping Scraper (Manual Mode)")
    print("=" * 60 + "\n")

    # Connect to browser
    driver = connect_to_browser()

    if not driver:
        print("\n✗ Failed to connect. Exiting.")
        print("\nQuick start:")
        print("1. Close all Chrome windows")
        print(
            "2. Run: /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222"
        )
        print("3. Login to opendata.judicial.gov.tw")
        print("4. Navigate to judgment dataset page")
        print("5. Run this script again")
        return

    try:
        # Check current page
        current_url = driver.current_url
        print(f"\nCurrent page: {current_url}")

        # Check if we can find RAR links
        rar_links = driver.find_elements(By.CSS_SELECTOR, "a.badge.badge-RAR-brd-r")

        if len(rar_links) == 0:
            print("\n⚠️  No RAR links found on current page!")
            print("\nPlease navigate to the judgment dataset page")
            print("(the page with monthly RAR download buttons)")
            input("\nPress Enter when ready...")

        # Start scraping
        all_mappings = scrape_all_pages(driver)

        # Save results
        print("\n" + "=" * 60)
        print(f"Scraped {len(all_mappings)} month-to-ID mappings")
        print("=" * 60 + "\n")

        if all_mappings:
            # Create output directory
            OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

            # Sort by month
            sorted_mappings = dict(sorted(all_mappings.items()))

            # Save to JSON
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(sorted_mappings, f, indent=2, ensure_ascii=False)

            print(f"✓ Saved to: {OUTPUT_FILE}")

            # Show sample
            print("\nSample mappings:")
            for month, file_id in list(sorted_mappings.items())[:5]:
                print(f"  {month} → {file_id}")
            if len(sorted_mappings) > 10:
                print("  ...")
                for month, file_id in list(sorted_mappings.items())[-5:]:
                    print(f"  {month} → {file_id}")
        else:
            print("✗ No mappings collected!")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()

    finally:
        print("\nDone! (Browser will stay open)")


if __name__ == "__main__":
    main()
