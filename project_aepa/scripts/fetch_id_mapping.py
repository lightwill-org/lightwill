#!/usr/bin/env python3
"""
Fetch FilesetLists ID mapping from Judicial Yuan Open Data Platform

This script:
1. Opens a browser to the login page
2. Waits for you to manually login
3. Automatically scrapes all month-to-ID mappings
4. Saves to id_mapping.json
"""

import json
import time
import re
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service


LOGIN_URL = "https://opendata.judicial.gov.tw/login"
DATASET_URL = "https://opendata.judicial.gov.tw/dataset?categoryTheme4Sys%5B0%5D=051&sort.publishedDate.order=asc"
OUTPUT_FILE = Path(__file__).parent.parent / "data" / "id_mapping.json"


def wait_for_login_and_navigate(driver, timeout=300):
    """Wait for user to manually login and navigate to dataset page"""
    print("\n" + "=" * 60)
    print("Please do the following in the browser window:")
    print("1. Login with your credentials")
    print("2. Navigate to the judgment dataset page")
    print("   (the page with monthly RAR files)")
    print("3. The script will detect and continue automatically")
    print("=" * 60)
    print("\nWaiting...")

    start_time = time.time()

    while time.time() - start_time < timeout:
        # Check if we can find RAR links (means we're on the right page)
        try:
            rar_links = driver.find_elements(By.CSS_SELECTOR, "a.badge.badge-RAR-brd-r")
            if len(rar_links) > 0:
                print(f"\n✓ Found {len(rar_links)} RAR links! Ready to scrape.")
                time.sleep(1)
                return True
        except Exception:
            pass

        time.sleep(1)

    print("\n✗ Timeout! Could not find RAR links.")
    print("Make sure you're on the judgment dataset page.")
    return False


def extract_month_id_mapping(driver):
    """Extract month and ID mapping from current page"""
    mappings = {}

    # Wait for page to load
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "title"))
        )
    except TimeoutException:
        print("⚠️  Timeout waiting for title elements")
        # Continue anyway, page might have loaded
        pass

    # Wait a bit more for dynamic content
    time.sleep(2)

    try:
        # Find all RAR download links
        rar_links = driver.find_elements(By.CSS_SELECTOR, "a.badge.badge-RAR-brd-r")

        print(f"  Found {len(rar_links)} RAR links on this page")

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

                # Find the corresponding title
                # Go up to parent container and find title
                parent = link
                title_text = None

                # Try to find title by going up the DOM tree
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
                # Skip items that don't have the expected structure
                continue

    except Exception as e:
        print(f"  ✗ Error extracting mappings: {e}")
        import traceback

        traceback.print_exc()

    return mappings


def scrape_all_pages(driver):
    """Scrape all pages of judgment datasets"""
    all_mappings = {}
    page = 1

    print("\n" + "=" * 60)
    print("Starting to scrape ID mappings...")
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

    while True:
        print(f"📄 Page {page}...")

        # Navigate to page (skip first page since we're already there)
        if page > 1:
            separator = "&" if "?" in base_url else "?"
            url = f"{base_url}{separator}page={page}"
            driver.get(url)
            time.sleep(2)

        # Extract mappings from current page
        page_mappings = extract_month_id_mapping(driver)

        if not page_mappings:
            print(f"  No mappings found on page {page}")
            # Check if we've reached the end
            if page > 1:
                print("  Assuming end of pages")
                break

        all_mappings.update(page_mappings)

        # Check if there's a next page
        try:
            # Look for "next page" button or pagination
            next_button = driver.find_element(By.CSS_SELECTOR, "[aria-label='Next']")
            if "disabled" in next_button.get_attribute("class"):
                print("\n  Reached last page")
                break
        except NoSuchElementException:
            # If we can't find next button, try one more page to be sure
            if page > 50:  # Safety limit
                print("\n  Reached page limit")
                break

        page += 1
        time.sleep(1)  # Be nice to the server

    return all_mappings


def main():
    """Main function"""
    print("\n" + "=" * 60)
    print("Judicial Yuan ID Mapping Scraper")
    print("=" * 60 + "\n")

    # Setup Chrome driver
    print("Setting up Chrome driver...")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)

    try:
        # Open login page
        print(f"\nOpening browser: {LOGIN_URL}")
        driver.get(LOGIN_URL)

        # Wait for manual login and navigation
        if not wait_for_login_and_navigate(driver):
            print("\n✗ Failed to reach dataset page. Exiting.")
            return

        # Get current URL to use as base for pagination
        current_url = driver.current_url
        print(f"\nCurrent URL: {current_url}")

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
            print("  ...")
            for month, file_id in list(sorted_mappings.items())[-5:]:
                print(f"  {month} → {file_id}")
        else:
            print("✗ No mappings found!")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback

        traceback.print_exc()

    finally:
        print("\nClosing browser...")
        driver.quit()
        print("Done!")


if __name__ == "__main__":
    main()
