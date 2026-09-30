#!/usr/bin/env python3
"""
Fetch FilesetLists ID mapping using requests
(Use browser cookies for authentication)

Steps:
1. Login to opendata.judicial.gov.tw in your browser
2. Copy cookies from browser
3. Paste when prompted
4. Script will fetch all pages automatically
"""

import json
import re
import time
import os
from pathlib import Path
from bs4 import BeautifulSoup
import requests


BASE_URL = "https://opendata.judicial.gov.tw/dataset"
OUTPUT_FILE = Path(__file__).parent.parent / "data" / "id_mapping.json"


def get_cookies_from_user():
    """Get cookies from user input or environment variable"""
    # Check environment variable first
    cookie_string = os.environ.get("JUDICIAL_COOKIES", "").strip()

    if cookie_string:
        print("\n✓ Using cookies from JUDICIAL_COOKIES environment variable")
    else:
        print("\n" + "=" * 60)
        print("Cookie Setup")
        print("=" * 60)
        print("\nPlease copy your browser cookies:")
        print("\n1. Login to opendata.judicial.gov.tw")
        print("2. Press F12 → Application/Storage → Cookies")
        print("3. Find 'opendata.judicial.gov.tw'")
        print("4. Copy the entire cookie string")
        print("\nOr use this format:")
        print("  session=xxx; othercookie=yyy")
        print("\nOr set JUDICIAL_COOKIES environment variable")
        print("\n" + "=" * 60)

        cookie_string = input("\nPaste cookies here: ").strip()

    if not cookie_string:
        print("✗ No cookies provided")
        return None

    # Parse cookie string
    cookies = {}
    for item in cookie_string.split(";"):
        item = item.strip()
        if "=" in item:
            key, value = item.split("=", 1)
            cookies[key.strip()] = value.strip()

    return cookies


def fetch_page(session, page_num):
    """Fetch a single page and extract mappings"""
    params = {
        "categoryTheme4Sys[0]": "051",
        "sort.publishedDate.order": "desc",
        "page": page_num,
    }

    try:
        response = session.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        mappings = {}

        # Find all RAR links
        rar_links = soup.find_all("a", class_="badge-RAR-brd-r")

        for link in rar_links:
            href = link.get("href", "")

            if "FilesetLists" not in href:
                continue

            # Extract ID
            id_match = re.search(r"FilesetLists/(\d+)/", href)
            if not id_match:
                continue

            file_id = int(id_match.group(1))

            # Find title (go up the tree)
            parent = link.parent
            title_elem = None

            for _ in range(10):  # Max 10 levels up
                if not parent:
                    break

                title_elem = parent.find("li", class_="title")
                if title_elem:
                    break

                parent = parent.parent

            if not title_elem:
                continue

            # Extract month
            title_text = title_elem.get_text()
            month_match = re.search(r"(\d{6})", title_text)

            if month_match:
                month_str = month_match.group(1)
                mappings[month_str] = file_id

        return mappings

    except Exception as e:
        print(f"    ✗ Error: {e}")
        return {}


def main():
    """Main function"""
    print("\n" + "=" * 60)
    print("Judicial Yuan ID Mapping Fetcher (Requests)")
    print("=" * 60 + "\n")

    # Get cookies
    cookies = get_cookies_from_user()

    if not cookies:
        print("\n✗ Failed to get cookies. Exiting.")
        return

    # Create session
    session = requests.Session()
    session.cookies.update(cookies)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
    )

    # Fetch all pages
    print("\n" + "=" * 60)
    print("Fetching all pages...")
    print("=" * 60 + "\n")

    total_pages = 37  # 368 items / 10 per page = 37 pages
    all_mappings = {}

    for page in range(total_pages):
        print(f"📄 Page {page}...", end=" ")

        mappings = fetch_page(session, page)

        if mappings:
            all_mappings.update(mappings)
            print(f"✓ Found {len(mappings)} items (Total: {len(all_mappings)})")
        else:
            print("✗ No items found")

        # Be nice to the server
        time.sleep(0.5)

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
        print("✗ No mappings found!")
        print("\nPossible issues:")
        print("- Cookies might be invalid or expired")
        print("- Need to login again")


if __name__ == "__main__":
    main()
