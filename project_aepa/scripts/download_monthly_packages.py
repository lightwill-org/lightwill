#!/usr/bin/env python3
"""
Download monthly judgment packages from Judicial Yuan Open Data Platform

Data range: 1996.01 - 2026.07 (ID: 70325 - 70691)
Total: 367 monthly packages

ID Mapping Formula:
    ID = 70325 + (Year - 1996) * 12 + (Month - 1)

    Example:
    - 1996.01: 70325 + (1996-1996)*12 + (1-1) = 70325
    - 2026.07: 70325 + (2026-1996)*12 + (7-1) = 70691

Note: IDs are sequential and increment by 1 for each month.
      When new monthly data is published, simply add to data/id_mapping.json:
      "YYYYMM": ID
"""

import os
import sys
import time
import json
import requests
import getpass
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Tuple
from dotenv import load_dotenv
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Configuration
BASE_URL = "https://opendata.judicial.gov.tw/api"
AUTH_ENDPOINT = f"{BASE_URL}/MemberTokens"
DOWNLOAD_ENDPOINT = f"{BASE_URL}/FilesetLists"

# ID mapping file
ID_MAPPING_FILE = Path(__file__).parent.parent / "data" / "id_mapping.json"

# Download settings
DOWNLOAD_DIR = Path(__file__).parent.parent / "data" / "raw" / "monthly_packages"
RETRY_TIMES = 3
RETRY_DELAY = 5  # seconds
MAX_WORKERS = 8  # concurrent downloads


def load_id_mapping() -> Dict[str, int]:
    """Load month-to-ID mapping from JSON file"""
    if not ID_MAPPING_FILE.exists():
        raise FileNotFoundError(
            f"ID mapping file not found: {ID_MAPPING_FILE}\n"
            f"Please run the ID mapping collection script first."
        )

    with open(ID_MAPPING_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


class JudicialDownloader:
    def __init__(self, account: str, password: str, verify_ssl: bool = True):
        self.account = account
        self.password = password
        self.token: Optional[str] = None
        self.token_expires: Optional[str] = None
        self.session = requests.Session()
        self.verify_ssl = verify_ssl
        self.id_mapping = load_id_mapping()
        self.print_lock = threading.Lock()  # Thread-safe printing

        if not verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            print("⚠️  SSL verification disabled")

    def authenticate(self) -> bool:
        """Get authentication token"""
        print("Authenticating...")

        payload = {
            "memberAccount": self.account,
            "pwd": self.password
        }

        try:
            response = self.session.post(
                AUTH_ENDPOINT,
                json=payload,
                headers={"Content-Type": "application/json"},
                verify=self.verify_ssl
            )
            response.raise_for_status()

            data = response.json()

            if "token" in data:
                self.token = data["token"]
                self.token_expires = data.get("expires", "unknown")
                print(f"✓ Authentication successful")
                print(f"  Token expires: {self.token_expires}")
                return True
            else:
                print(f"✗ Authentication failed: {data.get('message', 'Unknown error')}")
                return False

        except Exception as e:
            print(f"✗ Authentication error: {e}")
            return False

    def download_file(self, file_id: int, year: int, month: int, force: bool = False, idx: int = 0, total: int = 0) -> Tuple[bool, str]:
        """Download a single monthly package with Content-Length verification

        Returns:
            Tuple[bool, str]: (success, message)
        """

        if not self.token:
            return False, "No authentication token"

        # Create filename
        filename = f"{year}{month:02d}.rar"
        filepath = DOWNLOAD_DIR / filename

        # Skip if already exists (unless force is True)
        if filepath.exists() and not force:
            return True, f"⊙ {filename} already exists, skipping"

        url = f"{DOWNLOAD_ENDPOINT}/{file_id}/file"
        headers = {"Authorization": f"Bearer {self.token}"}

        for attempt in range(1, RETRY_TIMES + 1):
            try:
                prefix = f"[{idx}/{total}]" if total > 0 else ""

                response = self.session.get(url, headers=headers, stream=True, timeout=300, verify=self.verify_ssl)
                response.raise_for_status()

                # Get expected file size from Content-Length header
                expected_size = response.headers.get('Content-Length')
                if expected_size:
                    expected_size = int(expected_size)

                # Write to file
                downloaded_size = 0
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)

                # Verify file size
                actual_size = filepath.stat().st_size

                if expected_size and actual_size != expected_size:
                    # Size mismatch - file incomplete
                    filepath.unlink()
                    raise ValueError(
                        f"Size mismatch: expected {expected_size/1024/1024:.2f} MB, "
                        f"got {actual_size/1024/1024:.2f} MB"
                    )

                file_size_mb = actual_size / (1024 * 1024)
                return True, f"{prefix} ✓ {filename} ({file_size_mb:.2f} MB)"

            except Exception as e:
                if attempt < RETRY_TIMES:
                    time.sleep(RETRY_DELAY)
                else:
                    # Remove partial file if exists
                    if filepath.exists():
                        filepath.unlink()
                    return False, f"{prefix} ✗ {filename} failed: {e}"

        return False, f"{prefix} ✗ {filename} failed after {RETRY_TIMES} attempts"

    def download_all(self, start_month: Optional[str] = None, end_month: Optional[str] = None, force: bool = False):
        """Download all monthly packages with concurrent workers

        Args:
            start_month: Start month in YYYYMM format (e.g., "199601")
            end_month: End month in YYYYMM format (e.g., "202607")
            force: Force re-download existing files
        """
        # Get sorted list of months
        all_months = sorted(self.id_mapping.keys())

        # Filter by range if specified
        if start_month:
            all_months = [m for m in all_months if m >= start_month]
        if end_month:
            all_months = [m for m in all_months if m <= end_month]

        total = len(all_months)

        if total == 0:
            print("No months to download in specified range")
            return

        print(f"\n{'='*60}")
        print(f"Judicial Yuan Monthly Packages Downloader")
        print(f"{'='*60}")
        print(f"Download directory: {DOWNLOAD_DIR}")
        print(f"Date range: {all_months[0]} - {all_months[-1]} (Total: {total} files)")
        print(f"Concurrent workers: {MAX_WORKERS}")
        print(f"{'='*60}\n")

        # Create download directory
        DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

        # Authenticate
        if not self.authenticate():
            print("\n✗ Authentication failed. Exiting.")
            return

        print(f"\nStarting concurrent download...\n")

        # Track progress
        success_count = 0
        failed_items = []
        start_time = time.time()

        # Prepare download tasks
        tasks = []
        for idx, month_str in enumerate(all_months, 1):
            file_id = self.id_mapping[month_str]
            year = int(month_str[:4])
            month = int(month_str[4:6])
            tasks.append((file_id, year, month, idx))

        # Download concurrently with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self.download_file, file_id, year, month, force, idx, total): (month_str, file_id)
                for file_id, year, month, idx in tasks
            }

            # Process completed downloads
            for future in as_completed(future_to_task):
                month_str, file_id = future_to_task[future]
                try:
                    success, message = future.result()

                    # Thread-safe printing
                    with self.print_lock:
                        print(message)

                    if success:
                        success_count += 1
                    else:
                        failed_items.append((month_str, file_id))

                except Exception as e:
                    with self.print_lock:
                        print(f"✗ {month_str} failed with exception: {e}")
                    failed_items.append((month_str, file_id))

        # Summary
        elapsed = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"Download completed in {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
        print(f"Success: {success_count}/{total}")
        print(f"Average speed: {total/elapsed:.1f} files/second")

        if failed_items:
            print(f"\nFailed downloads ({len(failed_items)}):")
            for month_str, file_id in failed_items:
                year = int(month_str[:4])
                month = int(month_str[4:6])
                print(f"  - {year}.{month:02d} (ID: {file_id})")
        else:
            print(f"\n✓ All files downloaded successfully!")

        print(f"{'='*60}\n")


def parse_year_month(ym_string: str) -> str:
    """Parse year-month string and convert to YYYYMM format

    Accepts formats: YYYY.MM, YYYY-MM, YYYYMM

    Returns:
        str: Month in YYYYMM format
    """
    # Already in YYYYMM format
    if len(ym_string) == 6 and ym_string.isdigit():
        return ym_string

    # Parse other formats
    if '.' in ym_string:
        year, month = ym_string.split('.')
    elif '-' in ym_string:
        year, month = ym_string.split('-')
    else:
        raise ValueError(f"Invalid format: {ym_string}. Use YYYY.MM, YYYY-MM, or YYYYMM")

    return f"{int(year):04d}{int(month):02d}"


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Download monthly judgment packages from Judicial Yuan Open Data Platform',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  %(prog)s                          # Download all (1996.01 - 2026.07)
  %(prog)s 1996.01 1996.03          # Download specific range
  %(prog)s 2020.01 2020.12          # Download year 2020
  %(prog)s --force                  # Force re-download all files
  %(prog)s --no-ssl-verify          # Disable SSL verification (not recommended)
        '''
    )

    parser.add_argument('start', nargs='?', help='Start date (YYYY.MM, YYYY-MM, or YYYYMM)')
    parser.add_argument('end', nargs='?', help='End date (YYYY.MM, YYYY-MM, or YYYYMM)')
    parser.add_argument('--force', '-f', action='store_true',
                        help='Force re-download existing files')
    parser.add_argument('--no-ssl-verify', action='store_true',
                        help='Disable SSL certificate verification (use with caution)')

    args = parser.parse_args()

    # Load .env file from project root or scripts directory
    env_paths = [
        Path(__file__).parent / ".env",  # scripts/.env
        Path(__file__).parent.parent / ".env",  # project_aepa/.env
    ]

    for env_path in env_paths:
        if env_path.exists():
            load_dotenv(env_path)
            print(f"Loaded credentials from {env_path}")
            break

    # Get account from .env or environment variable
    account = os.environ.get("JUDICIAL_ACCOUNT")

    if not account:
        print("\n✗ Error: Account not found!")
        print("\nPlease create a .env file in project_aepa/ or project_aepa/scripts/")
        print("with the following content:")
        print("\n  JUDICIAL_ACCOUNT=your_account\n")
        sys.exit(1)

    # Get password - try environment variable first, then prompt for input
    password = os.environ.get("JUDICIAL_PASSWORD")

    if not password:
        print(f"\nAccount: {account}")
        password = getpass.getpass("Password: ")

        if not password:
            print("\n✗ Error: Password cannot be empty!\n")
            sys.exit(1)

    # Create downloader
    verify_ssl = not args.no_ssl_verify
    downloader = JudicialDownloader(account, password, verify_ssl=verify_ssl)

    # Parse date range
    start_month = None
    end_month = None

    if args.start:
        start_month = parse_year_month(args.start)
        year = int(start_month[:4])
        month = int(start_month[4:6])
        print(f"Start: {year}.{month:02d}")

    if args.end:
        end_month = parse_year_month(args.end)
        year = int(end_month[:4])
        month = int(end_month[4:6])
        print(f"End: {year}.{month:02d}")

    # Download
    downloader.download_all(start_month=start_month, end_month=end_month, force=args.force)


if __name__ == "__main__":
    main()
