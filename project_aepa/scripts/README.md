# Download Scripts

Scripts for downloading judgment data from Judicial Yuan Open Data Platform.

## Prerequisites

1. Register an account at https://opendata.judicial.gov.tw/
2. Install Python dependencies from project root:
   ```bash
   cd project_aepa
   pip install -r requirements.txt
   ```
3. Set up credentials:
   ```bash
   # From project root
   cp .env.example .env
   nano .env  # Edit and fill in your account
   ```

## Usage

The script will:
1. Load account from `.env` file (in `project_aepa/` or `project_aepa/scripts/`)
2. Prompt you to enter password (for security, password is not saved)

### Download All Monthly Packages (1996.01 - 2026.06)

From project root:
```bash
cd project_aepa
python scripts/download_monthly_packages.py
```

This will download 366 monthly RAR files (~112GB after extraction) to:
```
project_aepa/data/raw/monthly_packages/
```

### Download Specific Range

Download only a subset by specifying start and end dates:

```bash
# Download only 2020 (using year-month format)
python scripts/download_monthly_packages.py 2020.01 2020.12

# Or use hyphen format
python scripts/download_monthly_packages.py 2020-01 2020-12

# Download only first 3 months of 1996 for testing
python scripts/download_monthly_packages.py 1996.01 1996.03

# You can also use ID format (backward compatible)
python scripts/download_monthly_packages.py 68132 68134
```

### Available Date Range

- **Earliest**: 1996.01 (ID: 68132)
- **Latest**: 2026.06 (ID: 68497)
- **Total**: 366 monthly packages

---

## Extracting RAR Files

After downloading, use the extraction script to extract JSON files:

### Extract All Files

```bash
# From project root
python scripts/extract_rar.py
```

This will extract all RAR files from `data/raw/monthly_packages/` to individual directories.

### Extract Single File

```bash
# Extract specific month
python scripts/extract_rar.py data/raw/monthly_packages/199601.rar

# Extract to custom directory
python scripts/extract_rar.py data/raw/monthly_packages/199601.rar output/199601/
```

### Prerequisites for Extraction

Install system RAR tool first (see [DEVELOPMENT.md](../DEVELOPMENT.md#system-dependencies)):

| Platform | Installation |
|----------|--------------|
| macOS | `brew install unar` |
| Ubuntu/Debian | `sudo apt-get install unar` |
| Windows | Install [7-Zip](https://www.7-zip.org/) |

---

## Features

- ✓ Automatic authentication with token refresh
- ✓ Resume capability (skips already downloaded files)
- ✓ Retry on failure (3 attempts with 5s delay)
- ✓ Rate limiting (1s delay between requests)
- ✓ Progress tracking
- ✓ Download summary with failed file list

## Notes

- Each file is a RAR archive containing JSON files
- Total compressed size: ~34GB
- Total uncompressed size: ~112GB
- Download speed depends on network and server load
- Estimated time: several hours for full download
