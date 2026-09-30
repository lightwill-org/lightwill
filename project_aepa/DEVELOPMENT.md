# Development Environment Setup

Complete guide for setting up the development environment for the Animal Abuse and Crime Linkage Research project.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Python Environment Setup](#python-environment-setup)
- [System Dependencies](#system-dependencies)
- [DVC Setup](#dvc-setup)
- [Credentials Configuration](#credentials-configuration)
- [Verification](#verification)
- [Data Processing Workflow](#data-processing-workflow)
- [Common Issues](#common-issues)
- [Development Workflow](#development-workflow)

---

## Prerequisites

### Required Software

- **Python**: 3.8 or higher
- **Git**: For version control
- **Homebrew**: macOS package manager (for system dependencies)

### Check Current Versions

```bash
python3 --version    # Should be 3.8+
git --version
brew --version
```

### Disk Space Requirements

- **Processed Data** (DVC): 13GB (7.73M criminal cases: 1.2GB CSV + 12GB Parquet)
- **Raw Download** (optional): ~52GB (367 compressed RAR files)
- **Extracted** (temporary): ~150GB (22.5M JSON files - can be deleted after processing)
- **Working Space**: ~200GB recommended for full data processing workflow

---

## Python Environment Setup

### 1. Create Virtual Environment

```bash
cd project_aepa

# Create virtual environment
python3 -m venv .venv

# Activate
source .venv/bin/activate
```

### 2. Install Python Dependencies

```bash
# Ensure you're in the activated environment
pip install --upgrade pip

# Install project dependencies
pip install -r requirements.txt
```

**Current dependencies:**
- `requests>=2.31.0` - HTTP library for API calls
- `python-dotenv>=1.0.0` - Environment variable management
- `patool>=1.12` - Cross-platform archive extraction
- `pandas>=2.0.0` - Data processing and analysis
- `pyarrow>=12.0.0` - Parquet file format support
- `dvc[gdrive]` - Data version control with Google Drive backend

### 3. Deactivate When Done

```bash
deactivate
```

---

## System Dependencies

### RAR Extraction Tool

The downloaded files are in RAR format. We provide both Python-based and system-level solutions for cross-platform compatibility.

#### Option 1: Python Package (Cross-platform, Recommended)

Install `patool` - a Python library that works across all platforms:

```bash
# Already included in requirements.txt
pip install -r requirements.txt
```

**Platform-specific backend installation:**

| Platform | Command |
|----------|---------|
| **macOS** | `brew install unar` |
| **Ubuntu/Debian** | `sudo apt-get install unar` or `sudo apt-get install unrar` |
| **Windows** | Download from [WinRAR](https://www.rarlab.com/download.htm) or [7-Zip](https://www.7-zip.org/) |
| **Arch Linux** | `sudo pacman -S unrar` |

**Usage in Python:**

```python
import patool

# Extract a RAR file
patool.extract_archive('199601.rar', outdir='output_directory')
```

#### Option 2: System Tools (Platform-specific)

##### macOS
```bash
brew install unar
unar 199601.rar
```

##### Linux (Ubuntu/Debian)
```bash
# Option A: unar (open source)
sudo apt-get update
sudo apt-get install unar
unar 199601.rar

# Option B: unrar (proprietary but widely available)
sudo apt-get install unrar
unrar x 199601.rar
```

##### Windows
1. Install [7-Zip](https://www.7-zip.org/) or [WinRAR](https://www.rarlab.com/)
2. Right-click RAR file → Extract
3. Or use command line:
```cmd
"C:\Program Files\7-Zip\7z.exe" x 199601.rar
```

#### Verify Installation

```bash
# Check if backend is available
python -c "import patool; print('✓ patool installed')"

# Test extraction (after downloading test data)
python -c "import patool; patool.extract_archive('data/raw/monthly_packages/199601.rar', outdir='test_extract')"
```

---

## DVC Setup

DVC (Data Version Control) is used to track processed data files on Google Drive, avoiding the need to store large files in Git.

### 1. Install DVC

DVC is already included in `requirements.txt`. If you haven't installed it yet:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Initialize DVC (Already done for this project)

This project already has DVC initialized. You can verify by checking for `.dvc/` directory.

```bash
ls .dvc/
# Should show: config, .gitignore, etc.
```

### 3. Configure Google Drive Remote

To download processed data from Google Drive:

```bash
# Pull processed data from Google Drive
dvc pull
```

**First time setup**: DVC will prompt you to authenticate with Google Drive:
1. Click the authentication link in terminal
2. Sign in with Google account
3. Grant DVC access
4. Copy the authentication code back to terminal

### 4. Verify DVC Setup

```bash
# Check DVC status
dvc status

# List tracked files
dvc list . data/processed
```

### 5. Working with DVC

**Download processed data:**
```bash
dvc pull  # Downloads all DVC-tracked files
```

**Update processed data (after processing new data):**
```bash
# Add new processed files
dvc add data/processed/

# Commit DVC metadata
git add data/processed.dvc .gitignore
git commit -m "Update processed data"

# Push to Google Drive
dvc push
```

**Note**: Only `data/processed/` is tracked by DVC. Raw data (`data/raw/`) is too large and must be downloaded using scripts.

---

## Credentials Configuration

### 1. Register Account

Register for a member account at:
https://opendata.judicial.gov.tw/

**Registration Information:**
- Use research project name for organization
- Project lead (湘敏) as primary contact
- Academic/research purpose

### 2. Create `.env` File

```bash
# Copy example file
cp .env.example .env

# Edit with your credentials
nano .env  # or use your preferred editor
```

**Edit `.env` contents:**
```env
JUDICIAL_ACCOUNT=your_account_here
```

**Security Note**: Password is NOT saved in `.env` for security. The script will prompt for password during execution.

### 3. File Permissions (Recommended)

```bash
# Restrict .env file access to owner only
chmod 600 .env
```

---

## Verification

### 1. Test Python Environment

```bash
source .venv/bin/activate
python -c "import requests; import dotenv; print('✓ All dependencies installed')"
```

### 2. Test Download Script

```bash
# Download 3 months as a test
python scripts/download_monthly_packages.py 1996.01 1996.03
```

**Expected output:**
```
Loaded credentials from /path/to/.env
Account: your_account
Password: [prompted - enter your password]
Authenticating...
✓ Authentication successful
...
✓ All files downloaded successfully!
```

### 3. Verify Downloaded Files

```bash
ls -lh data/raw/monthly_packages/
# Should show: 199601.rar, 199602.rar, 199603.rar
```

### 4. Test RAR Extraction

```bash
cd data/raw/monthly_packages
mkdir test_extract
unar 199601.rar -o test_extract
ls test_extract/
# Should show extracted JSON files
```

---

## Data Processing Workflow

This section explains the complete workflow for processing raw judgment data into structured criminal case datasets.

### Overview

The data processing pipeline:
1. **Download**: Monthly packages from Judicial Yuan (367 RAR files, ~52GB)
2. **Extract**: RAR files to JSON (22.5M files, ~150GB temporary)
3. **Process**: JSON → Filter criminal cases → Export CSV + Parquet
4. **Track**: Upload processed data to DVC (Google Drive)
5. **Clean**: Delete extracted JSON files to save space

### Step 1: Download Raw Data

```bash
# Download all monthly packages (concurrent processing, 8 workers)
python scripts/download_monthly_packages.py

# Or download specific range
python scripts/download_monthly_packages.py 2020.01 2020.12
```

**Performance**: 10-20x faster with concurrent downloads

### Step 2: Extract RAR Files

```bash
# Install extraction tool first
brew install unar  # macOS

# Extract all RAR files (multiprocessing, uses all CPU cores)
python scripts/extract_rar.py

# Or extract specific range
python scripts/extract_rar.py --start-month 2020.01 --end-month 2020.12
```

**Performance**: 4-8x faster with multiprocessing

### Step 3: Process JSON Files

Located at `scripts/process_judgments.py`, this script:
- Scans 22.5 million JSON files using fast `os.walk()`
- Filters criminal cases (JID contains 'M' in court code) → 7.73M cases
- Extracts metadata: JID, court, year, case type, judgment date, title, full text, etc.
- Uses **streaming mode** to avoid memory issues
- Exports two formats:
  - **CSV** (1.2GB): Case summaries **WITHOUT full text** (for quick filtering)
  - **Parquet batch files** (12GB): Complete data **WITH full text** (for text analysis)

**Usage:**

```bash
# Process all downloaded data (22.5M files → 7.73M criminal cases)
python scripts/process_judgments.py

# Process specific time range (YYYY.MM or YYYYMM format)
python scripts/process_judgments.py --start-month 2020.01 --end-month 2020.12

# Force reprocess existing files
python scripts/process_judgments.py --start-month 199601 --end-month 199612 --force

# Only export CSV (skip Parquet)
python scripts/process_judgments.py --csv-only
```

**Performance:**
- Processing speed: ~40,000 files/sec with multiprocessing
- Memory usage: <1GB with streaming mode
- Time: ~1.5-2 hours for full dataset (22.5M files)

**Output Files:**

Processed files are saved to `data/processed/`:

```
data/processed/
├── criminal_judgments_all_20260930_summary.csv           # 1.2GB - Summaries without full text
└── criminal_judgments_all_20260930_batch*.parquet        # 12GB - Complete data with full text (batch files)
```

The Parquet data is split into batch files for memory efficiency:
- Each batch: ~500MB
- Total batches: ~24 files
- Can be read individually or all at once with pandas

### Step 4: Update DVC

After generating new processed data:

```bash
# Add to DVC
dvc add data/processed/

# Commit metadata
git add data/processed.dvc .gitignore
git commit -m "Update processed criminal judgments: 7.73M cases"

# Push to Google Drive (13GB)
dvc push
```

### Step 5: Clean Up Space

After processing and uploading to DVC, delete extracted JSON files to save space:

```bash
# Delete all extracted directories (keeps RAR files)
# WARNING: Make sure processing completed successfully before deleting
for dir in data/raw/monthly_packages/[0-9][0-9][0-9][0-9][0-9][0-9]; do
  rm -rf "$dir" &
done
wait

# This frees up ~150GB of disk space
```

**Space summary:**
- Before cleanup: ~215GB (52GB RAR + 150GB JSON + 13GB processed)
- After cleanup: ~65GB (52GB RAR + 13GB processed)

### Reading Processed Data with Pandas

**Reading CSV (case summaries, no full text):**

```python
import pandas as pd

# Read CSV file
df = pd.read_csv('data/processed/criminal_judgments_all_*_summary.csv')

# Explore data structure
print(df.columns)
print(df.head())
print(f"Total cases: {len(df):,}")
```

**Reading Parquet (complete data with full text):**

```python
import pandas as pd
import glob

# Read all batch files
parquet_files = glob.glob('data/processed/*_batch*.parquet')
df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)

# Check available columns
print(df.columns)  # Includes 'full_text'
print(f"Total cases: {len(df):,}")
```

**Usage Tips:**
- Use CSV for filtering by metadata (title, case_type, court, date, etc.)
- Use Parquet when you need full judgment text for analysis
- Can read individual batch files for memory efficiency during testing

---

## Common Issues

### Issue 1: `command not found: python3`

**Solution**: Install Python 3
```bash
brew install python@3.11
```

### Issue 2: `command not found: unrar`

**Solution**: Use `unar` instead
```bash
brew install unar
# Use 'unar' command, not 'unrar'
```

### Issue 3: Virtual environment not activating

**Symptoms**: `which python` still points to system Python

**Solution**:
```bash
# Ensure you're in project directory
cd project_aepa

# Re-activate
source .venv/bin/activate

# Verify
which python  # Should show .venv path
```

### Issue 4: Authentication failed

**Possible causes:**
- Incorrect account/password
- Account not yet activated
- Token expired (auto-refreshes, shouldn't happen)

**Solution**:
1. Verify credentials at https://opendata.judicial.gov.tw/
2. Check account is active and verified
3. Re-enter password carefully

### Issue 5: Download fails partway through

**Solution**: Script has resume capability
```bash
# Simply re-run the same command
python scripts/download_monthly_packages.py [start] [end]
# Already downloaded files will be skipped
```

### Issue 6: `pip install` fails with permission error

**Solution**: Ensure virtual environment is activated
```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### Issue 7: `pandas not installed` error during processing

**Symptoms**:
```
⚠️  pandas not installed, skipping Parquet export
Install with: pip install pandas pyarrow
```

**Solution**: Install data processing dependencies
```bash
source .venv/bin/activate
pip install -r requirements.txt
# Or specifically:
pip install pandas pyarrow
```

### Issue 8: DVC authentication fails

**Symptoms**: Cannot pull/push to Google Drive

**Solution**:
1. Run `dvc pull` and follow authentication prompts
2. Grant DVC access to Google Drive
3. Verify credentials: `dvc status`
4. Check remote configuration: `cat .dvc/config`

---

## Development Workflow

### Daily Workflow

```bash
# 1. Navigate to project
cd project_aepa

# 2. Activate environment
source .venv/bin/activate

# 3. Work on your task
python scripts/download_monthly_packages.py ...
# or other development tasks

# 4. Deactivate when done
deactivate
```

### Adding New Dependencies

```bash
# Activate environment
source .venv/bin/activate

# Install new package
pip install package_name

# Update requirements.txt
pip freeze > requirements.txt
```

### Git Workflow

```bash
# Check status
git status

# Add changes
git add .

# Commit (do NOT commit .env or data/)
git commit -m "Description of changes"

# Push
git push
```

**Important**: `.env` and `data/` are gitignored and should never be committed.

---

## Environment Variables Reference

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `JUDICIAL_ACCOUNT` | Yes | Judicial Yuan account | `your_email@example.com` |
| `JUDICIAL_PASSWORD` | No* | Password (prompted if not set) | Not recommended to save |

*Password can be set as environment variable for automation, but manual input is recommended for security.

---

## IDE Configuration (Optional)

### VSCode

Create `.vscode/settings.json`:
```json
{
  "python.defaultInterpreterPath": "${workspaceFolder}/.venv/bin/python",
  "python.terminal.activateEnvironment": true
}
```

### PyCharm

1. File → Settings → Project → Python Interpreter
2. Add Interpreter → Existing Environment
3. Select: `project_aepa/.venv/bin/python`

---

## Useful Aliases (Optional)

Add to `~/.zshrc` or `~/.bashrc`:

```bash
# Quick activate
alias vact='source .venv/bin/activate'

# Quick navigate + activate
alias cdaepa='cd ~/dev/lightwill/project_aepa && source .venv/bin/activate'
```

Usage:
```bash
cdaepa  # Instantly navigate and activate environment
```

---

## Next Steps

Once environment is setup:

1. **Get processed data**: Use `dvc pull` to download processed criminal judgment data (13GB)
2. **Explore data structure**: Load CSV/Parquet files to understand available columns and data format
3. **Develop analysis pipeline**: Define your filtering criteria and analysis methodology (see `animal-abuse-crime-linkage-data-plan.md` for research design)

### Optional: Process Raw Data

If you need to modify the processing pipeline or work with specific date ranges:

1. **Download raw data**: Use `download_monthly_packages.py` for specific time periods
2. **Extract RAR files**: Use `extract_rar.py`
3. **Process JSON**: Use `process_judgments.py` with date range parameters
4. **Update DVC**: Add processed files and push to Google Drive

---

## Troubleshooting Contact

If you encounter issues not covered here:

1. Check existing GitHub Issues
2. Review `animal-abuse-crime-linkage-data-plan.md` for design decisions
3. Contact project lead (湘敏)

---

*Last Updated: 2026-09-30*
