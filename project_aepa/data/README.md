# Data Directory Structure

This directory contains all data for the Animal Abuse and Crime Linkage Research project.

## Directory Structure

```
data/
├── raw/                    # Raw, immutable data (NOT tracked by DVC)
│   └── monthly_packages/   # Monthly judgment packages from Judicial Yuan
│       ├── 199601.rar      # Compressed RAR files (~52GB total)
│       ├── 199602.rar
│       └── ...
│
├── processed/              # Final, canonical datasets for analysis (tracked by DVC)
│   ├── criminal_judgments_all_*_summary.csv            # 1.2GB - Case summaries WITHOUT full text
│   └── criminal_judgments_all_*_batch*.parquet         # 12GB - Complete data WITH full text (batch files)
│
└── id_mapping.json         # Month-to-FilesetList ID mapping (1996.01 - 2026.07)
```

## Data Scale

- **Total criminal cases**: 7.73 million (from 22.5 million JSON files, 1996.01-2026.07)
- **CSV file**: 1.2GB - Case summaries without full judgment text
- **Parquet files**: 12GB - Complete data with full judgment text (batch files)
- **Total processed data**: 13GB (tracked by DVC on Google Drive)

## Data Management

- **Processed data**: Tracked by DVC (Data Version Control), stored in Google Drive
- **Raw data**: NOT tracked by DVC (too large, ~52GB compressed, ~150GB extracted)
- **Version control**: Only metadata files (.dvc) are in git, actual data files are not

## Usage

### Get Processed Data (Recommended)

```bash
# Download processed data from DVC (13GB: 1.2GB CSV + 12GB Parquet)
dvc pull
```

**Data includes:**

#### CSV File (1.2GB)
- **File**: `criminal_judgments_all_*_summary.csv`
- **Records**: 7.73 million criminal cases
- **Content**: Case summaries **WITHOUT full judgment text**
- **Use for**: Quick filtering, case statistics, metadata analysis

#### Parquet Files (12GB, batch files)
- **Files**: `criminal_judgments_all_*_batch*.parquet` (multiple batch files)
- **Records**: 7.73 million criminal cases
- **Content**: Complete data **WITH full judgment text**
- **Use for**: Text analysis, prior conviction extraction

This is sufficient for most analysis work.

### Using the Data with Pandas

#### Reading CSV File (Case Summaries)

```python
import pandas as pd

# Read CSV summary (faster, smaller memory footprint)
df = pd.read_csv('data/processed/criminal_judgments_all_*_summary.csv')

# Explore data structure
print(f"Total cases: {len(df):,}")
print(df.columns)
print(df.head())
print(df.info())

# Check unique values in categorical columns
print(df['case_category'].value_counts().head(20))
print(df['court'].value_counts().head(10))
```

#### Reading Parquet Files (Complete Data with Full Text)

```python
import pandas as pd
import glob

# Read all Parquet batch files at once
parquet_files = glob.glob('data/processed/*_batch*.parquet')
df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)

# Check available columns
print(df.columns)  # Includes 'full_text'
print(f"Total cases: {len(df):,}")

# Or read single batch for testing/exploration
df_batch = pd.read_parquet('data/processed/criminal_judgments_all_*_batch00000.parquet')
print(f"Batch size: {len(df_batch):,} cases")
```

**Usage Tips:**
- Use CSV for quick filtering and metadata analysis (faster, smaller memory footprint)
- Use Parquet when you need to search or analyze full judgment text
- Parquet batch files can be read all at once or individually for memory efficiency
- Common filtering operations: `.str.contains()`, `.isin()`, date range filtering, etc.

### Get Raw Data (Optional)

If you need to modify the processing pipeline or extract different features:

```bash
# Download all monthly packages (1996.01 - 2026.07)
python scripts/download_monthly_packages.py --no-ssl-verify

# Or download specific range
python scripts/download_monthly_packages.py 2020.01 2020.12 --no-ssl-verify
```

**Requirements**:
- Disk space: ~52GB (compressed) + ~150GB (extracted during processing)
- Account at https://opendata.judicial.gov.tw/
- Set credentials in `.env` file (see `.env.example`)
- Time: Several hours for full download

### Process Raw Data to Generate Processed Data

First, extract RAR files:

```bash
# macOS: Install unar
brew install unar

# Extract all downloaded RAR files (uses multiprocessing)
python scripts/extract_rar.py
```

Then process JSON files to structured datasets:

```bash
# Process all data (22.5M files → 7.73M criminal cases)
python scripts/process_judgments.py

# Process specific time range (YYYY.MM or YYYYMM format)
python scripts/process_judgments.py --start-month 2020.01 --end-month 2020.12

# Force reprocess existing files
python scripts/process_judgments.py --start-month 199601 --end-month 199612 --force

# Only generate CSV (skip Parquet)
python scripts/process_judgments.py --csv-only
```

**What the script does:**
- Scans and filters criminal cases (JID containing 'M') from 22.5 million JSON files
- Extracts metadata and full judgment text
- Uses multiprocessing and streaming to handle large datasets efficiently
- Generates two types of output:
  - **CSV**: Case summaries without full text (for quick filtering)
  - **Parquet batches**: Complete data with full text (for text analysis)

**Performance:**
- Processing speed: ~40,000 files/sec with multiprocessing
- Memory usage: <1GB with streaming mode
- Time: ~1.5-2 hours for full dataset

**Note**: The script automatically skips already processed ranges unless you use `--force`.

### Update Processed Data

```bash
# After generating new processed data
dvc add data/processed/
dvc push
git add data/processed.dvc .gitignore
git commit -m "Update processed data"
```

## Data Sources

- **Judicial Yuan Open Data Platform**: https://opendata.judicial.gov.tw/
- **License**: Subject to Judicial Yuan Open Data Usage Terms
- **Coverage**: 1996.01 - 2026.07 (367 monthly packages)
