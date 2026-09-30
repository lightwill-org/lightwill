# Quick Start Guide

For analysts who want to use the processed criminal judgment data for analysis.

**For developers**: See [DEVELOPMENT.md](DEVELOPMENT.md) for complete development environment setup and data processing workflow.

---

## Setup (One-time)

```bash
# 1. Navigate to project directory
cd project_aepa

# 2. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
```

## Get Processed Data

```bash
# Download processed data from DVC (13GB: 1.2GB CSV + 12GB Parquet)
dvc pull
```

**Data includes:**

### CSV File (1.2GB)
- **File**: `criminal_judgments_all_*_summary.csv`
- **Records**: 7.73 million criminal cases
- **Content**: Case summaries **WITHOUT full judgment text**
- **Use for**: Quick filtering, case statistics, metadata analysis

### Parquet Files (12GB, batch files)
- **Files**: `criminal_judgments_all_*_batch*.parquet` (multiple batch files)
- **Records**: 7.73 million criminal cases
- **Content**: Complete data **WITH full judgment text**
- **Use for**: Text analysis, prior conviction extraction

**How to use Parquet batch files:**

```python
import pandas as pd
import glob

# Read all batch files at once (pandas handles it automatically)
parquet_files = glob.glob('data/processed/*_batch*.parquet')
df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)

# Or read specific batches
df_batch1 = pd.read_parquet('data/processed/criminal_judgments_all_*_batch00000.parquet')
```

You can now use the processed data for analysis.

## Data Location

After running `dvc pull`, you'll find the processed data here:

```
project_aepa/data/processed/
├── criminal_judgments_all_*_summary.csv       # 1.2GB - Case summaries (no full text)
└── criminal_judgments_all_*_batch*.parquet    # 12GB - Complete data (with full text)
```

**Files:**
- **CSV**: 7.73M case summaries without full judgment text
- **Parquet**: 7.73M cases with full judgment text (split into ~24 batch files)

## Using the Data

### Reading CSV File (Case Summaries)

```python
import pandas as pd

# Read CSV summary (faster, smaller memory footprint)
df = pd.read_csv('data/processed/criminal_judgments_all_*_summary.csv')

# Explore data structure
print(f"Total cases: {len(df):,}")
print(df.columns)
print(df.head())

# Available columns for filtering (example):
# - JID, court, year, case_type, case_category
# - title, judgment_date, etc.
```

### Reading Parquet Files (Complete Data with Full Text)

```python
import pandas as pd
import glob

# Read all Parquet batch files
parquet_files = glob.glob('data/processed/*_batch*.parquet')
df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)

# Check available columns
print(df.columns)  # Includes 'full_text'
print(f"Total cases: {len(df):,}")

# Or read single batch for testing
df_batch = pd.read_parquet('data/processed/criminal_judgments_all_*_batch00000.parquet')
```

**Filtering suggestions:**
- Use CSV for filtering by metadata fields (title, case_type, court, etc.)
- Use Parquet when you need to search within full judgment text
- Common pandas operations: `.str.contains()`, `.isin()`, date filtering, etc.
