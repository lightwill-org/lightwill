# Animal Abuse and Crime Linkage Research

Research project analyzing the correlation between animal abuse and subsequent violent crimes through judicial judgment data analysis.

## Project Overview

This project aims to verify and present the phenomenon that animal abuse often correlates with or precedes serious violent crimes, enabling early warning systems when an individual shows both criminal behavior and animal abuse records.

**Data Source**: Judicial Yuan Open Data Platform (Taiwan)
**Data Range**: 1996.01 - 2026.07 (367 monthly packages)
**Data Volume**:
- Raw: ~52GB compressed RAR files, ~150GB extracted JSON
- Processed: 7.73 million criminal cases (1.2GB CSV + 12GB Parquet)

## Repository Structure

```
project_aepa/
├── README.md                    # Project overview (this file)
├── DEVELOPMENT.md               # Development environment setup
├── QUICKSTART.md                # Quick start guide for users
├── requirements.txt             # Python dependencies
├── .env.example                 # Configuration template
├── .gitignore                   # Git ignore rules
│
├── animal-abuse-crime-linkage-data-plan.md  # Research design document
│
├── scripts/
│   ├── download_monthly_packages.py         # Download raw data from Judicial Yuan
│   ├── extract_rar.py                       # Extract RAR files
│   ├── process_judgments.py                 # Process JSON to structured data
│   └── README.md                            # Script documentation
│
└── data/
    ├── raw/monthly_packages/                # Downloaded RAR files (not in git)
    ├── processed/                           # Processed criminal judgments (DVC tracked)
    └── processed.dvc                        # DVC metadata file
```

## Documentation

- **[QUICKSTART.md](QUICKSTART.md)** - Quickest way to get started
- **[DEVELOPMENT.md](DEVELOPMENT.md)** - Full development environment setup
- **[scripts/README.md](scripts/README.md)** - Download script documentation
- **[animal-abuse-crime-linkage-data-plan.md](animal-abuse-crime-linkage-data-plan.md)** - Detailed research design and data strategy

## Quick Start

### Option 1: Use Processed Data (Recommended)

```bash
# 1. Setup environment
cd project_aepa
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Get processed data (13GB: 1.2GB CSV + 12GB Parquet)
dvc pull
```

**Data includes:**
- **CSV**: 7.73 million criminal case summaries (without full judgment text)
- **Parquet**: Complete judgment data with full text (batch files)

### Option 2: Process Raw Data Yourself

```bash
# 1. Setup environment (same as above)

# 2. Configure credentials
cp .env.example .env
nano .env  # Fill in JUDICIAL_ACCOUNT

# 3. Download and process
python scripts/download_monthly_packages.py 1996.01 1996.03
python scripts/extract_rar.py
python scripts/process_judgments.py --start-month 1996.01 --end-month 1996.03
```

For detailed instructions, see [QUICKSTART.md](QUICKSTART.md).

## Project Status

**Phase**: Data Ready for Analysis
**Current**: 7.73 million criminal cases processed and ready
**Next**: Filter animal abuse cases and extract prior conviction information

### Completed
- ✅ Data source verification and licensing review
- ✅ API authentication and download script (367 monthly packages, 1996.01-2026.07)
- ✅ Full data download completed (~52GB compressed)
- ✅ RAR extraction pipeline (concurrent processing with multiprocessing)
- ✅ JSON processing (22.5 million files → 7.73 million criminal cases)
- ✅ DVC data versioning setup (13GB processed data on Google Drive)
- ✅ Structured datasets:
  - CSV: 7.73 million case summaries (1.2GB)
  - Parquet: Complete judgment text (12GB, batch files)

### In Progress
- 🔄 Filter animal abuse related cases
- 🔄 Extract prior conviction information from judgment text

### Planned
- Animal-first keyword search strategy
- Crime-first search strategy
- Statistical analysis and correlation study
- Visualization and reporting

## Research Design

This project uses a **retrospective approach** rather than prospective tracking:

**Research Question**: Among individuals who have committed violent crimes, what percentage have a history of animal abuse?

**Methodology**: Extract prior conviction narratives from criminal judgments, as judgments typically contain the defendant's criminal history within a single document.

**Data Limitation**: Cross-judgment person matching is not feasible due to redacted names in public judgments.

For detailed research design, see [animal-abuse-crime-linkage-data-plan.md](animal-abuse-crime-linkage-data-plan.md).

## License and Attribution

Data source: Judicial Yuan Open Data Platform (Taiwan)
Usage: Complies with [Judicial Yuan Open Data Usage Terms](https://legal.judicial.gov.tw/FLAW/dat02.aspx?lsid=FL095666)

When publishing research:
- Must clearly cite data source
- Must disclose any data modifications
- Must delete data if removed from official platform
- Cannot claim work was created or authorized by Judicial Yuan

## Contact

Project Lead: 湘敏 (Xiang-Min)
Repository: https://github.com/[your-org]/lightwill

---

*Last Updated: 2026-09-30*
