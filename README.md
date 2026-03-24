# JPMRGDPF Runbook

JPMorgan Real GDP Forecast data processing pipeline. Extracts "Real GDP % over year ago" forecasts from JPM Global Outlook Summary Excel files for 35 countries, and produces standardized DATA, META, and ZIP output files.

## Project Structure

```
JPMRGDPF_Runbook/
├── orchestrator.py       # Main entry point - runs the full pipeline
├── config.py             # Central configuration (paths, country mappings, settings)
├── data_loader.py        # Scans input folder, finds target sheet
├── parser.py             # Dynamic parser - detects sections, years, countries
├── file_generator.py     # Generates DATA, META, ZIP output files
├── logger_setup.py       # Logging configuration
├── requirements.txt      # Python dependencies
├── Input/                # Place input Excel files here
├── output/               # Generated output (timestamped + latest)
├── Master Data/          # Rolling window master data file
├── logs/                 # Timestamped log files
└── Project_information/  # Reference docs, samples, images
```

## Setup

```bash
pip install -r requirements.txt
```

**Dependencies:** openpyxl, pandas

## Usage

1. Place one or more input `.xlsx` / `.xlsm` files in the `Input/` folder
2. Run the pipeline:

```bash
python orchestrator.py
```

The pipeline will:
- Scan `Input/` for the newest Excel file
- Find the **Global Outlook Summary** tab (flexible matching)
- Dynamically locate the "Real GDP % over year ago" section
- Detect year columns automatically (handles 2, 3, 4+ years)
- Extract GDP values for all 35 target countries
- Compare with master data for changes
- If changes found: generate output files and update master
- If no changes: log and exit cleanly

## Dynamic Parser

The parser never hardcodes row/column positions. It:

1. **Scans** every cell to find the "Real GDP % over year ago" header
2. **Detects** year columns by finding 4-digit numbers near the header
3. **Locates** the Area column by label or heuristic fallback
4. **Matches** countries flexibly (strips whitespace, case-insensitive, handles aliases like "Korea" → "South Korea")
5. **Skips** region aggregates (Latin America, Asia/Pacific, etc.)

## Input Files

- Excel files (`.xlsx`, `.xlsm`) from JPM Global Outlook
- Must contain a **Global Outlook Summary** tab (or similar name)
- Section: "Real GDP % over year ago" with year columns and country rows
- Layout can shift between files - the parser adapts

## Output Files

Each run produces three files:

| File | Description |
|------|-------------|
| `JPMRGDPF_DATA_<timestamp>.xlsx` | Row 1: column codes, Row 2: descriptions, Row 3+: data |
| `JPMRGDPF_META_<timestamp>.xlsx` | Time series metadata (frequency, unit type, source, etc.) |
| `JPMRGDPF_<timestamp>.zip` | Archive containing DATA and META files |

Output is written to both a timestamped subfolder and a `latest/` subfolder.

## Master Data (Rolling Window)

The master file keeps only the years present in the latest input (last year, current year, future year). When new years appear, old years that are no longer in the input are dropped. This matches the runbook spec: "Save annual (last year, current and future year) data."

## Country Coverage (35 Countries)

Argentina, Australia, Brazil, Canada, Chile, China, Colombia, Czech Republic, Ecuador, Euro area, France, Germany, Hong Kong, Hungary, India, Indonesia, Israel, Italy, Japan, South Korea, Malaysia, Mexico, New Zealand, Peru, Philippines, Poland, Russia, Singapore, South Africa, Spain, Taiwan, Thailand, Turkey, United Kingdom, United States

## Column Code Format

- **Code:** `JPMRGDPF.REALGDP.ANNUAL.<ISO3>.A`
- **Description:** `Real GDP: Annual: <Country Name>`

Example: `JPMRGDPF.REALGDP.ANNUAL.USA.A` → "Real GDP: Annual: United States"

## Configuration

All settings are in `config.py`:
- **COUNTRY_MAPPING** - Maps input country names to ISO3 codes
- **COUNTRY_ORDER** - Controls output column ordering
- **TARGET_SECTION_HEADER** - Section to locate in input ("Real GDP % over year ago")
- **TARGET_TAB_NAMES** - Sheet names to search for
- **SKIP_AREAS** - Region/aggregate rows to ignore
- **METADATA_DEFAULTS** - Default values for META file fields
