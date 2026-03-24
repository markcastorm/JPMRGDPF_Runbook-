# JPMRGDPF Runbook - Development Memory

## Architecture Decision
- Based on BOACTAR runbook architecture (same SIMBA-RUNBOOKS project)
- 6-module pattern: `config.py`, `data_loader.py`, `parser.py`, `file_generator.py`, `logger_setup.py`, `orchestrator.py`
- BOACTAR reference code at: `D:\Projects\SIMBA-RUNBOOKS\BOACTAR\`

## Key Design Decisions

### Dynamic Parser (the brain)
- Parser NEVER hardcodes row/column positions
- Scans entire sheet to find "Real GDP % over year ago" header
- Detects year columns dynamically by finding 4-digit numbers (2020-2040 range) near the header
- Finds "Area" column by label scan, with heuristic fallback (counts country name matches per column)
- Matches countries with flexible name normalization (strips whitespace, case-insensitive, aliases)
- Skips region aggregates: Latin America, Asia/Pacific, Western Europe, EM Asia, Ex China/India, etc.

### Rolling Window Master (NOT cumulative)
- Runbook spec: "Save annual (last year, current and future year) data from Real GDP % over year ago"
- Master keeps ONLY the years present in the latest input
- When 2028 appears and 2025 disappears from input, master drops 2025
- This is different from BOACTAR which accumulates all historical dates
- Change was explicitly discussed and tested

### Year Column Format
- Years stored as clean integers (2025, not 2025.0)
- Initial bug: `float(year)` produced "2025.0" in output
- Fixed in `parser.py` (build_dataframe uses `int(year)`) and `file_generator.py` (`int(float(date_val))`)

### Single File Processing
- Only processes the newest file in Input/ folder (sorted by modification time)
- Unlike BOACTAR which processes all files chronologically
- Makes sense because each input file contains the complete rolling window, not incremental data

## Input File Structure (Global Outlook Summary tab)
- Row 4 area: Section headers ("Real GDP % over year ago", etc.)
- Row 5 area: Column labels (Area, Country, DQ Formula, 2025, 2026, 2027, ...)
- Row 6: Usually empty spacer
- Row 7+: Data rows. Country names in Area column, GDP values under year columns
- DQ Formula columns are interspersed - parser skips them (they contain formula strings, not years)
- Country names often have trailing spaces ("United States  ")
- "Korea" in input maps to "South Korea" in output

## Output Format
- **DATA file**: Row 1 = column codes, Row 2 = descriptions, Row 3+ = year rows with GDP values
- **META file**: CODE, DESCRIPTION, FREQUENCY(A), UNIT_TYPE(Percent), DATA_TYPE(Level), SOURCE(JPM), CATEGORY(RGDP)
- **ZIP**: Contains DATA + META
- Output goes to: timestamped subfolder + latest/ subfolder
- Column code pattern: `JPMRGDPF.REALGDP.ANNUAL.<ISO3>.A`
- Description pattern: `Real GDP: Annual: <Country Name>`

## 35 Target Countries (alphabetical by display name)
ARG, AUS, BRA, CAN, CHL, CHN, COL, CZE, ECU, EURA, FRA, DEU, HKG, HUN, IND, IDN, ISR, ITA, JPN, KOR, MYS, MEX, NZL, PER, PHL, POL, RUS, SGP, ZAF, ESP, TWN, THA, TUR, GBR, USA

## Tests Performed
1. Normal run with matching master → "NO CHANGES DETECTED" (correct)
2. Modified master value (999) → detected change, generated output, corrected master
3. New year (2028) injected → dynamically detected 4 years, added to output
4. Year rolloff (2025 dropped, 2028 added) → master rolling window updated, 2025 discarded
5. Year column format verified: clean integers, not floats

## File Paths
- Project: `D:\Projects\SIMBA-RUNBOOKS\JPMRGDPF_Runbook\`
- Input: `D:\Projects\SIMBA-RUNBOOKS\JPMRGDPF_Runbook\Input\`
- Master: `D:\Projects\SIMBA-RUNBOOKS\JPMRGDPF_Runbook\Master Data\Master_JPMRGDPF_DATA.xlsx`
- Output: `D:\Projects\SIMBA-RUNBOOKS\JPMRGDPF_Runbook\output\`
- Logs: `D:\Projects\SIMBA-RUNBOOKS\JPMRGDPF_Runbook\logs\`
- Reference BOACTAR: `D:\Projects\SIMBA-RUNBOOKS\BOACTAR\`

## Dependencies
- openpyxl >= 3.1.0
- pandas >= 2.0.0
