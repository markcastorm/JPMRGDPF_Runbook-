"""
JPMRGDPF Runbook Configuration
===============================
Central configuration for the JPM Real GDP Forecast data processing pipeline.

This runbook processes Excel files from JPMorgan containing Global Outlook Summary
data - specifically "Real GDP % over year ago" forecasts for 35 countries.
"""

import os
from datetime import datetime

# =============================================================================
# PATHS CONFIGURATION
# =============================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_DIR = os.path.join(BASE_DIR, 'Input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
MASTER_DIR = os.path.join(BASE_DIR, 'Master Data')
LOGS_DIR = os.path.join(BASE_DIR, 'logs')

MASTER_DATA_FILE = os.path.join(MASTER_DIR, 'Master_JPMRGDPF_DATA.xlsx')

# =============================================================================
# INPUT FILE CONFIGURATION
# =============================================================================

# Tab names to search for (in priority order, case-insensitive matching)
TARGET_TAB_NAMES = [
    'Global Outlook Summary',
]

# The section header we need to locate dynamically in the sheet
TARGET_SECTION_HEADER = 'Real GDP % over year ago'

# The label that marks the area/country column
AREA_COLUMN_LABEL = 'Area'

# =============================================================================
# COUNTRY MAPPING
# =============================================================================

# Maps input country/area names -> (ISO3 code, display name)
# The parser strips whitespace and does case-insensitive matching
COUNTRY_MAPPING = {
    'Argentina':        ('ARG',  'Argentina'),
    'Australia':        ('AUS',  'Australia'),
    'Brazil':           ('BRA',  'Brazil'),
    'Canada':           ('CAN',  'Canada'),
    'Chile':            ('CHL',  'Chile'),
    'China':            ('CHN',  'China'),
    'Colombia':         ('COL',  'Colombia'),
    'Czech Republic':   ('CZE',  'Czech Republic'),
    'Ecuador':          ('ECU',  'Ecuador'),
    'Euro area':        ('EURA', 'Euro area'),
    'Euro Area':        ('EURA', 'Euro area'),
    'France':           ('FRA',  'France'),
    'Germany':          ('DEU',  'Germany'),
    'Hong Kong':        ('HKG',  'Hong Kong'),
    'Hungary':          ('HUN',  'Hungary'),
    'India':            ('IND',  'India'),
    'Indonesia':        ('IDN',  'Indonesia'),
    'Israel':           ('ISR',  'Israel'),
    'Italy':            ('ITA',  'Italy'),
    'Japan':            ('JPN',  'Japan'),
    'South Korea':      ('KOR',  'South Korea'),
    'Korea':            ('KOR',  'South Korea'),
    'Malaysia':         ('MYS',  'Malaysia'),
    'Mexico':           ('MEX',  'Mexico'),
    'New Zealand':      ('NZL',  'New Zealand'),
    'Peru':             ('PER',  'Peru'),
    'Philippines':      ('PHL',  'Philippines'),
    'Poland':           ('POL',  'Poland'),
    'Russia':           ('RUS',  'Russia'),
    'South Africa':     ('ZAF',  'South Africa'),
    'Singapore':        ('SGP',  'Singapore'),
    'Spain':            ('ESP',  'Spain'),
    'Taiwan':           ('TWN',  'Taiwan'),
    'Thailand':         ('THA',  'Thailand'),
    'Turkey':           ('TUR',  'Turkey'),
    'United Kingdom':   ('GBR',  'United Kingdom'),
    'United States':    ('USA',  'United States'),
}

# Region/aggregate rows to skip (not individual countries)
SKIP_AREAS = [
    'Latin America',
    'Asia/Pacific',
    'Western Europe',
    'EM Asia',
    'Ex China/India',
    'Emerging Europe',
    'Africa/Middle East',
    'Global',
    'Developed markets',
    'Emerging markets',
]

# =============================================================================
# OUTPUT COLUMN CODE CONFIGURATION
# =============================================================================

# Pattern: JPMRGDPF.REALGDP.ANNUAL.<ISO3>.A
CODE_PREFIX = 'JPMRGDPF'
DATA_CATEGORY = 'REALGDP'
DATA_FREQUENCY_CODE = 'ANNUAL'
DATA_SUFFIX = 'A'


def build_column_code(iso3_code):
    """Build output column code for a country."""
    return f'{CODE_PREFIX}.{DATA_CATEGORY}.{DATA_FREQUENCY_CODE}.{iso3_code}.{DATA_SUFFIX}'


def build_description(display_name):
    """Build output column description for a country."""
    return f'Real GDP: Annual: {display_name}'


# Ordered list of (iso3, display_name) for consistent column ordering
# Sorted alphabetically by display name to match your master file
COUNTRY_ORDER = [
    ('ARG', 'Argentina'),
    ('AUS', 'Australia'),
    ('BRA', 'Brazil'),
    ('CAN', 'Canada'),
    ('CHL', 'Chile'),
    ('CHN', 'China'),
    ('COL', 'Colombia'),
    ('CZE', 'Czech Republic'),
    ('ECU', 'Ecuador'),
    ('EURA', 'Euro area'),
    ('FRA', 'France'),
    ('DEU', 'Germany'),
    ('HKG', 'Hong Kong'),
    ('HUN', 'Hungary'),
    ('IND', 'India'),
    ('IDN', 'Indonesia'),
    ('ISR', 'Israel'),
    ('ITA', 'Italy'),
    ('JPN', 'Japan'),
    ('KOR', 'South Korea'),
    ('MYS', 'Malaysia'),
    ('MEX', 'Mexico'),
    ('NZL', 'New Zealand'),
    ('PER', 'Peru'),
    ('PHL', 'Philippines'),
    ('POL', 'Poland'),
    ('RUS', 'Russia'),
    ('SGP', 'Singapore'),
    ('ZAF', 'South Africa'),
    ('ESP', 'Spain'),
    ('TWN', 'Taiwan'),
    ('THA', 'Thailand'),
    ('TUR', 'Turkey'),
    ('GBR', 'United Kingdom'),
    ('USA', 'United States'),
]

# Pre-built column order: list of (code, description) tuples
COLUMN_ORDER = [
    (build_column_code(iso3), build_description(name))
    for iso3, name in COUNTRY_ORDER
]

# =============================================================================
# METADATA CONFIGURATION
# =============================================================================

METADATA_DEFAULTS = {
    'FREQUENCY': 'A',       # Annual
    'UNIT_TYPE': 'Percent',
    'DATA_TYPE': 'Level',
    'SOURCE': 'JPM',
    'CATEGORY': 'RGDP',
}

# =============================================================================
# FILE NAMING CONFIGURATION
# =============================================================================

DATA_FILE_PREFIX = 'JPMRGDPF_DATA'
META_FILE_PREFIX = 'JPMRGDPF_META'
ZIP_FILE_PREFIX = 'JPMRGDPF'
TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'
LATEST_FOLDER = 'latest'

# =============================================================================
# DATA PROCESSING CONFIGURATION
# =============================================================================

NA_VALUE = 'NA'
NA_INPUT_VALUES = ['-', '--', 'N/A', 'NA', '', None]

# =============================================================================
# LOGGING CONFIGURATION
# =============================================================================

LOG_LEVEL = 'INFO'
DEBUG_MODE = False
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# =============================================================================
# PROCESSING OPTIONS
# =============================================================================

CONTINUE_ON_ERROR = True

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def get_timestamp():
    """Get current timestamp string for file naming."""
    return datetime.now().strftime(TIMESTAMP_FORMAT)


def normalize_country_name(name):
    """
    Normalize a country/area name for matching against COUNTRY_MAPPING.

    Strips whitespace, handles common variations.
    """
    if name is None:
        return None
    name = str(name).strip()
    # Direct match
    if name in COUNTRY_MAPPING:
        return name
    # Case-insensitive match
    for key in COUNTRY_MAPPING:
        if key.lower() == name.lower():
            return key
    return name


def is_target_country(name):
    """Check if a name matches one of our 35 target countries."""
    normalized = normalize_country_name(name)
    return normalized in COUNTRY_MAPPING


def is_skip_area(name):
    """Check if a name is a region/aggregate we should skip."""
    if name is None:
        return False
    name = str(name).strip()
    for skip in SKIP_AREAS:
        if skip.lower() == name.lower():
            return True
    return False


def get_country_info(name):
    """
    Get (iso3_code, display_name) for a country name.

    Returns None if not a target country.
    """
    normalized = normalize_country_name(name)
    if normalized in COUNTRY_MAPPING:
        return COUNTRY_MAPPING[normalized]
    return None


if __name__ == '__main__':
    print('JPMRGDPF Configuration Summary')
    print('=' * 50)
    print(f'Base Directory: {BASE_DIR}')
    print(f'Input Directory: {INPUT_DIR}')
    print(f'Output Directory: {OUTPUT_DIR}')
    print(f'Master Directory: {MASTER_DIR}')
    print(f'Target Countries: {len(COUNTRY_ORDER)}')
    print(f'Output Columns: {len(COLUMN_ORDER)}')
    print()
    print('Column codes:')
    for code, desc in COLUMN_ORDER:
        print(f'  {code}  ->  {desc}')
