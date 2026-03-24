"""
JPMRGDPF Orchestrator
=====================
Main entry point for the JPMRGDPF runbook pipeline.

Pipeline:
1. Scan input folder for Excel files
2. Load and dynamically parse "Real GDP % over year ago" data
3. Compare with master - detect new/changed data
4. If changes found: merge, generate DATA + META + ZIP, update master
5. If no changes: log and exit cleanly

Usage:
    python orchestrator.py
"""

import os
import sys
import logging
from datetime import datetime

import config
from logger_setup import setup_logging
from data_loader import JPMRGDPFDataLoader
from parser import JPMRGDPFParser
from file_generator import JPMRGDPFFileGenerator

logger = logging.getLogger(__name__)


def print_banner():
    """Print welcome banner."""
    banner = """
====================================================================
                     JPMRGDPF RUNBOOK
         JPM Real GDP Forecast Data Processing
====================================================================
    """
    print(banner)


def print_configuration():
    """Print current configuration."""
    print('Configuration:')
    print(f'  Input Directory:  {config.INPUT_DIR}')
    print(f'  Output Directory: {config.OUTPUT_DIR}')
    print(f'  Master Directory: {config.MASTER_DIR}')
    print(f'  Target Section:   {config.TARGET_SECTION_HEADER}')
    print(f'  Target Countries: {len(config.COUNTRY_ORDER)}')
    print()


def main():
    """
    Main orchestration function.

    Returns:
        int: Exit code (0 for success, 1 for failure).
    """
    timestamp = config.get_timestamp()
    log_file = setup_logging(timestamp)

    print_banner()
    print_configuration()

    try:
        # =================================================================
        # STEP 1: Load Input Data
        # =================================================================
        print('STEP 1: Scanning input folder...')
        logger.info('=' * 50)
        logger.info('STEP 1: Loading input data')
        logger.info('=' * 50)

        loader = JPMRGDPFDataLoader()
        all_loaded = loader.load_all_files()

        if not all_loaded:
            logger.info('No input files found to process')
            print('No input files found in the input directory.')
            print(f'Place input files in: {config.INPUT_DIR}')
            return 0

        print(f'  Found {len(all_loaded)} input file(s):')
        for data in all_loaded:
            print(f"    - {data['file_name']} (sheet: {data['sheet_name']})")

        # Process the most recent file (first in list, already sorted newest-first)
        loaded_data = all_loaded[0]
        print(f"\n  Processing: {loaded_data['file_name']}")

        # =================================================================
        # STEP 2: Parse Data Dynamically
        # =================================================================
        print('\nSTEP 2: Parsing data dynamically...')
        logger.info('=' * 50)
        logger.info('STEP 2: Parsing data')
        logger.info('=' * 50)

        parser = JPMRGDPFParser()
        extracted = parser.extract_data(loaded_data)

        print(f"  Years detected: {extracted['years']}")
        print(f"  Countries found: {len(extracted['countries_found'])}/{len(config.COUNTRY_ORDER)}")

        if extracted['countries_missing']:
            print(f"  Missing: {', '.join(extracted['countries_missing'])}")

        # Build DataFrame from extracted data
        new_df = parser.build_dataframe(extracted)
        print(f"  Data shape: {new_df.shape[0]} rows x {new_df.shape[1]} cols")

        # =================================================================
        # STEP 3: Compare with Master
        # =================================================================
        print('\nSTEP 3: Comparing with master data...')
        logger.info('=' * 50)
        logger.info('STEP 3: Comparing with master')
        logger.info('=' * 50)

        master_df = parser.load_master_data()

        if master_df.empty:
            print('  No existing master data - all data is new')
        else:
            print(f'  Master has {len(master_df)} existing rows')

        changes = parser.check_for_changes(master_df, new_df)

        if changes['new_years']:
            print(f"  New years: {[int(y) for y in changes['new_years']]}")
            logger.info(f"New years detected: {changes['new_years']}")

        if changes['updated_years']:
            print(f"  Updated years: {[int(y) for y in changes['updated_years']]}")
            logger.info(f"Updated years: {changes['updated_years']}")

        if changes['unchanged_years']:
            print(f"  Unchanged years: {[int(y) for y in changes['unchanged_years']]}")

        # =================================================================
        # STEP 4: Generate Output (if changes found)
        # =================================================================
        if not changes['has_changes']:
            print('\n  NO NEW DATA - nothing to generate.')
            logger.info('No new data detected. Skipping file generation.')
            print('\n' + '=' * 60)
            print('COMPLETED - NO CHANGES DETECTED')
            print('=' * 60)
            return 0

        print('\nSTEP 4: Generating output files...')
        logger.info('=' * 50)
        logger.info('STEP 4: Generating output files')
        logger.info('=' * 50)

        # Merge new data into master
        combined_df = parser.merge_data(master_df, new_df)
        column_order = parser.get_column_order()

        # Generate files
        generator = JPMRGDPFFileGenerator(column_order=column_order)
        generator.timestamp = timestamp
        result = generator.generate_files(combined_df)

        # =================================================================
        # Summary
        # =================================================================
        print('\n' + '=' * 60)
        print('EXECUTION SUMMARY')
        print('=' * 60)
        print(f"\n  Input File:       {loaded_data['file_name']}")
        print(f"  Years Processed:  {extracted['years']}")
        print(f"  Countries:        {len(extracted['countries_found'])}")
        print(f"  New Years:        {changes['new_years'] or 'None'}")
        print(f"  Updated Years:    {changes['updated_years'] or 'None'}")
        print(f"  Total Rows:       {len(combined_df)}")
        print(f"\n  Files Created:")
        print(f"    DATA: {os.path.basename(result['data_file'])}")
        print(f"    META: {os.path.basename(result['meta_file'])}")
        print(f"    ZIP:  {os.path.basename(result['zip_file'])}")
        print(f"\n  Output: {result['output_dir']}")
        print(f"  Master: {result['master_file']}")
        print('\n' + '=' * 60)
        print('COMPLETED SUCCESSFULLY')
        print('=' * 60)

        logger.info('Pipeline completed successfully')
        return 0

    except KeyboardInterrupt:
        logger.warning('Execution interrupted by user')
        print('\n\nExecution interrupted by user')
        return 130

    except Exception as e:
        logger.exception(f'Pipeline failed: {e}')
        print(f'\nERROR: {e}')
        print(f'See log file for details: {log_file}')
        return 1


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
