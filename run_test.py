#!/usr/bin/env python3
"""
run_test.py - Combined Testing and Warmup Script for Algorithm

This script combines testing and warmup functionality:
1. Signal Test Mode: Sends arbitrary test signals to verify API connectivity
2. Historical Backtest: Pulls 150k historical prices and runs through algorithm
3. Warmup Mode: Prepares zones and bars for algorithm initialization

The script runs all three modes in sequence when called by the algorithm.

DATABASE NOTE:
- Historical data (HistoData table) is fetched from TTG database
- All other operations (zones, signals, etc.) use FXStrat database
"""

import requests
import time
import datetime
import logging
import json
import pandas as pd
import pandas_ta as ta
import numpy as np
import pyodbc
import sys
import concurrent.futures
import threading

# Test configuration - Define early so logging can reference these
API_URL = "http://localhost:5000/trade_signal"
TEST_PRICE = 1.000
TEST_CURRENCY = "EUR.USD"
SIGNAL_DELAY = 0.5  # 0.5 seconds between signals
PATTERN_REPEATS = 3  # Repeat the pattern 3 times

# Database configuration (same as secondary_warmer_script.py)
DB_CONFIG = {
    'server': '192.168.50.100',
    'database': 'FXStrat',  # Changed from 'TTG' to 'FXStrat'
    'username': 'djaime',
    'password': 'Enrique30072000!2',
    'driver': 'ODBC Driver 17 for SQL Server'
}

# Define supported currencies (matching algorithm.py)
SUPPORTED_CURRENCIES = ["EUR.USD", "USD.CAD", "GBP.USD"]

# Configure logging with file output and clean formatting to match terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',  # Clean format without timestamps or log levels
    handlers=[
        logging.FileHandler('run_test.log', mode='w'),  # Overwrite mode for fresh logs
        logging.StreamHandler()
    ],
    force=True  # Force reconfiguration if already configured
)
logger = logging.getLogger("run_test")

# Ensure immediate flushing to prevent blank log files
logging.getLogger().handlers[0].flush()

# Log startup information including API endpoints
logger.info("="*80)
logger.info("RUN_TEST SCRIPT STARTUP - API ENDPOINT CONFIGURATION")
logger.info("="*80)
logger.info(f"Primary API Endpoint: {API_URL}")
logger.info(f"Batch API Endpoint: http://localhost:5000/trade_signal_batch")
logger.info(f"Signal Forward Endpoint: http://localhost:5000/trade_signal") 
logger.info("Signal Flow: Batch Processing -> Algorithm -> Forward to Primary Endpoint")
logger.info("="*80)

# Global variables to store warmup data
warmup_bars = {}
warmup_zones = {}

def get_db_connection(database_override=None):
    """
    Creates and returns a connection to the SQL Server database.
    
    Args:
        database_override (str, optional): Override the default database. If None, uses DB_CONFIG['database']
    """
    # Use override database if provided, otherwise use default
    database_name = database_override if database_override else DB_CONFIG['database']
    
    conn_str = (
        f"Driver={{{DB_CONFIG['driver']}}};"
        f"Server={DB_CONFIG['server']};"
        f"Database={database_name};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
    )
    
    try:
        conn = pyodbc.connect(conn_str)
        logger.info(f"Successfully connected to {database_name} on {DB_CONFIG['server']}")
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

def fetch_historical_data(currency_pair="EUR.USD", limit=60000):
    """
    Fetch the most recent historical prices for a specific currency from the database.
    
    Args:
        currency_pair (str): The currency pair to fetch
        limit (int): Number of records to fetch (60k for good historical coverage)
        
    Returns:
        DataFrame: Historical price data with Time and Price columns
    """
    conn = None
    try:
        # Use TTG database for historical data
        conn = get_db_connection(database_override="TTG")
        if conn is None:
            return pd.DataFrame()
        
        # Query to get the most recent prices in chronological order
        query = f"""
        SELECT [BarDateTime], [Close]
        FROM (
            SELECT TOP {limit} [BarDateTime], [Close]
            FROM HistoData 
            WHERE [Identifier] = '{currency_pair}'
            ORDER BY [BarDateTime] DESC
        ) AS recent_data
        ORDER BY [BarDateTime] ASC
        """
        
        logger.info(f"Fetching {limit:,} most recent {currency_pair} prices from TTG database...")
        
        # Execute query and load into DataFrame
        df = pd.read_sql(query, conn)
        
        # Rename columns to maintain compatibility with existing code
        df = df.rename(columns={'BarDateTime': 'Time', 'Close': 'Price'})
        
        # Convert Time column to datetime
        df['Time'] = pd.to_datetime(df['Time'])
        
        logger.info(f"Successfully fetched {len(df):,} price records for {currency_pair}")
        
        if not df.empty:
            logger.info(f"Date range: {df['Time'].min()} to {df['Time'].max()}")
            logger.info(f"Price range: {df['Price'].min():.5f} to {df['Price'].max():.5f}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching historical data: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

def send_historical_data_to_api(df, currency="EUR.USD", batch_size=300, delay_between_batches=0.02):
    """
    Send historical data to the API endpoint in batches using the batch endpoint.
    Also collects the zones that were created during backtesting.
    Now saves backtest signals to separate table.
    
    Args:
        df (DataFrame): Historical data to send
        currency (str): The currency pair
        batch_size (int): Number of records to send in each batch (optimized to 400)
        delay_between_batches (float): Delay in seconds between batches (optimized to 0.002)
        
    Returns:
        tuple: (successful_count, failed_count, signals_generated)
    """
    successful_count = 0
    failed_count = 0
    signals_generated = {'buy': 0, 'sell': 0, 'close': 0, 'hold': 0}
    
    # Generate backtest datetime for Mode 2
    backtest_datetime = datetime.datetime.now()
    
    total_records = len(df)
    total_batches = (total_records + batch_size - 1) // batch_size
    
    # Use the batch endpoint
    batch_api_url = "http://localhost:5000/trade_signal_batch"
    
    logger.info(f"Sending {total_records:,} records in {total_batches} batches of {batch_size}")
    print(f"Sending {total_records:,} records in {total_batches} batches of {batch_size}")
    logger.info("Using batch endpoint for efficient processing...")
    print("Using batch endpoint for efficient processing...")
    logger.info(f"Backtest DateTime: {backtest_datetime}")
    
    start_time = time.time()
    signals_buffer = []  # Buffer for batch saving
    
    for batch_num in range(total_batches):
        start_idx = batch_num * batch_size
        end_idx = min(start_idx + batch_size, total_records)
        batch_df = df.iloc[start_idx:end_idx]
        
        # Show progress every 5 batches (optimized for larger batch sizes)
        if batch_num % 5 == 0:
            progress = (batch_num / total_batches) * 100
            elapsed = time.time() - start_time
            eta = (elapsed / (batch_num + 1)) * (total_batches - batch_num - 1)
            progress_msg = f"{currency} Progress: {progress:.1f}% - Batch {batch_num+1}/{total_batches} - ETA: {eta:.1f}s"
            print(progress_msg)
            logger.info(progress_msg)
            
            # Periodic flush every 10 batches to prevent blank log files
            if batch_num % 10 == 0:
                logging.getLogger().handlers[0].flush()
        
        # Prepare batch data
        batch_data = []
        for idx, row in batch_df.iterrows():
            batch_data.append({
                "Time": row['Time'].strftime('%Y-%m-%d %H:%M:%S'),
                "Price": float(row['Price'])
            })
        
        # Send entire batch in one request
        payload = {
            "data": batch_data,
            "currency": currency
        }
        
        try:
            response = requests.post(batch_api_url, json=payload, timeout=60)  # Increased timeout for batch
            
            if response.status_code == 200:
                resp_json = response.json()
                summary = resp_json.get('summary', {})
                
                # Update counts
                successful_count += summary.get('successful', 0)
                failed_count += summary.get('failed', 0)
                
                # Update signal counts
                batch_signals = summary.get('signals_count', {})
                for signal_type, count in batch_signals.items():
                    signals_generated[signal_type] = signals_generated.get(signal_type, 0) + count
                
                # Collect non-hold signals for batch saving
                results = resp_json.get('results', [])
                for result in results:
                    if 'signal' in result and result['signal'] != 'hold':
                        signals_buffer.append({
                            'signal_type': result['signal'],
                            'price': result['price'],
                            'signal_time': result['time'],
                            'currency': currency,
                            'backtest_datetime': backtest_datetime
                        })
                
                # Save when buffer reaches 50 signals
                if len(signals_buffer) >= 50:
                    save_backtest_signals_batch(signals_buffer)
                    signals_buffer = []  # Clear buffer
                
                # Log significant signals (reduce duplicate logging)
                non_hold_signals = [r for r in results if 'signal' in r and r['signal'] != 'hold']
                
                # Log summary instead of individual signals to reduce duplicate logging
                if len(non_hold_signals) > 0:
                    # Only log first signal and count for this batch
                    first_signal = non_hold_signals[0]
                    if len(non_hold_signals) == 1:
                        logger.warning(
                            f"BACKTEST SIGNAL: {first_signal['signal'].upper()} "
                            f"at {first_signal['price']:.5f} ({first_signal['time']}) [DateTime: {backtest_datetime}]"
                        )
                    else:
                        logger.warning(f"Batch {batch_num+1}: {len(non_hold_signals)} signals generated [DateTime: {backtest_datetime}]")
            else:
                # If batch fails, count all as failed
                failed_count += len(batch_data)
                logger.error(f"Batch {batch_num} failed with status {response.status_code}")
                
        except Exception as e:
            failed_count += len(batch_data)
            if failed_count <= 5:  # Only log first 5 errors to avoid spam
                logger.error(f"Error sending batch: {e}")
        
        # Delay between batches to avoid overwhelming the API
        if batch_num < total_batches - 1:
            time.sleep(delay_between_batches)
    
    elapsed_time = time.time() - start_time
    
    # Save any remaining signals in buffer
    if signals_buffer:
        save_backtest_signals_batch(signals_buffer)
    
    # Final flush
    logging.getLogger().handlers[0].flush()
    
    logger.info(f"Completed in {elapsed_time:.1f} seconds")
    logger.info(f"Backtest signals saved to table with DateTime: {backtest_datetime}")
    print(f"Completed in {elapsed_time:.1f} seconds")
    print(f"Backtest signals saved to table with DateTime: {backtest_datetime}")
    
    return successful_count, failed_count, signals_generated

def send_test_signal(signal_type, price, currency, timestamp):
    """
    Send a test signal to the API endpoint in the exact format expected.
    
    Args:
        signal_type (str): The signal type ('buy', 'sell', 'close', 'hold')
        price (float): The price value
        currency (str): The currency pair
        timestamp (str): The timestamp string
        
    Returns:
        tuple: (success, response_data)
    """
    # Create payload in the exact format that server.py expects
    payload = {
        "data": {
            "Time": timestamp,
            "Price": price
        },
        "currency": currency
    }
    
    try:
        response = requests.post(API_URL, json=payload, timeout=10)
        
        if response.status_code == 200:
            resp_json = response.json()
            received_signal = resp_json.get('signal', 'unknown')
            received_currency = resp_json.get('currency', currency)
            return True, resp_json
        else:
            return False, None
            
    except requests.exceptions.ConnectionError:
        return False, None
    except requests.exceptions.Timeout:
        return False, None
    except Exception as e:
        return False, None

def run_signal_pattern():
    """
    Execute the complete signal pattern:
    - 10 hold signals
    - 1 buy signal
    - 1 close signal  
    - 10 hold signals
    - 1 sell signal
    - 1 close signal
    """
    pattern_signals = []
    
    # Build the pattern
    # 10 hold signals
    for i in range(10):
        pattern_signals.append('hold')
    
    # 1 buy signal
    pattern_signals.append('buy')
    
    # 1 close signal
    pattern_signals.append('close')
    
    # 10 more hold signals
    for i in range(10):
        pattern_signals.append('hold')
    
    # 1 sell signal
    pattern_signals.append('sell')
    
    # 1 close signal
    pattern_signals.append('close')
    
    return pattern_signals

def print_compressed_signals(signals, successful_count, failed_count):
    """
    Print signals in compressed format (e.g., "hold x10" instead of 10 individual holds)
    """
    i = 0
    while i < len(signals):
        current_signal = signals[i]
        count = 1
        
        # Count consecutive identical signals
        while i + count < len(signals) and signals[i + count] == current_signal:
            count += 1
        
        # Print the signal with count if > 1
        if count > 1:
            print(f"Sent: {current_signal} x{count}")
        else:
            print(f"Sent: {current_signal}")
        
        i += count

def run_mode_1_signal_test():
    """
    Mode 1: Send arbitrary test signals to verify API connectivity
    Note: Signals are NOT saved to database as they are arbitrary test patterns
    """
    print("="*60)
    print("MODE 1: SIGNAL TEST - STARTING API TEST SEQUENCE")
    print("="*60)
    print(f"Target API: {API_URL}")
    print(f"Test Price: {TEST_PRICE}")
    print(f"Test Currency: {TEST_CURRENCY}")
    print(f"Signal Delay: {SIGNAL_DELAY} seconds")
    print(f"Pattern Repeats: {PATTERN_REPEATS}")
    print("Note: Test signals will NOT be saved to database (arbitrary patterns)")
    print("")
    
    # Log endpoint information for this mode
    logger.info("MODE 1: API connectivity test using /trade_signal endpoint")
    logger.info("Mode 1 signals are NOT saved to database (arbitrary test patterns)")
    
    print("Testing API connectivity...")
    
    # Test connectivity
    test_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if not send_test_signal('hold', TEST_PRICE, TEST_CURRENCY, test_timestamp)[0]:
        print("ERROR: Could not connect to API")
        return False
    
    print("API connectivity confirmed")
    print("")
    
    pattern = run_signal_pattern()
    successful_signals = 0
    failed_signals = 0
    all_sent_signals = []
    
    # Execute the pattern 3 times
    for repeat in range(PATTERN_REPEATS):
        for signal_type in pattern:
            current_time = datetime.datetime.now()
            timestamp_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Send the signal to API (for connectivity testing only)
            success = send_test_signal(signal_type, TEST_PRICE, TEST_CURRENCY, timestamp_str)[0]
            
            if success:
                all_sent_signals.append(signal_type)
                successful_signals += 1
                # Note: No database saving for Mode 1 signals
            else:
                failed_signals += 1
            
            time.sleep(SIGNAL_DELAY)
    
    # Print compressed signals
    print_compressed_signals(all_sent_signals, successful_signals, failed_signals)
    
    # Print summary
    print("")
    print("="*60)
    print("MODE 1 COMPLETED")
    print("="*60)
    total_signals = successful_signals + failed_signals
    print(f"Total signals sent: {total_signals}")
    print(f"Successful: {successful_signals}")
    print(f"Failed: {failed_signals}")
    print("Note: Test signals were not saved to database")
    
    return failed_signals == 0

def process_single_currency(currency):
    """
    Process historical data for a single currency. 
    This function is designed to be called in parallel for multiple currencies.
    
    Args:
        currency (str): The currency pair to process
        
    Returns:
        dict: Results containing success/failure counts, signals, and processing info
    """
    try:
        print(f"\n{'='*60}")
        print(f"Processing {currency}")
        print(f"{'='*60}")
        
        # Fetch historical data for this currency
        historical_data = fetch_historical_data(currency, limit=60000)
        
        if historical_data.empty:
            logger.error(f"No historical data retrieved for {currency}")
            return {
                'currency': currency,
                'success': False,
                'successful': 0,
                'failed': 0,
                'signals': {'buy': 0, 'sell': 0, 'close': 0, 'hold': 0},
                'error': 'No historical data retrieved'
            }
        
        print(f"Retrieved {len(historical_data):,} historical prices")
        print(f"Date range: {historical_data['Time'].min()} to {historical_data['Time'].max()}")
        print("")
        
        # Send data to algorithm via API
        print(f"Feeding {currency} historical data to algorithm...")
        print("Trade signals will be saved to database as per normal operation")
        print("")
        
        # PARALLEL OPTIMIZED: Optimized batches to prevent server overload
        successful, failed, signals = send_historical_data_to_api(
            historical_data, 
            currency=currency,
            batch_size=300,  # Reduced from 400 to prevent server overload
            delay_between_batches=0.02  # Increased from 0.01 to give server more time
        )
        
        # Currency-specific summary
        print(f"\n{currency} Summary:")
        print(f"  Records processed: {successful + failed:,}")
        print(f"  Successful: {successful:,}")
        print(f"  Failed: {failed:,}")
        
        currency_total_signals = sum(v for k, v in signals.items() if k != 'hold')
        if currency_total_signals > 0:
            print(f"  Signals generated: {currency_total_signals}")
            for signal_type, count in signals.items():
                if count > 0 and signal_type != 'hold':
                    print(f"    {signal_type.upper()}: {count}")
        
        return {
            'currency': currency,
            'success': True,
            'successful': successful,
            'failed': failed,
            'signals': signals
        }
        
    except Exception as e:
        logger.error(f"Error processing {currency}: {e}", exc_info=True)
        return {
            'currency': currency,
            'success': False,
            'successful': 0,
            'failed': 0,
            'signals': {'buy': 0, 'sell': 0, 'close': 0, 'hold': 0},
            'error': str(e)
        }

def run_mode_2_historical_test():
    """
    Mode 2: Pull historical data from DB and feed to algorithm for all supported currencies
    OPTIMIZED: Processes all currencies in parallel for significantly faster performance
    """
    print("\n\n")
    print("="*60)
    print("MODE 2: HISTORICAL TEST - FEEDING DB DATA TO ALGORITHM")
    print("="*60)
    print("PERFORMANCE OPTIMIZED: Processing all currencies in parallel...")
    print("")
    
    # Log endpoint information for this mode
    logger.info("MODE 2: Historical backtest using /trade_signal_batch endpoint -> signals forwarded to /trade_signal")
    
    overall_success = True
    total_successful = 0
    total_failed = 0
    total_signals = {'buy': 0, 'sell': 0, 'close': 0, 'hold': 0}
    
    # Process all currencies in parallel using ThreadPoolExecutor
    print(f"Starting parallel processing of {len(SUPPORTED_CURRENCIES)} currencies...")
    
    try:
        # Using 3 workers to process all currencies truly in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            # Submit all currency processing tasks
            future_to_currency = {
                executor.submit(process_single_currency, currency): currency 
                for currency in SUPPORTED_CURRENCIES
            }
            
            # Collect results as they complete
            results = []
            for future in concurrent.futures.as_completed(future_to_currency):
                currency = future_to_currency[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Check for processing errors
                    if not result['success']:
                        overall_success = False
                        if 'error' in result:
                            logger.error(f"Failed to process {currency}: {result['error']}")
                            
                except Exception as e:
                    logger.error(f"Exception occurred while processing {currency}: {e}", exc_info=True)
                    overall_success = False
                    # Add error result for aggregation
                    results.append({
                        'currency': currency,
                        'success': False,
                        'successful': 0,
                        'failed': 0,
                        'signals': {'buy': 0, 'sell': 0, 'close': 0, 'hold': 0},
                        'error': str(e)
                    })
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Stopping parallel processing...")
        logger.warning("Mode 2 interrupted by user")
        return False
    
    # Aggregate results from all currencies
    for result in results:
        total_successful += result['successful']
        total_failed += result['failed']
        for signal_type, count in result['signals'].items():
            total_signals[signal_type] += count
    
    # Overall summary (same format as before)
    print("")
    print("="*60)
    print("MODE 2 COMPLETED - ALL CURRENCIES")
    print("="*60)
    print(f"Total records processed: {total_successful + total_failed:,}")
    print(f"Total successful: {total_successful:,}")
    print(f"Total failed: {total_failed:,}")
    print("\nTotal signals generated across all currencies:")
    for signal_type, count in total_signals.items():
        if count > 0:
            print(f"  {signal_type.upper()}: {count}")
    
    # Calculate overall signal rate
    total_non_hold_signals = sum(v for k, v in total_signals.items() if k != 'hold')
    signal_rate = (total_non_hold_signals / total_successful) * 100 if total_successful > 0 else 0
    print(f"\nOverall signal generation rate: {signal_rate:.2f}% (excluding holds)")
    
    return total_failed == 0 and overall_success

def run_mode_3_warmup():
    """
    Mode 3: Prepare warmup data (bars and zones) for all supported currencies
    Now retrieves data directly from the algorithm's current state via API
    """
    global warmup_bars, warmup_zones
    
    print("\n\n")
    print("="*60)
    print("MODE 3: WARMUP - PREPARING DATA FOR ALGORITHM")
    print("="*60)
    
    # Get algorithm state from API instead of database
    try:
        response = requests.get("http://localhost:5000/algorithm_state", timeout=10)
        
        if response.status_code != 200:
            logger.error(f"Failed to get algorithm state: HTTP {response.status_code}")
            # Initialize empty data on failure
            warmup_bars = {curr: pd.DataFrame() for curr in SUPPORTED_CURRENCIES}
            warmup_zones = {curr: {} for curr in SUPPORTED_CURRENCIES}
            return False
        
        state_data = response.json()
        currencies_data = state_data.get('currencies', {})
        
        # Process each currency's data
        for currency in SUPPORTED_CURRENCIES:
            if currency not in currencies_data:
                logger.warning(f"No state data for {currency}")
                warmup_bars[currency] = pd.DataFrame()
                warmup_zones[currency] = {}
                continue
            
            currency_state = currencies_data[currency]
            
            # Process zones - convert string keys back to tuples
            zones = {}
            for zone_key_str, zone_data in currency_state.get('zones', {}).items():
                # Parse the key format: "start_end"
                if '_' in zone_key_str:
                    parts = zone_key_str.split('_')
                    if len(parts) == 2:
                        try:
                            zone_id = (float(parts[0]), float(parts[1]))
                            zones[zone_id] = zone_data
                        except ValueError:
                            logger.warning(f"Invalid zone key format: {zone_key_str}")
            
            warmup_zones[currency] = zones
            
            # Process bars
            bars_list = currency_state.get('bars', [])
            if bars_list:
                # Convert to DataFrame
                df = pd.DataFrame(bars_list)
                df['time'] = pd.to_datetime(df['time'])
                df.set_index('time', inplace=True)
                
                # Ensure columns are in the expected order
                df = df[['open', 'high', 'low', 'close'] + 
                       [col for col in df.columns if col not in ['open', 'high', 'low', 'close']]]
                
                warmup_bars[currency] = df
            else:
                warmup_bars[currency] = pd.DataFrame()
            
            logger.info(f"Retrieved {currency} state: {len(warmup_bars[currency])} bars, {len(warmup_zones[currency])} zones")
    
    except Exception as e:
        logger.error(f"Error retrieving algorithm state: {e}", exc_info=True)
        # Initialize empty data on error
        warmup_bars = {curr: pd.DataFrame() for curr in SUPPORTED_CURRENCIES}
        warmup_zones = {curr: {} for curr in SUPPORTED_CURRENCIES}
        return False
    
    # Summary
    print("\nWarmup data prepared from algorithm state:")
    for currency in SUPPORTED_CURRENCIES:
        bar_count = len(warmup_bars[currency]) if currency in warmup_bars else 0
        zone_count = len(warmup_zones[currency]) if currency in warmup_zones else 0
        print(f"  {currency}: {bar_count} bars, {zone_count} zones")
    
    print("")
    print("="*60)
    print("MODE 3 COMPLETED")
    print("="*60)
    
    return True

def warmup_data(currency=None, skip_mode2=False, skip_mode3=False):
    """
    Main entry point called by the algorithm.
    Runs all three modes and returns warmup data.
    
    Args:
        currency (str, optional): Specific currency to get data for. If None, processes all currencies.
        skip_mode2 (bool): If True, skip Mode 2 (Historical Backtest)
        skip_mode3 (bool): If True, skip Mode 3 (Warmup Preparation)
        
    Returns:
        If currency is specified:
            tuple: (precomputed_bars, precomputed_zones) for the specified currency
        If currency is None:
            tuple: (bars_dict, zones_dict) dictionaries mapping currencies to their respective data
    """
    global warmup_bars, warmup_zones
    
    try:
        # Determine which modes to run
        modes_to_run = []
        if not False:  # Mode 1 always runs (can't skip API connectivity test)
            modes_to_run.append("Mode 1 (Signal Test)")
        if not skip_mode2:
            modes_to_run.append("Mode 2 (Historical Test)")
        if not skip_mode3:
            modes_to_run.append("Mode 3 (Warmup)")
        
        # Display startup message
        print("\n" + "="*80)
        print("STARTING COMBINED TEST AND WARMUP SEQUENCE")
        if skip_mode2 or skip_mode3:
            skipped_modes = []
            if skip_mode2:
                skipped_modes.append("Mode 2 (Historical)")
            if skip_mode3:
                skipped_modes.append("Mode 3 (Warmup)")
            print(f"SKIPPING: {', '.join(skipped_modes)}")
        print(f"RUNNING: {', '.join(modes_to_run)}")
        print("="*80 + "\n")
        
        # Mode 1: Signal Test (always runs)
        mode1_success = run_mode_1_signal_test()
        if not mode1_success:
            logger.error("Mode 1 failed - API connectivity issues")
            # Continue anyway for warmup
        
        # Mode 2: Historical Test (conditional)
        mode2_success = True  # Default to success if skipped
        if not skip_mode2:
            print("\nStarting Mode 2...")
            mode2_success = run_mode_2_historical_test()
            if not mode2_success:
                logger.warning("Mode 2 had some failures but continuing")
        else:
            print("\nSkipping Mode 2 (Historical Test) as requested")
            logger.info("MODE 2: Skipped by server request")
        
        # Mode 3: Warmup (conditional)
        mode3_success = True  # Default to success if skipped
        if not skip_mode3:
            print("\nStarting Mode 3...")
            logger.info("MODE 3: Warmup data preparation from database")
            mode3_success = run_mode_3_warmup()
        else:
            print("\nSkipping Mode 3 (Warmup) as requested")
            logger.info("MODE 3: Skipped by server request")
            # Initialize empty warmup data if mode 3 is skipped
            warmup_bars = {curr: pd.DataFrame() for curr in SUPPORTED_CURRENCIES}
            warmup_zones = {curr: {} for curr in SUPPORTED_CURRENCIES}
        
        # Overall summary
        print("\n\n")
        print("="*80)
        print("ALL MODES COMPLETED")
        print("="*80)
        print(f"Mode 1 (Signal Test): {'PASSED' if mode1_success else 'FAILED'}")
        if not skip_mode2:
            print(f"Mode 2 (Historical Test): {'PASSED' if mode2_success else 'PARTIAL'}")
        else:
            print("Mode 2 (Historical Test): SKIPPED")
        if not skip_mode3:
            print(f"Mode 3 (Warmup): {'PASSED' if mode3_success else 'FAILED'}")
        else:
            print("Mode 3 (Warmup): SKIPPED")

        # Final flush before returning
        logging.getLogger().handlers[0].flush()
        
        # Return warmup data in the format expected by the algorithm
        if currency is not None:
            # Return data for specific currency
            bars = warmup_bars.get(currency, pd.DataFrame())
            zones = warmup_zones.get(currency, {})
            return bars, zones
        else:
            # Return all currencies
            return warmup_bars, warmup_zones
            
    except Exception as e:
        logger.error(f"Fatal error in warmup_data: {e}", exc_info=True)
        # Final flush on error
        logging.getLogger().handlers[0].flush()
        # Return empty structures on error
        if currency is not None:
            return pd.DataFrame(), {}
        else:
            return {curr: pd.DataFrame() for curr in SUPPORTED_CURRENCIES}, {curr: {} for curr in SUPPORTED_CURRENCIES}

def main():
    """
    Main execution function when run standalone
    """
    # When run standalone, execute all modes and don't return warmup data
    warmup_data()
    return True

# NEW FUNCTION: Save backtest signals to separate table
def save_backtest_signals_batch(signals_batch):
    """
    Save multiple backtest signals in a single database transaction.
    """
    if not signals_batch:
        return 0
        
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return 0
        
        cursor = conn.cursor()
        
        # Prepare batch data (exclude hold signals)
        batch_data = [
            (s['signal_time'], s['signal_type'], round(s['price'], 5), 
             s['currency'], s['backtest_datetime'], 1)
            for s in signals_batch 
            if s['signal_type'] != 'hold'
        ]
        
        if batch_data:
            cursor.executemany("""
            INSERT INTO FXStrat_BackTestSignals 
            (SignalTime, SignalType, Price, Currency, BackTestDateTime, IsBacktest)
            VALUES (?, ?, ?, ?, ?, ?)
            """, batch_data)
            
            conn.commit()
            
    except Exception as e:
        logger.error(f"Error saving batch of {len(signals_batch)} signals: {e}")
        if conn:
            conn.rollback()
                
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
                
    return len(batch_data) if batch_data else 0

def save_backtest_signal(signal_type, price, signal_time, currency, backtest_datetime):
    """
    Save a backtest signal to the FXStrat_BackTestSignals table.
    Only used for Mode 2 (historical backtest) signals.
    
    Args:
        signal_type (str): The signal type ('buy', 'sell', 'close', 'hold')
        price (float): The price value
        signal_time (str): The timestamp string
        currency (str): The currency pair
        backtest_datetime (datetime): Date and time when the backtest was run
        
    Returns:
        bool: True if successful, False otherwise
    """
    if signal_type == 'hold':
        return True  # Don't save hold signals
        
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO FXStrat_BackTestSignals 
        (SignalTime, SignalType, Price, Currency, BackTestDateTime, IsBacktest)
        VALUES (?, ?, ?, ?, ?, 1)
        """, 
        signal_time,
        signal_type,
        round(price, 5),
        currency,
        backtest_datetime
        )
        
        conn.commit()
        return True
    
    except Exception as e:
        logger.error(f"Error saving backtest signal: {e}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        return False
    
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass

if __name__ == "__main__":
    try:
        # Check command line arguments for mode selection
        if len(sys.argv) > 1:
            mode = sys.argv[1].lower()
            if mode == "1" or mode == "signal":
                success = run_mode_1_signal_test()
            elif mode == "2" or mode == "historical":
                success = run_mode_2_historical_test()
            elif mode == "3" or mode == "warmup":
                success = run_mode_3_warmup()
            else:
                print("Usage: python run_test.py [1|2|3|signal|historical|warmup]")
                print("  1 or signal: Run signal test mode only")
                print("  2 or historical: Run historical test mode only")
                print("  3 or warmup: Run warmup mode only")
                print("  (no argument): Run all modes")
                success = False
        else:
            # No argument - run all modes
            success = main()
        
        exit_code = 0 if success else 1
        exit(exit_code)
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit(1) 