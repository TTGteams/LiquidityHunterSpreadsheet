import pandas as pd
import numpy as np
import pandas_ta as ta
import logging
import pyodbc
import datetime

# --------------------------------------------------------------------------
# Configure Logging (Optional)
# --------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("secondary_warmer_script_logger")

# Define supported currencies - matching the list in algorithm.py
SUPPORTED_CURRENCIES = ["EUR.USD", "USD.CAD", "GBP.USD"]

# --------------------------------------------------------------------------
# Your DB Credentials
# --------------------------------------------------------------------------
server = '192.168.50.100'
database = 'TTG'
username = 'djaime'
password = 'Enrique30072000!'

# --------------------------------------------------------------------------
# (1) Helper Functions to Replicate/Use Main Algo Logic
#    If you already have them in main algo, you can import them instead.
# --------------------------------------------------------------------------
def identify_liquidity_zones(data, current_valid_zones_dict, PIPS_REQUIRED=0.005):
    """
    Minimal replication of the main algo's logic for identifying liquidity zones.
    """
    data = data.copy()
    data['Liquidity_Zone'] = np.nan
    data['Zone_Size'] = np.nan
    data['Zone_Start_Price'] = np.nan
    data['Zone_End_Price'] = np.nan
    data['Zone_Length'] = np.nan
    data['zone_type'] = ''

    i = 0
    while i < len(data) - 1:
        start_index = i
        start_price = data['close'].iloc[start_index]
        for j in range(start_index + 1, len(data)):
            end_price = data['close'].iloc[j]
            price_movement = abs(end_price - start_price)
            if price_movement >= PIPS_REQUIRED:
                zone_length = j - start_index
                zone_type = 'demand' if end_price > start_price else 'supply'
                zone_id = (start_price, end_price)
                if zone_id not in current_valid_zones_dict:
                    current_valid_zones_dict[zone_id] = {
                        'start_price': start_price,
                        'end_price': end_price,
                        'zone_size': price_movement,
                        'loss_count': 0,
                        'zone_type': zone_type
                    }
                data.loc[data.index[start_index:j+1], 'Liquidity_Zone'] = start_price
                data.loc[data.index[start_index:j+1], 'Zone_Size'] = price_movement
                data.loc[data.index[start_index:j+1], 'Zone_Start_Price'] = start_price
                data.loc[data.index[start_index:j+1], 'Zone_End_Price'] = end_price
                data.loc[data.index[start_index:j+1], 'Zone_Length'] = zone_length
                data.loc[data.index[start_index:j+1], 'zone_type'] = zone_type
                i = j
                break
        i += 1

    logger.debug(f"Completed identify_liquidity_zones. Found {len(current_valid_zones_dict)} zones so far.")
    return data, current_valid_zones_dict


def set_support_resistance_lines(data, SUPPORT_RESISTANCE_ALLOWANCE=0.0011):
    data = data.copy()
    data['Support_Line'] = np.where(
        data['zone_type'] == 'demand',
        data['Liquidity_Zone'] - SUPPORT_RESISTANCE_ALLOWANCE,
        np.nan
    )
    data['Resistance_Line'] = np.where(
        data['zone_type'] == 'supply',
        data['Liquidity_Zone'] + SUPPORT_RESISTANCE_ALLOWANCE,
        np.nan
    )
    return data


def invalidate_zones_via_sup_and_resist(data, current_valid_zones_dict, SUPPORT_RESISTANCE_ALLOWANCE=0.0011):
    """
    A simplified single-pass invalidation, checking if the last row's close
    violates the support/resistance lines by more than the allowance.
    """
    if data.empty:
        return current_valid_zones_dict

    support_violation = data['close'] < (data['Support_Line'] - SUPPORT_RESISTANCE_ALLOWANCE)
    resistance_violation = data['close'] > (data['Resistance_Line'] + SUPPORT_RESISTANCE_ALLOWANCE)
    invalid_zones = support_violation | resistance_violation

    zones_to_invalidate = data.loc[invalid_zones, ['Zone_Start_Price', 'Zone_End_Price']].dropna()
    for _, row in zones_to_invalidate.iterrows():
        zone_id = (row['Zone_Start_Price'], row['Zone_End_Price'])
        if zone_id in current_valid_zones_dict:
            del current_valid_zones_dict[zone_id]

    return current_valid_zones_dict


# --------------------------------------------------------------------------
# (2) DB Connection & Data Fetch
# --------------------------------------------------------------------------
def fetch_data_from_sql(currency_pair="EUR.USD", days=3):
    """
    Fetch historical tick data from SQL server for a specific currency pair
    
    Args:
        currency_pair (str): The currency pair to fetch data for
        days (int): Number of days of historical data to fetch
        
    Returns:
        DataFrame: Historical tick data for the specified currency pair
    """
    try:
        conn_str = (
            f"Driver={{ODBC Driver 17 for SQL Server}};"
            f"Server={server};"
            f"Database={database};"
            f"UID={username};"
            f"PWD={password};"
        )
        
        # Calculate the start date (N days ago)
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=days)
        
        # Format dates for SQL query
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # Connect to the database
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Modified SQL query to filter by currency pair
        query = f"""
        SELECT [Time], [Price]
        FROM TickData
        WHERE [Time] >= '{start_date_str}'
        AND [Time] <= '{end_date_str}'
        AND [Currency] = '{currency_pair}'
        ORDER BY [Time] ASC;
        """
        
        # If there's no Currency column in the TickData table, use this fallback query
        # that assumes all data is for EUR.USD (for backward compatibility)
        fallback_query = f"""
        SELECT [Time], [Price]
        FROM TickData
        WHERE [Time] >= '{start_date_str}' 
        AND [Time] <= '{end_date_str}'
        ORDER BY [Time] ASC;
        """
        
        logger.info(f"Fetching historical data for {currency_pair} from {start_date_str} to {end_date_str}")
        
        try:
            # Try the query with currency filter
            df = pd.read_sql(query, conn)
            if len(df) == 0 and currency_pair == "EUR.USD":
                # If no data found and we're looking for EUR.USD, try the fallback query
                logger.warning(f"No data found for {currency_pair} with currency filter, trying fallback query")
                df = pd.read_sql(fallback_query, conn)
        except Exception as e:
            # If the query fails (e.g., no Currency column), use the fallback if requesting EUR.USD
            if currency_pair == "EUR.USD":
                logger.warning(f"Error with currency filter query: {e}. Using fallback query.")
                df = pd.read_sql(fallback_query, conn)
            else:
                # For other currencies, we really need the Currency column
                logger.error(f"Cannot fetch data for {currency_pair}: {e}")
                raise
        
        # Set Time column as the index
        df['Time'] = pd.to_datetime(df['Time'])
        df.set_index('Time', inplace=True)
        
        # Log results
        logger.info(f"Fetched {len(df)} ticks for {currency_pair}")
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching data from SQL: {e}", exc_info=True)
        return pd.DataFrame()  # Return empty DataFrame on error


# --------------------------------------------------------------------------
# (3) Resample & Precompute Logic
# --------------------------------------------------------------------------
def process_currency_data(currency_pair="EUR.USD"):
    """
    Process data for a specific currency pair.
    
    Args:
        currency_pair (str): The currency pair to process
        
    Returns:
        tuple: (bars_15min, zones_dict) for the specified currency
    """
    logger.info(f"Processing data for {currency_pair}")
    
    try:
        # Step 1: Fetch raw tick data
        raw_ticks = fetch_data_from_sql(currency_pair)
        
        if raw_ticks.empty:
            logger.warning(f"No tick data available for {currency_pair}")
            return pd.DataFrame(), {}
            
        # Step 2: Convert ticks to 15-minute bars
        # Floor timestamps to 15-minute boundaries
        raw_ticks.index = raw_ticks.index.floor('15min')
        
        # Create OHLC bars
        bars_15min = raw_ticks.groupby(level=0).agg({
            'Price': ['first', 'max', 'min', 'last']
        })
        
        # Flatten columns
        bars_15min.columns = ['open', 'high', 'low', 'close']
        
        # Step 3: Calculate indicators
        if len(bars_15min) >= 35:  # Need enough data for indicators
            # Calculate RSI (14-period)
            bars_15min['RSI'] = ta.rsi(bars_15min['close'], length=14)
            
            # Calculate MACD (12, 26, 9)
            macd_result = ta.macd(bars_15min['close'], fast=12, slow=26, signal=9)
            if macd_result is not None:
                bars_15min['MACD_Line'] = macd_result['MACD_12_26_9']
                bars_15min['Signal_Line'] = macd_result['MACDs_12_26_9']
                bars_15min['MACD_Histogram'] = macd_result['MACDh_12_26_9']
            
            logger.info(f"Calculated indicators for {currency_pair}: {len(bars_15min)} bars")
        else:
            logger.warning(f"Not enough data to calculate indicators for {currency_pair}: {len(bars_15min)} bars")
        
        # Step 4: Identify liquidity zones
        # This would typically call the same zone identification logic as in the main algorithm
        # For this update, we'll return an empty dict and let the main algorithm identify zones
        current_valid_zones_dict = {}
        
        # If there's existing zone identification logic, it would go here
        # current_valid_zones_dict = identify_liquidity_zones(bars_15min)
        
        logger.info(f"Completed processing for {currency_pair}: {len(bars_15min)} bars, {len(current_valid_zones_dict)} zones")
        return bars_15min, current_valid_zones_dict
        
    except Exception as e:
        logger.error(f"Error processing data for {currency_pair}: {e}", exc_info=True)
        return pd.DataFrame(), {}  # Return empty data on error


def warmup_data(currency=None):
    """
    Provides precomputed bars and zones for all supported currencies or a specific currency.
    
    Args:
        currency (str, optional): Specific currency to get data for. If None, processes all currencies.
        
    Returns:
        If currency is specified:
            tuple: (precomputed_bars, precomputed_zones) for the specified currency
        If currency is None:
            tuple: (bars_dict, zones_dict) dictionaries mapping currencies to their respective data
    """
    logger.info("Starting warmup data generation")
    
    bars_dict = {}
    zones_dict = {}
    
    # If a specific currency is requested, only process that one
    if currency is not None:
        if currency in SUPPORTED_CURRENCIES:
            logger.info(f"Processing single currency: {currency}")
            bars, zones = process_currency_data(currency)
            return bars, zones
        else:
            logger.error(f"Requested unsupported currency: {currency}")
            return pd.DataFrame(), {}
    
    # Process all supported currencies
    for currency_pair in SUPPORTED_CURRENCIES:
        try:
            logger.info(f"Processing {currency_pair}")
            bars, zones = process_currency_data(currency_pair)
            
            # Store the results in dictionaries
            bars_dict[currency_pair] = bars
            zones_dict[currency_pair] = zones
            
            logger.info(f"Completed {currency_pair}: {len(bars)} bars, {len(zones)} zones")
            
        except Exception as e:
            logger.error(f"Error processing {currency_pair}: {e}", exc_info=True)
            # Continue with next currency on error
            bars_dict[currency_pair] = pd.DataFrame()
            zones_dict[currency_pair] = {}
    
    # For backward compatibility: if only processing EUR.USD, return the direct data
    # rather than the dictionary
    if len(bars_dict) == 1 and "EUR.USD" in bars_dict:
        logger.info("Returning legacy format data (EUR.USD only)")
        return bars_dict["EUR.USD"], zones_dict["EUR.USD"]
    
    # Log summary of results
    logger.info(f"Warmup data summary:")
    for currency in bars_dict:
        logger.info(f"  {currency}: {len(bars_dict[currency])} bars, {len(zones_dict[currency])} zones")
    
    # Return dictionaries of all processed data
    return bars_dict["EUR.USD"], zones_dict["EUR.USD"]  # For backward compatibility


# Optional: If we want a direct "script" entry point
if __name__ == "__main__":
    bars, zones = warmup_data()
    print(" ")
    logger.info("Warmup data computed. This script can be imported by the main algo.")