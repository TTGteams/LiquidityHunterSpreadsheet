import pandas as pd
import numpy as np
import pandas_ta as ta
import logging
import pyodbc

# --------------------------------------------------------------------------
# Configure Logging (Optional)
# --------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("secondary_warmer_script_logger")

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
def fetch_data_from_sql():
    """
    Connects to the SQL Server, retrieves the last 300k rows for 'EUR.USD' 
    from [TTG].[dbo].[HistoData], ordered by BarDateTime descending.
    Returns a DataFrame with columns [BarDateTime, Open, High, Low, Close].
    """
    logger.info("Connecting to SQL Server and fetching data...")

    conn_str = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={server};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
    )
    cnxn = pyodbc.connect(conn_str)
    query = """
SELECT 
    [ID],
    [BarDateTime],
    [Identifier],
    [BarSize],
    [Open],
    [High],
    [Low],
    [Close]
FROM 
(
    SELECT TOP (300000)
        [ID],
        [BarDateTime],
        [Identifier],
        [BarSize],
        [Open],
        [High],
        [Low],
        [Close]
    FROM [TTG].[dbo].[HistoData]
    WHERE [Identifier] = 'EUR.USD'
    ORDER BY [BarDateTime] DESC
) AS Sub
ORDER BY [BarDateTime] ASC;

    """
    df = pd.read_sql(query, cnxn)
    cnxn.close()

    logger.info(f"Fetched {len(df)} rows from SQL.")
    return df


# --------------------------------------------------------------------------
# (3) Resample & Precompute Logic
# --------------------------------------------------------------------------
def warmup_data():
    """
    1. Fetch raw data from SQL (5-sec bars).
    2. Sort ascending by BarDateTime & parse as DatetimeIndex.
    3. Resample to 15-minute bars.
    4. Identify & invalidate liquidity zones.
    5. Calculate RSI & MACD.
    6. Return (bars_with_indicators, current_valid_zones_dict).
    """

    # Step 1: Get raw data
    df_raw = fetch_data_from_sql()

    # Step 2: Clean up columns, ensure ascending order by BarDateTime
    df_raw.rename(
        columns={
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'BarDateTime': 'datetime'
        }, 
        inplace=True
    )
    df_raw['datetime'] = pd.to_datetime(df_raw['datetime'])  # or omit utc=True if we want to use database time (local / MST???)
    df_raw.sort_values('datetime', inplace=True)
    df_raw.set_index('datetime', inplace=True)

    # Step 3: Resample to 15-minute bars
    # (We assume the raw data is ~5-second bars, so:
    #  we'll take the first open, max high, min low, last close in each 15-min window.)
    bars_15min = df_raw.resample('15T').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    })
    # Drop any rows with NaNs (e.g., empty 15-min buckets)
    bars_15min.dropna(subset=['open','high','low','close'], how='any', inplace=True)

    # Step 4: Identify & Invalidate Liquidity Zones in a single pass
    current_valid_zones_dict = {}
    bars_15min, current_valid_zones_dict = identify_liquidity_zones(
        bars_15min, current_valid_zones_dict, PIPS_REQUIRED=0.005
    )
    bars_15min = set_support_resistance_lines(bars_15min, SUPPORT_RESISTANCE_ALLOWANCE=0.0011)

    # Optionally, step through each bar to progressively invalidate zones:
    for i in range(len(bars_15min)):
        row_slice = bars_15min.iloc[[i]]
        current_valid_zones_dict = invalidate_zones_via_sup_and_resist(
            row_slice, current_valid_zones_dict, SUPPORT_RESISTANCE_ALLOWANCE=0.0011
        )

    # Step 5: Calculate RSI & MACD (matching main algo: RSI=14, MACD=(12,26,9))
    bars_15min['RSI'] = ta.rsi(bars_15min['close'], length=14)
    macd_df = ta.macd(bars_15min['close'], fast=12, slow=26, signal=9)
    if macd_df is not None and not macd_df.empty:
        bars_15min['MACD_Line'] = macd_df['MACD_12_26_9']
        bars_15min['Signal_Line'] = macd_df['MACDs_12_26_9']

    # We have a bars DataFrame that includes zones & indicators
    logger.info(f"Finished precomputation. Final bars shape: {bars_15min.shape}")
    logger.info(f"Valid zones: {len(current_valid_zones_dict)}")

    # Step 6: Return them in memory for the main script to consume
    print(" ")
    logger.info("Preview of RSI, MACD, and Signal Line columns:")
    print(" ")
    logger.info(bars_15min[['RSI', 'MACD_Line', 'Signal_Line']].tail(10).to_string())
    return bars_15min, current_valid_zones_dict


# Optional: If we want a direct "script" entry point
if __name__ == "__main__":
    bars, zones = warmup_data()
    print(" ")
    logger.info("Warmup data computed. This script can be imported by the main algo.")
