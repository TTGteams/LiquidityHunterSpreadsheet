#######  v2

import pandas as pd
import numpy as np
import pandas_ta as ta
import logging
import pyodbc
import datetime
import json
import os
import threading

# Use the named trade_logger and debug_logger created in app.py
trade_logger = logging.getLogger("trade_logger")
debug_logger = logging.getLogger("debug_logger")

# Define supported currencies
SUPPORTED_CURRENCIES = ["EUR.USD", "USD.CAD", "GBP.USD"]

# Constants (Trading Parameters)
WINDOW_LENGTH = 10
#PIPS_REQUIRED = 0.0030  
SUPPORT_RESISTANCE_ALLOWANCE = 0.0011
STOP_LOSS_PROPORTION = 0.11
TAKE_PROFIT_PROPORTION = 0.65
LIQUIDITY_ZONE_ALERT = 0.002
DEMAND_ZONE_RSI_TOP = 70
SUPPLY_ZONE_RSI_BOTTOM = 30
INVALIDATE_ZONE_LENGTH = 0.0013
COOLDOWN_SECONDS = 7200
RISK_PROPORTION = 0.03
MAX_PIP_LOSS = 0.0030  # 30 pips maximum loss

# Replace existing DB credential variables with a configuration dictionary
DB_CONFIG = {
    'server': '192.168.50.100',
    'database': 'FXStrat',  # Changed from 'TTG' to 'FXStrat'
    'username': 'djaime',
    'password': 'Enrique30072000!3',
    'driver': 'ODBC Driver 17 for SQL Server'
}

# Algorithm instance ID (set by server.py)
ALGO_INSTANCE = 1  # Default to 1, will be overridden by server.py

# Signal state tracking to prevent duplicate signals after restart
last_signals = {currency: 'hold' for currency in SUPPORTED_CURRENCIES}
signal_state_file = 'algorithm_signals.json'

# Live trading mode flag - only save signal state when in live mode
is_live_trading_mode = False

# Initialize global variables as dictionaries with currency keys
# Raw tick-by-tick data (trimmed to 12 hours)
historical_ticks = {currency: pd.DataFrame() for currency in SUPPORTED_CURRENCIES}

# Incrementally built 15-min bars
bars = {currency: pd.DataFrame(columns=["open", "high", "low", "close"]) for currency in SUPPORTED_CURRENCIES}

# Current bar metadata
current_bar_start = {currency: None for currency in SUPPORTED_CURRENCIES}
current_bar_data = {currency: None for currency in SUPPORTED_CURRENCIES}

# Trading zones and state
current_valid_zones_dict = {currency: {} for currency in SUPPORTED_CURRENCIES}
last_trade_time = {currency: None for currency in SUPPORTED_CURRENCIES}
trades = {currency: [] for currency in SUPPORTED_CURRENCIES}
balance = {currency: 100000 for currency in SUPPORTED_CURRENCIES}  # Initialize balance per currency

# Signal tracking
last_non_hold_signal = {currency: None for currency in SUPPORTED_CURRENCIES}

# Zone formation tracking (kept for backward compatibility but will be deprecated)
cumulative_zone_info = {currency: None for currency in SUPPORTED_CURRENCIES}

# NEW: External data sourcing system
external_indicators = {currency: {'rsi': None, 'macd_line': None, 'signal_line': None, 'timestamp': None, 'freshness_status': 'missing'} for currency in SUPPORTED_CURRENCIES}
invalidated_zones_memory = {currency: [] for currency in SUPPORTED_CURRENCIES}  # Track locally invalidated zones

# External data fetching configuration
EXTERNAL_DATA_FETCH_INTERVAL = 300  # 5 minutes in seconds
INDICATOR_STALENESS_WARNING = 600  # 10 minutes in seconds
MAX_INVALIDATED_ZONES_MEMORY = 500  # Maximum zones to remember per currency
external_data_timer = None  # Will hold the timer object

# Add at the top with other constants
VALID_PRICE_RANGES = {
    "EUR.USD": (0.5, 1.5),    # EUR.USD typically 1.0-1.2
    "USD.CAD": (1.2, 1.5),    # USD.CAD typically 1.3-1.4
    "GBP.USD": (0.8, 1.8)     # GBP.USD typically 1.2-1.4
}

# SQL script to clean duplicate zones - to be run manually
CLEAN_DUPLICATE_ZONES_SQL = """
-- Clean up duplicate zones in FXStrat_AlgorithmZones
-- This script identifies and removes duplicate zones, keeping only the oldest one

WITH DuplicateZones AS (
    SELECT 
        ID,
        Currency,
        ZoneType,
        ROUND(ZoneStartPrice, 6) AS RoundedStart,
        ROUND(ZoneEndPrice, 6) AS RoundedEnd,
        ConfirmationTime,
        CreatedAt,
        ROW_NUMBER() OVER (
            PARTITION BY 
                Currency, 
                ZoneType,
                ROUND(ZoneStartPrice, 4),  -- Group by 4 decimals (0.1 pip precision)
                ROUND(ZoneEndPrice, 4)
            ORDER BY 
                CreatedAt ASC,  -- Keep the oldest
                ID ASC          -- Tie-breaker
        ) AS RowNum
    FROM FXStrat_AlgorithmZones
)
DELETE FROM FXStrat_AlgorithmZones
WHERE ID IN (
    SELECT ID 
    FROM DuplicateZones 
    WHERE RowNum > 1
);

-- Show results
SELECT 
    Currency,
    COUNT(*) AS TotalZones,
    COUNT(DISTINCT CONCAT(
        Currency, '_',
        ZoneType, '_',
        ROUND(ZoneStartPrice, 4), '_',
        ROUND(ZoneEndPrice, 4)
    )) AS UniqueZones,
    COUNT(*) - COUNT(DISTINCT CONCAT(
        Currency, '_',
        ZoneType, '_',
        ROUND(ZoneStartPrice, 4), '_',
        ROUND(ZoneEndPrice, 4)
    )) AS DuplicatesRemaining
FROM FXStrat_AlgorithmZones
GROUP BY Currency;
"""

# --------------------------------------------------------------------------
# PRE-EXISTING DATA LOADING FUNCTIONS
# --------------------------------------------------------------------------
def load_preexisting_zones(zones_dict, currency="EUR.USD"):
    """
    Merges preexisting zones from an external source into current_valid_zones_dict.
    
    Args:
        zones_dict: Dictionary of zones to add
        currency: The currency pair these zones belong to
    """
    global current_valid_zones_dict
    debug_logger.info(f"Loading pre-existing liquidity zones for {currency}")

    # Initialize if not already
    if currency not in current_valid_zones_dict:
        current_valid_zones_dict[currency] = {}
        
    for zone_id, zone_data in zones_dict.items():
        # CRITICAL FIX: Round zone ID components to prevent floating point issues
        if isinstance(zone_id, tuple) and len(zone_id) == 2:
            rounded_zone_id = (round(zone_id[0], 6), round(zone_id[1], 6))
        else:
            rounded_zone_id = zone_id
            
        if rounded_zone_id not in current_valid_zones_dict[currency]:
            # Also ensure the zone_data has rounded prices
            if 'start_price' in zone_data:
                zone_data['start_price'] = round(zone_data['start_price'], 6)
            if 'end_price' in zone_data:
                zone_data['end_price'] = round(zone_data['end_price'], 6)
                
            current_valid_zones_dict[currency][rounded_zone_id] = zone_data
        
    debug_logger.info(f"After load, {currency} has {len(current_valid_zones_dict[currency])} zones.")


def load_preexisting_bars_and_indicators(precomputed_bars_df, currency="EUR.USD"):
    """
    Initializes the global 'bars' DataFrame with a precomputed set of 15-minute bars
    that should already have RSI, MACD, etc.
    
    Args:
        precomputed_bars_df: DataFrame containing precomputed bars
        currency: The currency pair these bars belong to
    """
    global bars
    debug_logger.info(f"Loading precomputed bars for {currency}")
    
    # Initialize if not already
    if currency not in bars:
        bars[currency] = pd.DataFrame(columns=["open", "high", "low", "close"])
        
    bars[currency] = precomputed_bars_df.copy()
    
    # Clean any duplicate timestamps after loading
    bars[currency] = clean_duplicate_bars(bars[currency], currency)
    
    debug_logger.info(f"Loaded {len(bars[currency])} bars for {currency}")

# --------------------------------------------------------------------------
# TRADE STATE VALIDATION
# --------------------------------------------------------------------------
def validate_trade_state(current_time, current_price, currency="EUR.USD"):
    """
    Ensures trade state consistency and fixes any issues found.
    Returns True if state was valid, False if fixes were needed.
    
    Args:
        current_time: The current time
        current_price: The current price
        currency: The currency pair to validate
    """
    global trades
    debug_logger.info(f"Validating trade state for {currency} at {current_time}")
    
    # Initialize if not already
    if currency not in trades:
        trades[currency] = []
    
    state_valid = True
    open_trades = [t for t in trades[currency] if t['status'] == 'open']
    if len(open_trades) > 1:
        debug_logger.error(f"Invalid state for {currency}: Found {len(open_trades)} open trades")
        state_valid = False
        # Keep only the earliest open trade
        earliest_trade = min(open_trades, key=lambda x: x['entry_time'])
        for trade in open_trades:
            if trade != earliest_trade:
                debug_logger.warning(f"Force closing duplicate {currency} trade from {trade['entry_time']}")
                trade.update({
                    'status': 'closed',
                    'exit_time': current_time,
                    'exit_price': current_price,
                    'profit_loss': 0,  # Force zero P/L for invalid trades
                    'close_reason': 'invalid_state'
                })

    # Check for any trades with invalid status
    for trade in trades[currency]:
        if trade['status'] not in ['open', 'closed']:
            debug_logger.error(f"Invalid trade status {trade['status']} found for {currency}")
            state_valid = False
            trade['status'] = 'closed'
            trade['close_reason'] = 'invalid_status'
    
    return state_valid

def log_trade_state(current_time, location, currency="EUR.USD"):
    """
    Enhanced logging of current trade state
    
    Args:
        current_time: The current time
        location: String describing where in the code this log is from
        currency: The currency pair to log
    """
    # Initialize if not already
    if currency not in trades:
        trades[currency] = []
        
    open_trades = [t for t in trades[currency] if t['status'] == 'open']
    debug_logger.info(f"Trade state for {currency} at {location} ({current_time}):")
    debug_logger.info(f"Open trades: {len(open_trades)}")
    for trade in open_trades:
        debug_logger.info(f"  Direction: {trade['direction']}, Entry: {trade['entry_price']}, Time: {trade['entry_time']}")

# --------------------------------------------------------------------------
# ENHANCED LOGGING FUNCTIONS - OPTIMIZATION
# --------------------------------------------------------------------------
# Replace existing log_zone_status function with optimized version
def log_zone_status(location="", currency="EUR.USD", force_detailed=False):
    """
    Creates a nicely formatted table of zone information in the debug logs.
    Displays up to 5 most recent zones with their type, price levels, and size in pips.
    Optimized to reduce output volume for better performance.
    
    Args:
        location (str): A string identifying where this log is coming from
        currency (str): The currency pair to log zones for
        force_detailed (bool): Whether to force detailed output regardless of settings
    """
    # Initialize if not already
    if currency not in current_valid_zones_dict:
        current_valid_zones_dict[currency] = {}
        
    if not current_valid_zones_dict[currency]:
        debug_logger.warning(f"ZONE STATUS for {currency} ({location}): No active zones")
        return
    
    # Basic summary only for better performance
    zone_count = len(current_valid_zones_dict[currency])
    
    # Make these calculations more robust by handling missing keys
    demands = sum(1 for z in current_valid_zones_dict[currency].values() if z.get('zone_type', '') == 'demand')
    supplies = sum(1 for z in current_valid_zones_dict[currency].values() if z.get('zone_type', '') == 'supply')
    
    debug_logger.warning(f"ZONE STATUS for {currency} ({location}): {zone_count} active zones ({demands} demand, {supplies} supply)")
    
    # Only show detailed table in specific cases (new zones, invalidations) or when forced
    if not force_detailed and location not in ["After Zone Detection", "After Zone Invalidation", "Initial Warmup"]:
        return
    
    # For detailed reporting, limit to 5 most recent zones
    zone_list = []
    for zone_id, zone_data in current_valid_zones_dict[currency].items():
        # Skip zones with missing required fields
        if 'start_price' not in zone_data or 'end_price' not in zone_data:
            debug_logger.warning(f"Skipping zone with missing required fields: {zone_id}")
            continue
            
        zone_info = {
            'start_price': zone_data['start_price'],
            'end_price': zone_data['end_price'],
            'zone_size': zone_data.get('zone_size', abs(zone_data['end_price'] - zone_data['start_price'])),
            'zone_type': zone_data.get('zone_type', 'unknown'),  # Default to 'unknown' if missing
            'confirmation_time': zone_data.get('confirmation_time', None),
        }
        zone_list.append(zone_info)
    
    # Sort zones by confirmation time (most recent first)
    sorted_zones = sorted(
        zone_list, 
        key=lambda x: x['confirmation_time'] if x['confirmation_time'] is not None else pd.Timestamp.min,
        reverse=True
    )
    
    # Take top 5 most recent zones (reduced from 10)
    recent_zones = sorted_zones[:5]
    
    # Simplified table header
    table = f"Recent zones for {currency} ({location}):\n"
    
    # Add zone data rows
    for zone in recent_zones:
        zone_type = zone['zone_type'].upper()
        confirmation_time = zone['confirmation_time'].strftime('%Y-%m-%d %H:%M') if zone['confirmation_time'] else 'N/A'
        table += f"{zone_type}: {zone['start_price']:.5f}-{zone['end_price']:.5f}, Size: {zone['zone_size']*10000:.1f} pips, Time: {confirmation_time}\n"
    
    # Log the simplified table
    debug_logger.warning(table)

# --------------------------------------------------------------------------
# INCREMENTAL BAR UPDATING
# --------------------------------------------------------------------------
def clean_duplicate_bars(bars_df, currency="EUR.USD"):
    """
    Remove duplicate timestamps from bars DataFrame, keeping the most recent one.
    This prevents the 'cannot reindex on an axis with duplicate labels' error.
    """
    if bars_df.empty or not bars_df.index.duplicated().any():
        return bars_df
    
    # Count duplicates before cleaning
    duplicate_count = bars_df.index.duplicated().sum()
    debug_logger.warning(f"Found {duplicate_count} duplicate bar timestamps for {currency}. Cleaning...")
    
    # Keep last occurrence of each timestamp (most recent data)
    cleaned_df = bars_df[~bars_df.index.duplicated(keep='last')]
    
    debug_logger.info(f"Removed {duplicate_count} duplicate bars for {currency}")
    return cleaned_df

def update_bars_with_tick(tick_time, tick_price, currency="EUR.USD"):
    """
    Updates bar data with a new price tick. If the tick is in a new 15-min period,
    the previous bar is finalized and saved.
    
    Args:
        tick_time: Timestamp of the tick
        tick_price: Price of the tick
        currency: The currency pair this tick belongs to
        
    Returns:
        dict or False: Dictionary with finalized bar data if a bar was completed, False otherwise
    """
    global bars, current_bar_start, current_bar_data
    
    # Initialize if not already
    if currency not in bars:
        bars[currency] = pd.DataFrame(columns=["open", "high", "low", "close"])
    if currency not in current_bar_start:
        current_bar_start[currency] = None
    if currency not in current_bar_data:
        current_bar_data[currency] = None

    # Floor to the 15-minute boundary
    bar_start = tick_time.floor("15min")

    if current_bar_start[currency] is None:
        # First incoming tick for this currency
        current_bar_start[currency] = bar_start
        current_bar_data[currency] = {
            "open": tick_price,
            "high": tick_price,
            "low": tick_price,
            "close": tick_price
        }
        return False  # No completed bar yet

    if bar_start == current_bar_start[currency]:
        # Still in the same bar => update OHLC
        current_bar_data[currency]["close"] = tick_price
        if tick_price > current_bar_data[currency]["high"]:
            current_bar_data[currency]["high"] = tick_price
        if tick_price < current_bar_data[currency]["low"]:
            current_bar_data[currency]["low"] = tick_price
        return False  # No completed bar yet
    else:
        # We have crossed into a new 15-min bar => finalize the old bar
        old_open = current_bar_data[currency]["open"]
        old_high = current_bar_data[currency]["high"]
        old_low = current_bar_data[currency]["low"]
        old_close = current_bar_data[currency]["close"]

        # Ensure bars has a proper DateTimeIndex
        if not isinstance(bars[currency].index, pd.DatetimeIndex):
            bars[currency].index = pd.to_datetime(bars[currency].index)

        # Remove any existing bar at this timestamp to prevent duplicates
        if current_bar_start[currency] in bars[currency].index:
            debug_logger.warning(f"Duplicate bar timestamp detected for {currency} at {current_bar_start[currency]}. Replacing with new bar data.")
            bars[currency] = bars[currency].drop(current_bar_start[currency])

        bars[currency].loc[current_bar_start[currency]] = current_bar_data[currency]
        
        bar_count = len(bars[currency])
        # Indicators are now sourced externally from IndicatorTracker; don't compute here
        rsi_value = None
        macd_value = None
        signal_value = None

        # Prepare bar data for storage
        finalized_bar_data = {
            'currency': currency,
            'bar_start_time': current_bar_start[currency],
            'bar_end_time': current_bar_start[currency] + pd.Timedelta(minutes=15),
            'open_price': old_open,
            'high_price': old_high,
            'low_price': old_low,
            'close_price': old_close,
            'zones_count': len(current_valid_zones_dict[currency]) if currency in current_valid_zones_dict else 0,
            'rsi': rsi_value,
            'macd_line': macd_value,
            'signal_line': signal_value
        }

        # Enhanced logging with better formatting
        rsi_str = "external" if rsi_value is None else f"{rsi_value:.2f}"
        macd_str = "external" if macd_value is None else f"{macd_value:.2f}"
        
        debug_logger.warning(
            f"\n\n{'='*40} {currency} BAR COMPLETED {'='*40}\n"
            f"Time: {current_bar_start[currency]} to {current_bar_start[currency] + pd.Timedelta(minutes=15)}\n"
            f"OHLC: Open={old_open:.5f}, High={old_high:.5f}, Low={old_low:.5f}, Close={old_close:.5f}\n"
            f"Indicators: RSI={rsi_str}, MACD={macd_str}\n"
            f"Stats: Bars count={bar_count}/384, Zones={len(current_valid_zones_dict[currency])}\n"
            f"{'='*90}\n"
        )

        # Start a new bar
        current_bar_start[currency] = bar_start
        current_bar_data[currency] = {
            "open": tick_price,
            "high": tick_price,
            "low": tick_price,
            "close": tick_price
        }

        # Keep last 72 hours of bars for indicator calculations
        cutoff_time = tick_time - pd.Timedelta(hours=72)
        
        # Ensure index is datetime before filtering
        if not isinstance(bars[currency].index, pd.DatetimeIndex):
            bars[currency].index = pd.to_datetime(bars[currency].index)
        
        bars[currency] = bars[currency][bars[currency].index >= cutoff_time]
        
        # Return the finalized bar data for storage
        return finalized_bar_data

# --------------------------------------------------------------------------
# ZONE DETECTION - OPTIMIZATION
# --------------------------------------------------------------------------
# Optimize identify_liquidity_zones to reduce logging
def identify_liquidity_zones(data, current_valid_zones_dict, currency="EUR.USD", pre_calculated_atr=None):
    # Deprecated: zones now sourced externally; return inputs unchanged
    return data, current_valid_zones_dict


def set_support_resistance_lines(data, currency="EUR.USD"):
    """
    Sets support and resistance lines based on liquidity zones.
    
    Args:
        data: DataFrame containing zone information
        currency: The currency pair to process
    """
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


def remove_consecutive_losers(trades_list, valid_zones_dict, currency="EUR.USD"):
    """
    Invalidates zones that have produced consecutive losing trades.
    Now also adds invalidated zones to memory to prevent re-import from external system.
    
    Args:
        trades_list: List of trades for this currency
        valid_zones_dict: Dictionary of current valid zones
        currency: The currency pair to process
    """
    # Initialize if not already
    if currency not in trades:
        trades[currency] = []
        
    # Get the trades for this currency
    trades_for_currency = trades_list if not isinstance(trades_list, dict) else trades_list[currency]
    
    # Find zones with consecutive losses
    zones_with_losses = {}
    
    for trade in trades_for_currency:
        if trade.get('status') == 'closed' and trade.get('close_reason') == 'stop_loss':
            # Handle the case where zone price info might be missing
            zone_start_price = trade.get('zone_start_price')
            zone_end_price = trade.get('zone_end_price')
            
            # Skip trades missing zone information
            if zone_start_price is None or zone_end_price is None:
                continue
                
            zone_id = (zone_start_price, zone_end_price)
            if zone_id in valid_zones_dict:
                if zone_id not in zones_with_losses:
                    zones_with_losses[zone_id] = 0
                zones_with_losses[zone_id] += 1
    
    # Remove zones with too many consecutive losses
    zones_to_remove = []
    for zone_id, loss_count in zones_with_losses.items():
        if loss_count >= 2:  # 2 consecutive losses
            zones_to_remove.append(zone_id)
            debug_logger.warning(f"\n\n{currency} zone {zone_id} removed due to {loss_count} consecutive losses\n")
    
    for zone_id in zones_to_remove:
        if zone_id in valid_zones_dict:
            # Add to invalidated memory before removing
            invalidation_time = datetime.datetime.now()
            add_zone_to_invalidated_memory(currency, zone_id[0], zone_id[1], invalidation_time)
            
            del valid_zones_dict[zone_id]
    
    return valid_zones_dict


def invalidate_zones_via_sup_and_resist(current_price, valid_zones_dict, currency="EUR.USD"):
    """
    Compare the current_price to each zone's threshold. 
    If price has violated it, remove the zone immediately.
    
    Args:
        current_price: The current market price
        valid_zones_dict: Dictionary of current valid zones
        currency: The currency pair to process
    """
    zones_to_invalidate = []
    
    # Initialize if not already
    if currency not in current_valid_zones_dict:
        current_valid_zones_dict[currency] = {}
        
    # This should check if valid_zones_dict is a nested dictionary (with currency keys)
    zones_dict = valid_zones_dict[currency] if currency in valid_zones_dict else valid_zones_dict
    
    for zone_id, zone_data in list(zones_dict.items()):
        # Skip zones missing required data
        if 'start_price' not in zone_data or 'zone_type' not in zone_data:
            debug_logger.warning(f"Skipping zone validation for incomplete zone: {zone_id}")
            continue
            
        zone_start = zone_data['start_price']
        zone_type = zone_data['zone_type']

        if zone_type == 'demand':
            support_line = zone_start - SUPPORT_RESISTANCE_ALLOWANCE
            # If price dips far below the zone's support line
            if current_price < (support_line - SUPPORT_RESISTANCE_ALLOWANCE):
                zones_to_invalidate.append((zone_id, "support line violation"))

        elif zone_type == 'supply':
            resistance_line = zone_start + SUPPORT_RESISTANCE_ALLOWANCE
            # If price rises far above the zone's resistance line
            if current_price > (resistance_line + SUPPORT_RESISTANCE_ALLOWANCE):
                zones_to_invalidate.append((zone_id, "resistance line violation"))

    # If any zones were invalidated, log them
    if zones_to_invalidate:
        debug_logger.warning(f"\n\n{'!'*20} {currency} ZONE INVALIDATIONS {'!'*20}")
        for zone_id, reason in zones_to_invalidate:
            zone_data = zones_dict[zone_id]
            zone_type = zone_data.get('zone_type', 'unknown')
            size_pips = zone_data.get('zone_size', 0) * 10000  # Convert to pips
            debug_logger.warning(
                f"Invalidating {zone_type.upper()} zone: "
                f"Start={zone_id[0]:.5f}, End={zone_id[1]:.5f}, "
                f"Size={size_pips:.1f} pips, Reason: {reason}"
            )
        debug_logger.warning(f"{'!'*53}\n")
        
        # Remove the invalidated zones
        for zone_id, _ in zones_to_invalidate:
            del zones_dict[zone_id]
        
        # Log updated zone status
        log_zone_status("After Zone Invalidation", currency)

    return zones_dict


def check_entry_conditions(data, bar_index, valid_zones_dict, currency="EUR.USD"):
    """
    Check if conditions are met to enter a trade based on liquidity zones and indicators.
    
    Args:
        data: DataFrame with price and indicator data
        bar_index: Index of the current bar
        valid_zones_dict: Dictionary of current valid zones
        currency: The currency pair to process
    """
    global trades, last_trade_time, balance
    
    # Add diagnostic logging to track critical values
    debug_logger.warning(f"\n\nENTRY CONDITION CHECK for {currency}:")
    debug_logger.warning(f"  Current price: {data['close'].iloc[bar_index]:.5f}")
    # Use external indicators for decision context
    ind = external_indicators.get(currency, {})
    rsi_val = ind.get('rsi')
    macd_val = ind.get('macd_line')
    sig_val = ind.get('signal_line')
    debug_logger.warning(f"  RSI: {rsi_val if rsi_val is not None else 'N/A'}")
    debug_logger.warning(f"  MACD: {macd_val if macd_val is not None else 'N/A'}")
    debug_logger.warning(f"  Signal: {sig_val if sig_val is not None else 'N/A'}")
    
    # Initialize if not already present
    if currency not in trades:
        trades[currency] = []
    if currency not in last_trade_time:
        last_trade_time[currency] = None
    if currency not in balance:
        balance[currency] = 100000
        
    # Get currency-specific data
    trades_for_currency = trades[currency]
    
    # IMPORTANT FIX: Don't create a new empty dictionary for this currency, use the one provided
    # The issue was that we were creating a new empty dictionary instead of using the one with zones
    # Zones are provided externally via current_valid_zones_dict
    zones_dict = valid_zones_dict
    debug_logger.warning(f"  Active zones: {len(zones_dict)}")
    
    # Debug log zones to confirm we have them
    if len(zones_dict) > 0:
        debug_logger.warning(f"  Available zones: {len(zones_dict)}")
        for zone_id, zone_data in list(zones_dict.items())[:3]:  # Show up to 3 zones
            if isinstance(zone_data, dict) and 'zone_type' in zone_data:
                debug_logger.warning(f"    {zone_data['zone_type'].upper()} zone: {zone_id[0]:.5f}-{zone_id[1]:.5f}")
    
    # Current price and time
    current_time = data.index[bar_index]
    current_price = data['close'].iloc[bar_index]
    
    # Check if we're in cooldown period
    if last_trade_time[currency] is not None:
        seconds_since_last_trade = (current_time - last_trade_time[currency]).total_seconds()
        if seconds_since_last_trade < COOLDOWN_SECONDS:
            debug_logger.warning(f"  Still in cooldown period ({seconds_since_last_trade:.0f}s / {COOLDOWN_SECONDS}s)")
            return zones_dict

    # Get the dynamic pip requirement
    current_pips_required = calculate_atr_pips_required(data, currency)
    debug_logger.warning(f"  Pips required: {current_pips_required*10000:.1f} pips")
    
    # If we have no zones, exit early
    if not zones_dict:
        debug_logger.warning("  No zones available for this currency")
        return zones_dict
    
    # Here we'll check if we're close to any of our liquidity zones
    for zone_id, zone_data in zones_dict.items():
        zone_start = zone_data['start_price']
        zone_end = zone_data['end_price']
        zone_size = zone_data['zone_size']
        zone_type = zone_data['zone_type']
        
        # Check distance from the current price to the zone's beginning
        distance_to_zone = abs(current_price - zone_start)
        
        debug_logger.warning(f"\n  Checking zone: {zone_type} at {zone_start:.5f}")
        debug_logger.warning(f"    Zone size: {zone_size*10000:.1f} pips")
        debug_logger.warning(f"    Distance to zone: {distance_to_zone*10000:.1f} pips (limit: {LIQUIDITY_ZONE_ALERT*10000:.1f} pips)")
        
        # FIX: Make this explicit comparison to avoid floating point issues
        is_close_to_zone = distance_to_zone <= LIQUIDITY_ZONE_ALERT
        debug_logger.warning(f"    Close to zone: {is_close_to_zone}")
        
        # If we're close enough to a zone, we may consider a trade
        if is_close_to_zone:
            # Set position size based on risk
            position_size = (balance[currency] * RISK_PROPORTION) / (zone_size * 10000)
            
            # FIX: Make explicit zone size check
            zone_size_sufficient = zone_size >= current_pips_required
            debug_logger.warning(f"    Zone size sufficient: {zone_size_sufficient}")
            
            if not zone_size_sufficient:
                debug_logger.warning(f"    Skipping zone: size too small ({zone_size*10000:.1f} < {current_pips_required*10000:.1f} pips)")
                continue
            
            # Check direction and indicators using external values
            if zone_type == 'demand' and current_price < zone_end:
                rsi_condition = (rsi_val is not None) and (rsi_val < DEMAND_ZONE_RSI_TOP)
                macd_condition = (macd_val is not None and sig_val is not None) and (macd_val > sig_val)
                
                debug_logger.warning(f"    DEMAND zone conditions:")
                debug_logger.warning(f"      RSI < {DEMAND_ZONE_RSI_TOP}: {rsi_condition} (value: {rsi_val if rsi_val is not None else 'N/A'})")
                debug_logger.warning(f"      MACD > Signal: {macd_condition} (MACD: {macd_val if macd_val is not None else 'N/A'}, Signal: {sig_val if sig_val is not None else 'N/A'})")
                
                if not (rsi_condition or macd_condition):
                    debug_logger.warning("    Skipping zone: RSI/MACD conditions not met")
                    continue

                entry_price = current_price
                stop_loss = entry_price - (STOP_LOSS_PROPORTION * zone_size)
                take_profit = entry_price + (TAKE_PROFIT_PROPORTION * zone_size)
            
            elif zone_type == 'supply' and current_price > zone_end:
                rsi_condition = (rsi_val is not None) and (rsi_val > SUPPLY_ZONE_RSI_BOTTOM)
                macd_condition = (macd_val is not None and sig_val is not None) and (macd_val < sig_val)
                
                debug_logger.warning(f"    SUPPLY zone conditions:")
                debug_logger.warning(f"      RSI > {SUPPLY_ZONE_RSI_BOTTOM}: {rsi_condition} (value: {rsi_val if rsi_val is not None else 'N/A'})")
                debug_logger.warning(f"      MACD < Signal: {macd_condition} (MACD: {macd_val if macd_val is not None else 'N/A'}, Signal: {sig_val if sig_val is not None else 'N/A'})")
                
                if not (rsi_condition or macd_condition):
                    debug_logger.warning("    Skipping zone: RSI/MACD conditions not met")
                    continue

                entry_price = current_price
                stop_loss = entry_price + (STOP_LOSS_PROPORTION * zone_size)
                take_profit = entry_price - (TAKE_PROFIT_PROPORTION * zone_size)
            else:
                debug_logger.warning("    Skipping zone: price not in correct position relative to zone")
                continue

            # Check if we already have an open trade
            if not any(trade.get('status', '') == 'open' for trade in trades_for_currency):
                # Create the trade
                debug_logger.warning(f"\n  >> TRADE SIGNAL GENERATED for {currency}: {zone_type} at {entry_price:.5f}!")
                
                trade_direction = 'long' if zone_type == 'demand' else 'short'
                
                trades_for_currency.append({
                    'entry_time': current_time,
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'take_profit': take_profit,
                    'position_size': position_size,
                    'status': 'open',
                    'direction': trade_direction,
                    'zone_start_price': zone_start,
                    'zone_end_price': zone_end,
                    'zone_length': zone_data.get('zone_length', 0),
                    'required_pips': current_pips_required,  # Added for analysis
                    'currency': currency  # Store the currency for reference
                })
                
                # Save position to database for crash recovery
                save_current_position_to_db(currency, trade_direction, current_time, entry_price)

                last_trade_time[currency] = current_time
                trade_logger.info(
                    f"{currency} Signal generated: {zone_type} trade at {entry_price} "
                    f"(Required pips: {current_pips_required:.5f}, Zone size: {zone_size:.5f})"
                )
                debug_logger.warning(f"\n\nNew {currency} {zone_type} trade opened at {entry_price}\n")

                # After creating a trade in check_entry_conditions:
                debug_logger.warning(f"  After trade creation, trades[{currency}] has {len(trades[currency])} trades")
                debug_logger.warning(f"  Open trades: {len([t for t in trades[currency] if t['status'] == 'open'])}")

                # Exit immediately after opening a trade
                return zones_dict

    return zones_dict


def get_last_closed_trade(currency):
    """
    Get the most recently closed trade for signal determination.
    Used to determine the opposite signal when closing a position.
    
    Args:
        currency: The currency pair to check
        
    Returns:
        dict: The most recently closed trade, or None if no closed trades exist
    """
    if currency not in trades or not trades[currency]:
        return None
        
    closed_trades = [t for t in trades[currency] if t['status'] == 'closed']
    if not closed_trades:
        return None
        
    # Return the most recently closed trade (by exit_time, fallback to entry_time)
    return max(closed_trades, key=lambda x: x.get('exit_time', x['entry_time']))


def manage_trades(current_price, current_time, currency="EUR.USD"):
    """
    Enhanced version with better state tracking and validation.
    Manages open trades for stop loss and take profit.
    """
    global trades, balance
    debug_logger.info(f"Managing trades at {current_time} price {current_price}")
    
    # Validate trade state before processing
    validate_trade_state(current_time, current_price, currency)
    log_trade_state(current_time, "before_manage", currency)
    
    closed_any_trade = False
    trades_to_process = [t for t in trades[currency] if t['status'] == 'open']
    
    for trade in trades_to_process:
        # Skip if trade is already closed
        if trade.get('status') != 'open':
            continue
            
        # Track if this specific trade was closed in this iteration
        trade_closed = False
        
        try:
            # ADDED: Check max pip loss first before regular SL/TP checks
            current_pip_loss = round(
                (trade['entry_price'] - current_price) if trade['direction'] == 'long' 
                else (current_price - trade['entry_price']), 
                5
            )
            
            # If max loss exceeded, close immediately
            if current_pip_loss >= MAX_PIP_LOSS:
                trade_result = round((current_price - trade['entry_price']) * trade['position_size'], 2)
                if trade['direction'] == 'short':
                    trade_result = -trade_result
                    
                balance[currency] += trade_result
                
                trade.update({
                    'status': 'closed',
                    'exit_time': current_time,
                    'exit_price': current_price,
                    'profit_loss': trade_result,
                    'close_reason': 'max_pip_loss'
                })
                
                # Remove position from database
                delete_current_position_from_db(currency)
                
                debug_logger.warning(
                    f"\n\nFORCE CLOSING {currency} TRADE - MAX PIP LOSS EXCEEDED IN MANAGEMENT\n"
                    f"Entry: {trade['entry_price']:.5f}\n"
                    f"Current: {current_price:.5f}\n"
                    f"Pip Loss: {current_pip_loss*10000:.1f}\n"
                    f"Time in trade: {current_time - trade['entry_time']}"
                )
                
                closed_any_trade = True
                trade_closed = True
                continue  # Skip to next trade
            
            # Only proceed with normal SL/TP checks if we haven't closed for max loss
            if trade['direction'] == 'long':
                if current_price <= trade['stop_loss'] and not trade_closed:
                    trade_result = (trade['stop_loss'] - trade['entry_price']) * trade['position_size']
                    balance[currency] += trade_result
                    trade.update({
                        'status': 'closed',
                        'exit_time': current_time,
                        'exit_price': trade['stop_loss'],
                        'profit_loss': trade_result,
                        'close_reason': 'stop_loss'
                    })
                    # Remove position from database
                    delete_current_position_from_db(currency)
                    trade_logger.info(f"{currency} Long trade hit stop loss at {trade['stop_loss']}. P/L: {trade_result}")
                    closed_any_trade = True
                    trade_closed = True

                elif current_price >= trade['take_profit'] and not trade_closed:
                    trade_result = (trade['take_profit'] - trade['entry_price']) * trade['position_size']
                    balance[currency] += trade_result
                    trade.update({
                        'status': 'closed',
                        'exit_time': current_time,
                        'exit_price': trade['take_profit'],
                        'profit_loss': trade_result,
                        'close_reason': 'take_profit'
                    })
                    # Remove position from database
                    delete_current_position_from_db(currency)
                    trade_logger.info(f"{currency} Long trade hit take profit at {trade['take_profit']}. P/L: {trade_result}")
                    closed_any_trade = True
                    trade_closed = True

            elif trade['direction'] == 'short':
                if current_price >= trade['stop_loss'] and not trade_closed:
                    trade_result = (trade['entry_price'] - trade['stop_loss']) * trade['position_size']
                    balance[currency] += trade_result
                    trade.update({
                        'status': 'closed',
                        'exit_time': current_time,
                        'exit_price': trade['stop_loss'],
                        'profit_loss': trade_result,
                        'close_reason': 'stop_loss'
                    })
                    # Remove position from database
                    delete_current_position_from_db(currency)
                    trade_logger.info(f"{currency} Short trade hit stop loss at {trade['stop_loss']}. P/L: {trade_result}")
                    closed_any_trade = True
                    trade_closed = True

                elif current_price <= trade['take_profit'] and not trade_closed:
                    trade_result = (trade['entry_price'] - trade['take_profit']) * trade['position_size']
                    balance[currency] += trade_result
                    trade.update({
                        'status': 'closed',
                        'exit_time': current_time,
                        'exit_price': trade['take_profit'],
                        'profit_loss': trade_result,
                        'close_reason': 'take_profit'
                    })
                    # Remove position from database
                    delete_current_position_from_db(currency)
                    trade_logger.info(f"{currency} Short trade hit take profit at {trade['take_profit']}. P/L: {trade_result}")
                    closed_any_trade = True
                    trade_closed = True
                    
        except Exception as e:
            debug_logger.error(f"Error in trade management: {e}", exc_info=True)
            # Force close trade on error as a safety measure
            trade.update({
                'status': 'closed',
                'exit_time': current_time,
                'exit_price': current_price,
                'profit_loss': 0,
                'close_reason': 'error_in_trade_management'
            })
            # Remove position from database
            delete_current_position_from_db(currency)
            closed_any_trade = True
            trade_closed = True

    # Validate trade state after processing
    validate_trade_state(current_time, current_price, currency)
    log_trade_state(current_time, "after_manage", currency)
    
    return closed_any_trade

# --------------------------------------------------------------------------
# EXTERNAL DATA FETCHING FUNCTIONS - NEW SYSTEM
# --------------------------------------------------------------------------
def fetch_active_zones_from_db():
    """
    Fetch all active zones from the ZoneTracker table.
    Returns zones organized by currency with proper data type conversion.
    
    Returns:
        dict: {currency: {(start_price, end_price): zone_object}} format
    """
    conn = None
    cursor = None
    fetched_zones = {currency: {} for currency in SUPPORTED_CURRENCIES}
    
    try:
        conn = get_db_connection()
        if conn is None:
            debug_logger.error("[EXTERNAL_ZONES] Failed to connect to database")
            return fetched_zones
        
        cursor = conn.cursor()
        
        # Query active zones for all supported currencies
        cursor.execute("""
        SELECT 
            Currency,
            ROUND(ZoneStartPrice, 6) as StartPrice,
            ROUND(ZoneEndPrice, 6) as EndPrice, 
            ZoneSize,
            ZoneType,
            ConfirmationTime
        FROM ZoneTracker
        WHERE IsActive = 1
        AND Currency IN (?, ?, ?)
        ORDER BY Currency, ConfirmationTime DESC
        """, SUPPORTED_CURRENCIES[0], SUPPORTED_CURRENCIES[1], SUPPORTED_CURRENCIES[2])
        
        zones = cursor.fetchall()
        zone_count = 0
        
        for row in zones:
            currency = row[0]
            zone_start = float(row[1])
            zone_end = float(row[2])
            zone_size = float(row[3])
            zone_type = row[4].lower()  # Ensure lowercase for consistency
            confirmation_time = row[5]
            
            # Create zone ID tuple
            zone_id = (zone_start, zone_end)
            
            # Check if this zone was locally invalidated
            if is_zone_locally_invalidated(currency, zone_start, zone_end, confirmation_time):
                debug_logger.info(f"[EXTERNAL_ZONES] Skipping locally invalidated {currency} zone: {zone_start:.5f}-{zone_end:.5f}")
                continue
            
            # Create zone object matching existing format
            zone_object = {
                'start_price': zone_start,
                'end_price': zone_end,
                'zone_size': zone_size,
                'zone_type': zone_type,
                'confirmation_time': confirmation_time,
                'start_time': confirmation_time,  # Use confirmation time as start time
                'source': 'external'  # Mark as externally sourced
            }
            
            fetched_zones[currency][zone_id] = zone_object
            zone_count += 1
        
        debug_logger.info(f"[EXTERNAL_ZONES] Fetched {zone_count} active zones from ZoneTracker")
        
        # Log zone counts per currency
        for currency in SUPPORTED_CURRENCIES:
            count = len(fetched_zones[currency])
            if count > 0:
                debug_logger.info(f"[EXTERNAL_ZONES] {currency}: {count} active zones")
        
        return fetched_zones
        
    except Exception as e:
        debug_logger.error(f"[EXTERNAL_ZONES] Error fetching zones from database: {e}", exc_info=True)
        return fetched_zones
    
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        # Don't close connection when using connection pooling

def fetch_latest_indicators_from_db():
    """
    Fetch the most recent indicators from the IndicatorTracker table.
    Returns indicators organized by currency.
    
    Returns:
        dict: {currency: {rsi, macd_line, signal_line, timestamp, freshness_status}}
    """
    conn = None
    cursor = None
    fetched_indicators = {currency: {'rsi': None, 'macd_line': None, 'signal_line': None, 'timestamp': None, 'freshness_status': 'missing'} for currency in SUPPORTED_CURRENCIES}
    
    try:
        conn = get_db_connection()
        if conn is None:
            debug_logger.error("[EXTERNAL_INDICATORS] Failed to connect to database")
            return fetched_indicators
        
        cursor = conn.cursor()
        
        # Get the most recent indicators for each currency
        for currency in SUPPORTED_CURRENCIES:
            try:
                cursor.execute("""
                SELECT TOP 1
                    RSI,
                    MACD,
                    MACDSignal,
                    CreatedAt
                FROM IndicatorTracker
                WHERE Currency = ?
                ORDER BY CreatedAt DESC
                """, currency)
                
                row = cursor.fetchone()
                if row:
                    rsi_value = float(row[0]) if row[0] is not None else None
                    macd_line = float(row[1]) if row[1] is not None else None
                    signal_line = float(row[2]) if row[2] is not None else None
                    timestamp = row[3]
                    
                    # Calculate data freshness
                    if timestamp:
                        time_diff = (datetime.datetime.now() - timestamp).total_seconds()
                        if time_diff <= INDICATOR_STALENESS_WARNING:
                            freshness_status = 'fresh'
                        else:
                            freshness_status = 'stale'
                            debug_logger.warning(f"[EXTERNAL_INDICATORS] {currency} indicators are stale ({time_diff/60:.1f} minutes old)")
                    else:
                        freshness_status = 'missing'
                    
                    fetched_indicators[currency] = {
                        'rsi': rsi_value,
                        'macd_line': macd_line,
                        'signal_line': signal_line,
                        'timestamp': timestamp,
                        'freshness_status': freshness_status
                    }
                    
                    rsi_str = f"{rsi_value:.2f}" if rsi_value is not None else "None"
                    macd_str = f"{macd_line:.6f}" if macd_line is not None else "None"
                    sig_str = f"{signal_line:.6f}" if signal_line is not None else "None"
                    debug_logger.info(f"[EXTERNAL_INDICATORS] {currency}: RSI={rsi_str}, MACD={macd_str}, Signal={sig_str}")
                else:
                    debug_logger.warning(f"[EXTERNAL_INDICATORS] No indicators found for {currency}")
                    
            except Exception as e:
                debug_logger.error(f"[EXTERNAL_INDICATORS] Error fetching indicators for {currency}: {e}")
                continue
        
        return fetched_indicators
        
    except Exception as e:
        debug_logger.error(f"[EXTERNAL_INDICATORS] Error fetching indicators from database: {e}", exc_info=True)
        return fetched_indicators
    
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        # Don't close connection when using connection pooling

def is_zone_locally_invalidated(currency, zone_start, zone_end, confirmation_time):
    """
    Check if a zone was locally invalidated due to losing trades.
    Compares zone confirmation time with local invalidation time.
    
    Args:
        currency: Currency pair
        zone_start: Zone start price
        zone_end: Zone end price
        confirmation_time: When the zone was confirmed in the external system
    
    Returns:
        bool: True if zone should be excluded due to local invalidation
    """
    if currency not in invalidated_zones_memory:
        return False
    
    # Check if this zone exists in our invalidated memory
    for invalidated_zone in invalidated_zones_memory[currency]:
        inv_start, inv_end, inv_time = invalidated_zone
        
        # Check if this is the same zone (with small tolerance for floating point comparison)
        start_match = abs(zone_start - inv_start) <= 0.00001
        end_match = abs(zone_end - inv_end) <= 0.00001
        
        if start_match and end_match:
            # Check if invalidation happened after confirmation
            if confirmation_time and inv_time > confirmation_time:
                debug_logger.info(f"[ZONE_INVALIDATION] {currency} zone {zone_start:.5f}-{zone_end:.5f} locally invalidated after confirmation")
                return True
    
    return False

def add_zone_to_invalidated_memory(currency, zone_start, zone_end, invalidation_time):
    """
    Add a zone to the invalidated zones memory with capacity management.
    
    Args:
        currency: Currency pair
        zone_start: Zone start price
        zone_end: Zone end price
        invalidation_time: When the zone was invalidated
    """
    if currency not in invalidated_zones_memory:
        invalidated_zones_memory[currency] = []
    
    # Add new invalidated zone
    new_entry = (zone_start, zone_end, invalidation_time)
    invalidated_zones_memory[currency].append(new_entry)
    
    # Maintain capacity limit - keep only most recent entries
    if len(invalidated_zones_memory[currency]) > MAX_INVALIDATED_ZONES_MEMORY:
        # Sort by invalidation time and keep the most recent ones
        invalidated_zones_memory[currency].sort(key=lambda x: x[2], reverse=True)
        invalidated_zones_memory[currency] = invalidated_zones_memory[currency][:MAX_INVALIDATED_ZONES_MEMORY]
        
        debug_logger.info(f"[ZONE_INVALIDATION] Trimmed {currency} invalidated zones memory to {MAX_INVALIDATED_ZONES_MEMORY} entries")
    
    debug_logger.info(f"[ZONE_INVALIDATION] Added {currency} zone {zone_start:.5f}-{zone_end:.5f} to invalidated memory")

def update_external_data():
    """
    Scheduled function to update both zones and indicators from external sources.
    Called every 5 minutes via timer.
    """
    global external_indicators, current_valid_zones_dict, external_data_timer
    
    try:
        debug_logger.info("[EXTERNAL_UPDATE] Starting scheduled external data update...")
        
        # Fetch fresh indicators
        new_indicators = fetch_latest_indicators_from_db()
        if new_indicators:
            external_indicators.update(new_indicators)
            debug_logger.info("[EXTERNAL_UPDATE] Updated external indicators")
        
        # Fetch fresh zones
        new_zones = fetch_active_zones_from_db()
        if new_zones:
            # Update the global zones dictionary
            for currency in SUPPORTED_CURRENCIES:
                if currency in new_zones:
                    current_valid_zones_dict[currency] = new_zones[currency]
                    debug_logger.info(f"[EXTERNAL_UPDATE] Updated {currency} zones: {len(new_zones[currency])} active zones")
        
        # Log summary of updated data
        total_zones = sum(len(zones) for zones in current_valid_zones_dict.values())
        fresh_indicators = sum(1 for ind in external_indicators.values() if ind['freshness_status'] == 'fresh')
        
        debug_logger.warning(f"[EXTERNAL_UPDATE] Update complete - Total zones: {total_zones}, Fresh indicators: {fresh_indicators}/{len(SUPPORTED_CURRENCIES)}")
        
    except Exception as e:
        debug_logger.error(f"[EXTERNAL_UPDATE] Error during scheduled update: {e}", exc_info=True)
    
    finally:
        # Schedule next update
        schedule_next_external_update()

def schedule_next_external_update():
    """
    Schedule the next external data update using threading.Timer.
    """
    global external_data_timer
    
    try:
        # Cancel existing timer if it exists
        if external_data_timer and external_data_timer.is_alive():
            external_data_timer.cancel()
        
        # Schedule next update
        external_data_timer = threading.Timer(EXTERNAL_DATA_FETCH_INTERVAL, update_external_data)
        external_data_timer.daemon = True  # Dies when main thread dies
        external_data_timer.start()
        
        debug_logger.info(f"[EXTERNAL_SCHEDULER] Next external data update scheduled in {EXTERNAL_DATA_FETCH_INTERVAL} seconds")
        
    except Exception as e:
        debug_logger.error(f"[EXTERNAL_SCHEDULER] Error scheduling next update: {e}")

def start_external_data_fetching():
    """
    Initialize the external data fetching system.
    Should be called during algorithm startup.
    """
    debug_logger.info("[EXTERNAL_SYSTEM] Starting external data fetching system...")
    
    # Perform initial fetch immediately
    try:
        update_external_data()
    except Exception as e:
        debug_logger.error(f"[EXTERNAL_SYSTEM] Error during initial data fetch: {e}")
        # Schedule first update anyway
        schedule_next_external_update()

def stop_external_data_fetching():
    """
    Stop the external data fetching system.
    Should be called during algorithm shutdown.
    """
    global external_data_timer
    
    try:
        if external_data_timer and external_data_timer.is_alive():
            external_data_timer.cancel()
            debug_logger.info("[EXTERNAL_SYSTEM] External data fetching system stopped")
        else:
            debug_logger.info("[EXTERNAL_SYSTEM] External data fetching system was not running")
    except Exception as e:
        debug_logger.error(f"[EXTERNAL_SYSTEM] Error stopping external data fetching: {e}")

# --------------------------------------------------------------------------
# DATABASE CONNECTION FUNCTIONS - OPTIMIZATION
# --------------------------------------------------------------------------
# Replace existing get_db_connection function with connection pooling version
def get_db_connection():
    """
    Creates and returns a connection to the SQL Server database using the DB_CONFIG.
    Uses connection pooling to avoid excessive connection overhead.
    
    Returns:
        pyodbc.Connection or None: Database connection object if successful, None otherwise
    """
    # Build connection string with pooling settings
    conn_str = (
        f"Driver={{{DB_CONFIG['driver']}}};"
        f"Server={DB_CONFIG['server']};"
        f"Database={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        f"Connection Timeout=30;"  # Add connection timeout
        f"TrustServerCertificate=yes;"  # Add for SSL/TLS issues
    )
    
    # Check for existing connection
    if not hasattr(get_db_connection, "_connection") or get_db_connection._connection is None:
        # Create new connection if none exists
        try:
            get_db_connection._connection = pyodbc.connect(conn_str, autocommit=False)
            get_db_connection._connection.timeout = 30  # Set query timeout
            debug_logger.info(f"Successfully connected to {DB_CONFIG['database']} on {DB_CONFIG['server']}")
        except Exception as e:
            debug_logger.error(f"Database connection error: {e}", exc_info=True)
            get_db_connection._connection = None
            return None
    
    # Test if connection is still valid, reconnect if needed
    if get_db_connection._connection is not None:
        try:
            # Use a simpler test query that should work even on busy servers
            cursor = get_db_connection._connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()  # Actually fetch the result
            cursor.close()
        except Exception as e:
            debug_logger.warning(f"Connection test failed, reconnecting: {e}")
            try:
                get_db_connection._connection.close()
            except:
                pass
            get_db_connection._connection = None
            
            # Create a fresh connection - don't use recursion to avoid stack overflow
            try:
                get_db_connection._connection = pyodbc.connect(conn_str, autocommit=False)
                get_db_connection._connection.timeout = 30
                debug_logger.info("Successfully reconnected to database")
            except Exception as reconnect_error:
                debug_logger.error(f"Failed to reconnect: {reconnect_error}")
                get_db_connection._connection = None
                return None
    
    return get_db_connection._connection

# Updated function to close the connection when needed
def close_db_connection():
    """Explicitly close the database connection when shutting down"""
    # Stop external data fetching system
    stop_external_data_fetching()
    
    if hasattr(get_db_connection, "_connection") and get_db_connection._connection is not None:
        try:
            get_db_connection._connection.close()
            get_db_connection._connection = None
            debug_logger.info("Database connection closed successfully")
        except Exception as e:
            debug_logger.error(f"Error closing database connection: {e}")

def create_tables_if_not_exist():
    """
    Ensures that all required tables exist in the database.
    Creates tables for algorithm bars, zones, and trade signals if they don't exist.
    
    Returns:
        bool: True if successful, False if there was an error
    """
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            debug_logger.error("Failed to create database tables - couldn't connect to database")
            return False
        
        cursor = conn.cursor()
        
        # Table 1: FXStrat_AlgorithmBars - renamed with prefix
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FXStrat_AlgorithmBars') 
        CREATE TABLE FXStrat_AlgorithmBars (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            Currency NVARCHAR(10),
            BarStartTime DATETIME,
            BarEndTime DATETIME,
            OpenPrice FLOAT,
            HighPrice FLOAT,
            LowPrice FLOAT,
            ClosePrice FLOAT,
            ZonesCount INT,
            RSI FLOAT NULL,
            MACD_Line FLOAT NULL,
            Signal_Line FLOAT NULL,
            AlgoInstance INT DEFAULT 1,
            CreatedAt DATETIME DEFAULT GETDATE()
        );
        
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FXStrat_AlgorithmBars_Currency_BarStartTime')
        CREATE INDEX IX_FXStrat_AlgorithmBars_Currency_BarStartTime ON FXStrat_AlgorithmBars(Currency, BarStartTime);
        """)
        
        # Table 2: FXStrat_AlgorithmZones - renamed with prefix
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FXStrat_AlgorithmZones')
        CREATE TABLE FXStrat_AlgorithmZones (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            Currency NVARCHAR(10),
            ZoneStartPrice FLOAT,
            ZoneEndPrice FLOAT,
            ZoneSize FLOAT,
            ZoneType NVARCHAR(10),
            ConfirmationTime DATETIME NULL,
            AlgoInstance INT DEFAULT 1,
            CreatedAt DATETIME DEFAULT GETDATE()
        );
        
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FXStrat_AlgorithmZones_Currency_ConfirmationTime')
        CREATE INDEX IX_FXStrat_AlgorithmZones_Currency_ConfirmationTime ON FXStrat_AlgorithmZones(Currency, ConfirmationTime);
        """)
        
        # Table 3: FXStrat_TradeSignalsSent - renamed with prefix
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FXStrat_TradeSignalsSent')
        CREATE TABLE FXStrat_TradeSignalsSent (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            SignalTime DATETIME,
            SignalType NVARCHAR(20),
            Price FLOAT,
            Currency NVARCHAR(10),
            SignalIntent NVARCHAR(20) NULL,  -- 'OPEN_LONG', 'CLOSE_LONG', 'OPEN_SHORT', 'CLOSE_SHORT'
            AlgoInstance INT DEFAULT 1,
            CreatedAt DATETIME DEFAULT GETDATE()
        );
        
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FXStrat_TradeSignalsSent_SignalTime')
        CREATE INDEX IX_FXStrat_TradeSignalsSent_SignalTime ON FXStrat_TradeSignalsSent(SignalTime);
        
        -- Add SignalIntent column to existing table if it doesn't exist
        IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID('FXStrat_TradeSignalsSent') AND name = 'SignalIntent')
            ALTER TABLE FXStrat_TradeSignalsSent ADD SignalIntent NVARCHAR(20) NULL;
        """)
        
        # Table 4: FXStrat_CurrentPositions - for crash recovery
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FXStrat_CurrentPositions')
        CREATE TABLE FXStrat_CurrentPositions (
            Currency NVARCHAR(10) NOT NULL,
            Direction NVARCHAR(5) NOT NULL,
            EntryTime DATETIME NOT NULL,
            EntryPrice DECIMAL(10,5) NOT NULL,
            AlgoInstance INT NOT NULL,
            CreatedAt DATETIME DEFAULT GETDATE(),
            PRIMARY KEY (Currency, AlgoInstance)
        );
        """)
        
        conn.commit()
        debug_logger.warning("\n\nSuccessfully created or verified all database tables\n")
        return True
    
    except Exception as e:
        debug_logger.error(f"Error creating database tables: {e}", exc_info=True)
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

def initialize_database():
    """
    Initializes the database connection and ensures all required tables exist.
    Now also starts the external data fetching system.
    Should be called once at application startup.
    
    Returns:
        bool: True if initialization was successful, False otherwise
    """
    debug_logger.info("Initializing database connection and tables...")
    
    # Test connection
    conn = get_db_connection()
    if conn is None:
        debug_logger.error("Database initialization failed - could not establish connection")
        return False
    
    conn.close()
    
    # Create necessary tables
    tables_created = create_tables_if_not_exist()
    if not tables_created:
        debug_logger.error("Database initialization failed - could not create required tables")
        return False

    # Startup sanity check: verify external tables have data
    try:
        conn_check = get_db_connection()
        if conn_check is not None:
            cur = conn_check.cursor()
            try:
                cur.execute("SELECT TOP 1 CreatedAt FROM IndicatorTracker ORDER BY CreatedAt DESC")
                row = cur.fetchone()
                latest_ind_ts = row[0] if row else None
                debug_logger.warning(f"[STARTUP_CHECK] Latest IndicatorTracker.CreatedAt: {latest_ind_ts}")
            except Exception as e:
                debug_logger.error(f"[STARTUP_CHECK] IndicatorTracker check failed: {e}")
            try:
                cur.execute("SELECT COUNT(*) FROM ZoneTracker WHERE IsActive = 1")
                row = cur.fetchone()
                active_zones = row[0] if row else 0
                debug_logger.warning(f"[STARTUP_CHECK] ZoneTracker active zones: {active_zones}")
            except Exception as e:
                debug_logger.error(f"[STARTUP_CHECK] ZoneTracker check failed: {e}")
            try:
                cur.close()
            except:
                pass
        else:
            debug_logger.error("[STARTUP_CHECK] Skipped table sanity checks - no DB connection")
    except Exception as e:
        debug_logger.error(f"[STARTUP_CHECK] Unexpected error during sanity check: {e}")
    
    # Only load signal state if we're in live trading mode (i.e., after a RESTART)
    # During initial startup, we want fresh signal state
    if is_live_trading_mode:
        load_algorithm_signal_state()
    else:
        debug_logger.info("[SIGNAL_STATE] Skipping signal state load - not in live trading mode yet")
    
    # NEW: Start external data fetching system
    debug_logger.info("Starting external data fetching system...")
    start_external_data_fetching()
    
    debug_logger.info("Database initialization completed successfully")
    return True

def save_bar_to_database(bar_data, currency="EUR.USD"):
    """
    Saves a completed 15-minute bar to the database.
    
    Args:
        bar_data: Dictionary containing bar OHLC data
        currency: The currency pair this bar belongs to
    """
    if bar_data is None or not isinstance(bar_data, dict):
        return False
    
    # Extract data from bar
    if currency not in current_bar_start:
        debug_logger.error(f"Cannot save bar: current_bar_start not found for {currency}")
        return False
        
    bar_start_time = current_bar_start[currency]
    bar_end_time = current_bar_start[currency] + pd.Timedelta(minutes=15)
    
    # Get indicators from external source (no endogenous calculation)
    rsi_value = None
    macd_value = None
    signal_value = None
    try:
        ind = external_indicators.get(currency, {})
        rsi_value = ind.get('rsi')
        macd_value = ind.get('macd_line')
        signal_value = ind.get('signal_line')
    except Exception as e:
        debug_logger.error(f"Error reading external indicators for {currency} DB: {e}")
    
    # Format decimal values as requested
    open_price = round(bar_data["open"], 5)
    high_price = round(bar_data["high"], 5)
    low_price = round(bar_data["low"], 5)
    close_price = round(bar_data["close"], 5)
    
    # Only round non-None values
    if rsi_value is not None:
        rsi_value = round(rsi_value, 5)
    if macd_value is not None:
        macd_value = round(macd_value, 10)
    if signal_value is not None:
        signal_value = round(signal_value, 10)
    
    # Insert to database
    try:
        conn = get_db_connection()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO FXStrat_AlgorithmBars 
        (Currency, BarStartTime, BarEndTime, OpenPrice, HighPrice, LowPrice, ClosePrice, 
         ZonesCount, RSI, MACD_Line, Signal_Line, AlgoInstance)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, 
        currency, 
        bar_start_time, 
        bar_end_time,
        open_price,
        high_price,
        low_price,
        close_price,
        len(current_valid_zones_dict[currency]) if currency in current_valid_zones_dict else 0,
        rsi_value,
        macd_value,
        signal_value,
        ALGO_INSTANCE
        )
        
        conn.commit()
        debug_logger.info(f"Saved {currency} bar to database: {bar_start_time}")
        return True
    
    except Exception as e:
        debug_logger.error(f"Error saving {currency} bar to database: {e}", exc_info=True)
        return False
    
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        # DO NOT close the connection when using connection pooling

def save_zone_to_database(zone_id, zone_data, currency="EUR.USD"):
    """
    Queue a zone for batch database saving instead of immediate saving.
    This is an optimization that removes redundant database operations.
    
    Args:
        zone_id: Tuple of (start_price, end_price) identifying the zone
        zone_data: Dictionary containing zone details
        currency: The currency pair this zone belongs to
    """
    # This implementation avoids redundant database operations
    # The actual save is handled by store_zones_in_sql
    debug_logger.info(f"Zone {zone_id} queued for batch database storage")
    return True

# --------------------------------------------------------------------------
# POSITION PERSISTENCE FUNCTIONS
# --------------------------------------------------------------------------
def save_current_position_to_db(currency, direction, entry_time, entry_price):
    """
    Save or update current position in database for crash recovery.
    """
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        
        # Use MERGE for upsert behavior
        cursor.execute("""
        MERGE FXStrat_CurrentPositions AS target
        USING (SELECT ? AS Currency, ? AS AlgoInstance) AS source
        ON (target.Currency = source.Currency AND target.AlgoInstance = source.AlgoInstance)
        WHEN MATCHED THEN 
            UPDATE SET Direction = ?, EntryTime = ?, EntryPrice = ?
        WHEN NOT MATCHED THEN
            INSERT (Currency, Direction, EntryTime, EntryPrice, AlgoInstance)
            VALUES (?, ?, ?, ?, ?);
        """, 
        currency, ALGO_INSTANCE,  # source values
        direction, entry_time, entry_price,  # update values
        currency, direction, entry_time, entry_price, ALGO_INSTANCE  # insert values
        )
        
        conn.commit()
        debug_logger.info(f"[POSITION] Saved {currency} {direction} position to DB")
        return True
        
    except Exception as e:
        debug_logger.error(f"Error saving position to DB: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def delete_current_position_from_db(currency):
    """
    Remove current position from database when trade closes.
    """
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        cursor.execute("""
        DELETE FROM FXStrat_CurrentPositions 
        WHERE Currency = ? AND AlgoInstance = ?
        """, currency, ALGO_INSTANCE)
        
        conn.commit()
        debug_logger.info(f"[POSITION] Deleted {currency} position from DB")
        return True
        
    except Exception as e:
        debug_logger.error(f"Error deleting position from DB: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_current_position_from_db(currency):
    """
    Get current position from database.
    
    Returns:
        dict: Position info or None if no position exists
    """
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return None
        
        cursor = conn.cursor()
        cursor.execute("""
        SELECT Direction, EntryTime, EntryPrice 
        FROM FXStrat_CurrentPositions 
        WHERE Currency = ? AND AlgoInstance = ?
        """, currency, ALGO_INSTANCE)
        
        row = cursor.fetchone()
        if row:
            return {
                'direction': row[0],
                'entry_time': row[1],
                'entry_price': row[2]
            }
        return None
        
    except Exception as e:
        debug_logger.error(f"Error getting position from DB: {e}")
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def load_all_positions_from_db():
    """
    Load all current positions for this algorithm instance.
    Used during restart to recover position state.
    
    Returns:
        dict: {currency: position_info} for all open positions
    """
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return {}
        
        cursor = conn.cursor()
        cursor.execute("""
        SELECT Currency, Direction, EntryTime, EntryPrice 
        FROM FXStrat_CurrentPositions 
        WHERE AlgoInstance = ?
        """, ALGO_INSTANCE)
        
        positions = {}
        for row in cursor.fetchall():
            currency = row[0]
            positions[currency] = {
                'direction': row[1],
                'entry_time': row[2],
                'entry_price': row[3]
            }
        
        debug_logger.info(f"[POSITION] Loaded {len(positions)} positions from DB: {list(positions.keys())}")
        return positions
        
    except Exception as e:
        debug_logger.error(f"Error loading positions from DB: {e}")
        return {}
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_current_position_state(currency):
    """
    Determine current position state based on open trades in memory and DB.
    
    Returns:
        str: 'FLAT', 'LONG', or 'SHORT'
    """
    try:
        # Check memory first
        if currency in trades:
            open_trades = [t for t in trades[currency] if t['status'] == 'open']
            if open_trades:
                # Should only have one open trade
                if len(open_trades) > 1:
                    debug_logger.warning(f"Multiple open trades detected for {currency} - using first one")
                return 'LONG' if open_trades[0]['direction'] == 'long' else 'SHORT'
        
        # Check DB as backup
        db_position = get_current_position_from_db(currency)
        if db_position:
            return 'LONG' if db_position['direction'] == 'long' else 'SHORT'
        
        return 'FLAT'
        
    except Exception as e:
        debug_logger.error(f"Error determining position state for {currency}: {e}")
        return 'FLAT'  # Default to flat on error

def determine_signal_intent(signal_type, position_before, position_after):
    """
    Determine the intent of a signal based on position transitions.
    
    Args:
        signal_type: 'buy' or 'sell'
        position_before: 'FLAT', 'LONG', 'SHORT'
        position_after: 'FLAT', 'LONG', 'SHORT'
        
    Returns:
        str: Signal intent like 'OPEN_LONG', 'CLOSE_SHORT', etc.
    """
    if position_before == 'FLAT' and position_after == 'LONG':
        return 'OPEN_LONG'
    elif position_before == 'FLAT' and position_after == 'SHORT':
        return 'OPEN_SHORT'
    elif position_before == 'LONG' and position_after == 'FLAT':
        return 'CLOSE_LONG'
    elif position_before == 'SHORT' and position_after == 'FLAT':
        return 'CLOSE_SHORT'
    elif position_before == 'LONG' and position_after == 'SHORT':
        return 'CLOSE_LONG_OPEN_SHORT'  # Close long and open short
    elif position_before == 'SHORT' and position_after == 'LONG':
        return 'CLOSE_SHORT_OPEN_LONG'  # Close short and open long
    else:
        return 'UNKNOWN'

def validate_position_against_signal_intent(currency, position_value):
    """
    Validate external position against the last SignalIntent for a currency.
    Handles position reconciliation and conflict resolution.
    
    Args:
        currency: Currency pair (e.g., 'USD.CAD')
        position_value: Position from external system (negative=short, positive=long, 0=flat, None=unknown)
        
    Returns:
        dict: {
            'is_valid': bool,
            'expected_position': str,  # 'LONG', 'SHORT', 'FLAT'
            'actual_position': str,    # 'LONG', 'SHORT', 'FLAT', 'UNKNOWN'
            'conflict_detected': bool,
            'last_signal_intent': str,
            'resolution_action': str   # What action was taken
        }
    """
    try:
        # Get the last SignalIntent from database
        last_intent_info = get_last_signal_intent_from_db(currency)
        
        # Determine expected position from SignalIntent
        if last_intent_info:
            intent = last_intent_info['signal_intent']
            if intent in ['OPEN_LONG', 'CLOSE_SHORT_OPEN_LONG']:
                expected_position = 'LONG'
            elif intent in ['OPEN_SHORT', 'CLOSE_LONG_OPEN_SHORT']:
                expected_position = 'SHORT'
            elif intent in ['CLOSE_LONG', 'CLOSE_SHORT']:
                expected_position = 'FLAT'
            else:
                expected_position = 'UNKNOWN'
        else:
            # No SignalIntent in database, assume FLAT
            intent = None
            expected_position = 'FLAT'
        
        # Determine actual position from external value
        if position_value is None:
            actual_position = 'UNKNOWN'
        elif position_value == 0:
            actual_position = 'FLAT'
        elif position_value > 0:
            actual_position = 'LONG'
        elif position_value < 0:
            actual_position = 'SHORT'
        else:
            actual_position = 'UNKNOWN'
        
        # Check for conflicts
        conflict_detected = False
        resolution_action = 'no_action'
        
        if actual_position != 'UNKNOWN' and expected_position != 'UNKNOWN':
            if actual_position != expected_position:
                conflict_detected = True
                # Position parameter is assumed correct - update our internal state
                resolution_action = 'update_internal_state'
                
                debug_logger.warning(
                    f"[POSITION_CONFLICT] {currency} position mismatch detected!\n"
                    f"  Expected (from SignalIntent {intent}): {expected_position}\n"
                    f"  Actual (from external system): {actual_position} (value: {position_value})\n"
                    f"  Resolution: Trusting external position as authoritative"
                )
        
        # Determine validation result
        is_valid = not conflict_detected or actual_position == 'UNKNOWN'
        
        result = {
            'is_valid': is_valid,
            'expected_position': expected_position,
            'actual_position': actual_position,
            'conflict_detected': conflict_detected,
            'last_signal_intent': intent,
            'resolution_action': resolution_action,
            'position_value': position_value
        }
        
        # Log validation result
        if conflict_detected:
            debug_logger.error(
                f"[POSITION_VALIDATION] {currency} CONFLICT: "
                f"Expected {expected_position}, Got {actual_position} ({position_value})"
            )
        else:
            debug_logger.info(
                f"[POSITION_VALIDATION] {currency} OK: "
                f"Position {actual_position} ({position_value}) matches expected {expected_position}"
            )
        
        return result
        
    except Exception as e:
        debug_logger.error(f"[POSITION_VALIDATION] Error validating {currency} position: {e}")
        return {
            'is_valid': False,
            'expected_position': 'UNKNOWN',
            'actual_position': 'UNKNOWN',
            'conflict_detected': True,
            'last_signal_intent': None,
            'resolution_action': 'error',
            'position_value': position_value
        }

def reconcile_position_conflict(currency, validation_result):
    """
    Handle position conflicts by updating internal state to match external position.
    
    Args:
        currency: Currency pair
        validation_result: Result from validate_position_against_signal_intent()
        
    Returns:
        bool: True if reconciliation was successful
    """
    try:
        if not validation_result['conflict_detected']:
            return True  # No conflict to resolve
        
        actual_position = validation_result['actual_position']
        position_value = validation_result['position_value']
        
        # Update internal signal state to match external position
        global last_signals
        
        if actual_position == 'LONG':
            corrected_signal = 'buy'
        elif actual_position == 'SHORT':
            corrected_signal = 'sell'
        elif actual_position == 'FLAT':
            corrected_signal = 'hold'
        else:
            corrected_signal = 'hold'  # Default for unknown
        
        # Update in-memory state
        old_signal = last_signals.get(currency, 'hold')
        last_signals[currency] = corrected_signal
        
        # Save corrected state to file if in live trading mode
        if is_live_trading_mode:
            save_algorithm_signal_state()
        
        debug_logger.warning(
            f"[POSITION_RECONCILIATION] {currency} internal state updated:\n"
            f"  Old signal state: {old_signal}\n"
            f"  New signal state: {corrected_signal}\n"
            f"  Based on external position: {actual_position} ({position_value})\n"
            f"  Reason: External position is authoritative"
        )
        
        return True
        
    except Exception as e:
        debug_logger.error(f"[POSITION_RECONCILIATION] Error reconciling {currency}: {e}")
        return False

def get_last_signal_intent_from_db(currency):
    """
    Get the most recent SignalIntent for a specific currency.
    Used for duplicate prevention and state recovery.
    
    Args:
        currency: Currency pair
        
    Returns:
        dict: {'signal_intent': str, 'signal_time': datetime, 'price': float} or None
    """
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return None
        
        cursor = conn.cursor()
        cursor.execute("""
        SELECT TOP 1 SignalIntent, SignalTime, Price, SignalType
        FROM FXStrat_TradeSignalsSent 
        WHERE Currency = ? 
        AND AlgoInstance = ?
        AND SignalIntent IS NOT NULL
        ORDER BY SignalTime DESC
        """, currency, ALGO_INSTANCE)
        
        row = cursor.fetchone()
        if row:
            return {
                'signal_intent': row[0],
                'signal_time': row[1],
                'price': row[2],
                'signal_type': row[3]
            }
        
        return None
        
    except Exception as e:
        debug_logger.error(f"Error getting last SignalIntent for {currency}: {e}")
        return None
    
    finally:
        if cursor:
            cursor.close()

def get_last_n_signals_from_db(currency, n=3):
    """
    Get the last N trade signals for a currency from the database.
    
    Args:
        currency: Currency pair
        n: Number of signals to retrieve (default 3)
        
    Returns:
        list: List of signal dictionaries with signal intent
    """
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return []
        
        cursor = conn.cursor()
        cursor.execute("""
        SELECT TOP (?) 
            SignalTime, SignalType, Price, SignalIntent
        FROM FXStrat_TradeSignalsSent 
        WHERE Currency = ? 
        AND AlgoInstance = ?
        ORDER BY SignalTime DESC
        """, n, currency, ALGO_INSTANCE)
        
        signals = []
        for row in cursor.fetchall():
            signals.append({
                'signal_time': row[0],
                'signal_type': row[1],
                'price': row[2],
                'signal_intent': row[3]
            })
        
        return signals
        
    except Exception as e:
        debug_logger.error(f"Error getting last {n} signals for {currency}: {e}")
        return []
    
    finally:
        if cursor:
            cursor.close()

def determine_position_from_signal_history(currency):
    """
    Determine current position based on signal history using SignalIntent.
    
    Args:
        currency: Currency pair
        
    Returns:
        str: 'FLAT', 'LONG', or 'SHORT'
    """
    try:
        # Get last signal first (most efficient)
        signals = get_last_n_signals_from_db(currency, 1)
        
        if not signals:
            debug_logger.info(f"[POSITION_HISTORY] No signals found for {currency}, assuming FLAT")
            return 'FLAT'
        
        latest_signal = signals[0]
        
        # Use SignalIntent to determine current position
        if latest_signal.get('signal_intent'):
            intent = latest_signal['signal_intent']
            if intent in ['OPEN_LONG', 'CLOSE_SHORT_OPEN_LONG']:
                debug_logger.info(f"[POSITION_HISTORY] {currency} position from SignalIntent: LONG ({intent})")
                return 'LONG'
            elif intent in ['OPEN_SHORT', 'CLOSE_LONG_OPEN_SHORT']:
                debug_logger.info(f"[POSITION_HISTORY] {currency} position from SignalIntent: SHORT ({intent})")
                return 'SHORT'
            elif intent in ['CLOSE_LONG', 'CLOSE_SHORT']:
                debug_logger.info(f"[POSITION_HISTORY] {currency} position from SignalIntent: FLAT ({intent})")
                return 'FLAT'
        
        # Fallback: Get last 3 signals for pattern analysis (old logic for backward compatibility)
        debug_logger.warning(f"[POSITION_HISTORY] {currency} missing SignalIntent, using fallback analysis")
        signals = get_last_n_signals_from_db(currency, 3)
        
        if len(signals) >= 2:
            last_two = [s['signal_type'] for s in signals[:2]]
            
            if last_two == ['sell', 'buy']:  # Most recent: sell, before: buy
                return 'FLAT'  # Likely closed a long position
            elif last_two == ['buy', 'sell']:  # Most recent: buy, before: sell
                return 'FLAT'  # Likely closed a short position
            elif signals[0]['signal_type'] == 'buy':
                return 'LONG'  # Likely opened long
            elif signals[0]['signal_type'] == 'sell':
                return 'SHORT'  # Likely opened short
        
        return 'FLAT'
        
    except Exception as e:
        debug_logger.error(f"Error determining position from signal history for {currency}: {e}")
        return 'FLAT'

def save_signal_to_database(signal_type, price, signal_time=None, currency="EUR.USD", signal_intent=None):
    """
    Saves a trade signal to the database with robust duplicate prevention and signal intent.
    Enhanced to prevent duplicate SignalIntent states per currency.
    
    Args:
        signal_type: 'buy' or 'sell'
        price: Signal price
        signal_time: When signal occurred
        currency: Currency pair
        signal_intent: 'OPEN_LONG', 'CLOSE_LONG', 'OPEN_SHORT', 'CLOSE_SHORT', etc.
    
    Returns:
        bool: True if signal was saved or duplicate was prevented, False on error
    """
    if signal_type == 'hold':
        return True
    
    # Validate required parameters
    if not signal_intent:
        debug_logger.error(f"Cannot save {currency} signal without SignalIntent")
        return False
        
    if signal_time is None:
        signal_time = datetime.datetime.now()
    
    price = round(price, 5)
    
    conn = None
    cursor = None    
    try:
        conn = get_db_connection()
        if conn is None:
            debug_logger.error(f"Failed to save {currency} signal - no database connection")
            return False
        
        cursor = conn.cursor()
        
        # ENHANCED DUPLICATE PREVENTION: Check for duplicate SignalIntent per currency
        # This prevents sending the same intent multiple times (e.g., multiple OPEN_LONG signals)
        cursor.execute("""
        SELECT TOP 1 SignalIntent, SignalTime, Price
        FROM FXStrat_TradeSignalsSent 
        WHERE Currency = ?
        AND AlgoInstance = ?
        ORDER BY SignalTime DESC
        """, currency, ALGO_INSTANCE)
        
        last_signal = cursor.fetchone()
        
        # Check for duplicate SignalIntent
        if last_signal and last_signal[0] == signal_intent:
            time_diff = (signal_time - last_signal[1]).total_seconds() if last_signal[1] else 0
            debug_logger.info(
                f"Prevented duplicate {currency} SignalIntent: {signal_intent} "
                f"(last sent {time_diff:.0f}s ago at {last_signal[2]:.5f})"
            )
            return True  # Not an error - duplicate prevention worked
        
        # Additional check: Prevent rapid-fire signals within same 15-minute bar
        cursor.execute("""
        SELECT COUNT(*) 
        FROM FXStrat_TradeSignalsSent 
        WHERE SignalType = ? 
        AND Currency = ?
        AND SignalTime BETWEEN DATEADD(minute, -15, ?) AND ?
        AND AlgoInstance = ?
        """, 
        signal_type, currency, signal_time, signal_time, ALGO_INSTANCE)
        
        rapid_fire_count = cursor.fetchone()[0]
        
        if rapid_fire_count > 0:
            debug_logger.info(f"Prevented rapid-fire {currency} {signal_type} signal within 15m bar")
            return True  # Not an error - duplicate prevention worked
        
        # Insert the new signal
        cursor.execute("""
        INSERT INTO FXStrat_TradeSignalsSent 
        (SignalTime, SignalType, Price, Currency, SignalIntent, AlgoInstance)
        VALUES (?, ?, ?, ?, ?, ?)
        """, 
        signal_time, signal_type, price, currency, signal_intent, ALGO_INSTANCE)
        
        conn.commit()
        
        # Enhanced logging with more context
        debug_logger.warning(
            f"✓ {currency} {signal_type.upper()} signal saved: {signal_intent} at {price:.5f}"
        )
        
        return True
    
    except Exception as e:
        debug_logger.error(f"Error saving {currency} signal to database: {e}", exc_info=True)
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
        # DO NOT close the connection when using connection pooling

# --------------------------------------------------------------------------
# SQL DATA STORAGE FUNCTIONS
# --------------------------------------------------------------------------
def sync_zones_to_database(currency="EUR.USD"):
    """
    Synchronizes the current valid zones in memory with the database for a specific currency.
    This function handles INSERT (new zones), UPDATE (if needed), and DELETE (invalidated zones).
    Called every 15 minutes when a bar completes.
    
    ENHANCED: Now properly handles floating point precision and prevents duplicate zones.
    
    Args:
        currency (str): The currency pair to sync zones for
        
    Returns:
        bool: True if successful, False otherwise
    """
    if currency not in current_valid_zones_dict:
        debug_logger.warning(f"No zone dictionary found for {currency}")
        return True  # Nothing to sync
    
    current_zones = current_valid_zones_dict[currency]
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            debug_logger.error("Failed to sync zones - no database connection")
            return False
        
        cursor = conn.cursor()
        
        # Step 1: Get all zones currently in database for this currency
        cursor.execute("""
        SELECT 
            ROUND(ZoneStartPrice, 6) as StartPrice, 
            ROUND(ZoneEndPrice, 6) as EndPrice, 
            ZoneType, 
            ConfirmationTime,
            ID
        FROM FXStrat_AlgorithmZones 
        WHERE Currency = ?
        AND AlgoInstance = ?
        ORDER BY ConfirmationTime DESC
        """, currency, ALGO_INSTANCE)
        
        db_zones = cursor.fetchall()
        
        # Create a mapping of rounded zone keys to database IDs for efficient deletion
        db_zone_map = {}
        for row in db_zones:
            zone_key = (float(row[0]), float(row[1]))  # Already rounded from query
            if zone_key not in db_zone_map:
                db_zone_map[zone_key] = []
            db_zone_map[zone_key].append(row[4])  # Store the ID
        
        # Step 2: Get current memory zones keys (ensure they're rounded)
        memory_zone_keys = set()
        for key in current_zones.keys():
            if isinstance(key, tuple) and len(key) == 2:
                rounded_key = (round(float(key[0]), 6), round(float(key[1]), 6))
                memory_zone_keys.add(rounded_key)
        
        # Step 3: Find zones to add (in memory but not in DB)
        zones_to_add = memory_zone_keys - set(db_zone_map.keys())
        
        # Step 4: Find zones to remove (in DB but not in memory)
        zones_to_remove = set(db_zone_map.keys()) - memory_zone_keys
        
        # Step 5: Add new zones to database
        zones_added = 0
        for zone_key in zones_to_add:
            # Find the matching zone data in current_zones
            matching_zone = None
            for original_key, zone_data in current_zones.items():
                rounded_original = (round(float(original_key[0]), 6), round(float(original_key[1]), 6))
                if rounded_original == zone_key:
                    matching_zone = zone_data
                    break
            
            if matching_zone:
                try:
                    confirmation_time = matching_zone.get('confirmation_time', datetime.datetime.now())
                    
                    cursor.execute("""
                    INSERT INTO FXStrat_AlgorithmZones 
                    (Currency, ZoneStartPrice, ZoneEndPrice, ZoneSize, ZoneType, ConfirmationTime, AlgoInstance)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, 
                    currency,
                    zone_key[0],  # Already rounded
                    zone_key[1],  # Already rounded
                    round(matching_zone.get('zone_size', abs(zone_key[1] - zone_key[0])), 6),
                    matching_zone.get('zone_type', 'unknown'),
                    confirmation_time,
                    ALGO_INSTANCE
                    )
                    zones_added += 1
                except Exception as e:
                    debug_logger.error(f"Error adding zone {zone_key}: {e}")
                    continue
        
        # Step 6: Remove invalidated zones from database
        zones_removed = 0
        for zone_key in zones_to_remove:
            try:
                # Remove ALL zones that match this key (handles duplicates)
                zone_ids = db_zone_map.get(zone_key, [])
                for zone_id in zone_ids:
                    cursor.execute("DELETE FROM FXStrat_AlgorithmZones WHERE ID = ?", zone_id)
                    zones_removed += cursor.rowcount
            except Exception as e:
                debug_logger.error(f"Error removing zone {zone_key}: {e}")
                continue
        
        # Commit all changes
        conn.commit()
        
        # Log results
        if zones_added > 0 or zones_removed > 0:
            debug_logger.warning(
                f"\n\n{'='*20} {currency} ZONE SYNC COMPLETED {'='*20}\n"
                f"Added to DB: {zones_added} zones\n"
                f"Removed from DB: {zones_removed} zones\n"
                f"Total zones in memory: {len(current_zones)}\n"
                f"Total zones in DB (estimated): {len(db_zone_map) + zones_added - len(zones_to_remove)}\n"
                f"{'='*65}\n"
            )
        else:
            debug_logger.info(f"{currency} zone sync completed - no changes needed")
        
        return True
        
    except Exception as e:
        debug_logger.error(f"Error syncing zones for {currency}: {e}", exc_info=True)
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
        # DO NOT close the connection when using connection pooling

def store_bar_in_sql(bar_data):
    """
    Stores a single bar's data in the SQL database.
    
    Args:
        bar_data (dict): Dictionary containing bar details
            
    Returns:
        bool: True if successful, False otherwise
    """
    if not bar_data or not isinstance(bar_data, dict):
        debug_logger.error("Invalid bar data provided to store_bar_in_sql")
        return False
    
    # Extract required fields
    required_fields = ['currency', 'bar_start_time', 'bar_end_time', 
                       'open_price', 'high_price', 'low_price', 'close_price']
    
    for field in required_fields:
        if field not in bar_data:
            debug_logger.error(f"Missing required field '{field}' in bar_data")
            return False
    
    # Get optional indicator values from external source
    ind = external_indicators.get(bar_data.get('currency', 'EUR.USD'), {})
    rsi_value = ind.get('rsi')
    macd_value = ind.get('macd_line')
    signal_value = ind.get('signal_line')
    zones_count = bar_data.get('zones_count', 0)
    
    # Format decimal values as requested: prices and RSI to 5 decimals, MACD to 10
    open_price = round(bar_data['open_price'], 5)
    high_price = round(bar_data['high_price'], 5)
    low_price = round(bar_data['low_price'], 5)
    close_price = round(bar_data['close_price'], 5)
    
    # Only round non-None values
    if rsi_value is not None:
        rsi_value = round(rsi_value, 5)
    if macd_value is not None:
        macd_value = round(macd_value, 10)
    if signal_value is not None:
        signal_value = round(signal_value, 10)
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO FXStrat_AlgorithmBars 
        (Currency, BarStartTime, BarEndTime, OpenPrice, HighPrice, LowPrice, ClosePrice, 
         ZonesCount, RSI, MACD_Line, Signal_Line, AlgoInstance)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, 
        bar_data['currency'], 
        bar_data['bar_start_time'], 
        bar_data['bar_end_time'],
        open_price,  # Using rounded values
        high_price, 
        low_price, 
        close_price,
        zones_count,
        rsi_value,
        macd_value,
        signal_value,
        ALGO_INSTANCE
        )
        
        conn.commit()
        debug_logger.info(f"Successfully stored {bar_data['currency']} bar from {bar_data['bar_start_time']} in SQL database")
        return True
    
    except Exception as e:
        debug_logger.error(f"Error storing bar data in SQL: {e}", exc_info=True)
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
        # DO NOT close the connection when using connection pooling
        # The connection is managed by get_db_connection()
        # Removing the connection close to fix the issue

def store_zones_in_sql(zones_data):
    """
    Stores multiple liquidity zones in the SQL database with enhanced duplicate prevention.
    """
    if not zones_data or not isinstance(zones_data, list):
        debug_logger.error("Invalid zones data provided to store_zones_in_sql")
        return False
    
    if len(zones_data) == 0:
        debug_logger.info("No zones to store in SQL database")
        return True
    
    required_fields = ['currency', 'zone_start_price', 'zone_end_price', 
                      'zone_size', 'zone_type']
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        
        zones_inserted = 0
        zones_skipped = 0
        
        for zone in zones_data:
            # Validate zone data
            is_valid = True
            for field in required_fields:
                if field not in zone:
                    debug_logger.error(f"Missing required field '{field}' in zone data")
                    is_valid = False
                    
            if not is_valid:
                continue
                
            try:
                confirmation_time = zone.get('confirmation_time', None)
                
                # Format zone prices and size to 6 decimals for consistency
                zone_start_price = round(zone['zone_start_price'], 6)
                zone_end_price = round(zone['zone_end_price'], 6)
                zone_size = round(zone['zone_size'], 6)
                
                # ENHANCED duplicate check: Look for ANY similar zones regardless of time
                cursor.execute("""
                SELECT COUNT(*) 
                FROM FXStrat_AlgorithmZones 
                WHERE Currency = ? 
                AND ZoneType = ?
                AND ABS(ZoneStartPrice - ?) <= 0.0001  -- 1 pip tolerance
                AND ABS(ZoneEndPrice - ?) <= 0.0001    -- 1 pip tolerance
                AND AlgoInstance = ?
                """, 
                zone['currency'],
                zone['zone_type'],
                zone_start_price,
                zone_end_price,
                ALGO_INSTANCE
                )
                
                count = cursor.fetchone()[0]
                
                if count == 0:  # Only insert if no similar zone exists
                    cursor.execute("""
                    INSERT INTO FXStrat_AlgorithmZones 
                    (Currency, ZoneStartPrice, ZoneEndPrice, ZoneSize, ZoneType, ConfirmationTime, AlgoInstance)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, 
                    zone['currency'],
                    zone_start_price,
                    zone_end_price,
                    zone_size,
                    zone['zone_type'],
                    confirmation_time,
                    ALGO_INSTANCE
                    )
                    zones_inserted += 1
                    debug_logger.info(f"New zone stored: {zone['currency']} {zone['zone_type']} at {zone_start_price:.6f}-{zone_end_price:.6f}")
                else:
                    zones_skipped += 1
                    debug_logger.info(f"Skipped duplicate zone: {zone['currency']} {zone['zone_type']} at {zone_start_price:.6f}-{zone_end_price:.6f}")
                
            except Exception as e:
                debug_logger.error(f"Error processing zone: {e}")
                continue
        
        conn.commit()
        
        if zones_inserted > 0 or zones_skipped > 0:
            debug_logger.warning(f"Zone storage complete: {zones_inserted} inserted, {zones_skipped} skipped as duplicates")
        
        return True
    
    except Exception as e:
        debug_logger.error(f"Error storing zones data in SQL: {e}", exc_info=True)
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
        # DO NOT close the connection when using connection pooling

# --------------------------------------------------------------------------
# ATR CALCULATION - OPTIMIZATION
# --------------------------------------------------------------------------
def calculate_atr_pips_required(data, currency="EUR.USD", use_cache=True):
    """
    Calculate the ATR-based pips required using a 4-day window.
    Returns a value between 0.002 (20 pips) and 0.018 (180 pips).
    Uses caching to avoid redundant calculations.
    """
    # Use cached value if available and requested
    if use_cache and hasattr(calculate_atr_pips_required, "_cache"):
        cache = calculate_atr_pips_required._cache
        if currency in cache and cache[currency]["timestamp"] >= data.index[-1] - pd.Timedelta(minutes=30):
            return cache[currency]["value"]
    
    try:
        # Calculate True Range
        data = data.copy()
        
        # Ensure data is numeric
        data['high'] = pd.to_numeric(data['high'])
        data['low'] = pd.to_numeric(data['low'])
        data['close'] = pd.to_numeric(data['close'])
        
        data['prev_close'] = data['close'].shift(1)
        
        # True Range calculation
        data['tr1'] = abs(data['high'] - data['low'])
        data['tr2'] = abs(data['high'] - data['prev_close'])
        data['tr3'] = abs(data['low'] - data['prev_close'])
        
        data['true_range'] = data[['tr1', 'tr2', 'tr3']].max(axis=1)
        
        # Check if we have enough data for ATR
        non_nan_count = data['true_range'].notna().sum()
        if non_nan_count < 14:  # Need at least 14 values for meaningful ATR
            debug_logger.warning(f"Not enough data for ATR calculation ({non_nan_count} values), using default 30 pips")
            return 0.003  # Default 30 pips
            
        # Calculate ATR with appropriate window size
        window_size = min(96, max(14, non_nan_count // 2))  # Adjust window based on available data
        atr = data['true_range'].rolling(window=window_size).mean().iloc[-1]
        
        if pd.isna(atr):
            debug_logger.warning(f"ATR calculation resulted in NaN, using default 30 pips")
            return 0.003  # Default value if ATR is NaN
        
        # Convert ATR to pips required (bounded between 20 and 180 pips)
        pips_required = min(max(atr, 0.002), 0.018)  # 0.002 = 20 pips, 0.018 = 180 pips
        
        # Log less frequently
        debug_logger.info(f"ATR value: {atr:.6f}, Pips required: {pips_required*10000:.1f} pips")
        
        # Cache the result
        if not hasattr(calculate_atr_pips_required, "_cache"):
            calculate_atr_pips_required._cache = {}
        
        calculate_atr_pips_required._cache[currency] = {
            "value": pips_required,
            "timestamp": data.index[-1]
        }
        
        return pips_required
        
    except Exception as e:
        debug_logger.error(f"Error in ATR calculation: {e}", exc_info=True)
        return 0.003  # Default to 30 pips on error

# --------------------------------------------------------------------------
# MAIN TICK HANDLER
# --------------------------------------------------------------------------
def process_market_data(new_data_point, currency="EUR.USD", external_position=None):
    """
    Process new market data for a specific currency with position validation.
    Optimized for better performance with multi-currency support.
    
    Args:
        new_data_point: DataFrame containing the new tick data
        currency: The currency pair to process (default: EUR.USD)
        external_position: Position from external system (negative=short, positive=long, 0=flat, None=unknown)
        
    Returns:
        tuple: (signal, currency) where signal is 'buy', 'sell', or 'hold'
    """
    global historical_ticks, bars, current_valid_zones_dict
    global trades, last_trade_time, balance
    global last_non_hold_signal

    try:
        # Extract currency from data if available, otherwise use the provided currency
        if 'Currency' in new_data_point.columns:
            extracted_currency = new_data_point['Currency'].iloc[0]
            if extracted_currency in SUPPORTED_CURRENCIES:
                currency = extracted_currency
                debug_logger.info(f"Using currency from data: {currency}")
            else:
                debug_logger.warning(f"Unsupported currency in data: {extracted_currency}. Using {currency} instead.")
        
        # POSITION VALIDATION: Validate external position against our SignalIntent records
        position_validation_result = None
        if external_position is not None:
            position_validation_result = validate_position_against_signal_intent(currency, external_position)
            
            # Handle position conflicts
            if position_validation_result['conflict_detected']:
                debug_logger.warning(
                    f"[POSITION_CONFLICT] {currency} position conflict detected - "
                    f"Expected: {position_validation_result['expected_position']}, "
                    f"External: {position_validation_result['actual_position']} ({external_position})"
                )
                
                # Reconcile the conflict (external position is authoritative)
                reconciliation_success = reconcile_position_conflict(currency, position_validation_result)
                
                if reconciliation_success:
                    debug_logger.info(f"[POSITION_CONFLICT] {currency} conflict resolved successfully")
                else:
                    debug_logger.error(f"[POSITION_CONFLICT] {currency} conflict resolution failed")
            else:
                debug_logger.debug(f"[POSITION_VALIDATION] {currency} position validated: {external_position}")
        else:
            debug_logger.debug(f"[POSITION_VALIDATION] {currency} no external position provided")
        
        # Initialize if not already present - combine into one block for efficiency
        if currency not in historical_ticks:
            historical_ticks[currency] = pd.DataFrame()
            bars[currency] = pd.DataFrame(columns=["open", "high", "low", "close"])
            current_valid_zones_dict[currency] = {}
            trades[currency] = []
            last_trade_time[currency] = None
            balance[currency] = 100000
            last_non_hold_signal[currency] = None
            debug_logger.info(f"Initialized data structures for {currency}")

        # 1) Basic checks on the incoming tick
        raw_price = float(new_data_point['Price'].iloc[0])
        
        # Get valid range for this currency
        min_price, max_price = VALID_PRICE_RANGES.get(currency, (0.5, 1.5))  # Default to EUR.USD range
        
        if raw_price < min_price or raw_price > max_price:
            debug_logger.error(
                f"Received {currency} price ({raw_price}) outside valid range "
                f"[{min_price}, {max_price}]. Ignoring."
            )
            return ('hold', currency)

        if pd.isna(new_data_point['Price'].iloc[0]):
            debug_logger.error(f"Received NaN or None price value for {currency}")
            return ('hold', currency)

        tick_time = new_data_point.index[-1]
        tick_price = float(new_data_point['Price'].iloc[-1])
        
        # 2) Validate trade state at the start - but log less verbose
        validate_trade_state(tick_time, tick_price, currency)
        
        # Reduce verbosity of trade state logging
        open_trades_count = len([t for t in trades[currency] if t['status'] == 'open'])
        debug_logger.info(f"Trade state for {currency}: {open_trades_count} open trades")

        # 3) Append to historical ticks, trim to 12 hours
        historical_ticks[currency] = pd.concat([historical_ticks[currency], new_data_point])
        latest_time = new_data_point.index[-1]
        cutoff_time = latest_time - pd.Timedelta(hours=12)
        historical_ticks[currency] = historical_ticks[currency][historical_ticks[currency].index >= cutoff_time]

        # 4) Update bars and check if a bar was finalized
        finalized_bar_data = update_bars_with_tick(tick_time, tick_price, currency)
        
        # If a bar was finalized, store it and sync zones to database
        if finalized_bar_data:
            # Store the finalized bar in SQL
            store_bar_in_sql(finalized_bar_data)
            
        # Zones are external; no DB sync required. Log status if we have zones.
            if current_valid_zones_dict[currency]:
                log_zone_status("After Bar Completion", currency, force_detailed=False)

        # 5) Manage existing trades (stop loss/take profit checks)
        closed_any_trade = manage_trades(tick_price, tick_time, currency)
        
        # Reduce verbosity of post-trade management logging
        if closed_any_trade:
            debug_logger.warning(f"TRADE CLOSED for {currency} at price {tick_price:.5f}")

        # 6) Re-validate after managing trades but with less logging
        validate_trade_state(tick_time, tick_price, currency)

        # 7) Check if we have enough bar data
        if bars[currency].empty:
            return ('hold', currency)

        rolling_window_data = bars[currency].tail(384).copy()
        if len(rolling_window_data) < 35:
            return ('hold', currency)

        rolling_window_data['close'] = rolling_window_data['close'].where(
            rolling_window_data['close'].notnull(), np.nan
        )

        # 8) Indicators are sourced externally; attach latest values for downstream consumers
        # Use the latest external indicators for this currency, if available
        latest_ind = external_indicators.get(currency, {})
        rolling_window_data['RSI'] = np.nan
        rolling_window_data['MACD_Line'] = np.nan
        rolling_window_data['Signal_Line'] = np.nan
        if latest_ind and latest_ind.get('rsi') is not None:
            rolling_window_data.iloc[-1, rolling_window_data.columns.get_loc('RSI')] = latest_ind['rsi']
        if latest_ind and latest_ind.get('macd_line') is not None:
            rolling_window_data.iloc[-1, rolling_window_data.columns.get_loc('MACD_Line')] = latest_ind['macd_line']
        if latest_ind and latest_ind.get('signal_line') is not None:
            rolling_window_data.iloc[-1, rolling_window_data.columns.get_loc('Signal_Line')] = latest_ind['signal_line']

        # 9) Zones are sourced externally, so skip endogenous detection. Keep invalidation only.
        # current_valid_zones_dict[currency] is populated by the external fetcher.

        # Support/Resistance lines were previously derived from endogenous zones.
        # With external zones, we skip adding those columns to the rolling data.
        latest_bar_idx = rolling_window_data.index[-1]
        i = rolling_window_data.index.get_loc(latest_bar_idx)

        # 10) Remove consecutive losers, invalidate zones
        current_valid_zones_dict[currency] = remove_consecutive_losers(trades[currency], current_valid_zones_dict[currency], currency)
        current_valid_zones_dict[currency] = invalidate_zones_via_sup_and_resist(tick_price, current_valid_zones_dict[currency], currency)

        # 11) Attempt to open a new trade if none is open
        trades_before = len(trades[currency])
        open_trades = [t for t in trades[currency] if t['status'] == 'open']
        if not open_trades:
            # IMPORTANT FIX: Pass the actual zones directly, don't re-wrap them in a currency dictionary
            updated_zones = check_entry_conditions(
                rolling_window_data, i, current_valid_zones_dict[currency], currency
            )
            if updated_zones is not None:
                current_valid_zones_dict[currency] = updated_zones
        new_trade_opened = (len(trades[currency]) > trades_before)

        # 12) Final validations and logging - reduced verbosity
        validate_trade_state(tick_time, tick_price, currency)

        # Add pip-based loss check right after existing trade management
        pip_loss_closure = check_pip_based_loss(tick_price, tick_time, currency)
        
        # If we closed due to pip loss, treat it like any other closure
        if pip_loss_closure:
            closed_any_trade = True

        # -----------------------------------------------------------------
        # 13) Determine final signal: buy, sell, or 'hold' (NO MORE CLOSE SIGNALS)
        # -----------------------------------------------------------------
        if closed_any_trade:
            # Find the just-closed trade's direction and send opposite signal
            last_closed_trade = get_last_closed_trade(currency)
            if last_closed_trade and last_closed_trade['direction'] == 'long':
                raw_signal = 'sell'  # Close long position with sell signal
                debug_logger.info(f"[SIGNAL] {currency} long trade closed, sending sell signal")
            elif last_closed_trade and last_closed_trade['direction'] == 'short':
                raw_signal = 'buy'   # Close short position with buy signal
                debug_logger.info(f"[SIGNAL] {currency} short trade closed, sending buy signal")
            else:
                # Fallback if we can't determine the closed trade direction
                # This could indicate a restart scenario where trade history was lost
                debug_logger.error(f"[SIGNAL] CRITICAL: {currency} trade closed but direction unknown!")
                debug_logger.error(f"[SIGNAL] This may indicate trade state was lost during restart")
                debug_logger.error(f"[SIGNAL] External app may have orphaned position - manual intervention needed")
                raw_signal = 'hold'
        elif new_trade_opened:
            # Figure out direction of the newly opened trade
            if trades[currency][-1]['direction'] == 'long':
                raw_signal = 'buy'
            else:
                raw_signal = 'sell'
        else:
            raw_signal = 'hold'

        # 14) Block consecutive signals of the same type
        # This prevents duplicate buy/buy or sell/sell signals
        if raw_signal in ['buy', 'sell'] and last_non_hold_signal[currency] == raw_signal:
            # Already had the same signal before, so return hold to prevent duplicates
            raw_signal = 'hold'

        # 15) Validate position state before sending non-hold signals
        if raw_signal in ['buy', 'sell']:
            try:
                db_position = get_current_position_from_db(currency)
                memory_position = None
                
                # Get current memory position
                open_trades = [t for t in trades[currency] if t['status'] == 'open']
                if open_trades:
                    memory_position = {
                        'direction': open_trades[0]['direction'],
                        'entry_time': open_trades[0]['entry_time'],
                        'entry_price': open_trades[0]['entry_price']
                    }
                
                # Compare positions
                if db_position and memory_position:
                    if (db_position['direction'] != memory_position['direction'] or 
                        abs(db_position['entry_price'] - memory_position['entry_price']) > 0.00001):
                        debug_logger.warning(
                            f"[POSITION_VALIDATION] {currency} position mismatch detected!\n"
                            f"  DB: {db_position['direction']} at {db_position['entry_price']}\n"
                            f"  Memory: {memory_position['direction']} at {memory_position['entry_price']}\n"
                            f"  Signal: {raw_signal} (proceeding anyway)"
                        )
                elif db_position and not memory_position:
                    debug_logger.warning(
                        f"[POSITION_VALIDATION] {currency} DB has position but memory doesn't!\n"
                        f"  DB: {db_position['direction']} at {db_position['entry_price']}\n"
                        f"  Signal: {raw_signal} (proceeding anyway)"
                    )
                elif memory_position and not db_position:
                    debug_logger.warning(
                        f"[POSITION_VALIDATION] {currency} Memory has position but DB doesn't!\n"
                        f"  Memory: {memory_position['direction']} at {memory_position['entry_price']}\n"
                        f"  Signal: {raw_signal} (proceeding anyway)"
                    )
                else:
                    debug_logger.debug(f"[POSITION_VALIDATION] {currency} positions in sync for {raw_signal} signal")
                    
            except Exception as e:
                debug_logger.error(f"[POSITION_VALIDATION] Error validating {currency} position: {e}")
                # Continue with signal anyway - validation is non-blocking

        # 16) Check against previous signal to prevent duplicates after restart
        previous_signal = last_signals.get(currency, 'hold')
        
        # Only process if signal actually changed
        if raw_signal != previous_signal:
            # Update the signal state
            last_signals[currency] = raw_signal
            
            # Save signal state to disk for restart persistence (only in live trading mode)
            if is_live_trading_mode:
                save_algorithm_signal_state()
            
            # Update the last_non_hold_signal if we have a real signal
            if raw_signal not in ['hold']:
                last_non_hold_signal[currency] = raw_signal
                
                # DETERMINE SIGNAL INTENT WITH ENHANCED VALIDATION
                position_before = get_current_position_state(currency)
                
                # Determine position after based on signal and current state
                if raw_signal == 'buy':
                    if closed_any_trade:
                        # This is a close signal (closing short position)
                        position_after = 'FLAT'
                    else:
                        # This is an open signal (opening long position)
                        position_after = 'LONG'
                elif raw_signal == 'sell':
                    if closed_any_trade:
                        # This is a close signal (closing long position)
                        position_after = 'FLAT'
                    else:
                        # This is an open signal (opening short position)
                        position_after = 'SHORT'
                else:
                    position_after = position_before  # No change for hold
                
                # Determine signal intent
                signal_intent = determine_signal_intent(raw_signal, position_before, position_after)
                
                # VALIDATION: Check for invalid signal intent
                if signal_intent == 'UNKNOWN':
                    debug_logger.error(
                        f"[SIGNAL_INTENT] Invalid signal transition for {currency}: "
                        f"{raw_signal} from {position_before} to {position_after}"
                    )
                    # Don't save invalid signal intent
                    pass
                else:
                    # ENHANCED: Check against last SignalIntent to prevent duplicates
                    last_intent_info = get_last_signal_intent_from_db(currency)
                    if last_intent_info and last_intent_info['signal_intent'] == signal_intent:
                        debug_logger.info(
                            f"[SIGNAL_INTENT] Prevented duplicate {currency} SignalIntent: {signal_intent} "
                            f"(last sent at {last_intent_info['signal_time']})"
                        )
                        # Don't save duplicate signal intent
                        pass
                    else:
                        # SAVE NON-HOLD SIGNAL TO DATABASE WITH VALIDATED SIGNAL INTENT
                        success = save_signal_to_database(raw_signal, tick_price, tick_time, currency, signal_intent)
                        
                        if not success:
                            debug_logger.error(f"[SIGNAL_SAVE] Failed to save {currency} signal: {signal_intent}")
                        else:
                            debug_logger.info(f"[SIGNAL_SAVE] Successfully saved {currency} signal: {signal_intent}")

                # Enhanced signal logging - but less verbose
                debug_logger.warning(
                    f"{currency} TRADE SIGNAL: {raw_signal.upper()} at {tick_price:.5f}, Time: {tick_time}"
                )

            # 16) Log only non-hold signals that actually changed
            if raw_signal not in ['hold']:
                if is_live_trading_mode:
                    trade_logger.info(f"{currency} signal determined: {raw_signal}")
                else:
                    debug_logger.debug(f"[BACKTEST] {currency} signal determined: {raw_signal}")
        else:
            # Signal hasn't changed, don't generate duplicate
            if is_live_trading_mode:
                debug_logger.debug(f"[SIGNAL_STATE] {currency} signal unchanged: {raw_signal}")
            # Still update last_signals to ensure it's current (in case of restart)
            last_signals[currency] = raw_signal

        return (raw_signal, currency)

    except Exception as e:
        debug_logger.error(f"Error in process_market_data for {currency}: {e}", exc_info=True)
        return ('hold', currency)

def main():
    """Minimal startup: initialize DB and external data fetching system."""
    try:
        db_initialized = initialize_database()
        if not db_initialized:
            debug_logger.error("Database initialization failed, continuing with minimal state")
        else:
            debug_logger.info("Database successfully initialized")
        trade_logger.info("Startup complete. Ready for live ticks.")
    except Exception as e:
        debug_logger.error(f"Unexpected error in main initialization: {e}", exc_info=True)
        debug_logger.warning("Algorithm continuing with minimal initialization")

def check_pip_based_loss(current_price, current_time, currency):
    """
    Monitors open trades for pip-based losses and forces closure if loss exceeds MAX_PIP_LOSS.
    Added safety checks and precise pip calculation.
    """
    if currency not in SUPPORTED_CURRENCIES:
        debug_logger.error(f"Unsupported currency in check_pip_based_loss: {currency}")
        return False
        
    global trades, balance
    
    force_closed = False
    open_trades = [t for t in trades[currency] if t['status'] == 'open']
    
    for trade in open_trades:
        try:
            # More precise pip loss calculation with rounding to avoid floating point issues
            current_pip_loss = round(
                (trade['entry_price'] - current_price) if trade['direction'] == 'long' 
                else (current_price - trade['entry_price']), 
                5  # Round to 5 decimal places for precision
            )
            
            # Double validation of pip loss
            pip_loss_check = current_pip_loss >= MAX_PIP_LOSS
            absolute_price_diff = abs(trade['entry_price'] - current_price)
            absolute_check = absolute_price_diff >= MAX_PIP_LOSS
            
            # Log significant losses even if not at max loss yet
            if current_pip_loss >= (MAX_PIP_LOSS * 0.8):  # Log at 80% of max loss
                debug_logger.warning(
                    f"SIGNIFICANT LOSS ALERT - {currency}\n"
                    f"Entry: {trade['entry_price']:.5f}\n"
                    f"Current: {current_price:.5f}\n"
                    f"Loss: {current_pip_loss*10000:.1f} pips\n"
                    f"Time in trade: {current_time - trade['entry_time']}"
                )
            
            # Force close if EITHER check indicates we've hit max loss
            if pip_loss_check or absolute_check:
                debug_logger.warning(
                    f"\n\nFORCE CLOSING {currency} TRADE - MAX PIP LOSS EXCEEDED\n"
                    f"Entry: {trade['entry_price']:.5f}\n"
                    f"Current: {current_price:.5f}\n"
                    f"Pip Loss: {current_pip_loss*10000:.1f}\n"
                    f"Time in trade: {current_time - trade['entry_time']}"
                )
                
                # Calculate actual loss with precise rounding
                trade_result = round((current_price - trade['entry_price']) * trade['position_size'], 2)
                if trade['direction'] == 'short':
                    trade_result = -trade_result
                    
                balance[currency] += trade_result
                
                trade.update({
                    'status': 'closed',
                    'exit_time': current_time,
                    'exit_price': current_price,
                    'profit_loss': trade_result,
                    'close_reason': 'max_pip_loss'
                })
                
                trade_logger.warning(
                    f"{currency} trade force closed due to max pip loss.\n"
                    f"Loss: {current_pip_loss*10000:.1f} pips, P/L: {trade_result:.2f}\n"
                    f"Trade duration: {current_time - trade['entry_time']}"
                )
                
                force_closed = True
                
        except Exception as e:
            debug_logger.error(f"Error in pip loss calculation: {e}", exc_info=True)
            # Force close trade on error as a safety measure
            trade.update({
                'status': 'closed',
                'exit_time': current_time,
                'exit_price': current_price,
                'profit_loss': 0,
                'close_reason': 'error_in_pip_loss_check'
            })
            force_closed = True
            
    return force_closed

def load_recent_zones_from_database(limit=10):
    """
    Load the most recent zones from the database for all supported currencies.
    This is used when SKIP_WARMUP is enabled to quickly populate zones without full warmup.
    
    Args:
        limit (int): Number of recent zones to load per currency (default: 10)
        
    Returns:
        dict: Dictionary with currency as key and zones dict as value
    """
    conn = None
    cursor = None
    loaded_zones = {currency: {} for currency in SUPPORTED_CURRENCIES}
    
    try:
        conn = get_db_connection()
        if conn is None:
            debug_logger.error("Failed to load recent zones - no database connection")
            return loaded_zones
        
        cursor = conn.cursor()
        
        for currency in SUPPORTED_CURRENCIES:
            try:
                # Get the most recent zones for this currency
                cursor.execute("""
                SELECT TOP (?)
                    ROUND(ZoneStartPrice, 6) as StartPrice,
                    ROUND(ZoneEndPrice, 6) as EndPrice,
                    ZoneSize,
                    ZoneType,
                    ConfirmationTime
                FROM FXStrat_AlgorithmZones
                WHERE Currency = ?
                AND AlgoInstance = ?
                ORDER BY ConfirmationTime DESC
                """, limit, currency, ALGO_INSTANCE)
                
                zones = cursor.fetchall()
                zone_count = 0
                
                for row in zones:
                    zone_start = float(row[0])
                    zone_end = float(row[1])
                    zone_size = float(row[2])
                    zone_type = row[3]
                    confirmation_time = row[4]
                    
                    zone_id = (zone_start, zone_end)
                    
                    # Create zone object
                    zone_object = {
                        'start_price': zone_start,
                        'end_price': zone_end,
                        'zone_size': zone_size,
                        'zone_type': zone_type,
                        'confirmation_time': confirmation_time,
                        'start_time': confirmation_time  # Use confirmation time as start time
                    }
                    
                    loaded_zones[currency][zone_id] = zone_object
                    zone_count += 1
                
                if zone_count > 0:
                    debug_logger.warning(
                        f"Loaded {zone_count} recent zones from database for {currency}"
                    )
                    
                    # Log the loaded zones
                    for zone_id, zone_data in loaded_zones[currency].items():
                        debug_logger.info(
                            f"  {zone_data['zone_type'].upper()} zone: "
                            f"{zone_id[0]:.5f}-{zone_id[1]:.5f}, "
                            f"Size: {zone_data['zone_size']*10000:.1f} pips"
                        )
                else:
                    debug_logger.warning(f"No zones found in database for {currency}")
                    
            except Exception as e:
                debug_logger.error(f"Error loading zones for {currency}: {e}")
                continue
        
        return loaded_zones
        
    except Exception as e:
        debug_logger.error(f"Error loading recent zones from database: {e}", exc_info=True)
        return loaded_zones
        
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        # DO NOT close the connection when using connection pooling

def load_recent_bars_with_indicators_from_database(limit=100):
    """
    Load recent bars with pre-calculated indicators from database.
    This is used for smart restart to quickly populate bars with calculated indicators.
    """
    global bars_data_with_indicators
    
    try:
        conn = get_db_connection()
        if not conn:
            debug_logger.error("Could not connect to database to load recent bars")
            return 0
        
        cursor = conn.cursor()
        total_loaded = 0
        
        for currency in SUPPORTED_CURRENCIES:
            try:
                # Query to get recent bars with indicators
                cursor.execute("""
                SELECT TOP (?) 
                    BarStartTime, [Open], High, Low, [Close], Volume,
                    SMA_20, EMA_12, EMA_26, MACD_Line, MACD_Signal, MACD_Histogram,
                    RSI_14, BollingerUpper, BollingerLower, ATR_14
                FROM FXStrat_AlgorithmBars
                WHERE Currency = ?
                AND AlgoInstance = ?
                ORDER BY BarStartTime DESC
                """, limit, currency, ALGO_INSTANCE)
                
                bars_data = cursor.fetchall()
                
                if bars_data:
                    # Convert to DataFrame
                    df_data = []
                    for row in bars_data:
                        df_data.append({
                            'Time': row[0],
                            'Open': float(row[1]),
                            'High': float(row[2]),
                            'Low': float(row[3]),
                            'Close': float(row[4]),
                            'Volume': int(row[5]) if row[5] else 0,
                            'SMA_20': float(row[6]) if row[6] else None,
                            'EMA_12': float(row[7]) if row[7] else None,
                            'EMA_26': float(row[8]) if row[8] else None,
                            'MACD_Line': float(row[9]) if row[9] else None,
                            'MACD_Signal': float(row[10]) if row[10] else None,
                            'MACD_Histogram': float(row[11]) if row[11] else None,
                            'RSI_14': float(row[12]) if row[12] else None,
                            'BollingerUpper': float(row[13]) if row[13] else None,
                            'BollingerLower': float(row[14]) if row[14] else None,
                            'ATR_14': float(row[15]) if row[15] else None
                        })
                    
                    # Create DataFrame and reverse order (oldest first)
                    df = pd.DataFrame(df_data)
                    df = df.iloc[::-1].reset_index(drop=True)
                    df['Time'] = pd.to_datetime(df['Time'])
                    df.set_index('Time', inplace=True)
                    
                    # Store in global variable
                    bars_data_with_indicators[currency] = df
                    total_loaded += len(df)
                    
                    debug_logger.info(f"Loaded {len(df)} bars with indicators for {currency}")
                    
            except Exception as e:
                debug_logger.error(f"Error loading bars for {currency}: {e}")
                continue
        
        conn.close()
        debug_logger.info(f"Successfully loaded {total_loaded} total bars with indicators from database")
        return total_loaded
        
    except Exception as e:
        debug_logger.error(f"Error in load_recent_bars_with_indicators_from_database: {e}")
        return 0

def save_algorithm_signal_state():
    """Save current signal state to disk for restoration after restart"""
    try:
        global last_signals
        data = {
            'last_signals': last_signals,
            'timestamp': datetime.datetime.now().isoformat(),
            'algo_instance': ALGO_INSTANCE
        }
        with open(signal_state_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        debug_logger.info(f"[SIGNAL_STATE] Saved signal state: {last_signals}")
        return True
    except Exception as e:
        debug_logger.error(f"[SIGNAL_STATE] Failed to save signal state: {e}")
        return False

def load_algorithm_signal_state():
    """
    Load signal state from disk and validate against database SignalIntent records.
    Enhanced to use SignalIntent for accurate state recovery.
    """
    try:
        global last_signals
        
        # First, try to load from file
        file_loaded = False
        if os.path.exists(signal_state_file):
            with open(signal_state_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Verify this is for the same instance
            if data.get('algo_instance') == ALGO_INSTANCE:
                loaded_signals = data.get('last_signals', {})
                if isinstance(loaded_signals, dict):
                    for currency in SUPPORTED_CURRENCIES:
                        signal = loaded_signals.get(currency, 'hold')
                        if signal in ('buy', 'sell', 'hold'):
                            last_signals[currency] = signal
                        else:
                            last_signals[currency] = 'hold'
                    
                    saved_time = data.get('timestamp')
                    debug_logger.info(f"[SIGNAL_STATE] Loaded signal state from file: {last_signals}")
                    if saved_time:
                        debug_logger.info(f"[SIGNAL_STATE] File was saved at: {saved_time}")
                    file_loaded = True
                else:
                    debug_logger.warning("[SIGNAL_STATE] Invalid format in signal state file")
            else:
                debug_logger.info(f"[SIGNAL_STATE] Signal state file is for different instance ({data.get('algo_instance')} vs {ALGO_INSTANCE})")
        
        # ENHANCED: Also validate against database SignalIntent records
        debug_logger.info("[SIGNAL_STATE] Validating signal state against database SignalIntent records...")
        
        db_state_loaded = False
        for currency in SUPPORTED_CURRENCIES:
            try:
                last_intent_info = get_last_signal_intent_from_db(currency)
                if last_intent_info:
                    intent = last_intent_info['signal_intent']
                    signal_time = last_intent_info['signal_time']
                    
                    # Determine expected signal state from SignalIntent
                    if intent in ['OPEN_LONG', 'CLOSE_SHORT_OPEN_LONG']:
                        expected_signal = 'buy'  # Should be in long position
                    elif intent in ['OPEN_SHORT', 'CLOSE_LONG_OPEN_SHORT']:
                        expected_signal = 'sell'  # Should be in short position
                    elif intent in ['CLOSE_LONG', 'CLOSE_SHORT']:
                        expected_signal = 'hold'  # Should be flat
                    else:
                        expected_signal = 'hold'  # Default for unknown intents
                    
                    # Compare with file-loaded state
                    file_signal = last_signals.get(currency, 'hold')
                    if file_signal != expected_signal:
                        debug_logger.warning(
                            f"[SIGNAL_STATE] {currency} state mismatch! "
                            f"File: {file_signal}, DB SignalIntent: {expected_signal} ({intent})"
                        )
                        # Use database SignalIntent as authoritative source
                        last_signals[currency] = expected_signal
                        db_state_loaded = True
                    
                    debug_logger.info(
                        f"[SIGNAL_STATE] {currency}: {expected_signal} "
                        f"(from SignalIntent: {intent} at {signal_time})"
                    )
                else:
                    # No SignalIntent in database, keep file state or default to hold
                    if currency not in last_signals:
                        last_signals[currency] = 'hold'
                    debug_logger.info(f"[SIGNAL_STATE] {currency}: {last_signals[currency]} (no DB SignalIntent)")
                    
            except Exception as e:
                debug_logger.error(f"[SIGNAL_STATE] Error validating {currency} state: {e}")
                # Ensure we have a valid state
                if currency not in last_signals:
                    last_signals[currency] = 'hold'
        
        if db_state_loaded:
            debug_logger.warning("[SIGNAL_STATE] Used database SignalIntent to correct signal state")
            # Save corrected state back to file
            save_algorithm_signal_state()
        
        debug_logger.info(f"[SIGNAL_STATE] Final signal state: {last_signals}")
        return file_loaded or db_state_loaded
        
    except Exception as e:
        debug_logger.error(f"[SIGNAL_STATE] Failed to load signal state: {e}")
        # Ensure all currencies have valid state on error
        for currency in SUPPORTED_CURRENCIES:
            if currency not in last_signals:
                last_signals[currency] = 'hold'
        return False

def recover_positions_from_database():
    """
    Recover open positions from database after restart.
    Rebuilds in-memory trade state from persistent position data.
    """
    global trades
    
    try:
        # Load all positions from database
        saved_positions = load_all_positions_from_db()
        
        if not saved_positions:
            debug_logger.info("[POSITION_RECOVERY] No saved positions found in database")
            return True
        
        # Rebuild trade objects for each recovered position
        for currency, position_info in saved_positions.items():
            if currency not in trades:
                trades[currency] = []
            
            # Create a trade object from the saved position
            recovered_trade = {
                'entry_time': position_info['entry_time'],
                'entry_price': position_info['entry_price'],
                'stop_loss': 0,  # Will be recalculated if needed
                'take_profit': 0,  # Will be recalculated if needed
                'position_size': 1,  # Default size
                'status': 'open',
                'direction': position_info['direction'],
                'zone_start_price': 0,
                'zone_end_price': 0,
                'zone_length': 0,
                'required_pips': 0,
                'currency': currency,
                'close_reason': None,
                'recovered_from_db': True  # Mark as recovered
            }
            
            trades[currency].append(recovered_trade)
            
            debug_logger.info(
                f"[POSITION_RECOVERY] Recovered {currency} {position_info['direction']} position "
                f"from {position_info['entry_time']} at {position_info['entry_price']}"
            )
        
        debug_logger.info(f"[POSITION_RECOVERY] Successfully recovered {len(saved_positions)} positions")
        return True
        
    except Exception as e:
        debug_logger.error(f"[POSITION_RECOVERY] Error recovering positions: {e}")
        return False

def reset_trading_state_for_live_trading():
    """
    Reset all trading-related state to start fresh for live trading.
    This is called after backtest/warmup to ensure no backtest trades affect live trading.
    
    KEEPS: Zones, indicators, bars, market analysis state
    RESETS: Signal state, trade history (but recovers real positions from DB)
    """
    global last_signals, last_non_hold_signal, is_live_trading_mode
    
    try:
        # Reset signal state to fresh 'hold' for all currencies
        last_signals = {currency: 'hold' for currency in SUPPORTED_CURRENCIES}
        
        # Reset last non-hold signal tracking
        last_non_hold_signal = {currency: 'hold' for currency in SUPPORTED_CURRENCIES}
        
        # Enable live trading mode - now signal state will be saved
        is_live_trading_mode = True
        
        # Clear any existing signal state file (we want fresh start)
        if os.path.exists(signal_state_file):
            os.remove(signal_state_file)
            debug_logger.info("[TRADING_RESET] Removed old signal state file")
        
        # IMPORTANT: Recover any open positions from database before clearing trades
        debug_logger.info("[TRADING_RESET] Recovering positions from database...")
        recover_positions_from_database()
        
        # Save fresh signal state
        save_algorithm_signal_state()
        
        debug_logger.info("[TRADING_RESET] Trading state reset for live trading - all signals set to 'hold'")
        debug_logger.info("[TRADING_RESET] Live trading mode ENABLED - signal state will now be saved")
        debug_logger.info("[TRADING_RESET] Zones and indicators preserved from backtest")
        debug_logger.info(f"[TRADING_RESET] Fresh signal state: {last_signals}")
        
        return True
        
    except Exception as e:
        debug_logger.error(f"[TRADING_RESET] Error resetting trading state: {e}")
        return False