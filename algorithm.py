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

# Create dedicated signal flow logger for tracking signal processing
signal_flow_logger = logging.getLogger("signal_flow_logger")
signal_flow_logger.setLevel(logging.INFO)
signal_flow_logger.propagate = False

# Define supported currencies
SUPPORTED_CURRENCIES = ["EUR.USD", "USD.CAD", "GBP.USD"]

# Constants (Trading Parameters)
WINDOW_LENGTH = 10

SUPPORT_RESISTANCE_ALLOWANCE = 0.0011
STOP_LOSS_PROPORTION = 0.11  # Default stop loss proportion (kept for backward compatibility)
TAKE_PROFIT_PROPORTION = 0.65

# Dynamic Stop Loss Proportions based on zone size (in pips)
STOP_LOSS_LARGE_ZONE_THRESHOLD = 40  # this doesnt mean a max of 40 pip loss, it means any zone larger than this has a different sl
STOP_LOSS_SMALL_ZONE_PROPORTION = 0.15  # 15% for zones 30-40 pips
STOP_LOSS_LARGE_ZONE_PROPORTION = 0.11  # 11% for zones > 40 pips
LIQUIDITY_ZONE_ALERT = 0.002
DEMAND_ZONE_RSI_TOP = 40
SUPPLY_ZONE_RSI_BOTTOM = 60
INVALIDATE_ZONE_LENGTH = 0.0013
COOLDOWN_SECONDS = 3600  # 1 hour cooldown after losses only
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

# Algorithm instance ID (read from environment variable)
ALGO_INSTANCE = int(os.environ.get('ALGO_INSTANCE', '1'))  # Read from environment, default to 1

# Signal state tracking to prevent duplicate signals after restart
last_signals = {currency: 'hold' for currency in SUPPORTED_CURRENCIES}
signal_state_file = 'algorithm_signals.json'

# Signal timing buffer - ignore position updates for 15 seconds after sending signal
last_signal_sent_time = {currency: None for currency in SUPPORTED_CURRENCIES}
POSITION_BUFFER_SECONDS = 15  # Wait 15 seconds after signal before trusting position updates

# Tick processing buffer - prevent rapid tick processing after non-hold signals
last_tick_processed_time = {currency: None for currency in SUPPORTED_CURRENCIES}
TICK_PROCESSING_BUFFER_SECONDS = 1.5  # Wait 1.5 seconds after non-hold signal before processing next tick

# Live trading mode is always enabled - no conditional signal state updates
is_live_trading_mode = True

# Initialize global variables as dictionaries with currency keys
# Raw tick-by-tick data (trimmed to 12 hours)
historical_ticks = {currency: pd.DataFrame() for currency in SUPPORTED_CURRENCIES}

# Trading zones and state
current_valid_zones_dict = {currency: {} for currency in SUPPORTED_CURRENCIES}
last_trade_time = {currency: None for currency in SUPPORTED_CURRENCIES}
last_loss_time = {currency: None for currency in SUPPORTED_CURRENCIES}  # Track losses for cooldown
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
INDICATOR_STALENESS_WARNING = 1800  # 30 minutes in seconds
MAX_INVALIDATED_ZONES_MEMORY = 500  # Maximum zones to remember per currency
external_data_timer = None  # Will hold the timer object

# Add at the top with other constants
VALID_PRICE_RANGES = {
    "EUR.USD": (1.0, 1.5),    # EUR.USD tightened from 0.5-1.5 to 1.0-1.5
    "USD.CAD": (1.2, 1.5),    # USD.CAD unchanged (already tight)
    "GBP.USD": (0.8, 1.5)     # GBP.USD tightened from 0.8-1.8 to 0.8-1.5
}

# Function to calculate dynamic stop loss proportion based on zone size
def get_stop_loss_proportion(zone_size):
    """
    Calculate stop loss proportion based on zone size in pips.
    
    Args:
        zone_size (float): Zone size in price units (not pips)
    
    Returns:
        float: Stop loss proportion to use
    """
    zone_size_pips = zone_size * 10000  # Convert to pips
    
    if zone_size_pips < STOP_LOSS_LARGE_ZONE_THRESHOLD:
        return STOP_LOSS_SMALL_ZONE_PROPORTION  # 15% for zones 30-40 pips
    else:
        return STOP_LOSS_LARGE_ZONE_PROPORTION  # 11% for zones > 40 pips

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


def check_entry_conditions(current_time, current_price, valid_zones_dict, currency="EUR.USD"):
    """
    Check if conditions are met to enter a trade based on liquidity zones and indicators.
    
    Args:
        current_time: Current timestamp
        current_price: Current price
        valid_zones_dict: Dictionary of current valid zones
        currency: The currency pair to process
    """
    global trades, last_trade_time, last_loss_time, balance
    
    # Add diagnostic logging to track critical values
    debug_logger.warning(f"\n\nENTRY CONDITION CHECK for {currency}:")
    debug_logger.warning(f"  Current price: {current_price:.5f}")
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
    if currency not in last_loss_time:
        last_loss_time[currency] = None
    if currency not in balance:
        balance[currency] = 100000
        
    # Get currency-specific data
    trades_for_currency = trades[currency]
    
    # Use the zones provided from external system
    zones_dict = valid_zones_dict
    debug_logger.warning(f"  Active zones: {len(zones_dict)}")
    
    # Debug log zones to confirm we have them
    if len(zones_dict) > 0:
        debug_logger.warning(f"  Available zones: {len(zones_dict)}")
        for zone_id, zone_data in list(zones_dict.items())[:3]:  # Show up to 3 zones
            if isinstance(zone_data, dict) and 'zone_type' in zone_data:
                debug_logger.warning(f"    {zone_data['zone_type'].upper()} zone: {zone_id[0]:.5f}-{zone_id[1]:.5f}")
    
    # Check if we're in cooldown period (only after losses)
    if last_loss_time[currency] is not None:
        seconds_since_last_loss = (current_time - last_loss_time[currency]).total_seconds()
        if seconds_since_last_loss < COOLDOWN_SECONDS:
            debug_logger.warning(f"  Still in loss cooldown period ({seconds_since_last_loss:.0f}s / {COOLDOWN_SECONDS}s)")
            return zones_dict


    
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
                stop_loss_proportion = get_stop_loss_proportion(zone_size)
                stop_loss = entry_price - (stop_loss_proportion * zone_size)
                take_profit = entry_price + (TAKE_PROFIT_PROPORTION * zone_size)
                debug_logger.warning(f"    Zone size: {zone_size*10000:.1f} pips, Using stop loss proportion: {stop_loss_proportion*100:.0f}%")
            
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
                stop_loss_proportion = get_stop_loss_proportion(zone_size)
                stop_loss = entry_price + (stop_loss_proportion * zone_size)
                take_profit = entry_price - (TAKE_PROFIT_PROPORTION * zone_size)
                debug_logger.warning(f"    Zone size: {zone_size*10000:.1f} pips, Using stop loss proportion: {stop_loss_proportion*100:.0f}%")
            else:
                debug_logger.warning("    Skipping zone: price not in correct position relative to zone")
                continue

            # Check if we already have an open trade
            if not any(trade.get('status', '') == 'open' for trade in trades_for_currency):
                # Create the trade
                debug_logger.warning(f"\n  >> TRADE SIGNAL GENERATED for {currency}: {zone_type} at {entry_price:.5f}!")
                
                trade_direction = 'long' if zone_type == 'demand' else 'short'
                signal_type = 'buy' if zone_type == 'demand' else 'sell'
                signal_flow_logger.info(f"[SIGNAL_GEN] {currency} {signal_type} generated at {entry_price:.5f}")
                
                new_trade = {
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
                    'currency': currency  # Store the currency for reference
                }
                trades_for_currency.append(new_trade)
                signal_flow_logger.info(f"[TRADE_CREATED] {currency} {trade_direction} trade created at {entry_price:.5f}")
                
                # Save position to database for crash recovery
                save_current_position_to_db(currency, trade_direction, current_time, entry_price)

                last_trade_time[currency] = current_time
                
                # IMPORTANT: Do not save to FXStrat_TradeSignalsSent here.
                # Signal will be saved exactly once in process_market_data() after final decision.
                
                trade_logger.info(
                    f"{currency} Signal generated: {zone_type} trade at {entry_price} "
                    f"(Zone size: {zone_size:.5f})"
                )
                debug_logger.warning(f"\n\nNew {currency} {zone_type} trade opened at {entry_price}\n")

                # After creating a trade in check_entry_conditions:
                debug_logger.warning(f"  After trade creation, trades[{currency}] has {len(trades[currency])} trades")
                debug_logger.warning(f"  Open trades: {len([t for t in trades[currency] if t['status'] == 'open'])}")

                # Exit immediately after opening a trade
                # NOTE: Signal state will be updated in process_market_data after API return
                # Return signal info so process_market_data can handle the API return
                
                return zones_dict, signal_type

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
    global trades, balance, last_loss_time
    debug_logger.info(f"Managing trades at {current_time} price {current_price}")
    
    # Validate trade state before processing
    validate_trade_state(current_time, current_price, currency)
    log_trade_state(current_time, "before_manage", currency)
    
    closed_any_trade = False
    trades_to_process = [t for t in trades[currency] if t['status'] in ['open', 'closing']]
    
    for trade in trades_to_process:
        # Handle different trade statuses
        if trade.get('status') == 'closing':
            # This is a force-close operation - check if position was actually closed
            # For now, assume external app will close it and mark as closed after buffer period
            time_since_close_signal = (current_time - trade.get('close_signal_sent', current_time)).total_seconds()
            if time_since_close_signal > POSITION_BUFFER_SECONDS:
                # Assume position was closed after buffer period
                trade.update({
                    'status': 'closed',
                    'exit_time': current_time,
                    'exit_price': current_price,
                    'profit_loss': 0,  # Unknown P&L for force close
                    'close_reason': 'force_close_completed'
                })
                debug_logger.info(f"[FORCE_CLOSE] {currency} marked force-close trade as completed")
                closed_any_trade = True
            continue  # Don't apply normal SL/TP logic to closing trades
            
        # Skip if trade is already closed
        elif trade.get('status') != 'open':
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
                
                # Set cooldown after loss
                last_loss_time[currency] = current_time
                
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
                    # Set cooldown after loss
                    last_loss_time[currency] = current_time
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
                    # Set cooldown after loss
                    last_loss_time[currency] = current_time
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
        if conn:
            conn.close()

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
                    
                    # Calculate data freshness with MST timezone handling
                    if timestamp:
                        import pytz
                        # Get current time in MST (database timezone)
                        mst_tz = pytz.timezone('US/Mountain')
                        current_time_mst = datetime.datetime.now(mst_tz).replace(tzinfo=None)
                        
                        # Database timestamp is already in MST, treat as timezone-naive
                        time_diff = (current_time_mst - timestamp).total_seconds()
                        
                        # Log the calculation for debugging
                        debug_logger.info(f"[EXTERNAL_INDICATORS] {currency} time calculation: Current_MST={current_time_mst}, DB_MST={timestamp}, Diff={time_diff/60:.1f} min")
                        
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
        if conn:
            conn.close()

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
    Creates and returns a NEW connection to the SQL Server database using the DB_CONFIG.
    Each call gets its own connection to prevent concurrent access issues.
    
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
    
    try:
        # Create a fresh connection for each call
        connection = pyodbc.connect(conn_str, autocommit=False)
        connection.timeout = 30  # Set query timeout
        debug_logger.debug(f"Created new database connection to {DB_CONFIG['database']}")
        return connection
    except Exception as e:
        debug_logger.error(f"Database connection error: {e}", exc_info=True)
        return None

# Updated function to close the connection when needed
def close_db_connection():
    """Explicitly close the database connection when shutting down"""
    # Stop external data fetching system
    stop_external_data_fetching()
    debug_logger.info("Database connections will be closed automatically (individual connections per call)")

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

def reconcile_position_conflict(currency, validation_result, current_price, current_zones):
    """
    Handle position conflicts with intelligent trade recovery logic.
    
    Args:
        currency: Currency pair
        validation_result: Result from validate_position_against_signal_intent()
        current_price: Current market price
        current_zones: Dictionary of current valid zones for this currency
        
    Returns:
        dict: {
            'recovery_action': str,  # 'assume_trade', 'force_close', 'align_state'
            'recovery_signal': str,  # Signal to send to API for position correction
            'trade_details': dict    # Trade details if assuming a trade
        }
    """
    try:
        if not validation_result['conflict_detected']:
            return {'recovery_action': 'none', 'recovery_signal': 'hold'}
        
        expected_position = validation_result['expected_position']
        actual_position = validation_result['actual_position']
        position_value = validation_result['position_value']
        
        debug_logger.warning(
            f"[POSITION_RECOVERY] {currency} conflict detected:\n"
            f"  Expected: {expected_position}\n"
            f"  Actual: {actual_position} ({position_value})\n"
            f"  Current price: {current_price:.5f}"
        )
        
        # Case 1: Expected LONG/FLAT but actually SHORT
        if actual_position == 'SHORT' and expected_position in ['LONG', 'FLAT']:
            return handle_unexpected_short_position(currency, current_price, current_zones, validation_result)
        
        # Case 2: Expected SHORT/FLAT but actually LONG  
        elif actual_position == 'LONG' and expected_position in ['SHORT', 'FLAT']:
            return handle_unexpected_long_position(currency, current_price, current_zones, validation_result)
        
        # Case 3: Expected position but actually FLAT (trade didn't execute or was closed)
        elif actual_position == 'FLAT' and expected_position in ['LONG', 'SHORT']:
            return handle_unexpected_flat_position(currency, expected_position, validation_result)
        
        # Default case - simple alignment
        else:
            return align_signal_state(currency, actual_position, validation_result)
        
    except Exception as e:
        debug_logger.error(f"[POSITION_RECOVERY] Error in {currency} recovery: {e}")
        return {'recovery_action': 'error', 'recovery_signal': 'hold'}

def handle_unexpected_short_position(currency, current_price, current_zones, validation_result):
    """Handle case where we expected LONG/FLAT but external system shows SHORT"""
    
    # Find nearest supply zone to current price
    supply_zones = [(zid, zdata) for zid, zdata in current_zones.items() 
                   if zdata.get('zone_type') == 'supply']
    
    if not supply_zones:
        debug_logger.warning(f"[RECOVERY] {currency} unexpected SHORT but no supply zones available")
        return {
            'recovery_action': 'force_close',
            'recovery_signal': 'buy',
            'reason': 'No supply zones to reference'
        }
    
    # Find closest supply zone to current price
    closest_zone = None
    min_distance = float('inf')
    
    for zone_id, zone_data in supply_zones:
        zone_start = zone_data['start_price']
        distance = abs(current_price - zone_start)
        if distance < min_distance:
            min_distance = distance
            closest_zone = (zone_id, zone_data)
    
    if closest_zone:
        zone_id, zone_data = closest_zone
        zone_start = zone_data['start_price']
        zone_end = zone_data['end_price']
        zone_size = zone_data['zone_size']
        
        # Check if we're within tight proximity of supply zone start
        # Inside zone: 0.3x zone size from start, Outside zone: 0.2x zone size from start
        if zone_start >= zone_end:  # Supply zone: start > end (descending)
            inside_zone = zone_end <= current_price <= zone_start
        else:  # Supply zone: start < end (ascending)
            inside_zone = zone_start <= current_price <= zone_end
        
        max_distance_inside = zone_size * 0.3   # 30% of zone size inside
        max_distance_outside = zone_size * 0.2  # 20% of zone size outside
        max_allowed_distance = max_distance_inside if inside_zone else max_distance_outside
        
        debug_logger.info(
            f"[RECOVERY] {currency} supply zone proximity check:\n"
            f"  Zone: {zone_start:.5f}-{zone_end:.5f} (size: {zone_size*10000:.1f} pips)\n"
            f"  Current: {current_price:.5f}\n"
            f"  Inside zone: {inside_zone}\n"
            f"  Distance: {min_distance*10000:.1f} pips\n"
            f"  Max allowed: {max_allowed_distance*10000:.1f} pips ({'inside' if inside_zone else 'outside'} zone)"
        )
        
        if min_distance <= max_allowed_distance:
            # Assume we entered a short trade from this supply zone
            stop_loss_proportion = get_stop_loss_proportion(zone_size)
            entry_price = zone_start  # Assume entry at zone start
            stop_loss = entry_price + (stop_loss_proportion * zone_size)
            take_profit = entry_price - (TAKE_PROFIT_PROPORTION * zone_size)
            
            debug_logger.warning(
                f"[RECOVERY] {currency} assuming SHORT trade from supply zone:\n"
                f"  Zone: {zone_start:.5f}-{zone_data['end_price']:.5f}\n"
                f"  Entry: {entry_price:.5f}\n"
                f"  Current: {current_price:.5f}\n"
                f"  SL: {stop_loss:.5f}, TP: {take_profit:.5f}\n"
                f"  Distance from zone: {min_distance*10000:.1f} pips"
            )
            
            return {
                'recovery_action': 'assume_trade',
                'recovery_signal': 'sell',  # Confirm the short position
                'trade_details': {
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'take_profit': take_profit,
                    'zone_id': zone_id,
                    'zone_data': zone_data,
                    'direction': 'short'
                }
            }
        else:
            debug_logger.warning(
                f"[RECOVERY] {currency} SHORT position too far from supply zones:\n"
                f"  Distance: {min_distance*10000:.1f} pips\n"
                f"  Max allowed: {max_allowed_distance*10000:.1f} pips ({'inside' if inside_zone else 'outside'} zone)"
            )
            return {
                'recovery_action': 'force_close',
                'recovery_signal': 'buy',
                'reason': 'Too far from supply zones'
            }

def handle_unexpected_long_position(currency, current_price, current_zones, validation_result):
    """Handle case where we expected SHORT/FLAT but external system shows LONG"""
    
    # Find nearest demand zone to current price
    demand_zones = [(zid, zdata) for zid, zdata in current_zones.items() 
                   if zdata.get('zone_type') == 'demand']
    
    if not demand_zones:
        debug_logger.warning(f"[RECOVERY] {currency} unexpected LONG but no demand zones available")
        return {
            'recovery_action': 'force_close',
            'recovery_signal': 'sell',
            'reason': 'No demand zones to reference'
        }
    
    # Find closest demand zone to current price
    closest_zone = None
    min_distance = float('inf')
    
    for zone_id, zone_data in demand_zones:
        zone_start = zone_data['start_price']
        distance = abs(current_price - zone_start)
        if distance < min_distance:
            min_distance = distance
            closest_zone = (zone_id, zone_data)
    
    if closest_zone:
        zone_id, zone_data = closest_zone
        zone_start = zone_data['start_price']
        zone_end = zone_data['end_price']
        zone_size = zone_data['zone_size']
        
        # Check if we're within tight proximity of demand zone start
        # Inside zone: 0.3x zone size from start, Outside zone: 0.2x zone size from start
        if zone_start <= zone_end:  # Demand zone: start < end (ascending)
            inside_zone = zone_start <= current_price <= zone_end
        else:  # Demand zone: start > end (descending)
            inside_zone = zone_end <= current_price <= zone_start
        
        max_distance_inside = zone_size * 0.3   # 30% of zone size inside
        max_distance_outside = zone_size * 0.2  # 20% of zone size outside
        max_allowed_distance = max_distance_inside if inside_zone else max_distance_outside
        
        debug_logger.info(
            f"[RECOVERY] {currency} demand zone proximity check:\n"
            f"  Zone: {zone_start:.5f}-{zone_end:.5f} (size: {zone_size*10000:.1f} pips)\n"
            f"  Current: {current_price:.5f}\n"
            f"  Inside zone: {inside_zone}\n"
            f"  Distance: {min_distance*10000:.1f} pips\n"
            f"  Max allowed: {max_allowed_distance*10000:.1f} pips ({'inside' if inside_zone else 'outside'} zone)"
        )
        
        if min_distance <= max_allowed_distance:
            # Assume we entered a long trade from this demand zone
            stop_loss_proportion = get_stop_loss_proportion(zone_size)
            entry_price = zone_start  # Assume entry at zone start
            stop_loss = entry_price - (stop_loss_proportion * zone_size)
            take_profit = entry_price + (TAKE_PROFIT_PROPORTION * zone_size)
            
            debug_logger.warning(
                f"[RECOVERY] {currency} assuming LONG trade from demand zone:\n"
                f"  Zone: {zone_start:.5f}-{zone_data['end_price']:.5f}\n"
                f"  Entry: {entry_price:.5f}\n"
                f"  Current: {current_price:.5f}\n"
                f"  SL: {stop_loss:.5f}, TP: {take_profit:.5f}\n"
                f"  Distance from zone: {min_distance*10000:.1f} pips"
            )
            
            return {
                'recovery_action': 'assume_trade',
                'recovery_signal': 'buy',  # Confirm the long position
                'trade_details': {
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'take_profit': take_profit,
                    'zone_id': zone_id,
                    'zone_data': zone_data,
                    'direction': 'long'
                }
            }
        else:
            debug_logger.warning(
                f"[RECOVERY] {currency} LONG position too far from demand zones:\n"
                f"  Distance: {min_distance*10000:.1f} pips\n"
                f"  Max allowed: {max_allowed_distance*10000:.1f} pips ({'inside' if inside_zone else 'outside'} zone)"
            )
            return {
                'recovery_action': 'force_close',
                'recovery_signal': 'sell',
                'reason': 'Too far from demand zones'
            }

def handle_unexpected_flat_position(currency, expected_position, validation_result):
    """Handle case where we expected LONG/SHORT but external system shows FLAT"""
    debug_logger.warning(
        f"[RECOVERY] {currency} expected {expected_position} but position is FLAT\n"
        f"  Likely: Trade didn't execute or was closed externally\n"
        f"  Action: Reset to flat state and look for new opportunities"
    )
    
    return {
        'recovery_action': 'align_state',
        'recovery_signal': 'hold',
        'reason': 'Position was closed or never opened'
    }

def align_signal_state(currency, actual_position, validation_result):
    """Simple state alignment for other cases"""
    if actual_position == 'LONG':
        signal = 'buy'
    elif actual_position == 'SHORT':
        signal = 'sell'
    else:
        signal = 'hold'
    
    return {
        'recovery_action': 'align_state',
        'recovery_signal': signal,
        'reason': 'Simple state alignment'
    }

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
        
        # Use SignalIntent to determine current position (handles OVERWRITE variants)
        if latest_signal.get('signal_intent'):
            intent = latest_signal['signal_intent']
            # Remove _OVERWRITE suffix for analysis
            base_intent = intent.replace('_OVERWRITE', '') if intent else ''
            
            if base_intent in ['OPEN_LONG', 'CLOSE_SHORT_OPEN_LONG']:
                debug_logger.info(f"[POSITION_HISTORY] {currency} position from SignalIntent: LONG ({intent})")
                return 'LONG'
            elif base_intent in ['OPEN_SHORT', 'CLOSE_LONG_OPEN_SHORT']:
                debug_logger.info(f"[POSITION_HISTORY] {currency} position from SignalIntent: SHORT ({intent})")
                return 'SHORT'
            elif base_intent in ['CLOSE_LONG', 'CLOSE_SHORT', 'SYSTEM_RESET']:
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

def determine_expected_position_from_intent(signal_intent):
    """
    Determine what position should be based on SignalIntent.
    Handles both regular and OVERWRITE variants.
    
    Args:
        signal_intent: The SignalIntent from database
        
    Returns:
        str: 'FLAT', 'LONG', or 'SHORT'
    """
    # Remove _OVERWRITE suffix if present for analysis
    base_intent = signal_intent.replace('_OVERWRITE', '') if signal_intent else ''
    
    if base_intent in ['OPEN_LONG', 'CLOSE_SHORT_OPEN_LONG']:
        return 'LONG'
    elif base_intent in ['OPEN_SHORT', 'CLOSE_LONG_OPEN_SHORT']:
        return 'SHORT'
    elif base_intent in ['CLOSE_LONG', 'CLOSE_SHORT']:
        return 'FLAT'
    else:
        return 'UNKNOWN'

def determine_position_from_external_value(external_position):
    """
    Convert external position value to position state.
    
    Args:
        external_position: Position from external system (negative=short, positive=long, 0=flat)
        
    Returns:
        str: 'FLAT', 'LONG', or 'SHORT'
    """
    if external_position is None:
        return 'UNKNOWN'
    elif external_position == 0:
        return 'FLAT'
    elif external_position > 0:
        return 'LONG'
    elif external_position < 0:
        return 'SHORT'
    else:
        return 'UNKNOWN'

def save_signal_to_database(signal_type, price, signal_time=None, currency="EUR.USD", signal_intent=None, external_position=None):
    """
    FIXED DUPLICATE PREVENTION: Prevents duplicate SignalIntent for NEXT TRADE ONLY.
    Can be overridden if external position indicates previous signal was incorrect.
    
    Args:
        signal_type: 'buy' or 'sell'
        price: Signal price
        signal_time: When signal occurred
        currency: Currency pair
        signal_intent: 'OPEN_LONG', 'CLOSE_LONG', 'OPEN_SHORT', 'CLOSE_SHORT', etc.
        external_position: External position to validate against (for override logic)
    
    Returns:
        bool: True if signal was saved, False on database error only
    """
    if signal_type == 'hold':
        return True
    
    # Save signal even without SignalIntent
    if not signal_intent:
        signal_intent = 'UNKNOWN_INTENT'
        debug_logger.warning(f"Saving {currency} signal with UNKNOWN_INTENT - this needs investigation")
        
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
        
        # CORRECT DUPLICATE PREVENTION: Check last signal for this currency only
        cursor.execute("""
        SELECT TOP 1 SignalIntent, SignalTime, Price
        FROM FXStrat_TradeSignalsSent 
        WHERE Currency = ?
        AND AlgoInstance = ?
        ORDER BY SignalTime DESC
        """, currency, ALGO_INSTANCE)
        
        last_signal = cursor.fetchone()
        
        # Check for duplicate SignalIntent for NEXT TRADE only
        if last_signal and last_signal[0] == signal_intent:
            # Check if external position indicates previous signal was incorrect
            position_override = False
            if external_position is not None:
                # Determine what position should be based on last SignalIntent
                expected_position = determine_expected_position_from_intent(last_signal[0])
                actual_position = determine_position_from_external_value(external_position)
                
                if expected_position != actual_position:
                    position_override = True
                    debug_logger.warning(
                        f"[POSITION_OVERRIDE] {currency} external position ({actual_position}) "
                        f"doesn't match expected ({expected_position}) from last SignalIntent ({last_signal[0]}). "
                        f"Allowing duplicate SignalIntent: {signal_intent}"
                    )
            
            if not position_override:
                time_diff = (signal_time - last_signal[1]).total_seconds() if last_signal[1] else 0
                debug_logger.info(
                    f"Prevented duplicate {currency} SignalIntent: {signal_intent} "
                    f"(last sent {time_diff:.0f}s ago at {last_signal[2]:.5f}). "
                    f"Next different SignalIntent will be allowed."
                )
                return True  # Block duplicate for next trade only
            else:
                # Position override occurred - mark signal as OVERWRITE
                signal_intent = f"{signal_intent}_OVERWRITE"
                debug_logger.warning(
                    f"[POSITION_OVERRIDE] {currency} SignalIntent marked as OVERWRITE: {signal_intent}"
                )
        
        # Insert the signal
        cursor.execute("""
        INSERT INTO FXStrat_TradeSignalsSent 
        (SignalTime, SignalType, Price, Currency, SignalIntent, AlgoInstance)
        VALUES (?, ?, ?, ?, ?, ?)
        """, 
        signal_time, signal_type, price, currency, signal_intent, ALGO_INSTANCE)
        
        conn.commit()
        
        # Enhanced logging
        debug_logger.warning(
            f"✅ {currency} {signal_type.upper()} signal SAVED: {signal_intent} at {price:.5f}"
        )
        
        return True
    
    except Exception as e:
        debug_logger.error(f"❌ CRITICAL ERROR saving {currency} signal to database: {e}", exc_info=True)
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
        if conn:
            conn.close()



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
        if conn:
            conn.close()



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
        tuple: (signal, currency) where signal is 'buy', 'sell', 'hold', or 'StaleData'
    """
    global historical_ticks, current_valid_zones_dict
    global trades, last_trade_time, last_loss_time, balance
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
        
        # Check indicator freshness - return StaleData if indicators are stale
        ind = external_indicators.get(currency, {})
        freshness_status = ind.get('freshness_status', 'missing')
        if freshness_status == 'stale':
            debug_logger.warning(f"[STALE_DATA] {currency} indicators are stale - returning StaleData signal")
            return ('StaleData', currency)
        elif freshness_status == 'missing':
            debug_logger.warning(f"[MISSING_DATA] {currency} indicators are missing - returning StaleData signal")
            return ('StaleData', currency)
        
        # POSITION VALIDATION: Check buffer period (detailed processing after price extraction)
        position_validation_result = None
        recovery_result = None
        
        if external_position is not None:
            # Check if we're still in buffer period after sending a signal
            current_time_obj = datetime.datetime.now()
            last_sent = last_signal_sent_time.get(currency)
            
            if last_sent and (current_time_obj - last_sent).total_seconds() < POSITION_BUFFER_SECONDS:
                remaining_buffer = POSITION_BUFFER_SECONDS - (current_time_obj - last_sent).total_seconds()
                signal_flow_logger.info(f"[BUFFER_ACTIVE] {currency} position ignored ({remaining_buffer:.1f}s remaining)")
                debug_logger.info(
                    f"[POSITION_BUFFER] {currency} ignoring position update - "
                    f"still in {remaining_buffer:.1f}s buffer period after signal"
                )
            else:
                # Outside buffer period - validate position (detailed processing after price extraction)
                position_validation_result = validate_position_against_signal_intent(currency, external_position)
        else:
            debug_logger.debug(f"[POSITION_VALIDATION] {currency} no external position provided")
        
        # Initialize if not already present - combine into one block for efficiency
        if currency not in historical_ticks:
            historical_ticks[currency] = pd.DataFrame()
            current_valid_zones_dict[currency] = {}
            trades[currency] = []
            last_trade_time[currency] = None
            last_loss_time[currency] = None
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

        # Extract tick time and price early for use throughout function
        tick_time = new_data_point.index[-1]
        tick_price = float(new_data_point['Price'].iloc[-1])
        
        # Handle position conflicts with intelligent recovery (now that tick_price is defined)
        if position_validation_result and position_validation_result['conflict_detected']:
            debug_logger.warning(
                f"[POSITION_CONFLICT] {currency} position conflict detected - "
                f"Expected: {position_validation_result['expected_position']}, "
                f"External: {position_validation_result['actual_position']} ({external_position})"
            )
            
            # Use intelligent recovery system
            recovery_result = reconcile_position_conflict(
                currency, 
                position_validation_result, 
                tick_price, 
                current_valid_zones_dict.get(currency, {})
            )
            
            debug_logger.warning(f"[RECOVERY] {currency} recovery action: {recovery_result.get('recovery_action')}")
        
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



        # 5) Manage existing trades (stop loss/take profit checks)
        closed_any_trade = manage_trades(tick_price, tick_time, currency)
        
        # Reduce verbosity of post-trade management logging
        if closed_any_trade:
            debug_logger.warning(f"TRADE CLOSED for {currency} at price {tick_price:.5f}")

        # 6) Re-validate after managing trades but with less logging
        validate_trade_state(tick_time, tick_price, currency)

        # 7) Zones are sourced externally, no bar data dependency needed
        # current_valid_zones_dict[currency] is populated by the external fetcher.

        # 10) Remove consecutive losers, invalidate zones
        current_valid_zones_dict[currency] = remove_consecutive_losers(trades[currency], current_valid_zones_dict[currency], currency)
        current_valid_zones_dict[currency] = invalidate_zones_via_sup_and_resist(tick_price, current_valid_zones_dict[currency], currency)

        # 8) Check tick processing buffer before attempting new trades
        trades_before = len(trades[currency])
        open_trades = [t for t in trades[currency] if t['status'] == 'open']
        entry_signal = None
        
        # Check tick processing buffer - prevent rapid signal generation
        current_time_obj = datetime.datetime.now()
        last_processed = last_tick_processed_time.get(currency)
        
        if (not open_trades and last_processed and 
            (current_time_obj - last_processed).total_seconds() < TICK_PROCESSING_BUFFER_SECONDS):
            remaining_buffer = TICK_PROCESSING_BUFFER_SECONDS - (current_time_obj - last_processed).total_seconds()
            signal_flow_logger.info(f"[TICK_BUFFER] {currency} new trade check skipped ({remaining_buffer:.1f}s remaining)")
        elif not open_trades:
            # Pass tick data directly to entry conditions
            entry_result = check_entry_conditions(
                tick_time, tick_price, current_valid_zones_dict[currency], currency
            )
            # Handle both old return format (zones only) and new format (zones, signal)
            if isinstance(entry_result, tuple) and len(entry_result) == 2:
                updated_zones, entry_signal = entry_result
                current_valid_zones_dict[currency] = updated_zones
                signal_flow_logger.info(f"[ENTRY_SIGNAL] {currency} entry conditions returned: {entry_signal}")
            elif entry_result is not None:
                current_valid_zones_dict[currency] = entry_result
        
        # Check for new trades AFTER entry conditions
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
        
        # PRIORITY 1: Handle recovery signals from position conflicts
        if recovery_result and recovery_result.get('recovery_action') != 'none':
            recovery_action = recovery_result.get('recovery_action', 'unknown')
            
            debug_logger.warning(f"[RECOVERY] {currency} recovery action: {recovery_action}")
            
            # Handle different recovery actions
            if recovery_result.get('recovery_action') == 'assume_trade':
                # Create trade object to track existing external position
                trade_details = recovery_result.get('trade_details', {})
                
                trades[currency].append({
                    'entry_time': tick_time,
                    'entry_price': trade_details['entry_price'],
                    'stop_loss': trade_details['stop_loss'],
                    'take_profit': trade_details['take_profit'],
                    'position_size': 1.0,  # Default size for recovery trades
                    'status': 'open',
                    'direction': trade_details['direction'],
                    'zone_start_price': trade_details['zone_data']['start_price'],
                    'zone_end_price': trade_details['zone_data']['end_price'],
                    'zone_length': trade_details['zone_data'].get('zone_size', 0),
                    'currency': currency,
                    'recovery_trade': True  # Mark as recovery trade
                })
                
                debug_logger.warning(
                    f"[RECOVERY_TRADE] {currency} created assumed {trade_details['direction']} trade:\n"
                    f"  Entry: {trade_details['entry_price']:.5f}\n"
                    f"  SL: {trade_details['stop_loss']:.5f}\n"
                    f"  TP: {trade_details['take_profit']:.5f}\n"
                    f"  NO API SIGNAL SENT - position already exists externally"
                )
                
                # CRITICAL: Don't send signal to API - position already exists!
                raw_signal = 'hold'  # Return hold to avoid duplicate position
                
            elif recovery_result.get('recovery_action') == 'force_close':
                # Send close signal to API AND track the close operation in memory
                raw_signal = recovery_result.get('recovery_signal', 'hold')
                expected_pos = position_validation_result['expected_position']
                actual_pos = position_validation_result['actual_position']
                
                # Create temporary trade object to track the close operation
                # This prevents duplicate close signals and provides audit trail
                close_direction = 'long' if actual_pos == 'LONG' else 'short'
                
                trades[currency].append({
                    'entry_time': tick_time,
                    'entry_price': tick_price,  # Current price as proxy
                    'stop_loss': 0,  # Not applicable for force close
                    'take_profit': 0,  # Not applicable for force close
                    'position_size': 1.0,
                    'status': 'closing',  # Special status for pending close
                    'direction': close_direction,
                    'zone_start_price': 0,
                    'zone_end_price': 0,
                    'zone_length': 0,
                    'currency': currency,
                    'force_close_trade': True,  # Mark as force close operation
                    'close_signal_sent': tick_time,
                    'close_reason': 'force_close_unwanted_position'
                })
                
                debug_logger.warning(
                    f"[RECOVERY_CLOSE] {currency} sending close signal: {raw_signal} to exit unwanted {close_direction} position\n"
                    f"  Created tracking trade object with status='closing'"
                )
                
            elif recovery_result.get('recovery_action') == 'align_state':
                # CRITICAL: Update memory state to match external reality
                expected_pos = position_validation_result['expected_position']
                actual_pos = position_validation_result['actual_position']
                
                if actual_pos == 'FLAT' and expected_pos in ['LONG', 'SHORT']:
                    # Close any phantom trades in memory to align with flat reality
                    open_trades = [t for t in trades[currency] if t['status'] == 'open']
                    for trade in open_trades:
                        trade.update({
                            'status': 'closed',
                            'exit_time': tick_time,
                            'exit_price': tick_price,
                            'profit_loss': 0,  # Unknown P&L for phantom trade
                            'close_reason': 'align_to_flat_reality'
                        })
                        debug_logger.warning(
                            f"[ALIGN_STATE] {currency} closed phantom {trade['direction']} trade - "
                            f"external system shows FLAT"
                        )
                    
                    # Also remove from DB position tracking
                    delete_current_position_from_db(currency)
                
                # Update signal state to match external reality
                if actual_pos == 'FLAT':
                    last_signals[currency] = 'hold'
                elif actual_pos == 'LONG':
                    last_signals[currency] = 'buy'
                elif actual_pos == 'SHORT':
                    last_signals[currency] = 'sell'
                
                debug_logger.warning(
                    f"[RECOVERY_ALIGN] {currency} memory state aligned to reality: {actual_pos}\n"
                    f"  Signal state updated to: {last_signals[currency]}"
                )
                raw_signal = 'hold'  # Return hold - external position already correct
        
        else:
            # PRIORITY 2: Normal trading logic
            # Store the signal state from before this tick processing
            signal_before_tick = last_signals.get(currency, 'hold')
            
            # If a new trade was opened, use the signal from entry conditions
            signal_flow_logger.info(f"[SIGNAL_DEBUG] {currency} new_trade_opened: {new_trade_opened}, entry_signal: {entry_signal}")
            if new_trade_opened and entry_signal:
                raw_signal = entry_signal  # Use the signal returned from check_entry_conditions
                signal_flow_logger.info(f"[SIGNAL_LOGIC] {currency} using signal from entry conditions: {raw_signal}")
                debug_logger.info(f"[SIGNAL] {currency} using signal from entry conditions: {raw_signal}")
            elif closed_any_trade:
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
                signal_flow_logger.info(f"[SIGNAL_LOGIC] {currency} no conditions met → hold")

        # 14) Block consecutive signals of the same type
        # This prevents duplicate buy/buy or sell/sell signals
        if raw_signal in ['buy', 'sell'] and last_non_hold_signal[currency] == raw_signal:
            signal_flow_logger.info(f"[DUPLICATE_BLOCKED] {currency} {raw_signal} → hold (last_non_hold: {last_non_hold_signal[currency]})")
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
                        abs(float(db_position['entry_price']) - float(memory_position['entry_price'])) > 0.00001):
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

        # Store the signal for database saving and logging, but don't update in-memory state yet
        signal_to_return = raw_signal
        
        # Only save to database and log if signal actually changed
        previous_signal = last_signals.get(currency, 'hold')
        if signal_to_return != previous_signal and signal_to_return not in ['hold']:
            # DETERMINE SIGNAL INTENT WITH ENHANCED VALIDATION
            try:
                position_before = get_current_position_state(currency)
            except Exception as e:
                debug_logger.error(f"[POSITION_STATE] Error getting position state for {currency}: {e}")
                position_before = 'FLAT'  # Default to FLAT on error
            
            # Determine position after based on signal and current state
            if signal_to_return == 'buy':
                if closed_any_trade:
                    # This is a close signal (closing short position)
                    position_after = 'FLAT'
                else:
                    # This is an open signal (opening long position)
                    position_after = 'LONG'
            elif signal_to_return == 'sell':
                if closed_any_trade:
                    # This is a close signal (closing long position)
                    position_after = 'FLAT'
                else:
                    # This is an open signal (opening short position)
                    position_after = 'SHORT'
            else:
                position_after = position_before  # No change for hold
            
            # Determine signal intent
            signal_intent = determine_signal_intent(signal_to_return, position_before, position_after)
            
            # Log validation issues but don't block saving
            if signal_intent == 'UNKNOWN':
                debug_logger.error(
                    f"[SIGNAL_INTENT] Invalid signal transition for {currency}: "
                    f"{signal_to_return} from {position_before} to {position_after} - SAVING ANYWAY"
                )
            
            # SAVE ALL NON-HOLD SIGNALS TO DATABASE - NO EXCEPTIONS
            if recovery_result and recovery_result.get('recovery_action') != 'none':
                # Recovery signal - don't save to DB, just send to API for position correction
                debug_logger.info(f"[NATURAL_CONVERGENCE] {currency} recovery signal NOT saved to DB - natural convergence will occur on next legitimate trade")
                success = True  # Mark as successful for buffer logic
            else:
                # Normal trading signal - save to DB as usual
                success = save_signal_to_database(signal_to_return, tick_price, tick_time, currency, signal_intent, external_position)
            
            # Signal timestamp is now set after return value creation for ALL signals
            
            if not success:
                debug_logger.error(f"❌ [SIGNAL_SAVE] CRITICAL: Failed to save {currency} signal: {signal_intent}")
            else:
                debug_logger.info(f"✅ [SIGNAL_SAVE] Successfully saved {currency} signal: {signal_intent}")

            # Enhanced signal logging
            debug_logger.warning(
                f"{currency} TRADE SIGNAL: {signal_to_return.upper()} at {tick_price:.5f}, Time: {tick_time}"
            )
            
            trade_logger.info(f"{currency} signal determined: {signal_to_return}")

        # CRITICAL: Return signal BEFORE updating in-memory state
        return_value = (signal_to_return, currency)
        
        # Set signal timestamp for ALL non-hold signals (for position buffer logic)
        if signal_to_return not in ['hold']:
            last_signal_sent_time[currency] = datetime.datetime.now()
            last_tick_processed_time[currency] = datetime.datetime.now()  # Set tick buffer
            signal_flow_logger.info(f"[API_RETURN] {currency} → {signal_to_return} (15s position buffer + 1.5s tick buffer activated)")
        
        # NOW update in-memory state after signal has been determined for return
        if signal_to_return != previous_signal:
            last_signals[currency] = signal_to_return
            if signal_to_return not in ['hold']:
                last_non_hold_signal[currency] = signal_to_return
                signal_flow_logger.info(f"[STATE_UPDATED] {currency} last_non_hold_signal → {signal_to_return}")
            # Signal state is managed in-memory only - no file persistence needed
        else:
            # Signal unchanged - just ensure state is current
            last_signals[currency] = signal_to_return

        return return_value

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
        
    global trades, balance, last_loss_time
    
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
                
                # Set cooldown after loss
                last_loss_time[currency] = current_time
                
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
        if conn:
            conn.close()



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
                    
                    # Determine expected signal state from SignalIntent (handles OVERWRITE variants)
                    base_intent = intent.replace('_OVERWRITE', '') if intent else ''
                    
                    if base_intent in ['OPEN_LONG', 'CLOSE_SHORT_OPEN_LONG']:
                        expected_signal = 'buy'  # Should be in long position
                    elif base_intent in ['OPEN_SHORT', 'CLOSE_LONG_OPEN_SHORT']:
                        expected_signal = 'sell'  # Should be in short position
                    elif base_intent in ['CLOSE_LONG', 'CLOSE_SHORT', 'SYSTEM_RESET']:
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
            # Signal state managed in-memory only - no file needed
        
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
        
        # Signal state managed in-memory only
        
        debug_logger.info("[TRADING_RESET] Trading state reset for live trading - all signals set to 'hold'")
        debug_logger.info("[TRADING_RESET] Live trading mode ENABLED - signal state will now be saved")
        debug_logger.info("[TRADING_RESET] Zones and indicators preserved from backtest")
        debug_logger.info(f"[TRADING_RESET] Fresh signal state: {last_signals}")
        
        return True
        
    except Exception as e:
        debug_logger.error(f"[TRADING_RESET] Error resetting trading state: {e}")
        return False