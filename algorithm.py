#######  v2

import pandas as pd
import numpy as np
import pandas_ta as ta
import logging
import pyodbc
import datetime

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
COOLDOWN_SECONDS = 3600
RISK_PROPORTION = 0.03

# Replace existing DB credential variables with a configuration dictionary
DB_CONFIG = {
    'server': '192.168.50.100',
    'database': 'TTG',
    'username': 'djaime',
    'password': 'Enrique30072000!',
    'driver': 'ODBC Driver 17 for SQL Server'
}

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

# Zone formation tracking
cumulative_zone_info = {currency: None for currency in SUPPORTED_CURRENCIES}

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
        if zone_id not in current_valid_zones_dict[currency]:
            current_valid_zones_dict[currency][zone_id] = zone_data
        
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

        bars[currency].loc[current_bar_start[currency]] = current_bar_data[currency]
        
        bar_count = len(bars[currency])
        rsi_value = None
        macd_value = None
        signal_value = None

        # If we have at least 35 bars, calculate indicators
        if bar_count >= 35:
            rolling_window_data = bars[currency].tail(384).copy()
            close_series = rolling_window_data["close"].dropna()
            if len(close_series) >= 35:
                rsi_vals = ta.rsi(close_series, length=14)
                macd_vals = ta.macd(close_series, fast=12, slow=26, signal=9)
                if rsi_vals is not None and not rsi_vals.dropna().empty:
                    rsi_value = float(rsi_vals.iloc[-1])
                if macd_vals is not None and not macd_vals.dropna().empty:
                    macd_value = float(macd_vals["MACD_12_26_9"].iloc[-1])
                    signal_value = float(macd_vals["MACDs_12_26_9"].iloc[-1])

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
        rsi_str = "Not enough data" if rsi_value is None else f"{rsi_value:.2f}"
        macd_str = "Not enough data" if macd_value is None else f"{macd_value:.2f}"
        
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
    # Keep only ONE global declaration
    global cumulative_zone_info
    
    # Add diagnostic logging at the start - simplified
    debug_logger.info(f"Identifying zones for {currency}: Processing {len(data)} bars, existing zones: {len(current_valid_zones_dict)}")
    
    # Ensure currency is initialized in cumulative_zone_info
    if currency not in cumulative_zone_info:
        cumulative_zone_info[currency] = None
    
    data = data.copy()
    new_zones_detected = 0
    new_zones_for_sql = []

    # Initialize columns
    data.loc[:, 'Liquidity_Zone'] = np.nan
    data.loc[:, 'Zone_Size'] = np.nan
    data.loc[:, 'Zone_Start_Price'] = np.nan
    data.loc[:, 'Zone_End_Price'] = np.nan
    data.loc[:, 'Zone_Length'] = np.nan
    data.loc[:, 'zone_type'] = ''
    data.loc[:, 'Confirmation_Time'] = pd.NaT

    # Calculate dynamic PIPS_REQUIRED based on ATR (use pre-calculated value if provided)
    if pre_calculated_atr is not None:
        dynamic_pips_required = pre_calculated_atr
    else:
        dynamic_pips_required = calculate_atr_pips_required(data, currency)
    
    debug_logger.info(f"Dynamic pip threshold: {dynamic_pips_required*10000:.1f} pips")

    # Add candle direction column
    data['candle_direction'] = np.where(data['close'] > data['open'], 'green', 
                                      np.where(data['close'] < data['open'], 'red', 'doji'))

    # Track new zones detected in this run
    new_zones_detected = 0
    new_zones_for_sql = []  # List to store new zones for SQL insertion

    # If we have a zone in progress from previous window, restore it
    current_run = None
    if cumulative_zone_info[currency] is not None:
        current_run = cumulative_zone_info[currency]
        if current_run['start_index'] < len(data):
            current_run['start_index'] = 0
        else:
            current_run = None

    # Only log the first few candles to reduce logging overhead
    for i in range(len(data)):
        # On the first few candles, log values to understand what's happening
        if i < 3:  # Reduced from 5 to 3
            debug_logger.info(f"Candle {i}: Open={data.iloc[i]['open']:.5f}, Close={data.iloc[i]['close']:.5f}, Direction={data.iloc[i]['candle_direction']}")
        
        current_candle = data.iloc[i]
        
        if current_run is None:
            # Start new run
            current_run = {
                'start_index': i,
                'start_price': current_candle['open'],
                'direction': current_candle['candle_direction'],
                'high': current_candle['high'],
                'low': current_candle['low'],
                'start_time': data.index[i]
            }
            continue

        # Check if this candle continues the run
        if current_candle['candle_direction'] == current_run['direction'] and \
           current_candle['candle_direction'] != 'doji':
            
            # Update run's high/low
            current_run['high'] = max(current_run['high'], current_candle['high'])
            current_run['low'] = min(current_run['low'], current_candle['low'])
            
            # Calculate total movement
            if current_run['direction'] == 'green':
                total_move = current_run['high'] - current_run['start_price']
            else:  # red
                total_move = current_run['start_price'] - current_run['low']

            # Check if movement meets dynamic pip requirement
            if abs(total_move) >= dynamic_pips_required:
                # Log zone qualification less verbosely
                debug_logger.info(f"ZONE QUALIFICATION: Move={abs(total_move)*10000:.1f} pips >= Required={dynamic_pips_required*10000:.1f} pips")
                zone_type = 'demand' if current_run['direction'] == 'green' else 'supply'
                
                if zone_type == 'demand':
                    zone_start = current_run['low']
                    zone_end = current_run['high']
                else:
                    zone_start = current_run['high']
                    zone_end = current_run['low']

                zone_id = (zone_start, zone_end)
                
                if zone_id not in current_valid_zones_dict:
                    # Create zone object with all required fields
                    zone_object = {
                        'start_price': zone_start,
                        'end_price': zone_end,
                        'zone_size': abs(zone_end - zone_start),
                        'confirmation_time': data.index[i],
                        'zone_type': zone_type,
                        'start_time': current_run['start_time']
                    }
                    
                    # Add to local dictionary and ensure key is (float, float) tuple
                    current_valid_zones_dict[zone_id] = zone_object
                    
                    new_zones_detected += 1
                    
                    # Queue for batched DB operation instead of immediate save
                    new_zones_for_sql.append({
                        'currency': currency,
                        'zone_start_price': zone_start,
                        'zone_end_price': zone_end,
                        'zone_size': abs(zone_end - zone_start),
                        'zone_type': zone_type,
                        'confirmation_time': data.index[i]
                    })

                    # Mark the zone in the data
                    zone_indices = slice(current_run['start_index'], i + 1)
                    data.loc[data.index[zone_indices], 'Liquidity_Zone'] = zone_start
                    data.loc[data.index[zone_indices], 'Zone_Size'] = abs(zone_end - zone_start)
                    data.loc[data.index[zone_indices], 'Zone_Start_Price'] = zone_start
                    data.loc[data.index[zone_indices], 'Zone_End_Price'] = zone_end
                    data.loc[data.index[zone_indices], 'Zone_Length'] = i - current_run['start_index'] + 1
                    data.loc[data.index[zone_indices], 'zone_type'] = zone_type
                    data.loc[data.index[zone_indices], 'Confirmation_Time'] = data.index[i]

                    # Enhanced logging for new zone detection - simplified
                    debug_logger.warning(
                        f"NEW {currency} {zone_type.upper()} ZONE: Start={zone_start:.5f}, End={zone_end:.5f}, "
                        f"Size={abs(zone_end - zone_start)*10000:.1f} pips, Time={data.index[i]}"
                    )

                # Reset run after creating zone
                current_run = None
            # Log zone rejections less verbosely
            elif abs(total_move) >= dynamic_pips_required * 0.7:  # Only log rejections close to threshold
                debug_logger.info(f"ZONE REJECTED: Move={abs(total_move)*10000:.1f} pips < Required={dynamic_pips_required*10000:.1f} pips")

        else:
            # Direction changed, reset the run
            current_run = {
                'start_index': i,
                'start_price': current_candle['open'],
                'direction': current_candle['candle_direction'],
                'high': current_candle['high'],
                'low': current_candle['low'],
                'start_time': data.index[i]
            }

    # Save current run state for next window
    cumulative_zone_info[currency] = current_run
    
    # Log zone status after processing only if new zones were detected
    if new_zones_detected > 0:
        log_zone_status("After Zone Detection", currency)

    # After all zone identification, batch store the new zones in SQL
    if new_zones_for_sql:
        store_zones_in_sql(new_zones_for_sql)

    # Log results more concisely
    debug_logger.info(f"ZONE DETECTION RESULTS: New zones: {new_zones_detected}, Total: {len(current_valid_zones_dict)}")
    
    # Debug the zones that have been successfully constructed
    if len(current_valid_zones_dict) > 0:
        debug_logger.warning(f"Current zones (count: {len(current_valid_zones_dict)}):")
        for zone_key, zone_data in list(current_valid_zones_dict.items())[:5]:  # Show up to 5 zones
            if isinstance(zone_data, dict) and 'zone_type' in zone_data:
                debug_logger.warning(f"  {zone_data['zone_type'].upper()} zone: {zone_key[0]:.5f}-{zone_key[1]:.5f}")
            else:
                debug_logger.warning(f"  Invalid zone structure: {zone_key}")

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
    debug_logger.warning(f"  RSI: {data['RSI'].iloc[bar_index]:.2f}")
    debug_logger.warning(f"  MACD: {data['MACD_Line'].iloc[bar_index]:.6f}")
    debug_logger.warning(f"  Signal: {data['Signal_Line'].iloc[bar_index]:.6f}")
    
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
            
            # Check direction and indicators
            if zone_type == 'demand' and current_price < zone_end:
                # FIX: Make explicit RSI/MACD checks and log each condition
                rsi_condition = data['RSI'].iloc[bar_index] < DEMAND_ZONE_RSI_TOP
                macd_condition = data['MACD_Line'].iloc[bar_index] > data['Signal_Line'].iloc[bar_index]
                
                debug_logger.warning(f"    DEMAND zone conditions:")
                debug_logger.warning(f"      RSI < {DEMAND_ZONE_RSI_TOP}: {rsi_condition} (value: {data['RSI'].iloc[bar_index]:.2f})")
                debug_logger.warning(f"      MACD > Signal: {macd_condition} (MACD: {data['MACD_Line'].iloc[bar_index]:.6f}, Signal: {data['Signal_Line'].iloc[bar_index]:.6f})")
                
                if not (rsi_condition or macd_condition):
                    debug_logger.warning("    Skipping zone: RSI/MACD conditions not met")
                    continue

                entry_price = current_price
                stop_loss = entry_price - (STOP_LOSS_PROPORTION * zone_size)
                take_profit = entry_price + (TAKE_PROFIT_PROPORTION * zone_size)
            
            elif zone_type == 'supply' and current_price > zone_end:
                # FIX: Make explicit RSI/MACD checks and log each condition
                rsi_condition = data['RSI'].iloc[bar_index] > SUPPLY_ZONE_RSI_BOTTOM
                macd_condition = data['MACD_Line'].iloc[bar_index] < data['Signal_Line'].iloc[bar_index]
                
                debug_logger.warning(f"    SUPPLY zone conditions:")
                debug_logger.warning(f"      RSI > {SUPPLY_ZONE_RSI_BOTTOM}: {rsi_condition} (value: {data['RSI'].iloc[bar_index]:.2f})")
                debug_logger.warning(f"      MACD < Signal: {macd_condition} (MACD: {data['MACD_Line'].iloc[bar_index]:.6f}, Signal: {data['Signal_Line'].iloc[bar_index]:.6f})")
                
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
                
                trades_for_currency.append({
                    'entry_time': current_time,
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'take_profit': take_profit,
                    'position_size': position_size,
                    'status': 'open',
                    'direction': 'long' if zone_type == 'demand' else 'short',
                    'zone_start_price': zone_start,
                    'zone_end_price': zone_end,
                    'zone_length': zone_data.get('zone_length', 0),
                    'required_pips': current_pips_required,  # Added for analysis
                    'currency': currency  # Store the currency for reference
                })

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


def manage_trades(current_price, current_time, currency="EUR.USD"):
    """
    Enhanced version with better state tracking and validation.
    Manages open trades for stop loss and take profit.
    
    Args:
        current_price: The current market price
        current_time: The current timestamp
        currency: The currency pair to manage trades for
    """
    global trades, balance
    debug_logger.info(f"Managing trades at {current_time} price {current_price}")
    
    # Validate trade state before processing
    validate_trade_state(current_time, current_price, currency)
    log_trade_state(current_time, "before_manage", currency)
    
    closed_any_trade = False
    trades_to_process = [t for t in trades[currency] if t['status'] == 'open']
    
    for trade in trades_to_process:
        # Skip if trade is already closed (shouldn't happen but safe check)
        if trade.get('status') != 'open':
            continue
            
        # Track if this specific trade was closed in this iteration
        trade_closed = False
        
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
                trade_logger.info(f"{currency} Short trade hit take profit at {trade['take_profit']}. P/L: {trade_result}")
                closed_any_trade = True
                trade_closed = True

    # Validate trade state after processing
    validate_trade_state(current_time, current_price, currency)
    log_trade_state(current_time, "after_manage", currency)
    
    # Add at the end before returning:
    debug_logger.warning(f"\n\nMANAGE_TRADES RESULT for {currency}:")
    debug_logger.warning(f"  Current price: {current_price:.5f}")
    debug_logger.warning(f"  Open trades before: {len(trades_to_process)}")
    debug_logger.warning(f"  Trades closed: {closed_any_trade}")
    if closed_any_trade:
        debug_logger.warning(f"  >>> IMPORTANT: Trade was closed but did it generate a signal?")
    
    return closed_any_trade

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
    # Check for existing connection
    if not hasattr(get_db_connection, "_connection") or get_db_connection._connection is None:
        # Create new connection if none exists
        conn_str = (
            f"Driver={{{DB_CONFIG['driver']}}};"
            f"Server={DB_CONFIG['server']};"
            f"Database={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']};"
        )
        
        try:
            get_db_connection._connection = pyodbc.connect(conn_str)
            debug_logger.info(f"Successfully connected to {DB_CONFIG['database']} on {DB_CONFIG['server']}")
        except Exception as e:
            debug_logger.error(f"Database connection error: {e}", exc_info=True)
            get_db_connection._connection = None
    
    # Test if connection is still valid, reconnect if needed
    if get_db_connection._connection is not None:
        try:
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
                conn_str = (
                    f"Driver={{{DB_CONFIG['driver']}}};"
                    f"Server={DB_CONFIG['server']};"
                    f"Database={DB_CONFIG['database']};"
                    f"UID={DB_CONFIG['username']};"
                    f"PWD={DB_CONFIG['password']};"
                )
                get_db_connection._connection = pyodbc.connect(conn_str)
            except Exception as reconnect_error:
                debug_logger.error(f"Failed to reconnect: {reconnect_error}")
                get_db_connection._connection = None
    
    return get_db_connection._connection

# Updated function to close the connection when needed
def close_db_connection():
    """Explicitly close the database connection when shutting down"""
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
            CreatedAt DATETIME DEFAULT GETDATE()
        );
        
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_FXStrat_TradeSignalsSent_SignalTime')
        CREATE INDEX IX_FXStrat_TradeSignalsSent_SignalTime ON FXStrat_TradeSignalsSent(SignalTime);
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
    
    # Get indicators if available
    rsi_value = None
    macd_value = None
    signal_value = None
    
    if currency in bars and len(bars[currency]) >= 35:
        try:
            recent_data = bars[currency].tail(384).copy()
            close_series = recent_data["close"].dropna()
            
            if len(close_series) >= 35:
                rsi_values = ta.rsi(close_series, length=14)
                macd_values = ta.macd(close_series, fast=12, slow=26, signal=9)
                
                if rsi_values is not None and not rsi_values.dropna().empty:
                    rsi_value = float(rsi_values.iloc[-1])
                    
                if macd_values is not None and not macd_values.dropna().empty:
                    macd_value = float(macd_values["MACD_12_26_9"].iloc[-1])
                    signal_value = float(macd_values["MACDs_12_26_9"].iloc[-1])
        except Exception as e:
            debug_logger.error(f"Error calculating indicators for {currency} DB: {e}")
    
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
         ZonesCount, RSI, MACD_Line, Signal_Line)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        signal_value
        )
        
        conn.commit()
        debug_logger.info(f"Saved {currency} bar to database: {bar_start_time}")
        return True
    
    except Exception as e:
        debug_logger.error(f"Error saving {currency} bar to database: {e}", exc_info=True)
        return False
    
    finally:
        if conn:
            conn.close()

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

def save_signal_to_database(signal_type, price, signal_time=None, currency="EUR.USD"):
    """
    Saves a trade signal to the database.
    
    Args:
        signal_type: Type of signal (buy, sell, close)
        price: The price when the signal was generated
        signal_time: The time when the signal was generated
        currency: The currency pair this signal belongs to
    """
    if signal_type == 'hold':
        return True  # Don't log 'hold' signals
        
    if signal_time is None:
        signal_time = datetime.datetime.now()
    
    # Format price to 5 decimals as requested
    price = round(price, 5)
    
    conn = None
    cursor = None    
    try:
        conn = get_db_connection()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO FXStrat_TradeSignalsSent 
        (SignalTime, SignalType, Price, Currency)
        VALUES (?, ?, ?, ?)
        """, 
        signal_time,
        signal_type,
        price,
        currency
        )
        
        conn.commit()
        debug_logger.info(f"Saved {currency} signal to database: {signal_type} at {price}")
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
        if conn:
            try:
                conn.close()
            except:
                pass

# --------------------------------------------------------------------------
# SQL DATA STORAGE FUNCTIONS
# --------------------------------------------------------------------------
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
    
    # Get optional indicator values with defaults
    rsi_value = bar_data.get('rsi', None)
    macd_value = bar_data.get('macd_line', None)
    signal_value = bar_data.get('signal_line', None)
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
         ZonesCount, RSI, MACD_Line, Signal_Line)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        signal_value
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
        if conn:
            try:
                conn.close()
            except:
                pass

def store_zones_in_sql(zones_data):
    """
    Stores multiple liquidity zones in the SQL database.
    
    Args:
        zones_data (list): List of zone dictionaries
            
    Returns:
        bool: True if all zones were stored successfully, False otherwise
    """
    if not zones_data or not isinstance(zones_data, list):
        debug_logger.error("Invalid zones data provided to store_zones_in_sql")
        return False
    
    if len(zones_data) == 0:
        debug_logger.info("No zones to store in SQL database")
        return True
    
    # Track success/failure for each zone
    success_count = 0
    failure_count = 0
    
    # Required fields for each zone
    required_fields = ['currency', 'zone_start_price', 'zone_end_price', 
                       'zone_size', 'zone_type']
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        
        for zone in zones_data:
            # Validate zone data
            is_valid = True
            for field in required_fields:
                if field not in zone:
                    debug_logger.error(f"Missing required field '{field}' in zone data")
                    is_valid = False
                    
            if not is_valid:
                failure_count += 1
                continue
                
            try:
                confirmation_time = zone.get('confirmation_time', None)
                
                # Format zone prices and size to 6 decimals as requested
                zone_start_price = round(zone['zone_start_price'], 6)
                zone_end_price = round(zone['zone_end_price'], 6)
                zone_size = round(zone['zone_size'], 6)
                
                cursor.execute("""
                INSERT INTO FXStrat_AlgorithmZones 
                (Currency, ZoneStartPrice, ZoneEndPrice, ZoneSize, ZoneType, ConfirmationTime)
                VALUES (?, ?, ?, ?, ?, ?)
                """, 
                zone['currency'],
                zone_start_price,
                zone_end_price,
                zone_size,
                zone['zone_type'],
                confirmation_time
                )
                
                success_count += 1
                
            except Exception as e:
                debug_logger.error(f"Error inserting zone: {e}")
                failure_count += 1
        
        # Commit all successful inserts
        conn.commit()
        
        debug_logger.info(f"Zone storage complete: {success_count} succeeded, {failure_count} failed")
        return failure_count == 0  # Only return True if all succeeded
    
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
            try:
                conn.close()
            except:
                pass

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
def process_market_data(new_data_point, currency="EUR.USD"):
    """
    Process new market data for a specific currency.
    Optimized for better performance with multi-currency support.
    
    Args:
        new_data_point: DataFrame containing the new tick data
        currency: The currency pair to process (default: EUR.USD)
        
    Returns:
        tuple: (signal, currency) where signal is 'buy', 'sell', 'close', or 'hold'
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
        if raw_price < 0.5 or raw_price > 1.5:
            debug_logger.error(f"Received {currency} price ({raw_price}) outside normal range [0.5, 1.5]. Ignoring.")
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
        
        # If a bar was finalized, store it and log zones
        if finalized_bar_data:
            # Store the finalized bar in SQL
            store_bar_in_sql(finalized_bar_data)
            
            # Log zone status after bar completion only if we have zones
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

        # 8) Calculate indicators (RSI, MACD)
        rolling_window_data['RSI'] = ta.rsi(rolling_window_data['close'], length=14)
        macd = ta.macd(rolling_window_data['close'], fast=12, slow=26, signal=9)
        
        if macd is None or macd.empty:
            return ('hold', currency)

        macd.dropna(inplace=True)
        if macd.empty:
            return ('hold', currency)

        rolling_window_data['MACD_Line'] = macd['MACD_12_26_9']
        rolling_window_data['Signal_Line'] = macd['MACDs_12_26_9']

        # 9) Identify and invalidate zones (CUMULATIVE logic for zone detection)
        # Calculate ATR once and reuse it for better performance
        atr_value = calculate_atr_pips_required(rolling_window_data, currency)
        
        # FIX: Store the return value properly
        rolling_window_data, zones_for_currency = identify_liquidity_zones(
            rolling_window_data, current_valid_zones_dict[currency], currency, atr_value
        )

        # FIX: Make sure we're properly assigning the zones dictionary
        if zones_for_currency is not None:  
            current_valid_zones_dict[currency] = zones_for_currency
            debug_logger.info(f"Updated zones dictionary for {currency}, now has {len(current_valid_zones_dict[currency])} zones")

        rolling_window_data = set_support_resistance_lines(rolling_window_data, currency)
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

        # -----------------------------------------------------------------
        # 13) Determine final signal: buy, sell, close, or 'hold'
        # -----------------------------------------------------------------
        if closed_any_trade:
            # Only generate close signal if we haven't already generated one for this trade
            if last_non_hold_signal[currency] not in ['close']:
                raw_signal = 'close'
            else:
                raw_signal = 'hold'
        elif new_trade_opened:
            # Figure out direction of the newly opened trade
            if trades[currency][-1]['direction'] == 'long':
                raw_signal = 'buy'
            else:
                raw_signal = 'sell'
        else:
            raw_signal = 'hold'

        # 14) Block consecutive opens (either 'buy' or 'sell')
        if raw_signal in ['buy', 'sell'] and last_non_hold_signal[currency] in ['buy', 'sell']:
            # Already had a 'buy' or 'sell' before, so return hold
            raw_signal = 'hold'

        # 15) Update the last_non_hold_signal if we have a real signal
        if raw_signal not in ['hold']:
            last_non_hold_signal[currency] = raw_signal
            
            # SAVE NON-HOLD SIGNAL TO DATABASE
            save_signal_to_database(raw_signal, tick_price, tick_time, currency)

            # Enhanced signal logging - but less verbose
            debug_logger.warning(
                f"{currency} TRADE SIGNAL: {raw_signal.upper()} at {tick_price:.5f}, Time: {tick_time}"
            )

        # 16) Log only non-hold signals
        if raw_signal not in ['hold']:
            trade_logger.info(f"{currency} signal determined: {raw_signal}")

        return (raw_signal, currency)

    except Exception as e:
        debug_logger.error(f"Error in process_market_data for {currency}: {e}", exc_info=True)
        return ('hold', currency)

# --------------------------------------------------------------------------
# OPTIONAL: Warmup script integration
# --------------------------------------------------------------------------
def main():
    """
    Initialize the algorithm with database setup and historical data.
    Loads data for all supported currencies and sets up initial state.
    """
    try:
        # Initialize database first
        db_initialized = initialize_database()
        if not db_initialized:
            debug_logger.error("Database initialization failed, but continuing with algorithm startup")
        else:
            debug_logger.info("Database successfully initialized")
        
        # Import warmup_data function with currency-specific handling capabilities
        try:
            from secondary_warmer_script import warmup_data
            
            # Process each supported currency individually to handle both old and new return formats
            for currency in SUPPORTED_CURRENCIES:
                try:
                    debug_logger.info(f"Initializing data for {currency}")
                    
                    # Call warmup_data with explicit currency to get data for just this currency
                    result = warmup_data(currency)
                    
                    # Check the type of the result to handle different return formats
                    if isinstance(result, tuple) and len(result) == 2:
                        # Unpack the result - could be (DataFrame, dict) or (dict, dict)
                        bars_data, zones_data = result
                        
                        # Check if we received dictionaries of currencies or direct data
                        if isinstance(bars_data, dict) and bars_data and isinstance(next(iter(bars_data.keys()) if bars_data else None), str):
                            # We received a dictionary with DataFrames for each currency
                            if currency in bars_data:
                                precomputed_bars = bars_data[currency]
                                precomputed_zones = zones_data[currency]
                                debug_logger.info(f"Extracted {currency} data from multi-currency dictionary")
                            else:
                                debug_logger.warning(f"Currency {currency} not found in returned data")
                                continue
                        else:
                            # We received direct (DataFrame, dict) data for the requested currency
                            precomputed_bars = bars_data
                            precomputed_zones = zones_data
                            debug_logger.info(f"Received direct data for {currency}")
                    else:
                        debug_logger.error(f"Unexpected result type from warmup_data for {currency}")
                        continue
                    
                    # Skip empty data
                    if isinstance(precomputed_bars, pd.DataFrame) and precomputed_bars.empty:
                        debug_logger.warning(f"Empty bars dataset received for {currency}")
                        continue
                        
                    # Load the data into algorithm state
                    load_preexisting_bars_and_indicators(precomputed_bars, currency)
                    load_preexisting_zones(precomputed_zones, currency)
                    
                    # Get the latest indicator values for logging
                    if isinstance(precomputed_bars, pd.DataFrame) and not precomputed_bars.empty:
                        latest_rsi = precomputed_bars['RSI'].iloc[-1] if 'RSI' in precomputed_bars.columns and not precomputed_bars['RSI'].empty else "N/A"
                        latest_macd = precomputed_bars['MACD_Line'].iloc[-1] if 'MACD_Line' in precomputed_bars.columns and not precomputed_bars['MACD_Line'].empty else "N/A"
                        latest_signal = precomputed_bars['Signal_Line'].iloc[-1] if 'Signal_Line' in precomputed_bars.columns and not precomputed_bars['Signal_Line'].empty else "N/A"
                        
                        # Enhanced logging with better formatting
                        debug_logger.warning(
                            f"\n\n{'='*30} {currency} ALGORITHM WARMUP COMPLETE {'='*30}\n"
                            f"Loaded {len(precomputed_bars)} historical bars\n"
                            f"Initialized {len(current_valid_zones_dict[currency])} liquidity zones\n\n"
                            f"Latest Indicators:\n"
                            f"  * RSI: {latest_rsi}\n"
                            f"  * MACD: {latest_macd}\n"
                            f"  * Signal: {latest_signal}\n"
                            f"{'='*80}\n"
                        )
                        
                        # Log initial zone status
                        if len(current_valid_zones_dict[currency]) > 0:
                            log_zone_status("Initial Warmup", currency)
                
                except Exception as e:
                    debug_logger.error(f"Error warming up {currency}: {e}", exc_info=True)
                    debug_logger.warning(f"\n\nInitializing empty structures for {currency} due to error\n")
            
            trade_logger.info("Warm-up complete. Ready for live ticks.")
            
        except ImportError:
            debug_logger.warning("\n\nsecondary_warmer_script not found. Running with empty bars and zones.\n")
            
    except Exception as e:
        debug_logger.error(f"Unexpected error in main initialization: {e}", exc_info=True)
        debug_logger.warning("Algorithm continuing with minimal initialization")