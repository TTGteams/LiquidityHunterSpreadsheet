#######   Live algo using ATR, Min zone size, PROFIBALE 

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
# ENHANCED LOGGING FUNCTIONS
# --------------------------------------------------------------------------
def log_zone_status(location="", currency="EUR.USD"):
    """
    Creates a nicely formatted table of zone information in the debug logs.
    Displays up to 10 most recent zones with their type, price levels, and size in pips.
    
    Args:
        location (str): A string identifying where this log is coming from
        currency (str): The currency pair to log zones for
    """
    # Initialize if not already
    if currency not in current_valid_zones_dict:
        current_valid_zones_dict[currency] = {}
        
    if not current_valid_zones_dict[currency]:
        debug_logger.warning(f"\n\nZONE STATUS for {currency} ({location}): No active zones found\n")
        return
    
    # Convert zones to a list of dictionaries with extra info for sorting
    zone_list = []
    for zone_id, zone_data in current_valid_zones_dict[currency].items():
        zone_info = {
            'start_price': zone_data['start_price'],
            'end_price': zone_data['end_price'],
            'zone_size': zone_data['zone_size'],
            'zone_type': zone_data['zone_type'],
            'confirmation_time': zone_data.get('confirmation_time', None),
        }
        zone_list.append(zone_info)
    
    # Sort zones by confirmation time (most recent first)
    sorted_zones = sorted(
        zone_list, 
        key=lambda x: x['confirmation_time'] if x['confirmation_time'] is not None else pd.Timestamp.min,
        reverse=True
    )
    
    # Take top 10 most recent zones
    recent_zones = sorted_zones[:10]
    
    # Prepare the formatted table header
    header = f"\n\n{'='*80}\n"
    header += f"ZONE STATUS for {currency} ({location}) - {len(current_valid_zones_dict[currency])} Active Zones - "
    header += f"Showing {len(recent_zones)} Most Recent\n"
    header += f"{'='*80}\n\n"
    
    # Add table headers
    table = f"{'ZONE TYPE':<10} | {'START PRICE':<12} | {'END PRICE':<12} | {'SIZE (PIPS)':<12} | {'TIME DETECTED':<19}\n"
    table += f"{'-'*10} | {'-'*12} | {'-'*12} | {'-'*12} | {'-'*19}\n"
    
    # Add zone data rows
    for zone in recent_zones:
        zone_type = zone['zone_type'].upper()
        confirmation_time = zone['confirmation_time'].strftime('%Y-%m-%d %H:%M') if zone['confirmation_time'] else 'N/A'
        table += f"{zone_type:<10} | {zone['start_price']:<12.5f} | {zone['end_price']:<12.5f} | "
        table += f"{zone['zone_size']*10000:<12.1f} | {confirmation_time:<19}\n"
    
    # Log the complete table
    debug_logger.warning(header + table + "\n")

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
# ZONE DETECTION (CUMULATIVE LOGIC)
# --------------------------------------------------------------------------
def calculate_atr_pips_required(data, currency="EUR.USD"):
    """
    Calculate the ATR-based pips required using a 4-day window.
    Returns a value between 0.002 (20 pips) and 0.018 (180 pips).
    
    Args:
        data: DataFrame containing OHLC data
        currency: The currency pair to calculate for
    """
    # Calculate True Range
    data = data.copy()
    data['high'] = pd.to_numeric(data['high'])
    data['low'] = pd.to_numeric(data['low'])
    data['close'] = pd.to_numeric(data['close'])
    
    data['prev_close'] = data['close'].shift(1)
    
    # True Range calculation
    data['tr1'] = abs(data['high'] - data['low'])
    data['tr2'] = abs(data['high'] - data['prev_close'])
    data['tr3'] = abs(data['low'] - data['prev_close'])
    
    data['true_range'] = data[['tr1', 'tr2', 'tr3']].max(axis=1)
    
    # Calculate 4-day ATR (96 periods for 15-min bars)
    atr = data['true_range'].rolling(window=96).mean().iloc[-1]
    
    # Convert ATR to pips required (bounded between 20 and 180 pips)
    pips_required = min(max(atr, 0.002), 0.018)  # 0.002 = 20 pips, 0.018 = 180 pips
    
    debug_logger.info(f"\n\n{currency} ATR-based PIPS_REQUIRED: {pips_required:.5f} ({pips_required*10000:.1f} pips)\n")
    
    return pips_required


def identify_liquidity_zones(data, current_valid_zones_dict, currency="EUR.USD"):
    """
    Identifies liquidity zones based on consecutive directional price moves within a rolling window.
    Now uses dynamic ATR-based PIPS_REQUIRED.
    
    Args:
        data: DataFrame containing OHLC and indicator data
        current_valid_zones_dict: Dictionary of current valid zones for the currency
        currency: The currency pair to identify zones for
    """
    global cumulative_zone_info
    data = data.copy()

    # Initialize if not already
    if currency not in cumulative_zone_info:
        cumulative_zone_info[currency] = None

    # Initialize columns
    data.loc[:, 'Liquidity_Zone'] = np.nan
    data.loc[:, 'Zone_Size'] = np.nan
    data.loc[:, 'Zone_Start_Price'] = np.nan
    data.loc[:, 'Zone_End_Price'] = np.nan
    data.loc[:, 'Zone_Length'] = np.nan
    data.loc[:, 'zone_type'] = ''
    data.loc[:, 'Confirmation_Time'] = pd.NaT

    # Calculate dynamic PIPS_REQUIRED based on ATR
    PIPS_REQUIRED = calculate_atr_pips_required(data, currency)

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

    for i in range(len(data)):
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
            if abs(total_move) >= PIPS_REQUIRED:
                zone_type = 'demand' if current_run['direction'] == 'green' else 'supply'
                
                if zone_type == 'demand':
                    zone_start = current_run['low']
                    zone_end = current_run['high']
                else:
                    zone_start = current_run['high']
                    zone_end = current_run['low']

                zone_id = (zone_start, zone_end)
                
                if zone_id not in current_valid_zones_dict:
                    confirmation_time = data.index[i]
                    zone_data = {
                        'start_price': zone_start,
                        'end_price': zone_end,
                        'zone_size': abs(zone_end - zone_start),
                        'confirmation_time': confirmation_time,
                        'zone_type': zone_type,
                        'start_time': current_run['start_time']
                    }
                    
                    current_valid_zones_dict[zone_id] = zone_data
                    new_zones_detected += 1
                    
                    # SAVE NEW ZONE TO DATABASE
                    save_zone_to_database(zone_id, zone_data, currency)

                    # Mark the zone in the data
                    zone_indices = slice(current_run['start_index'], i + 1)
                    data.loc[data.index[zone_indices], 'Liquidity_Zone'] = zone_start
                    data.loc[data.index[zone_indices], 'Zone_Size'] = abs(zone_end - zone_start)
                    data.loc[data.index[zone_indices], 'Zone_Start_Price'] = zone_start
                    data.loc[data.index[zone_indices], 'Zone_End_Price'] = zone_end
                    data.loc[data.index[zone_indices], 'Zone_Length'] = i - current_run['start_index'] + 1
                    data.loc[data.index[zone_indices], 'zone_type'] = zone_type
                    data.loc[data.index[zone_indices], 'Confirmation_Time'] = confirmation_time

                    # Enhanced logging for new zone detection
                    debug_logger.warning(
                        f"\n\n{'*'*20} NEW {currency} {zone_type.upper()} ZONE DETECTED {'*'*20}\n"
                        f"Start Price: {zone_start:.5f}\n"
                        f"End Price: {zone_end:.5f}\n"
                        f"Size: {abs(zone_end - zone_start)*10000:.1f} pips (Required: {PIPS_REQUIRED*10000:.1f} pips)\n"
                        f"Candles in Formation: {i - current_run['start_index'] + 1}\n"
                        f"Confirmation Time: {confirmation_time}\n"
                        f"{'*'*65}\n"
                    )

                    # Add to the SQL storage list
                    new_zone_data = {
                        'currency': currency,
                        'zone_start_price': zone_start,
                        'zone_end_price': zone_end,
                        'zone_size': abs(zone_end - zone_start),
                        'zone_type': zone_type,
                        'confirmation_time': confirmation_time
                    }
                    new_zones_for_sql.append(new_zone_data)

                # Reset run after creating zone
                current_run = None

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
    
    # Log zone status after processing
    if new_zones_detected > 0:
        log_zone_status("After Zone Detection", currency)

    # After all zone identification, store the new zones in SQL
    if new_zones_for_sql:
        store_zones_in_sql(new_zones_for_sql)

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
            zone_id = (trade.get('zone_start_price'), trade.get('zone_end_price'))
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
            zone_type = zone_data['zone_type']
            size_pips = zone_data['zone_size'] * 10000  # Convert to pips
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
    
    # Initialize if not already present
    if currency not in trades:
        trades[currency] = []
    if currency not in last_trade_time:
        last_trade_time[currency] = None
    if currency not in balance:
        balance[currency] = 100000
        
    # Get currency-specific data - make sure valid_zones_dict is properly accessed
    trades_for_currency = trades[currency]
    
    # FIXED LOGIC: Always expect valid_zones_dict to be our main dictionary with currency keys
    if currency not in valid_zones_dict:
        valid_zones_dict[currency] = {}
    zones_dict = valid_zones_dict[currency]
    
    # Current price and time
    current_time = data.index[bar_index]
    current_price = data['close'].iloc[bar_index]
    
    # Check if we're in cooldown period
    if last_trade_time[currency] is not None:
        seconds_since_last_trade = (current_time - last_trade_time[currency]).total_seconds()
        if seconds_since_last_trade < COOLDOWN_SECONDS:
            return zones_dict

    # Get the dynamic pip requirement
    current_pips_required = calculate_atr_pips_required(data, currency)
    
    # Here we'll check if we're close to any of our liquidity zones
    for zone_id, zone_data in zones_dict.items():
        zone_start = zone_data['start_price']
        zone_end = zone_data['end_price']
        zone_size = zone_data['zone_size']
        zone_type = zone_data['zone_type']
        
        # Check distance from the current price to the zone's beginning
        distance_to_zone = abs(current_price - zone_start)
        
        # If we're close enough to a zone, we may consider a trade
        if distance_to_zone <= LIQUIDITY_ZONE_ALERT:
            # Set position size based on risk
            position_size = (balance[currency] * RISK_PROPORTION) / (zone_size * 10000)
            
            # Check direction and indicators
            if zone_type == 'demand' and current_price < zone_end:
                # If RSI < 70 OR MACD > Signal
                if not (data['RSI'].iloc[bar_index] < DEMAND_ZONE_RSI_TOP or
                        data['MACD_Line'].iloc[bar_index] > data['Signal_Line'].iloc[bar_index]):
                    continue

                entry_price = current_price
                stop_loss = entry_price - (STOP_LOSS_PROPORTION * zone_size)
                take_profit = entry_price + (TAKE_PROFIT_PROPORTION * zone_size)
            
            elif zone_type == 'supply' and current_price > zone_end:
                # If RSI > 30 OR MACD < Signal
                if not (data['RSI'].iloc[bar_index] > SUPPLY_ZONE_RSI_BOTTOM or
                        data['MACD_Line'].iloc[bar_index] < data['Signal_Line'].iloc[bar_index]):
                    continue

                entry_price = current_price
                stop_loss = entry_price + (STOP_LOSS_PROPORTION * zone_size)
                take_profit = entry_price - (TAKE_PROFIT_PROPORTION * zone_size)
            else:
                continue

            # Check if we already have an open trade
            if not any(trade.get('status', '') == 'open' for trade in trades_for_currency):
                # Create the trade
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
    
    # Initialize if not already
    if currency not in trades:
        trades[currency] = []
    if currency not in balance:
        balance[currency] = 100000
        
    debug_logger.info(f"Managing {currency} trades at {current_time} price {current_price}")
    
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
    
    return closed_any_trade

# --------------------------------------------------------------------------
# DATABASE CONNECTION FUNCTIONS
# --------------------------------------------------------------------------
def get_db_connection():
    """
    Creates and returns a connection to the SQL Server database using the DB_CONFIG.
    
    Returns:
        pyodbc.Connection or None: Database connection object if successful, None otherwise
    """
    conn_str = (
        f"Driver={{{DB_CONFIG['driver']}}};"
        f"Server={DB_CONFIG['server']};"
        f"Database={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
    )
    
    try:
        connection = pyodbc.connect(conn_str)
        debug_logger.info(f"Successfully connected to {DB_CONFIG['database']} on {DB_CONFIG['server']}")
        return connection
    except Exception as e:
        debug_logger.error(f"Database connection error: {e}", exc_info=True)
        return None
        
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
        
        # Table 1: AlgorithmBars
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'AlgorithmBars') 
        CREATE TABLE AlgorithmBars (
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
        
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_AlgorithmBars_Currency_BarStartTime')
        CREATE INDEX IX_AlgorithmBars_Currency_BarStartTime ON AlgorithmBars(Currency, BarStartTime);
        """)
        
        # Table 2: AlgorithmZones
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'AlgorithmZones')
        CREATE TABLE AlgorithmZones (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            Currency NVARCHAR(10),
            ZoneStartPrice FLOAT,
            ZoneEndPrice FLOAT,
            ZoneSize FLOAT,
            ZoneType NVARCHAR(10),
            ConfirmationTime DATETIME NULL,
            CreatedAt DATETIME DEFAULT GETDATE()
        );
        
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_AlgorithmZones_Currency_ConfirmationTime')
        CREATE INDEX IX_AlgorithmZones_Currency_ConfirmationTime ON AlgorithmZones(Currency, ConfirmationTime);
        """)
        
        # Table 3: TradeSignalsSent
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'TradeSignalsSent')
        CREATE TABLE TradeSignalsSent (
            ID INT IDENTITY(1,1) PRIMARY KEY,
            SignalTime DATETIME,
            SignalType NVARCHAR(20),
            Price FLOAT,
            Currency NVARCHAR(10),
            CreatedAt DATETIME DEFAULT GETDATE()
        );
        
        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_TradeSignalsSent_SignalTime')
        CREATE INDEX IX_TradeSignalsSent_SignalTime ON TradeSignalsSent(SignalTime);
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
    
    # Insert to database
    try:
        conn = get_db_connection()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO AlgorithmBars 
        (Currency, BarStartTime, BarEndTime, OpenPrice, HighPrice, LowPrice, ClosePrice, 
         ZonesCount, RSI, MACD_Line, Signal_Line)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, 
        currency, 
        bar_start_time, 
        bar_end_time,
        bar_data["open"], 
        bar_data["high"], 
        bar_data["low"], 
        bar_data["close"],
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
    Saves a newly identified liquidity zone to the database.
    
    Args:
        zone_id: Tuple of (start_price, end_price) identifying the zone
        zone_data: Dictionary containing zone details
        currency: The currency pair this zone belongs to
    """
    try:
        conn = get_db_connection()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        confirmation_time = zone_data.get('confirmation_time', None)
        
        cursor.execute("""
        INSERT INTO AlgorithmZones 
        (Currency, ZoneStartPrice, ZoneEndPrice, ZoneSize, ZoneType, ConfirmationTime)
        VALUES (?, ?, ?, ?, ?, ?)
        """, 
        currency,
        zone_data['start_price'],
        zone_data['end_price'],
        zone_data['zone_size'],
        zone_data['zone_type'],
        confirmation_time
        )
        
        conn.commit()
        debug_logger.info(f"Saved {currency} zone to database: {zone_id}")
        return True
    
    except Exception as e:
        debug_logger.error(f"Error saving {currency} zone to database: {e}", exc_info=True)
        return False
    
    finally:
        if conn:
            conn.close()

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
    
    conn = None
    cursor = None    
    try:
        conn = get_db_connection()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO TradeSignalsSent 
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
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if conn is None:
            return False
        
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO AlgorithmBars 
        (Currency, BarStartTime, BarEndTime, OpenPrice, HighPrice, LowPrice, ClosePrice, 
         ZonesCount, RSI, MACD_Line, Signal_Line)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, 
        bar_data['currency'], 
        bar_data['bar_start_time'], 
        bar_data['bar_end_time'],
        bar_data['open_price'], 
        bar_data['high_price'], 
        bar_data['low_price'], 
        bar_data['close_price'],
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
                
                cursor.execute("""
                INSERT INTO AlgorithmZones 
                (Currency, ZoneStartPrice, ZoneEndPrice, ZoneSize, ZoneType, ConfirmationTime)
                VALUES (?, ?, ?, ?, ?, ?)
                """, 
                zone['currency'],
                zone['zone_start_price'],
                zone['zone_end_price'],
                zone['zone_size'],
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
# MAIN TICK HANDLER
# --------------------------------------------------------------------------
def process_market_data(new_data_point, currency="EUR.USD"):
    """
    Process new market data for a specific currency.
    
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
                debug_logger.warning(f"\n\nUnsupported currency in data: {extracted_currency}. Using {currency} instead.\n")
        
        # Initialize if not already present
        if currency not in historical_ticks:
            historical_ticks[currency] = pd.DataFrame()
            debug_logger.info(f"Initialized historical_ticks for {currency}")
        if currency not in bars:
            bars[currency] = pd.DataFrame(columns=["open", "high", "low", "close"])
            debug_logger.info(f"Initialized bars for {currency}")
        if currency not in current_valid_zones_dict:
            current_valid_zones_dict[currency] = {}
            debug_logger.info(f"Initialized zones for {currency}")
        if currency not in trades:
            trades[currency] = []
            debug_logger.info(f"Initialized trades for {currency}")
        if currency not in last_trade_time:
            last_trade_time[currency] = None
        if currency not in balance:
            balance[currency] = 100000
        if currency not in last_non_hold_signal:
            last_non_hold_signal[currency] = None

        # 1) Basic checks on the incoming tick
        raw_price = float(new_data_point['Price'].iloc[0])
        if raw_price < 0.5 or raw_price > 1.5:
            debug_logger.error(f"\n\nReceived {currency} price ({raw_price}) outside normal range [0.5, 1.5]. Ignoring.\n")
            return ('hold', currency)

        if pd.isna(new_data_point['Price'].iloc[0]):
            debug_logger.error(f"\n\nReceived NaN or None price value for {currency}\n")
            return ('hold', currency)

        tick_time = new_data_point.index[-1]
        tick_price = float(new_data_point['Price'].iloc[-1])
        
        # 2) Validate trade state at the start
        validate_trade_state(tick_time, tick_price, currency)
        log_trade_state(tick_time, "start_processing", currency)

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
            
            # Log zone status after bar completion
            if current_valid_zones_dict[currency]:
                log_zone_status("After Bar Completion", currency)
                
                # Prepare zone data for SQL storage
                zones_for_sql = []
                for zone_id, zone_data in current_valid_zones_dict[currency].items():
                    zones_for_sql.append({
                        'currency': currency,
                        'zone_start_price': zone_data['start_price'],
                        'zone_end_price': zone_data['end_price'],
                        'zone_size': zone_data['zone_size'],
                        'zone_type': zone_data['zone_type'],
                        'confirmation_time': zone_data.get('confirmation_time', None)
                    })
                
                # Store the current zones in SQL
                if zones_for_sql:
                    store_zones_in_sql(zones_for_sql)

        # 5) Manage existing trades (stop loss/take profit checks)
        closed_any_trade = manage_trades(tick_price, tick_time, currency)

        # 6) Re-validate after managing trades
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

        # Log initial zone status before processing
        if len(current_valid_zones_dict[currency]) > 0:
            log_zone_status("Before Processing", currency)

        # 9) Identify and invalidate zones (CUMULATIVE logic for zone detection)
        rolling_window_data, current_valid_zones_dict[currency] = identify_liquidity_zones(
            rolling_window_data, current_valid_zones_dict[currency], currency
        )
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
            current_valid_zones_dict[currency] = check_entry_conditions(
                rolling_window_data, i, current_valid_zones_dict[currency], currency
            )
        new_trade_opened = (len(trades[currency]) > trades_before)

        # 12) Final validations and logging
        validate_trade_state(tick_time, tick_price, currency)
        log_trade_state(tick_time, "end_processing", currency)

        # Add final zone status log if we have zones
        if len(current_valid_zones_dict[currency]) > 0:
            log_zone_status("End of Processing", currency)

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

            # Enhanced signal logging
            debug_logger.warning(
                f"\n\n{'#'*25} {currency} TRADE SIGNAL GENERATED {'#'*25}\n"
                f"Signal: {raw_signal.upper()}\n"
                f"Price: {tick_price:.5f}\n"
                f"Time: {tick_time}\n"
                f"{'#'*70}\n"
            )

        # 16) Log only non-hold signals
        if raw_signal not in ['hold']:
            trade_logger.info(f"{currency} signal determined: {raw_signal}")

        return (raw_signal, currency)

    except Exception as e:
        debug_logger.error(f"\n\nError in process_market_data for {currency}: {e}\n", exc_info=True)
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
