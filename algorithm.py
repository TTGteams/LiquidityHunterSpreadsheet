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

# Initialize global variables as dictionaries keyed by currency
historical_ticks = {}      # Raw tick-by-tick data (trimmed to 12 hours)
bars = {}                  # Incrementally built 15-min bars
current_bar_start = {}
current_bar_data = {}

current_valid_zones_dict = {}
last_trade_time = {}
trades = {}
balance = {}               # Initialize balance
cumulative_zone_info = {}  # Track "cumulative zone" in-progress

# NEW: Track last non-hold signal (None, 'open', or 'close')
last_non_hold_signal = {}

# Zone formation tracking
cumulative_zone_info = {currency: None for currency in SUPPORTED_CURRENCIES}

# Add this near the top of your file, right after imports
# Performance optimization: Control verbose logging
DEBUG_LOGGING = False  # Set to True for detailed debugging, False for production

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
    # Initialize if not already present
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
def log_zone_status(context, currency="EUR.USD"):
    """Log the status of all zones for debugging purposes."""
    if not DEBUG_LOGGING:
        return
        
    debug_logger.warning(f"\n{context} ZONE STATUS for {currency}:")
    if currency not in current_valid_zones_dict or not current_valid_zones_dict[currency]:
        debug_logger.warning(f"  No valid zones found for {currency}")
        return

    for zone_id, zone_data in current_valid_zones_dict[currency].items():
        try:
            # Extract zone_start and zone_end either from keys or from the zone_id tuple
            if isinstance(zone_id, tuple) and len(zone_id) >= 2:
                zone_start, zone_end = zone_id[0], zone_id[1]
            else:
                zone_start = zone_data.get('start_price', zone_data.get('start', None))
                zone_end = zone_data.get('end_price', zone_data.get('end', None))
                
            zone_type = zone_data.get('type', zone_data.get('zone_type', 'unknown'))
            zone_size = zone_data.get('zone_size', abs(zone_end - zone_start) if zone_end and zone_start else 0)
            
            debug_logger.warning(
                f"  Zone ID: {zone_id}, Type: {zone_type}, "
                f"Start: {zone_start:.5f}, End: {zone_end:.5f}, "
                f"Size: {zone_size*10000:.1f} pips"
            )
        except Exception as e:
            debug_logger.warning(f"  Error logging zone {zone_id}: {e}")
            debug_logger.warning(f"  Zone data: {zone_data}")

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
            f"Stats: Bars count={bar_count}/384, Zones={len(current_valid_zones_dict[currency]) if currency in current_valid_zones_dict else 0}\n"
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
    """
    try:
        # Performance optimization: Only copy once
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
        
        # Performance optimization: Only log ATR details in debug mode
        if DEBUG_LOGGING:
            debug_logger.warning(f"ATR value: {atr:.6f}, Pips required: {pips_required*10000:.1f} pips")
        
        return pips_required
        
    except Exception as e:
        debug_logger.error(f"Error in ATR calculation: {e}", exc_info=True)
        return 0.003  # Default to 30 pips on error


def identify_liquidity_zones(data, currency="EUR.USD"):
    """
    Identifies liquidity zones based on consecutive directional price moves.
    Modified to match single-currency version's behavior.
    
    Args:
        data: DataFrame with price data
        currency: The currency pair to process
        
    Returns:
        DataFrame: The modified data frame with zone information
    """
    global current_valid_zones_dict, cumulative_zone_info
    
    debug_logger.warning(f"Processing zones for {currency} - {len(data)} bars")
    
    # Initialize structures if needed
    if currency not in current_valid_zones_dict:
        current_valid_zones_dict[currency] = {}
    if currency not in cumulative_zone_info:
        cumulative_zone_info[currency] = None
    
    # Create a copy to avoid modifying the original
    data = data.copy()
    
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
    
    # If we have a zone in progress from previous window, restore it
    current_run = None
    if cumulative_zone_info[currency] is not None:
        current_run = cumulative_zone_info[currency]
    
    # Process each bar
    for i in range(len(data)):
        current_candle = data.iloc[i]
        
        # Start or continue a run based on candle direction
        if current_run is None:
            # Start a new run
            current_run = {
                'start_index': i,
                'start_price': current_candle['open'],
                'direction': current_candle['candle_direction'],
                'high': current_candle['high'],
                'low': current_candle['low'],
                'start_time': data.index[i]
            }
        elif current_run['direction'] == current_candle['candle_direction']:
            # Continue the current run, updating high and low
            if current_candle['high'] > current_run['high']:
                current_run['high'] = current_candle['high']
            if current_candle['low'] < current_run['low']:
                current_run['low'] = current_candle['low']
                
            # Calculate total move size
            if current_run['direction'] == 'green':
                zone_start = current_run['low']
                zone_end = current_run['high']
            else:
                zone_start = current_run['high']
                zone_end = current_run['low']
                
            total_move = abs(zone_end - zone_start)
            
            # Check if movement meets dynamic pip requirement
            if abs(total_move) >= PIPS_REQUIRED:
                zone_type = 'demand' if current_run['direction'] == 'green' else 'supply'
                
                # Create zone ID with consistent precision
                zone_start_rounded = round(zone_start, 5)
                zone_end_rounded = round(zone_end, 5)
                zone_id = (zone_start_rounded, zone_end_rounded)
                
                # Only add zone if it doesn't already exist
                if zone_id not in current_valid_zones_dict[currency]:
                    # Create zone with both naming conventions for compatibility
                    current_valid_zones_dict[currency][zone_id] = {
                        'start_price': zone_start,
                        'end_price': zone_end,
                        'zone_type': zone_type,
                        'type': zone_type,
                        'zone_size': abs(zone_end - zone_start),
                        'confirmation_time': data.index[i],
                        'start_time': current_run['start_time'],
                        'candles_in_formation': i - current_run['start_index'] + 1
                    }
                    
                    # Mark the zone in the data
                    zone_indices = slice(current_run['start_index'], i + 1)
                    data.loc[data.index[zone_indices], 'Liquidity_Zone'] = zone_start
                    data.loc[data.index[zone_indices], 'Zone_Size'] = abs(zone_end - zone_start)
                    data.loc[data.index[zone_indices], 'Zone_Start_Price'] = zone_start
                    data.loc[data.index[zone_indices], 'Zone_End_Price'] = zone_end
                    data.loc[data.index[zone_indices], 'zone_type'] = zone_type
                    data.loc[data.index[zone_indices], 'Confirmation_Time'] = data.index[i]
                    data.loc[data.index[zone_indices], 'Zone_Length'] = i - current_run['start_index'] + 1
                
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
    
    # CRITICAL FIX: Return only the dataframe
    return data


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


def invalidate_zones_via_sup_and_resist(current_price, current_valid_zones_dict, currency):
    """
    Compare the current_price to each zone's threshold. 
    If price has violated it, remove the zone immediately.
    """
    zones_to_invalidate = []
    
    # Skip if no zones for this currency
    if currency not in current_valid_zones_dict:
        return current_valid_zones_dict
        
    for zone_id, zone_data in list(current_valid_zones_dict[currency].items()):
        zone_start = zone_data['start_price']
        zone_type = zone_data['zone_type']

        if zone_type == 'demand':
            support_line = zone_start - SUPPORT_RESISTANCE_ALLOWANCE
            # If price dips far below the zone's support line
            if current_price < (support_line - SUPPORT_RESISTANCE_ALLOWANCE):
                zones_to_invalidate.append(zone_id)
                if DEBUG_LOGGING:
                    debug_logger.warning(f"Invalidating DEMAND zone: Start={zone_start}, End={zone_data['end_price']}, Size={zone_data['zone_size']*10000:.1f} pips, Reason: support line violation")

        elif zone_type == 'supply':
            resistance_line = zone_start + SUPPORT_RESISTANCE_ALLOWANCE
            # If price rises far above the zone's resistance line
            if current_price > (resistance_line + SUPPORT_RESISTANCE_ALLOWANCE):
                zones_to_invalidate.append(zone_id)
                if DEBUG_LOGGING:
                    debug_logger.warning(f"Invalidating SUPPLY zone: Start={zone_start}, End={zone_data['end_price']}, Size={zone_data['zone_size']*10000:.1f} pips, Reason: resistance line violation")

    for zone_id in zones_to_invalidate:
        del current_valid_zones_dict[currency][zone_id]
        
    if zones_to_invalidate and DEBUG_LOGGING:
        debug_logger.warning(f"\n!!!!!!!!!!!!!!!!!!!! {currency} ZONE INVALIDATIONS !!!!!!!!!!!!!!!!!!!!")
        debug_logger.warning(f"Invalidated {len(zones_to_invalidate)} zones")
        debug_logger.warning(f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n")

    return current_valid_zones_dict


def check_entry_conditions(data, i, current_valid_zones, currency="EUR.USD"):
    """
    Modified to match the exact behavior of the single-currency version.
    """
    # Get access to global state the same way the single version does
    global trades, last_trade_time, balance, last_non_hold_signal
    
    # Strict check for ANY open trades, exactly like the single version
    if any(trade.get('status', '') == 'open' for trade in trades[currency]):
        debug_logger.debug("Found open trade, blocking new entries")
        return current_valid_zones
    
    debug_logger.debug("Latest Row:\n{}".format(data.iloc[i]))
    
    current_price = data['close'].iloc[i]
    current_time = data.index[i]
    
    # Calculate ATR-based pip requirement using the same method
    current_pips_required = calculate_atr_pips_required(data, currency)
    
    # Check each valid zone with EXACT same logic as single version
    for zone_id, zone_data in list(current_valid_zones.items()):
        # Access zone data the same way
        zone_start = zone_data.get('start_price', zone_data.get('start', None))
        zone_end = zone_data.get('end_price', zone_data.get('end', None))
        zone_type = zone_data.get('zone_type', zone_data.get('type', None))
        zone_size = abs(zone_end - zone_start)
        
        # Apply EXACT same conditions in same order
        if abs(current_price - zone_start) > LIQUIDITY_ZONE_ALERT:
            continue
            
        # Check cooldown with identical logic
        if trades[currency] and trades[currency][-1]['status'] == 'closed' and trades[currency][-1].get('profit_loss', 0) < 0:
            if currency in last_trade_time and last_trade_time[currency] and (current_time - last_trade_time[currency]).total_seconds() < COOLDOWN_SECONDS:
                continue
                
        # Check zone size with identical logic
        if zone_size < current_pips_required or pd.isna(zone_size):
            debug_logger.debug(f"Invalid zone size {zone_size}, required: {current_pips_required} for zone {zone_id}")
            continue
            
        position_size = 1  # Placeholder, same as single version
        
        # RSI/MACD conditions - EXACTLY as in single version
        if zone_type == 'demand':
            # If RSI < 70 OR MACD>Signal  
            if not (data['RSI'].iloc[i] < DEMAND_ZONE_RSI_TOP or 
                    data['MACD_Line'].iloc[i] > data['Signal_Line'].iloc[i]):
                continue
                
            entry_price = current_price
            stop_loss = entry_price - (STOP_LOSS_PROPORTION * zone_size) 
            take_profit = entry_price + (TAKE_PROFIT_PROPORTION * zone_size)
            
        elif zone_type == 'supply':
            # If RSI > 30 OR MACD<Signal
            if not (data['RSI'].iloc[i] > SUPPLY_ZONE_RSI_BOTTOM or
                    data['MACD_Line'].iloc[i] < data['Signal_Line'].iloc[i]):
                continue
                
            entry_price = current_price
            stop_loss = entry_price + (STOP_LOSS_PROPORTION * zone_size)
            take_profit = entry_price - (TAKE_PROFIT_PROPORTION * zone_size)
        else:
            continue
            
        # Create trade using IDENTICAL format as single version
        if not any(trade.get('status', '') == 'open' for trade in trades[currency]):
            trades[currency].append({
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
                'required_pips': current_pips_required
            })
            
            last_trade_time[currency] = current_time
            trade_logger.info(f"Signal generated: {zone_type} trade for {currency} at {entry_price}")
            debug_logger.debug(f"New {zone_type} trade opened for {currency}")
            
            # Exit immediately after opening a trade - CRITICAL behavior from single version
            return current_valid_zones
            
    return current_valid_zones


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

def determine_signal(closed_any_trade, new_trade_opened, trades_list, last_signal, currency="EUR.USD"):
    """
    Standardized trade signal determination function used by both single and multi-currency versions.
    
    Args:
        closed_any_trade: Boolean indicating if any trade was closed in this cycle
        new_trade_opened: Boolean indicating if any new trade was opened
        trades_list: List of trades for the currency
        last_signal: Previous non-hold signal
        currency: Currency pair
        
    Returns:
        str: 'buy', 'sell', 'close', or 'hold'
    """
    # Get the most recent trade if exists
    recent_trade = None
    if trades_list:
        recent_trade = max(trades_list, key=lambda t: t.get('entry_time', pd.Timestamp.min))
    
    # RULE 1: If we closed a trade and haven't yet emitted a 'close' signal
    if closed_any_trade:
        if last_signal not in ['close']:
            debug_logger.warning(f"[{currency}] Signal determined by RULE 1: Close signal after trade close")
            return 'close'
        else:
            return 'hold'
    
    # RULE 2: If we opened a new trade
    if new_trade_opened and recent_trade and recent_trade.get('status') == 'open':
        direction = recent_trade.get('direction')
        if direction == 'long':
            debug_logger.warning(f"[{currency}] Signal determined by RULE 2: Buy signal from new long trade")
            return 'buy'
        elif direction == 'short':
            debug_logger.warning(f"[{currency}] Signal determined by RULE 3: Sell signal from new short trade")
            return 'sell'
    
    # Default - no signal
    if DEBUG_LOGGING:
        debug_logger.debug(f"[{currency}] No trade signal conditions met, returning 'hold'")
    return 'hold'

# --------------------------------------------------------------------------
# MAIN TICK HANDLER
# --------------------------------------------------------------------------
def process_market_data(new_data_point, currency="EUR.USD"):
    """
    Main function to process incoming market data and generate trading signals.
    Follows the exact same 16-step process as the single-currency version.
    
    Args:
        new_data_point: DataFrame containing the new market data point
        currency: The currency pair to process
        
    Returns:
        tuple: (signal, currency) - The trading signal and the currency that was processed
    """
    global historical_ticks, bars, current_valid_zones_dict
    global trades, last_trade_time, balance, last_non_hold_signal
    
    try:
        # Initialize if not already
        if currency not in historical_ticks:
            historical_ticks[currency] = pd.DataFrame()
        if currency not in trades:
            trades[currency] = []
        if currency not in balance:
            balance[currency] = 100000
        if currency not in last_non_hold_signal:
            last_non_hold_signal[currency] = None
        if currency not in last_trade_time:
            last_trade_time[currency] = None
            
        # 1) Basic checks on the incoming tick
        if new_data_point.empty:
            debug_logger.error(f"Received empty data point for {currency}")
            return 'hold', currency
            
        raw_price = float(new_data_point['Price'].iloc[0])
        if pd.isna(raw_price) or raw_price <= 0:
            debug_logger.error(f"Received invalid price value for {currency}: {raw_price}")
            return 'hold', currency
            
        # Perform currency-specific price validation
        if currency == "EUR.USD" and (raw_price < 0.5 or raw_price > 1.5):
            debug_logger.error(f"Received {currency} price ({raw_price}) outside normal range [0.5, 1.5]. Ignoring.")
            return 'hold', currency
        elif currency == "GBP.USD" and (raw_price < 1.0 or raw_price > 2.0):
            debug_logger.error(f"Received {currency} price ({raw_price}) outside normal range [1.0, 2.0]. Ignoring.")
            return 'hold', currency
        elif currency == "USD.CAD" and (raw_price < 1.0 or raw_price > 1.8):
            debug_logger.error(f"Received {currency} price ({raw_price}) outside normal range [1.0, 1.8]. Ignoring.")
            return 'hold', currency

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

        # 4) Update bars
        bar_completed = update_bars_with_tick(tick_time, tick_price, currency)

        # 5) Manage existing trades (stop loss/take profit checks)
        closed_any_trade = manage_trades(tick_price, tick_time, currency)

        # 6) Re-validate after managing trades
        validate_trade_state(tick_time, tick_price, currency)

        # 7) Check if we have enough bar data
        if currency not in bars or bars[currency].empty:
            debug_logger.info(f"Not enough bar data for {currency} yet")
            return 'hold', currency

        rolling_window_data = bars[currency].tail(384).copy()
        if len(rolling_window_data) < 35:
            debug_logger.info(f"Need at least 35 bars for {currency}, have {len(rolling_window_data)}")
            return 'hold', currency

        # Clean NaNs in close prices
        rolling_window_data['close'] = rolling_window_data['close'].where(
            rolling_window_data['close'].notnull(), np.nan
        )

        # 8) Calculate indicators (RSI, MACD)
        debug_logger.debug(f"Calculating indicators for {currency}")
        rolling_window_data['RSI'] = ta.rsi(rolling_window_data['close'], length=14)
        macd = ta.macd(rolling_window_data['close'], fast=12, slow=26, signal=9)
        
        if macd is None or macd.empty:
            debug_logger.info(f"MACD calculation returned None/empty for {currency}")
            return 'hold', currency

        macd.dropna(inplace=True)
        if macd.empty:
            debug_logger.info(f"MACD contains only NaN values for {currency}")
            return 'hold', currency

        rolling_window_data['MACD_Line'] = macd['MACD_12_26_9']
        rolling_window_data['Signal_Line'] = macd['MACDs_12_26_9']

        # 9) Identify and process zones (CUMULATIVE logic for zone detection)
        debug_logger.debug(f"Identifying liquidity zones for {currency}")
        rolling_window_data = identify_liquidity_zones(rolling_window_data, currency)
        rolling_window_data = set_support_resistance_lines(rolling_window_data, currency)
        
        # Make sure we have the latest bar index
        latest_bar_idx = rolling_window_data.index[-1]
        i = rolling_window_data.index.get_loc(latest_bar_idx)

        # 10) Remove consecutive losers, invalidate zones
        if currency in current_valid_zones_dict:
            debug_logger.debug(f"Processing zone invalidations for {currency}")
            current_valid_zones_dict[currency] = remove_consecutive_losers(trades[currency], current_valid_zones_dict[currency], currency)
            current_valid_zones_dict[currency] = invalidate_zones_via_sup_and_resist(tick_price, current_valid_zones_dict[currency], currency)
            log_zone_status("After zone processing", currency)
        else:
            debug_logger.debug(f"No zones yet established for {currency}")
            current_valid_zones_dict[currency] = {}

        # 11) Attempt to open a new trade if none is open
        trades_before = len(trades[currency])
        open_trades = [t for t in trades[currency] if t['status'] == 'open']
        
        if not open_trades and currency in current_valid_zones_dict:
            debug_logger.debug(f"Checking entry conditions for {currency} with {len(current_valid_zones_dict[currency])} zones")
            current_valid_zones_dict[currency] = check_entry_conditions(
                rolling_window_data, i, current_valid_zones_dict[currency], currency
            )
        else:
            debug_logger.debug(f"Skipping entry check - open trades: {len(open_trades)}, zones: {len(current_valid_zones_dict.get(currency, {}))}")
        
        new_trade_opened = (len(trades[currency]) > trades_before)

        # 12) Final validations and logging
        validate_trade_state(tick_time, tick_price, currency)
        log_trade_state(tick_time, "end_processing", currency)

        # -----------------------------------------------------------------
        # 13) Determine final signal: buy, sell, close, or 'hold'
        # -----------------------------------------------------------------
        raw_signal = determine_signal(
            closed_any_trade, 
            new_trade_opened, 
            trades[currency], 
            last_non_hold_signal[currency],
            currency
        )
        
        # 14) Block consecutive opens (either 'buy' or 'sell')
        if raw_signal in ['buy', 'sell'] and last_non_hold_signal[currency] in ['buy', 'sell']:
            debug_logger.warning(f"[{currency}] Blocking consecutive {raw_signal} after previous {last_non_hold_signal[currency]}")
            raw_signal = 'hold'
        
        # 15) Update the last_non_hold_signal if we have a real signal
        if raw_signal not in ['hold']:
            last_non_hold_signal[currency] = raw_signal
            
        # 16) Log only non-hold signals
        if raw_signal not in ['hold']:
            trade_logger.info(f"Signal determined for {currency}: {raw_signal}")

        # Return both the signal and the currency that was processed
        return raw_signal, currency

    except Exception as e:
        debug_logger.error(f"Error in process_market_data for {currency}: {e}", exc_info=True)
        return 'hold', currency

# --------------------------------------------------------------------------
# DATABASE INTEGRATION FUNCTIONS
# --------------------------------------------------------------------------
def initialize_database():
    """
    Creates all required database tables for multi-currency trading.
    Tables will include currency_pair column for identification.
    """
    try:
        # Establish connection using the DB_CONFIG dictionary
        connection_string = f"DRIVER={{{DB_CONFIG['driver']}}};" \
                           f"SERVER={DB_CONFIG['server']};" \
                           f"DATABASE={DB_CONFIG['database']};" \
                           f"UID={DB_CONFIG['username']};" \
                           f"PWD={DB_CONFIG['password']}"
        
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Create trades table if it doesn't exist
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'trades')
        BEGIN
            CREATE TABLE trades (
                id INT IDENTITY(1,1) PRIMARY KEY,
                currency_pair VARCHAR(10) NOT NULL,
                entry_time DATETIME,
                entry_price FLOAT,
                exit_time DATETIME NULL,
                exit_price FLOAT NULL,
                direction VARCHAR(10),
                status VARCHAR(10),
                stop_loss FLOAT,
                take_profit FLOAT,
                profit_loss FLOAT NULL,
                position_size FLOAT,
                close_reason VARCHAR(20) NULL,
                zone_start_price FLOAT,
                zone_end_price FLOAT,
                required_pips FLOAT
            )
        END
        """)
        
        # Create signals table if it doesn't exist
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'signals')
        BEGIN
            CREATE TABLE signals (
                id INT IDENTITY(1,1) PRIMARY KEY,
                currency_pair VARCHAR(10) NOT NULL,
                signal_time DATETIME,
                signal_type VARCHAR(10),
                price FLOAT
            )
        END
        """)
        
        # Create zones table if it doesn't exist
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'zones')
        BEGIN
            CREATE TABLE zones (
                id INT IDENTITY(1,1) PRIMARY KEY,
                currency_pair VARCHAR(10) NOT NULL,
                zone_type VARCHAR(10),
                start_price FLOAT,
                end_price FLOAT,
                zone_size FLOAT,
                confirmation_time DATETIME,
                created_at DATETIME DEFAULT GETDATE()
            )
        END
        """)
        
        # Create bars table if it doesn't exist
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'bars')
        BEGIN
            CREATE TABLE bars (
                id INT IDENTITY(1,1) PRIMARY KEY,
                currency_pair VARCHAR(10) NOT NULL,
                bar_start_time DATETIME,
                bar_end_time DATETIME,
                open_price FLOAT,
                high_price FLOAT,
                low_price FLOAT,
                close_price FLOAT,
                zones_count INT,
                rsi FLOAT NULL,
                macd_line FLOAT NULL,
                signal_line FLOAT NULL
            )
        END
        """)
        
        conn.commit()
        debug_logger.info("Database tables initialized successfully")
        
        return True
    
    except Exception as e:
        debug_logger.error(f"Error initializing database: {e}", exc_info=True)
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()


def save_trade_to_database(trade, currency="EUR.USD"):
    """
    Saves a trade to the database with the specified currency.
    
    Args:
        trade: Dictionary containing trade information
        currency: The currency pair for this trade
    """
    try:
        connection_string = f"DRIVER={{{DB_CONFIG['driver']}}};" \
                           f"SERVER={DB_CONFIG['server']};" \
                           f"DATABASE={DB_CONFIG['database']};" \
                           f"UID={DB_CONFIG['username']};" \
                           f"PWD={DB_CONFIG['password']}"
        
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Prepare fields (make sure to handle potentially missing keys)
        entry_time = trade.get('entry_time')
        entry_price = trade.get('entry_price')
        exit_time = trade.get('exit_time')
        exit_price = trade.get('exit_price')
        direction = trade.get('direction')
        status = trade.get('status')
        stop_loss = trade.get('stop_loss')
        take_profit = trade.get('take_profit')
        profit_loss = trade.get('profit_loss')
        position_size = trade.get('position_size')
        close_reason = trade.get('close_reason')
        zone_start_price = trade.get('zone_start_price')
        zone_end_price = trade.get('zone_end_price')
        required_pips = trade.get('required_pips')
        
        # Check if this trade already exists
        cursor.execute("""
        SELECT id FROM trades 
        WHERE currency_pair = ? AND entry_time = ? AND entry_price = ? AND direction = ?
        """, (currency, entry_time, entry_price, direction))
        
        existing_trade = cursor.fetchone()
        
        if existing_trade:
            # Update existing trade
            cursor.execute("""
            UPDATE trades SET
                exit_time = ?,
                exit_price = ?,
                status = ?,
                profit_loss = ?,
                close_reason = ?
            WHERE id = ?
            """, (exit_time, exit_price, status, profit_loss, close_reason, existing_trade[0]))
            
            debug_logger.debug(f"Updated existing trade for {currency} in database, ID: {existing_trade[0]}")
            
        else:
            # Insert new trade
            cursor.execute("""
            INSERT INTO trades (
                currency_pair, entry_time, entry_price, exit_time, exit_price, 
                direction, status, stop_loss, take_profit, profit_loss,
                position_size, close_reason, zone_start_price, zone_end_price, required_pips
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                currency, entry_time, entry_price, exit_time, exit_price,
                direction, status, stop_loss, take_profit, profit_loss,
                position_size, close_reason, zone_start_price, zone_end_price, required_pips
            ))
            
            debug_logger.debug(f"Inserted new trade for {currency} into database")
        
        conn.commit()
        return True
    
    except Exception as e:
        debug_logger.error(f"Error saving trade to database: {e}", exc_info=True)
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()


def save_signal_to_database(signal_type, price, signal_time, currency="EUR.USD"):
    """
    Saves a trading signal to the database.
    
    Args:
        signal_type: The type of signal (buy, sell, close)
        price: The price at which the signal was generated
        signal_time: The time of the signal
        currency: The currency pair for this signal
    """
    try:
        connection_string = f"DRIVER={{{DB_CONFIG['driver']}}};" \
                           f"SERVER={DB_CONFIG['server']};" \
                           f"DATABASE={DB_CONFIG['database']};" \
                           f"UID={DB_CONFIG['username']};" \
                           f"PWD={DB_CONFIG['password']}"
        
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Insert the signal
        cursor.execute("""
        INSERT INTO signals (
            currency_pair, signal_time, signal_type, price
        ) VALUES (?, ?, ?, ?)
        """, (currency, signal_time, signal_type, price))
        
        conn.commit()
        debug_logger.debug(f"Saved {signal_type} signal for {currency} to database")
        return True
    
    except Exception as e:
        debug_logger.error(f"Error saving signal to database: {e}", exc_info=True)
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()


def save_zone_to_database(zone_data, currency="EUR.USD"):
    """
    Saves a liquidity zone to the database.
    
    Args:
        zone_data: Dictionary containing zone information
        currency: The currency pair for this zone
    """
    try:
        connection_string = f"DRIVER={{{DB_CONFIG['driver']}}};" \
                           f"SERVER={DB_CONFIG['server']};" \
                           f"DATABASE={DB_CONFIG['database']};" \
                           f"UID={DB_CONFIG['username']};" \
                           f"PWD={DB_CONFIG['password']}"
        
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Extract zone information
        zone_type = zone_data.get('zone_type', zone_data.get('type'))
        start_price = zone_data.get('start_price')
        end_price = zone_data.get('end_price')
        zone_size = zone_data.get('zone_size')
        confirmation_time = zone_data.get('confirmation_time')
        
        # Check if this zone already exists
        cursor.execute("""
        SELECT id FROM zones 
        WHERE currency_pair = ? AND start_price = ? AND end_price = ? AND zone_type = ?
        """, (currency, start_price, end_price, zone_type))
        
        existing_zone = cursor.fetchone()
        
        if not existing_zone:
            # Insert new zone
            cursor.execute("""
            INSERT INTO zones (
                currency_pair, zone_type, start_price, end_price, zone_size, confirmation_time
            ) VALUES (?, ?, ?, ?, ?, ?)
            """, (currency, zone_type, start_price, end_price, zone_size, confirmation_time))
            
            debug_logger.debug(f"Saved new {zone_type} zone for {currency} to database")
        
        conn.commit()
        return True
    
    except Exception as e:
        debug_logger.error(f"Error saving zone to database: {e}", exc_info=True)
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()


def save_bar_to_database(bar_data):
    """
    Saves a completed bar to the database.
    
    Args:
        bar_data: Dictionary containing bar information including currency
    """
    try:
        if not bar_data:
            return False
            
        connection_string = f"DRIVER={{{DB_CONFIG['driver']}}};" \
                           f"SERVER={DB_CONFIG['server']};" \
                           f"DATABASE={DB_CONFIG['database']};" \
                           f"UID={DB_CONFIG['username']};" \
                           f"PWD={DB_CONFIG['password']}"
        
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        # Extract bar information
        currency = bar_data.get('currency')
        bar_start_time = bar_data.get('bar_start_time')
        bar_end_time = bar_data.get('bar_end_time')
        open_price = bar_data.get('open_price')
        high_price = bar_data.get('high_price')
        low_price = bar_data.get('low_price')
        close_price = bar_data.get('close_price')
        zones_count = bar_data.get('zones_count', 0)
        rsi = bar_data.get('rsi')
        macd_line = bar_data.get('macd_line')
        signal_line = bar_data.get('signal_line')
        
        # Insert the bar
        cursor.execute("""
        INSERT INTO bars (
            currency_pair, bar_start_time, bar_end_time, open_price, high_price,
            low_price, close_price, zones_count, rsi, macd_line, signal_line
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            currency, bar_start_time, bar_end_time, open_price, high_price,
            low_price, close_price, zones_count, rsi, macd_line, signal_line
        ))
        
        conn.commit()
        debug_logger.debug(f"Saved completed bar for {currency} to database")
        return True
    
    except Exception as e:
        debug_logger.error(f"Error saving bar to database: {e}", exc_info=True)
        return False
    
    finally:
        if 'conn' in locals():
            conn.close()


def get_warmup_data_for_currency(currency="EUR.USD"):
    """
    Retrieves warmup data for a specific currency from the database.
    
    Args:
        currency: The currency pair to get warmup data for
        
    Returns:
        tuple: (DataFrame of bars with indicators, Dictionary of zones)
    """
    try:
        connection_string = f"DRIVER={{{DB_CONFIG['driver']}}};" \
                           f"SERVER={DB_CONFIG['server']};" \
                           f"DATABASE={DB_CONFIG['database']};" \
                           f"UID={DB_CONFIG['username']};" \
                           f"PWD={DB_CONFIG['password']}"
        
        conn = pyodbc.connect(connection_string)
        
        # Get latest bars
        query = f"""
        SELECT TOP 384 
            bar_start_time, open_price as 'open', high_price as 'high', 
            low_price as 'low', close_price as 'close', 
            rsi as 'RSI', macd_line as 'MACD_Line', signal_line as 'Signal_Line'
        FROM bars 
        WHERE currency_pair = '{currency}'
        ORDER BY bar_start_time DESC
        """
        
        bars_df = pd.read_sql(query, conn, index_col='bar_start_time')
        bars_df.sort_index(inplace=True)  # Sort by time ascending
        
        # Get active zones
        zones_query = f"""
        SELECT 
            zone_type, start_price, end_price, zone_size, confirmation_time
        FROM zones 
        WHERE currency_pair = '{currency}'
        AND confirmation_time >= DATEADD(day, -7, GETDATE())
        """
        
        zones_df = pd.read_sql(zones_query, conn)
        zones_dict = {}
        
        for _, row in zones_df.iterrows():
            zone_id = (float(row['start_price']), float(row['end_price']))
            zones_dict[zone_id] = {
                'zone_type': row['zone_type'],
                'start_price': float(row['start_price']),
                'end_price': float(row['end_price']),
                'zone_size': float(row['zone_size']),
                'confirmation_time': row['confirmation_time']
            }
        
        debug_logger.info(f"Retrieved {len(bars_df)} bars and {len(zones_dict)} zones for {currency} warmup")
        return bars_df, zones_dict
    
    except Exception as e:
        debug_logger.error(f"Error getting warmup data for {currency}: {e}", exc_info=True)
        return pd.DataFrame(), {}
    
    finally:
        if 'conn' in locals():
            conn.close()


# --------------------------------------------------------------------------
# MAIN INITIALIZATION
# --------------------------------------------------------------------------
def main():
    """
    Initialize the algorithm for all supported currencies.
    This includes database setup and loading warmup data.
    """
    try:
        debug_logger.warning("\n\n" + "="*80)
        debug_logger.warning("INITIALIZING MULTI-CURRENCY TRADING ALGORITHM")
        debug_logger.warning("="*80 + "\n")
        
        # Initialize database tables
        initialize_database()
        
        # Initialize algorithm for each supported currency
        for currency in SUPPORTED_CURRENCIES:
            debug_logger.warning(f"\nInitializing algorithm for {currency}")
            
            try:
                # Try to import the warmup module
                from secondary_warmer_script import warmup_data
                
                # Get warmup data for this currency
                precomputed_bars, precomputed_zones = warmup_data(currency)
                
                # Load the data into our algorithm
                load_preexisting_bars_and_indicators(precomputed_bars, currency)
                load_preexisting_zones(precomputed_zones, currency)
                
                # Log the warmup state
                latest_rsi = "N/A"
                latest_macd = "N/A" 
                latest_signal = "N/A"
                
                if not precomputed_bars.empty:
                    latest_rsi = precomputed_bars['RSI'].iloc[-1] if 'RSI' in precomputed_bars.columns else "N/A"
                    latest_macd = precomputed_bars['MACD_Line'].iloc[-1] if 'MACD_Line' in precomputed_bars.columns else "N/A"
                    latest_signal = precomputed_bars['Signal_Line'].iloc[-1] if 'Signal_Line' in precomputed_bars.columns else "N/A"
                
                # Enhanced logging
                debug_logger.warning(
                    f"Completed warmup for {currency}:\n"
                    f"- Loaded {len(precomputed_bars)} bars\n"
                    f"- Initialized {len(precomputed_zones)} liquidity zones\n"
                    f"- Latest indicators:\n"
                    f"  * RSI: {latest_rsi}\n"
                    f"  * MACD: {latest_macd}\n"
                    f"  * Signal: {latest_signal}"
                )
            
            except ImportError:
                debug_logger.warning(f"secondary_warmer_script not found for {currency}. Fetching from database...")
                
                # Get warmup data from database
                precomputed_bars, precomputed_zones = get_warmup_data_for_currency(currency)
                
                # Load the data into our algorithm
                load_preexisting_bars_and_indicators(precomputed_bars, currency)
                load_preexisting_zones(precomputed_zones, currency)
                
                debug_logger.warning(
                    f"Completed database warmup for {currency}:\n"
                    f"- Loaded {len(precomputed_bars)} bars\n"
                    f"- Initialized {len(precomputed_zones)} liquidity zones"
                )
                
            except Exception as e:
                debug_logger.error(f"Error during {currency} warmup: {e}", exc_info=True)
                debug_logger.warning(f"Continuing without warmup data for {currency}")
        
        # Final initialization status        
        trade_logger.info("Multi-currency algorithm warmup complete. Ready for live ticks.")
        
        return True
        
    except Exception as e:
        debug_logger.error(f"Error in main initialization: {e}", exc_info=True)
        return False


# Run initialization if this script is executed directly
if __name__ == "__main__":
    main()