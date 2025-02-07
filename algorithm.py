import pandas as pd
import numpy as np
import pandas_ta as ta
import logging
import pyodbc

# Use the named trade_logger and debug_logger created in app.py
trade_logger = logging.getLogger("trade_logger")
debug_logger = logging.getLogger("debug_logger")

# Constants (Trading Parameters)
WINDOW_LENGTH = 10
PIPS_REQUIRED = 0.005  # changed to 10 pips ONLY FOR TESTING, change back to 0.005 for 50 pips
SUPPORT_RESISTANCE_ALLOWANCE = 0.0011
STOP_LOSS_PROPORTION = 0.11
TAKE_PROFIT_PROPORTION = 0.65
LIQUIDITY_ZONE_ALERT = 0.002
DEMAND_ZONE_RSI_TOP = 70
SUPPLY_ZONE_RSI_BOTTOM = 30
INVALIDATE_ZONE_LENGTH = 0.0013
COOLDOWN_SECONDS = 3600
RISK_PROPORTION = 0.03

# Initialize global variables
historical_ticks = pd.DataFrame()      # Raw tick-by-tick data (trimmed to 12 hours)
bars = pd.DataFrame(columns=["open", "high", "low", "close"])  # Incrementally built 15-min bars
current_bar_start = None
current_bar_data = None

current_valid_zones_dict = {}
last_trade_time = None
trades = []
balance = 100000  # Initialize balance

# NEW: Track last non-hold signal (None, 'open', or 'close')
last_non_hold_signal = None

# NEW: Track the "cumulative zone" in-progress
cumulative_zone_info = None

# --------------------------------------------------------------------------
# PRE-EXISTING DATA LOADING FUNCTIONS
# --------------------------------------------------------------------------
def load_preexisting_zones(zones_dict):
    """
    Merges preexisting zones from an external source into current_valid_zones_dict.
    """
    global current_valid_zones_dict
    debug_logger.info("loading pre-existing liquidity zones and using it to initialize current_valid_zones_dict")

    for zone_id, zone_data in zones_dict.items():
        if zone_id not in current_valid_zones_dict:
            current_valid_zones_dict[zone_id] = zone_data
        else:
            debug_logger.info(f"After load, current_valid_zones_dict has {len(current_valid_zones_dict)} zones.")


def load_preexisting_bars_and_indicators(precomputed_bars_df):
    """
    Initializes the global 'bars' DataFrame with a precomputed set of 15-minute bars
    that should already have RSI, MACD, etc.
    """
    global bars
    debug_logger.info("Loading precomputed bars into 'bars' dataframe.")
    bars = precomputed_bars_df.copy()
    debug_logger.info(f"Loaded {len(bars)} bars from external source.")

# --------------------------------------------------------------------------
# TRADE STATE VALIDATION
# --------------------------------------------------------------------------
def validate_trade_state(current_time, current_price):
    """
    Ensures trade state consistency and fixes any issues found.
    Returns True if state was valid, False if fixes were needed.
    """
    global trades
    debug_logger.info(f"Validating trade state at {current_time}")
    
    state_valid = True
    open_trades = [t for t in trades if t['status'] == 'open']
    
    if len(open_trades) > 1:
        debug_logger.error(f"Invalid state: Found {len(open_trades)} open trades")
        state_valid = False
        # Keep only the earliest open trade
        earliest_trade = min(open_trades, key=lambda x: x['entry_time'])
        for trade in open_trades:
            if trade != earliest_trade:
                debug_logger.warning(f"Force closing duplicate trade from {trade['entry_time']}")
                trade.update({
                    'status': 'closed',
                    'exit_time': current_time,
                    'exit_price': current_price,
                    'profit_loss': 0,  # Force zero P/L for invalid trades
                    'close_reason': 'invalid_state'
                })

    # Check for any trades with invalid status
    for trade in trades:
        if trade['status'] not in ['open', 'closed']:
            debug_logger.error(f"Invalid trade status {trade['status']} found")
            state_valid = False
            trade['status'] = 'closed'
            trade['close_reason'] = 'invalid_status'
    
    return state_valid

def log_trade_state(current_time, location):
    """
    Enhanced logging of current trade state
    """
    open_trades = [t for t in trades if t['status'] == 'open']
    debug_logger.info(f"Trade state at {location} ({current_time}):")
    debug_logger.info(f"Open trades: {len(open_trades)}")
    for trade in open_trades:
        debug_logger.info(f"  Direction: {trade['direction']}, Entry: {trade['entry_price']}, Time: {trade['entry_time']}")

# --------------------------------------------------------------------------
# INCREMENTAL BAR UPDATING
# --------------------------------------------------------------------------
def update_bars_with_tick(tick_time, tick_price):
    """
    Incrementally build or update the current 15-minute bar,
    then finalize it when the bar ends and start a new bar.
    """
    global current_bar_start, current_bar_data, bars

    # Floor to the 15-minute boundary
    bar_start = tick_time.floor("15min")

    if current_bar_start is None:
        # First incoming tick
        current_bar_start = bar_start
        current_bar_data = {
            "open": tick_price,
            "high": tick_price,
            "low": tick_price,
            "close": tick_price
        }
        return

    if bar_start == current_bar_start:
        # Still in the same bar => update OHLC
        current_bar_data["close"] = tick_price
        if tick_price > current_bar_data["high"]:
            current_bar_data["high"] = tick_price
        if tick_price < current_bar_data["low"]:
            current_bar_data["low"] = tick_price
    else:
        # We have crossed into a new 15-min bar => finalize the old bar
        old_open = current_bar_data["open"]
        old_high = current_bar_data["high"]
        old_low = current_bar_data["low"]
        old_close = current_bar_data["close"]

        # Ensure bars has a proper DateTimeIndex
        if not isinstance(bars.index, pd.DatetimeIndex):
            bars.index = pd.to_datetime(bars.index)

        bars.loc[current_bar_start] = current_bar_data

        bar_count = len(bars)
        rsi_str = "Not enough data"
        macd_str = "Not enough data"

        # If we have at least 35 bars, attempt to compute RSI/MACD
        if bar_count >= 35:
            rolling_window_data = bars.tail(384).copy()
            close_series = rolling_window_data["close"].dropna()
            if len(close_series) >= 35:
                rsi_vals = ta.rsi(close_series, length=14)
                macd_vals = ta.macd(close_series, fast=12, slow=26, signal=9)
                if rsi_vals is not None and not rsi_vals.dropna().empty:
                    rsi_str = f"{rsi_vals.iloc[-1]:.2f}"
                if macd_vals is not None and not macd_vals.dropna().empty:
                    macd_line = macd_vals["MACD_12_26_9"]
                    macd_str = f"{macd_line.iloc[-1]:.2f}"

        debug_logger.warning(
            f"Finalized bar at {current_bar_start} | "
            f"OHLC=({old_open},{old_high},{old_low},{old_close}) | "
            f"Rolling bars count={bar_count}, max=384 | "
            f"RSI={rsi_str}, MACD={macd_str} | "
            f"Zones in dict={len(current_valid_zones_dict)}"
        )

        # Start a new bar
        current_bar_start = bar_start
        current_bar_data = {
            "open": tick_price,
            "high": tick_price,
            "low": tick_price,
            "close": tick_price
        }

    # Keep last 72 hours of bars for indicator calculations
    cutoff_time = tick_time - pd.Timedelta(hours=72)
    
    # Ensure index is datetime before filtering
    if not isinstance(bars.index, pd.DatetimeIndex):
        bars.index = pd.to_datetime(bars.index)
    
    bars = bars[bars.index >= cutoff_time]
# --------------------------------------------------------------------------
# ZONE DETECTION (CUMULATIVE LOGIC)
# --------------------------------------------------------------------------
def identify_liquidity_zones(data, current_valid_zones_dict):
    """
    Updated for CUMULATIVE zone detection:
      - We track a single 'cumulative_zone_info' across bars (persisting between calls).
      - A zone is finalized when abs(current_price - start_price) >= PIPS_REQUIRED.
    """
    global cumulative_zone_info
    data = data.copy()

    # For convenience, ensure these columns exist:
    data.loc[:, 'Liquidity_Zone'] = np.nan
    data.loc[:, 'Zone_Size'] = np.nan
    data.loc[:, 'Zone_Start_Price'] = np.nan
    data.loc[:, 'Zone_End_Price'] = np.nan
    data.loc[:, 'Zone_Length'] = np.nan
    data.loc[:, 'zone_type'] = ''

    for i in range(len(data)):
        if cumulative_zone_info is None:
            # Start tracking a new zone from this bar
            cumulative_zone_info = {
                'start_index': i,
                'start_price': data['close'].iloc[i]
            }

        current_close = data['close'].iloc[i]
        price_diff = current_close - cumulative_zone_info['start_price']

        # Check if we've moved enough from start_price
        if abs(price_diff) >= PIPS_REQUIRED:
            zone_type = 'demand' if price_diff > 0 else 'supply'
            start_i = cumulative_zone_info['start_index']
            end_i = i
            zone_size = abs(price_diff)

            zone_id = (cumulative_zone_info['start_price'], current_close)
            if zone_id not in current_valid_zones_dict:
                current_valid_zones_dict[zone_id] = {
                    'start_price': cumulative_zone_info['start_price'],
                    'end_price': current_close,
                    'zone_size': zone_size,
                    'loss_count': 0,
                    'zone_type': zone_type
                }
                debug_logger.debug(f"New zone added: {zone_id}, size={zone_size:.5f}, type={zone_type}")

            # Mark the bars from start_i to end_i with zone info
            data.loc[data.index[start_i:end_i+1], 'Liquidity_Zone'] = cumulative_zone_info['start_price']
            data.loc[data.index[start_i:end_i+1], 'Zone_Size'] = zone_size
            data.loc[data.index[start_i:end_i+1], 'Zone_Start_Price'] = cumulative_zone_info['start_price']
            data.loc[data.index[start_i:end_i+1], 'Zone_End_Price'] = current_close
            data.loc[data.index[start_i:end_i+1], 'Zone_Length'] = (end_i - start_i)
            data.loc[data.index[start_i:end_i+1], 'zone_type'] = zone_type

            # Reset: look for a new zone after this
            cumulative_zone_info = None

    debug_logger.debug("After identify_liquidity_zones (cumulative):")
    debug_logger.debug(data.tail(10))
    for col in ['Liquidity_Zone', 'Zone_Size', 'Zone_Start_Price', 'Zone_End_Price']:
        none_count = data[col].isna().sum()
        debug_logger.debug(f"{col} NaN count: {none_count}")

    debug_logger.debug(
        f"identify_liquidity_zones complete. {len(current_valid_zones_dict)} zones in current_valid_zones_dict."
    )
    return data, current_valid_zones_dict


def set_support_resistance_lines(data):
    """
    Adds a 'Support_Line' or 'Resistance_Line' to the bar data 
    based on zone type. This is unchanged.
    """
    data = data.copy()
    data.loc[:, 'Support_Line'] = np.where(
        data['zone_type'] == 'demand',
        data['Liquidity_Zone'] - SUPPORT_RESISTANCE_ALLOWANCE,
        np.nan
    )
    data.loc[:, 'Resistance_Line'] = np.where(
        data['zone_type'] == 'supply',
        data['Liquidity_Zone'] + SUPPORT_RESISTANCE_ALLOWANCE,
        np.nan
    )

    debug_logger.debug("After set_support_resistance_lines:")
    debug_logger.debug(data.tail(10))
    debug_logger.debug("Support_Line types: {}".format(data['Support_Line'].apply(lambda x: type(x)).unique()))
    debug_logger.debug("Resistance_Line types: {}".format(data['Resistance_Line'].apply(lambda x: type(x)).unique()))
    return data

# --------------------------------------------------------------------------
# REVERTED remove_consecutive_losers to ALLOW second chances
# --------------------------------------------------------------------------
def remove_consecutive_losers(trades, current_valid_zones_dict):
    """
    We look at each zone in current_valid_zones_dict and check
    the last consecutive trades for that zone. If there are
    2 consecutive losing trades in a row, we remove the zone.

    Once removed, if the same (start_price, end_price) zone
    is re-detected in the future, it starts fresh.
    """
    for zone_id in list(current_valid_zones_dict.keys()):
        relevant_trades = [
            t for t in trades
            if t['status'] == 'closed'
            and (t['zone_start_price'], t['zone_end_price']) == zone_id
        ]
        if not relevant_trades:
            continue

        # Sort them by exit_time descending, so we look at the most recent trades first
        relevant_trades.sort(key=lambda x: x['exit_time'], reverse=True)

        consecutive_losses = 0
        # check if the last trades are losers consecutively
        for t in relevant_trades:
            if t['profit_loss'] < 0:
                consecutive_losses += 1
            else:
                # A profitable or break-even trade resets consecutive losers
                break

            if consecutive_losses >= 2:
                debug_logger.debug(
                    f"Zone {zone_id} had 2 consecutive losing trades. Removing from valid zones."
                )
                del current_valid_zones_dict[zone_id]
                break

    return current_valid_zones_dict

# --------------------------------------------------------------------------
# Check current price vs. support/resistance to invalidate zones
# --------------------------------------------------------------------------
def invalidate_zones_via_sup_and_resist(current_price, current_valid_zones_dict):
    """
    Compare the current_price to each zone's threshold. 
    If price has violated it, remove the zone immediately.
    """
    zones_to_invalidate = []
    for zone_id, zone_data in list(current_valid_zones_dict.items()):
        zone_start = zone_data['start_price']
        zone_type = zone_data['zone_type']

        if zone_type == 'demand':
            support_line = zone_start - SUPPORT_RESISTANCE_ALLOWANCE
            # If price dips far below the zone's support line
            if current_price < (support_line - SUPPORT_RESISTANCE_ALLOWANCE):
                zones_to_invalidate.append(zone_id)

        elif zone_type == 'supply':
            resistance_line = zone_start + SUPPORT_RESISTANCE_ALLOWANCE
            # If price rises far above the zone's resistance line
            if current_price > (resistance_line + SUPPORT_RESISTANCE_ALLOWANCE):
                zones_to_invalidate.append(zone_id)

    for zone_id in zones_to_invalidate:
        debug_logger.debug(f"Price violated zone {zone_id}; removing.")
        del current_valid_zones_dict[zone_id]

    return current_valid_zones_dict

# --------------------------------------------------------------------------
# check_entry_conditions with explicit one-trade logic
# --------------------------------------------------------------------------
def check_entry_conditions(data, i, current_valid_zones_dict):
    """
    We only open a new trade if there are no trades currently open.
    The last trade MUST be closed before a new one can open.
    """
    global trades, last_trade_time, balance
    debug_logger.debug("In check_entry_conditions:")

    # Strict check for ANY open trades
    if any(trade.get('status', '') == 'open' for trade in trades):
        debug_logger.debug("Found open trade, blocking new entries")
        return current_valid_zones_dict

    debug_logger.debug("Latest Row:\n{}".format(data.iloc[i]))
    debug_logger.debug(
        f"RSI: {data['RSI'].iloc[i]}, "
        f"MACD_Line: {data['MACD_Line'].iloc[i]}, "
        f"Signal_Line: {data['Signal_Line'].iloc[i]}"
    )
    debug_logger.debug(f"Close price: {data['close'].iloc[i]}")

    current_price = data['close'].iloc[i]
    current_time = data.index[i]

    # Check each valid zone
    for zone_id, zone_data in list(current_valid_zones_dict.items()):
        zone_start = zone_data['start_price']
        zone_end = zone_data['end_price']
        zone_type = zone_data['zone_type']
        zone_size = abs(zone_end - zone_start)

        debug_logger.debug(f"Checking zone: {zone_id} | Type: {zone_type} | Size: {zone_size}")

        # Price must be within LIQUIDITY_ZONE_ALERT pips of zone start
        if abs(current_price - zone_start) > LIQUIDITY_ZONE_ALERT:
            continue

        # 1-hour cooldown after losing trade
        if trades and trades[-1]['status'] == 'closed' and trades[-1].get('profit_loss', 0) < 0:
            if last_trade_time and (current_time - last_trade_time).total_seconds() < COOLDOWN_SECONDS:
                continue

        # Must be >= PIPS_REQUIRED
        if zone_size < PIPS_REQUIRED or pd.isna(zone_size):
            debug_logger.debug(f"Invalid zone size {zone_size} for zone {zone_id}")
            continue

        position_size = 1  # placeholder

        # RSI/MACD conditions
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

        if not any(trade.get('status', '') == 'open' for trade in trades):
            # Create the trade
            trades.append({
                'entry_time': current_time,
                'entry_price': entry_price,
                'stop_loss': stop_loss,
                'take_profit': take_profit,
                'position_size': position_size,
                'status': 'open',
                'direction': 'long' if zone_type == 'demand' else 'short',
                'zone_start_price': zone_start,
                'zone_end_price': zone_end,
                'zone_length': zone_data.get('zone_length', 0)
            })

            last_trade_time = current_time
            trade_logger.info(f"Signal generated: {zone_type} trade at {entry_price}")
            debug_logger.debug(f"New {zone_type} trade opened")

            # Exit immediately after opening a trade
            return current_valid_zones_dict

    return current_valid_zones_dict

# --------------------------------------------------------------------------
# manage_trades
# --------------------------------------------------------------------------
def manage_trades(current_price, current_time):
    """
    Enhanced version with better state tracking and validation
    """
    global trades, balance
    debug_logger.info(f"Managing trades at {current_time} price {current_price}")
    
    # Validate trade state before processing
    validate_trade_state(current_time, current_price)
    log_trade_state(current_time, "before_manage")
    
    closed_any_trade = False
    trades_to_process = [t for t in trades if t['status'] == 'open']
    
    for trade in trades_to_process:
        # Skip if trade is already closed (shouldn't happen but safe check)
        if trade.get('status') != 'open':
            continue
            
        # Track if this specific trade was closed in this iteration
        trade_closed = False
        
        if trade['direction'] == 'long':
            if current_price <= trade['stop_loss'] and not trade_closed:
                trade_result = (trade['stop_loss'] - trade['entry_price']) * trade['position_size']
                balance += trade_result
                trade.update({
                    'status': 'closed',
                    'exit_time': current_time,
                    'exit_price': trade['stop_loss'],
                    'profit_loss': trade_result,
                    'close_reason': 'stop_loss'
                })
                trade_logger.info(f"Long trade hit stop loss at {trade['stop_loss']}. P/L: {trade_result}")
                closed_any_trade = True
                trade_closed = True

            elif current_price >= trade['take_profit'] and not trade_closed:
                trade_result = (trade['take_profit'] - trade['entry_price']) * trade['position_size']
                balance += trade_result
                trade.update({
                    'status': 'closed',
                    'exit_time': current_time,
                    'exit_price': trade['take_profit'],
                    'profit_loss': trade_result,
                    'close_reason': 'take_profit'
                })
                trade_logger.info(f"Long trade hit take profit at {trade['take_profit']}. P/L: {trade_result}")
                closed_any_trade = True
                trade_closed = True

        elif trade['direction'] == 'short':
            if current_price >= trade['stop_loss'] and not trade_closed:
                trade_result = (trade['entry_price'] - trade['stop_loss']) * trade['position_size']
                balance += trade_result
                trade.update({
                    'status': 'closed',
                    'exit_time': current_time,
                    'exit_price': trade['stop_loss'],
                    'profit_loss': trade_result,
                    'close_reason': 'stop_loss'
                })
                trade_logger.info(f"Short trade hit stop loss at {trade['stop_loss']}. P/L: {trade_result}")
                closed_any_trade = True
                trade_closed = True

            elif current_price <= trade['take_profit'] and not trade_closed:
                trade_result = (trade['entry_price'] - trade['take_profit']) * trade['position_size']
                balance += trade_result
                trade.update({
                    'status': 'closed',
                    'exit_time': current_time,
                    'exit_price': trade['take_profit'],
                    'profit_loss': trade_result,
                    'close_reason': 'take_profit'
                })
                trade_logger.info(f"Short trade hit take profit at {trade['take_profit']}. P/L: {trade_result}")
                closed_any_trade = True
                trade_closed = True

    # Validate trade state after processing
    validate_trade_state(current_time, current_price)
    log_trade_state(current_time, "after_manage")
    
    return closed_any_trade
# --------------------------------------------------------------------------
# MAIN TICK HANDLER
# --------------------------------------------------------------------------
def process_market_data(new_data_point):
    global historical_ticks, bars, current_valid_zones_dict
    global trades, last_trade_time, balance
    global last_non_hold_signal  # Keep track of the last non-hold signal

    try:
        # 1) Basic checks on the incoming tick
        raw_price = float(new_data_point['Price'].iloc[0])
        if raw_price < 0.5 or raw_price > 1.5:
            debug_logger.error(f"Received price ({raw_price}) outside normal range [0.5, 1.5]. Ignoring.")
            return 'hold'

        if pd.isna(new_data_point['Price'].iloc[0]):
            debug_logger.error("Received NaN or None price value")
            return 'hold'

        tick_time = new_data_point.index[-1]
        tick_price = float(new_data_point['Price'].iloc[-1])
        
        # 2) Validate trade state at the start
        validate_trade_state(tick_time, tick_price)
        log_trade_state(tick_time, "start_processing")

        # 3) Append to historical ticks, trim to 12 hours
        global historical_ticks
        historical_ticks = pd.concat([historical_ticks, new_data_point])
        latest_time = new_data_point.index[-1]
        cutoff_time = latest_time - pd.Timedelta(hours=12)
        historical_ticks = historical_ticks[historical_ticks.index >= cutoff_time]

        # 4) Update bars
        update_bars_with_tick(tick_time, tick_price)

        # 5) Manage existing trades (stop loss/take profit checks)
        closed_any_trade = manage_trades(tick_price, tick_time)

        # 6) Re-validate after managing trades
        validate_trade_state(tick_time, tick_price)

        # 7) Check if we have enough bar data
        if bars.empty:
            return 'hold'

        rolling_window_data = bars.tail(384).copy()
        if len(rolling_window_data) < 35:
            return 'hold'

        rolling_window_data['close'] = rolling_window_data['close'].where(
            rolling_window_data['close'].notnull(), np.nan
        )

        # 8) Calculate indicators (RSI, MACD)
        rolling_window_data['RSI'] = ta.rsi(rolling_window_data['close'], length=14)
        macd = ta.macd(rolling_window_data['close'], fast=12, slow=26, signal=9)
        
        if macd is None or macd.empty:
            return 'hold'

        macd.dropna(inplace=True)
        if macd.empty:
            return 'hold'

        rolling_window_data['MACD_Line'] = macd['MACD_12_26_9']
        rolling_window_data['Signal_Line'] = macd['MACDs_12_26_9']

        # 9) Identify and invalidate zones (CUMULATIVE logic for zone detection)
        rolling_window_data, current_valid_zones_dict = identify_liquidity_zones(
            rolling_window_data, current_valid_zones_dict
        )
        rolling_window_data = set_support_resistance_lines(rolling_window_data)
        latest_bar_idx = rolling_window_data.index[-1]
        i = rolling_window_data.index.get_loc(latest_bar_idx)

        # 10) Remove consecutive losers, invalidate zones
        current_valid_zones_dict = remove_consecutive_losers(trades, current_valid_zones_dict)
        current_valid_zones_dict = invalidate_zones_via_sup_and_resist(tick_price, current_valid_zones_dict)

        # 11) Attempt to open a new trade if none is open
        trades_before = len(trades)
        open_trades = [t for t in trades if t['status'] == 'open']
        if not open_trades:
            current_valid_zones_dict = check_entry_conditions(
                rolling_window_data, i, current_valid_zones_dict
            )
        new_trade_opened = (len(trades) > trades_before)

        # 12) Final validations and logging
        validate_trade_state(tick_time, tick_price)
        log_trade_state(tick_time, "end_processing")

        # -----------------------------------------------------------------
        # 13) Determine final signal: buy, sell, close, or 'hold'
        # -----------------------------------------------------------------
        if closed_any_trade:
            # Only generate close signal if we haven't already generated one for this trade
            if last_non_hold_signal not in ['close']:
                raw_signal = 'close'
            else:
                raw_signal = 'hold'
        elif new_trade_opened:
            # Figure out direction of the newly opened trade
            if trades[-1]['direction'] == 'long':
                raw_signal = 'buy'
            else:
                raw_signal = 'sell'
        else:
            raw_signal = 'hold'

        # 14) Block consecutive opens (either 'buy' or 'sell')
        if raw_signal in ['buy', 'sell'] and last_non_hold_signal in ['buy', 'sell']:
            # Already had a 'buy' or 'sell' before, so return hold
            raw_signal = 'hold'

        # 15) Update the last_non_hold_signal if we have a real signal
        if raw_signal not in ['hold']:
            last_non_hold_signal = raw_signal

        # 16) Log only non-hold signals
        if raw_signal not in ['hold']:
            trade_logger.info(f"Signal determined: {raw_signal}")

        return raw_signal

    except Exception as e:
        debug_logger.error(f"Error in process_market_data: {e}", exc_info=True)
        return 'hold'
# --------------------------------------------------------------------------
# OPTIONAL: Warmup script integration
# --------------------------------------------------------------------------
def main():
    try:
        from secondary_warmer_script import warmup_data
        precomputed_bars, precomputed_zones = warmup_data()
        load_preexisting_bars_and_indicators(precomputed_bars)
        load_preexisting_zones(precomputed_zones)
        
        # Get the latest RSI and MACD values
        latest_rsi = precomputed_bars['RSI'].iloc[-1] if not precomputed_bars['RSI'].empty else "N/A"
        latest_macd = precomputed_bars['MACD_Line'].iloc[-1] if not precomputed_bars['MACD_Line'].empty else "N/A"
        latest_signal = precomputed_bars['Signal_Line'].iloc[-1] if not precomputed_bars['Signal_Line'].empty else "N/A"
        
        # Enhanced logging
        debug_logger.warning(
            f"Algorithm warmup completed successfully:\n"
            f"- Loaded {len(precomputed_bars)} bars\n"
            f"- Initialized {len(current_valid_zones_dict)} liquidity zones\n"
            f"- Latest indicators:\n"
            f"  * RSI: {latest_rsi:.2f}\n"
            f"  * MACD: {latest_macd:.6f}\n"
            f"  * Signal: {latest_signal:.6f}"
        )
        trade_logger.info("Warm-up complete. Ready for live ticks.")
        
    except ImportError:
        debug_logger.warning("secondary_warmer_script not found. Running with empty bars and zones.")


