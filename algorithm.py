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
global_loss_count_dict = {}
last_trade_time = None
trades = []
balance = 100000  # Initialize balance

# --------------------------------------------------------------------------
# PRE-EXISTING DATA LOADING FUNCTIONS
# --------------------------------------------------------------------------
def load_preexisting_zones (zones_dict):
    """
    Merges preexisting zones from an external source into the current_valid_zones_dict.
    """
    global current_valid_zones_dict
    debug_logger.info("loading pre-existing liquidity zones and using it to initialize current_valid_zones_dict")

    #now we merge them in (but we could also overwirte them by using '= zones_dict.copy()' , for now I think its best to merge )
    for zone_id, zone_data in zones_dict.items():
        if zone_id not in current_valid_zones_dict: #they shouldnt be in, the dict is being initialized
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
        bars.loc[current_bar_start] = current_bar_data

        # Start a new bar
        current_bar_start = bar_start
        current_bar_data = {
            "open": tick_price,
            "high": tick_price,
            "low": tick_price,
            "close": tick_price
        }

    # Always trim bars to last 12 hours (in case we want older bars gone)
    cutoff_time = tick_time - pd.Timedelta(hours=12)
    bars = bars[bars.index >= cutoff_time]


def identify_liquidity_zones(data, current_valid_zones_dict):
    data = data.copy()
    data.loc[:, 'Liquidity_Zone'] = np.nan
    data.loc[:, 'Zone_Size'] = np.nan
    data.loc[:, 'Zone_Start_Price'] = np.nan
    data.loc[:, 'Zone_End_Price'] = np.nan
    data.loc[:, 'Zone_Length'] = np.nan
    data.loc[:, 'zone_type'] = ''

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

                # DEBUG: Log that we found a zone
                debug_logger.debug(
                    f"Zone found from i={start_index} to j={j}: "
                    f"start_price={start_price:.5f}, end_price={end_price:.5f}, "
                    f"size={price_movement:.5f}, type={zone_type}"
                )

                zone_id = (start_price, end_price)
                if zone_id not in current_valid_zones_dict:
                    current_valid_zones_dict[zone_id] = {
                        'start_price': start_price,
                        'end_price': end_price,
                        'zone_size': price_movement,
                        'loss_count': 0,
                        'zone_type': zone_type
                    }
                    # DEBUG: Log new zone added
                    debug_logger.debug(f"New zone added to dict: {zone_id}")

                data.loc[data.index[start_index:j+1], 'Liquidity_Zone'] = start_price
                data.loc[data.index[start_index:j+1], 'Zone_Size'] = price_movement
                data.loc[data.index[start_index:j+1], 'Zone_Start_Price'] = start_price
                data.loc[data.index[start_index:j+1], 'Zone_End_Price'] = end_price
                data.loc[data.index[start_index:j+1], 'Zone_Length'] = zone_length
                data.loc[data.index[start_index:j+1], 'zone_type'] = zone_type

                i = j
                break

        i += 1

    debug_logger.debug("After identify_liquidity_zones:")
    debug_logger.debug(data.tail(10))
    for col in ['Liquidity_Zone', 'Zone_Size', 'Zone_Start_Price', 'Zone_End_Price']:
        none_count = data[col].isna().sum()
        debug_logger.debug(f"{col} NaN count: {none_count}")

    # DEBUG: Log the size of current_valid_zones_dict
    debug_logger.debug(
        f"identify_liquidity_zones complete. {len(current_valid_zones_dict)} zones in current_valid_zones_dict."
    )

    return data, current_valid_zones_dict


def set_support_resistance_lines(data):
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


def remove_consecutive_losers(trades, current_valid_zones_dict):
    loss_count_dict = {}
    for trade in trades:
        if trade['status'] == 'closed' and 'profit_loss' in trade:
            zone_id = (trade['zone_start_price'], trade['zone_end_price'])
            if trade['profit_loss'] < 0:
                loss_count_dict[zone_id] = loss_count_dict.get(zone_id, 0) + 1
                if loss_count_dict[zone_id] >= 2:
                    if zone_id in current_valid_zones_dict:
                        del current_valid_zones_dict[zone_id]
            else:
                loss_count_dict[zone_id] = 0

    return loss_count_dict, current_valid_zones_dict


def invalidate_zones_via_sup_and_resist(data, current_valid_zones_dict):
    data = data.copy()
    debug_logger.debug("Before invalidate_zones_via_sup_and_resist:")
    debug_logger.debug(data.tail(5))
    debug_logger.debug("Support_Line sample: {}, Resistance_Line sample: {}".format(
        data['Support_Line'].iloc[-1], data['Resistance_Line'].iloc[-1])
    )

    if data['Support_Line'].iloc[-1] is None or data['Resistance_Line'].iloc[-1] is None:
        debug_logger.debug("Found None in Support/Resistance lines!")

    support_violation = data['close'] < (data['Support_Line'] - SUPPORT_RESISTANCE_ALLOWANCE)
    resistance_violation = data['close'] > (data['Resistance_Line'] + SUPPORT_RESISTANCE_ALLOWANCE)
    invalid_zones = support_violation | resistance_violation

    zones_to_invalidate = data.loc[invalid_zones, ['Zone_Start_Price', 'Zone_End_Price']].dropna()
    for _, zone in zones_to_invalidate.iterrows():
        zone_id = (zone['Zone_Start_Price'], zone['Zone_End_Price'])
        if zone_id in current_valid_zones_dict:
            del current_valid_zones_dict[zone_id]

    return current_valid_zones_dict


def check_entry_conditions(data, i, loss_count_dict, current_valid_zones_dict):
    global trades, last_trade_time, balance
    debug_logger.debug("In check_entry_conditions:")
    debug_logger.debug("Latest Row:\n{}".format(data.iloc[i]))
    debug_logger.debug(
        f"RSI: {data['RSI'].iloc[i]}, "
        f"MACD_Line: {data['MACD_Line'].iloc[i]}, "
        f"Signal_Line: {data['Signal_Line'].iloc[i]}"
    )
    debug_logger.debug(f"Close price: {data['close'].iloc[i]}")

    # If any trade is currently open, skip opening a new one
    if any(trade['status'] == 'open' for trade in trades):
        return current_valid_zones_dict

    current_price = data['close'].iloc[i]
    zone_id = (data['Zone_Start_Price'].iloc[i], data['Zone_End_Price'].iloc[i])
    debug_logger.debug(f"zone_id: {zone_id}")

    if pd.isna(zone_id[0]) or pd.isna(zone_id[1]):
        return current_valid_zones_dict

    if zone_id not in current_valid_zones_dict:
        return current_valid_zones_dict

    zone_type = current_valid_zones_dict[zone_id]['zone_type']
    liquidity_zone = data['Liquidity_Zone'].iloc[i]
    zone_size = data['Zone_Size'].iloc[i]
    debug_logger.debug(f"zone_type: {zone_type}, liquidity_zone: {liquidity_zone}, zone_size: {zone_size}")

    if trades and trades[-1]['status'] == 'closed' and trades[-1]['profit_loss'] < 0:
        if last_trade_time is not None:
            if (data.index[i] - last_trade_time).total_seconds() < COOLDOWN_SECONDS:
                return current_valid_zones_dict

    if abs(current_price - liquidity_zone) <= LIQUIDITY_ZONE_ALERT:
        if zone_id in loss_count_dict and loss_count_dict[zone_id] >= 2:
            return current_valid_zones_dict

        risk_amount = balance * RISK_PROPORTION
        if zone_size == 0 or pd.isna(zone_size):
            debug_logger.debug("zone_size is zero or NaN, cannot open position.")
            return current_valid_zones_dict

        position_size = risk_amount / (STOP_LOSS_PROPORTION * zone_size)
        position_size = max(position_size, 1)
        debug_logger.debug(
            f"Trying to open trade: {zone_type}, RSI: {data['RSI'].iloc[i]}, "
            f"MACD: {data['MACD_Line'].iloc[i]}, Signal: {data['Signal_Line'].iloc[i]}"
        )

        if zone_type == 'demand':
            # Buy logic
            if (data['RSI'].iloc[i] < DEMAND_ZONE_RSI_TOP) or (data['MACD_Line'].iloc[i] > data['Signal_Line'].iloc[i]):
                entry_price = current_price
                stop_loss = entry_price - (STOP_LOSS_PROPORTION * zone_size)
                take_profit = entry_price + (TAKE_PROFIT_PROPORTION * zone_size)
                trades.append({
                    'entry_time': data.index[i],
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'take_profit': take_profit,
                    'position_size': position_size,
                    'status': 'open',
                    'direction': 'long',
                    'zone_start_price': data['Zone_Start_Price'].iloc[i],
                    'zone_end_price': data['Zone_End_Price'].iloc[i],
                    'zone_length': data['Zone_Length'].iloc[i]
                })
                last_trade_time = data.index[i]
                trade_logger.info(f"Opened long trade at {entry_price} with position size {position_size}")
                debug_logger.debug("Opened long trade.")
        elif zone_type == 'supply':
            # Sell logic
            if (data['RSI'].iloc[i] > SUPPLY_ZONE_RSI_BOTTOM) or (data['MACD_Line'].iloc[i] < data['Signal_Line'].iloc[i]):
                entry_price = current_price
                stop_loss = entry_price + (STOP_LOSS_PROPORTION * zone_size)
                take_profit = entry_price - (TAKE_PROFIT_PROPORTION * zone_size)
                trades.append({
                    'entry_time': data.index[i],
                    'entry_price': entry_price,
                    'stop_loss': stop_loss,
                    'take_profit': take_profit,
                    'position_size': position_size,
                    'status': 'open',
                    'direction': 'short',
                    'zone_start_price': data['Zone_Start_Price'].iloc[i],
                    'zone_end_price': data['Zone_End_Price'].iloc[i],
                    'zone_length': data['Zone_Length'].iloc[i]
                })
                last_trade_time = data.index[i]
                trade_logger.info(f"Opened short trade at {entry_price} with position size {position_size}")
                debug_logger.debug("Opened short trade.")

    return current_valid_zones_dict


def manage_trades(latest_bar):
    """
    Checks the last bar's close to see if any open trades should close.
    IMPORTANT: We produce a 'close' signal if a trade is closed,
    even if it opened and closed in the same call.
    """
    global trades, balance
    debug_logger.debug("In manage_trades:")
    debug_logger.debug(f"Current trades: {trades}")

    if trades:
        current_price = latest_bar['close'].iloc[-1]
        current_time = latest_bar.index[-1]

        closed_any_trade = False

        for trade in trades:
            if trade['status'] == 'closed':
                continue

            # Check if we must close the trade
            if trade['direction'] == 'long':
                if current_price <= trade['stop_loss']:
                    trade_result = (trade['stop_loss'] - trade['entry_price']) * trade['position_size']
                    balance += trade_result
                    trade.update({
                        'status': 'closed',
                        'exit_time': current_time,
                        'exit_price': trade['stop_loss'],
                        'profit_loss': trade_result
                    })
                    trade_logger.info(f"Long trade hit stop loss at {trade['stop_loss']}. Profit/Loss: {trade_result}")
                    debug_logger.debug("Long trade closed at stop loss.")
                    closed_any_trade = True
                elif current_price >= trade['take_profit']:
                    trade_result = (trade['take_profit'] - trade['entry_price']) * trade['position_size']
                    balance += trade_result
                    trade.update({
                        'status': 'closed',
                        'exit_time': current_time,
                        'exit_price': trade['take_profit'],
                        'profit_loss': trade_result
                    })
                    trade_logger.info(f"Long trade hit take profit at {trade['take_profit']}. Profit/Loss: {trade_result}")
                    debug_logger.debug("Long trade closed at take profit.")
                    closed_any_trade = True

            elif trade['direction'] == 'short':
                if current_price >= trade['stop_loss']:
                    trade_result = (trade['entry_price'] - trade['stop_loss']) * trade['position_size']
                    balance += trade_result
                    trade.update({
                        'status': 'closed',
                        'exit_time': current_time,
                        'exit_price': trade['stop_loss'],
                        'profit_loss': trade_result
                    })
                    trade_logger.info(f"Short trade hit stop loss at {trade['stop_loss']}. Profit/Loss: {trade_result}")
                    debug_logger.debug("Short trade closed at stop loss.")
                    closed_any_trade = True
                elif current_price <= trade['take_profit']:
                    trade_result = (trade['entry_price'] - trade['take_profit']) * trade['position_size']
                    balance += trade_result
                    trade.update({
                        'status': 'closed',
                        'exit_time': current_time,
                        'exit_price': trade['take_profit'],
                        'profit_loss': trade_result
                    })
                    trade_logger.info(f"Short trade hit take profit at {trade['take_profit']}. Profit/Loss: {trade_result}")
                    debug_logger.debug("Short trade closed at take profit.")
                    closed_any_trade = True

        return closed_any_trade
    return False


def process_market_data(new_data_point):
    """
    Receives the latest tick in new_data_point,
    increments bars, calculates indicators, enters/exits trades, etc.
    """
    try:
        global historical_ticks, bars, current_valid_zones_dict
        global trades, last_trade_time, global_loss_count_dict, balance

        # ------------------------------------------------------------------
        # NUMERICAL SAFEGUARD - Discard prices outside [0.5, 1.5]
        # ------------------------------------------------------------------
        
        raw_price = float(new_data_point['Price'].iloc[0])
        if raw_price < 0.5 or raw_price > 1.5:
            debug_logger.error(f"Received price ({raw_price}) outside normal range [0.5, 1.5]. Ignoring this tick.")
            return 'hold'
        

        # Validate new data point
        if pd.isna(new_data_point['Price'].iloc[0]):
            debug_logger.error("Received NaN or None price value")
            return 'hold'

        # Append the new data point to a separate DataFrame of ticks
        historical_ticks = pd.concat([historical_ticks, new_data_point])

        # Trim ticks to last 12 hours
        latest_time = new_data_point.index[-1]
        cutoff_time = latest_time - pd.Timedelta(hours=12)
        historical_ticks = historical_ticks[historical_ticks.index >= cutoff_time]

        # Perform incremental bar update
        tick_time = new_data_point.index[-1]
        tick_price = float(new_data_point['Price'].iloc[-1])
        update_bars_with_tick(tick_time, tick_price)

        if bars.empty:
            trade_logger.info("No valid bars. Holding position.")
            return 'hold'

        # We'll take the last 60 bars for indicators
        rolling_window_data = bars.tail(60).copy()

        # If fewer than ~35 bars, MACD won't be stable
        if len(rolling_window_data) < 35:
            trade_logger.info("Not enough bars for stable MACD. Holding position.")
            return 'hold'

        # Convert any None to NaN explicitly
        rolling_window_data['close'] = rolling_window_data['close'].where(
            rolling_window_data['close'].notnull(), np.nan
        )

        debug_logger.debug("Close values before MACD:")
        debug_logger.debug(rolling_window_data['close'].tail(60))
        debug_logger.debug("Check for None in close: {}".format(
            rolling_window_data['close'].isnull().any())
        )
        debug_logger.debug("Types in close column: {}".format(
            rolling_window_data['close'].apply(type).value_counts())
        )

        # Calculate RSI
        rolling_window_data['RSI'] = ta.rsi(rolling_window_data['close'], length=14)

        # Calculate MACD
        macd = ta.macd(rolling_window_data['close'], fast=12, slow=26, signal=9)
        if macd is None or macd.empty:
            debug_logger.debug("MACD returned None or empty. Skipping signal generation.")
            return 'hold'

        macd.dropna(inplace=True)
        if macd.empty:
            debug_logger.debug("MACD all NaN in the 60-bar window. Holding.")
            return 'hold'

        rolling_window_data['MACD_Line'] = macd['MACD_12_26_9']
        rolling_window_data['Signal_Line'] = macd['MACDs_12_26_9']

        debug_logger.debug("After indicators:")
        debug_logger.debug(rolling_window_data.tail(5))

        # Identify liquidity zones
        rolling_window_data, current_valid_zones_dict = identify_liquidity_zones(
            rolling_window_data, current_valid_zones_dict
        )

        # Set S/R lines
        rolling_window_data = set_support_resistance_lines(rolling_window_data)

        # Prepare the "latest bar" as a 1-row DataFrame (for trade management)
        latest_data_point = rolling_window_data.iloc[[-1]]

        # Remove consecutive losers
        global_loss_count_dict, current_valid_zones_dict = remove_consecutive_losers(
            trades, current_valid_zones_dict
        )

        # Invalidate zones
        current_valid_zones_dict = invalidate_zones_via_sup_and_resist(
            latest_data_point, current_valid_zones_dict
        )

        trades_before = len(trades)

        # Check entry conditions
        current_valid_zones_dict = check_entry_conditions(
            rolling_window_data, -1, global_loss_count_dict, current_valid_zones_dict
        )

        trades_before_manage = trades.copy()

        # Manage trades => returns True if any trades closed
        closed_any_trade = manage_trades(latest_data_point)

        new_trade_opened = (len(trades) > trades_before)
        open_trades_before = [t for t in trades_before_manage if t['status'] == 'open']
        open_trades_after = [t for t in trades if t['status'] == 'open']

        # We must produce exactly one signal per call,
        # ensuring 'close' if any trade closed, else new trade signals, else hold.
        if closed_any_trade:
            # If trades closed, we produce a 'close' signal
            signal = 'close'
        elif new_trade_opened:
            # If we opened a new trade
            if trades[-1]['direction'] == 'long':
                signal = 'buy'
            else:
                signal = 'sell'
        else:
            signal = 'hold'

        trade_logger.info(f"Signal determined: {signal}")
        return signal

    except Exception as e:
        debug_logger.error(f"Error in process_market_data: {e}", exc_info=True)
        return 'hold'


# --------------------------------------------------------------------------
# NEW SECTION: CALLING THE SECONDARY SCRIPT BEFORE LIVE TRADING
# --------------------------------------------------------------------------
"""
Below, we import `warmup_data()` from the secondary warmer script, run it, 
and use the resulting DataFrame + dictionary to initialize the global 
`bars` and `current_valid_zones_dict` BEFORE processing live data.
"""

try:
    # 1) Import the function from your secondary script
    from secondary_warmer_script import warmup_data

    def main():
        # 2) Call the secondary script's function
        precomputed_bars, precomputed_zones = warmup_data()

        # 3) Initialize our global 'bars' and 'current_valid_zones_dict'
        load_preexisting_bars_and_indicators(precomputed_bars)
        load_preexisting_zones(precomputed_zones)

        # 4) Now that everything is warm, proceed as normal (e.g. run a loop for live data)
        # For demonstration, do nothing else:
        trade_logger.info("Warm-up complete. Ready for live ticks.")

    if __name__ == "__main__":
        # If this file is run directly, call main().
        main()

except ImportError:
    # If the secondary script isn't available, you can decide how to proceed:
    debug_logger.warning("secondary_warmer_script not found. Running with empty bars and zones.")

