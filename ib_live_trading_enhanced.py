#!/usr/bin/env python3
"""
Enhanced Interactive Brokers Live Trading Script
Consolidated version with all optimizations:
- Robust reconnection with exponential backoff
- Hybrid market detection (data flow + timezone backup)
- Frequency-optimized monitoring (30s data checks, 5min context updates)
- Enhanced error handling and position management
"""

from ib_insync import *
import nest_asyncio
# Apply nest_asyncio to handle event loop conflicts when run as subprocess
nest_asyncio.apply()

import requests
import logging
import datetime
import time
import threading
import pytz
import os
from collections import defaultdict
from typing import Dict, Optional
from datetime import timedelta
from flask import Flask, request, jsonify
from waitress import serve

# Configure logging with UTF-8 encoding to handle Unicode characters
logging.basicConfig(
    level=logging.INFO,  # Back to INFO level - no more debug spam
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ib_trading_enhanced.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# For Windows compatibility, handle console encoding
import sys
try:
    if hasattr(sys.stdout, 'reconfigure') and sys.stdout.encoding != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
except Exception:
    # If reconfiguration fails, continue with default encoding
    pass

class EnhancedIBTradingBot:
    def __init__(self, 
                 ib_host='127.0.0.1', 
                 ib_port=7497,  # TWS paper trading port
                 client_id=1,
                 api_url='http://localhost:5000/trade_signal',
                 position_size=30000):
        """Initialize Enhanced IB Trading Bot with all optimizations"""
        
        self.ib = IB()
        self.ib_host = ib_host
        self.ib_port = ib_port
        self.client_id = client_id
        self.api_url = api_url
        self.position_size = position_size
        
        # Currency pairs - using standard IB format without dots
        self.currency_pairs = {
            'EUR.USD': 'EURUSD',
            'USD.CAD': 'USDCAD', 
            'GBP.USD': 'GBPUSD'
        }
        
        # Position tracking
        self.positions: Dict[str, Optional[str]] = {currency: None for currency in self.currency_pairs.keys()}
        self.last_signals: Dict[str, str] = {currency: 'hold' for currency in self.currency_pairs.keys()}
        
        # Contract and ticker storage
        self.contracts = {}
        self.tickers = {}
        
        # Order tracking
        self.pending_orders = {}
        self.price_logs = []
        
        # Connection management
        self.is_connected = False
        self.reconnection_attempts = 0
        self.max_reconnection_attempts = 10
        self.shutdown_requested = False
        
        # Data monitoring
        self.last_tick_times = {currency: None for currency in self.currency_pairs.keys()}
        self.last_heartbeat = datetime.datetime.now()
        
        # Recent prices tracking for RECONNECT status
        self.recent_prices = {currency: [] for currency in self.currency_pairs.keys()}
        
        # Market context cache (for frequency optimization)
        self.market_context_cache = {
            'timezone_says_open': False,
            'active_sessions': [],
            'last_updated': None,
            'total_liquidity_score': 0
        }
        
        # Rate limiting
        self._last_recovery_attempt = datetime.datetime.now() - timedelta(minutes=3)
        self._detailed_check_counter = 0
        
        # Price throttling - only send at most 2 updates per second per currency
        self.price_throttle_seconds = 0.5  # 500ms = 2/second
        self.last_price_send_times = {currency: datetime.datetime.min for currency in self.currency_pairs.keys()}
        
        # Thread lock
        self.position_lock = threading.Lock()
        
        # HTTP API for command handling
        self.app = Flask(__name__)
        self.setup_http_api()
        self.http_port = int(os.environ.get('IB_API_PORT', '5001'))  # Default port 5001
        
        logger.info(f"[INIT] Enhanced IB Trading Bot initialized for {len(self.currency_pairs)} currencies")
        logger.info(f"[INIT] IB connection will use: {self.ib_host}:{self.ib_port} (clientId: {self.client_id})")
        logger.info(f"[INIT] Price throttling: {1/self.price_throttle_seconds:.1f} updates/second per currency (max {3/self.price_throttle_seconds:.1f} total)")
        logger.info(f"[INIT] HTTP API will listen on port {self.http_port}")

    def setup_http_api(self):
        """Setup HTTP API endpoints for command handling"""
        
        @self.app.route('/command', methods=['POST'])
        def handle_command():
            try:
                content = request.json
                command = content.get('command', '').strip().upper()
                
                if not command:
                    return jsonify({'error': 'No command provided'}), 400
                
                # Execute command through the existing execute_command method
                result = self.execute_command(command)
                
                return jsonify({
                    'command': command,
                    'result': result,
                    'status': 'success'
                }), 200
                
            except Exception as e:
                logger.error(f"[HTTP-API] Error handling command: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/status', methods=['GET'])
        def get_status():
            try:
                status_data = {
                    'connected': self.is_connected,
                    'positions': {},
                    'pending_orders': len(self.pending_orders),
                    'last_heartbeat': self.last_heartbeat.isoformat() if self.last_heartbeat else None,
                    'market_data': {}
                }
                
                # Add position information
                with self.position_lock:
                    for currency, position in self.positions.items():
                        status_data['positions'][currency] = position if position else 'flat'
                
                # Add market data status
                for currency, last_tick in self.last_tick_times.items():
                    if last_tick:
                        seconds_since = (datetime.datetime.now() - last_tick).total_seconds()
                        status_data['market_data'][currency] = {
                            'last_tick': last_tick.isoformat(),
                            'seconds_ago': seconds_since,
                            'status': 'active' if seconds_since < 30 else 'stale'
                        }
                    else:
                        status_data['market_data'][currency] = {
                            'last_tick': None,
                            'seconds_ago': None,
                            'status': 'no_data'
                        }
                
                return jsonify(status_data), 200
                
            except Exception as e:
                logger.error(f"[HTTP-API] Error getting status: {e}")
                return jsonify({'error': str(e)}), 500
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            """Simple health check endpoint"""
            return jsonify({
                'status': 'running',
                'connected': self.is_connected
            }), 200

    def start_http_server(self):
        """Start the HTTP API server in a separate thread"""
        def run_server():
            logger.info(f"[HTTP-API] Starting HTTP API server on port {self.http_port}")
            serve(self.app, host='0.0.0.0', port=self.http_port, threads=4)
        
        server_thread = threading.Thread(target=run_server, daemon=True)
        server_thread.start()
        logger.info(f"[HTTP-API] HTTP API server started on port {self.http_port}")

    def connect_to_ib(self):
        """Connect to IB with enhanced error handling"""
        try:
            logger.info(f"Connecting to IB at {self.ib_host}:{self.ib_port}")
            
            if self.ib.isConnected():
                self.ib.disconnect()
                time.sleep(2)
            
            self.ib.connect(self.ib_host, self.ib_port, clientId=self.client_id)
            
            # Set up all event handlers
            self.ib.orderStatusEvent += self.on_order_status
            self.ib.execDetailsEvent += self.on_execution
            self.ib.pendingTickersEvent += self.on_pending_tickers
            self.ib.disconnectedEvent += self.on_disconnected
            self.ib.errorEvent += self.on_error
            
            self.is_connected = True
            self.reconnection_attempts = 0
            self.last_heartbeat = datetime.datetime.now()
            
            logger.info("[OK] Successfully connected to IB")
            return True
            
        except Exception as e:
            logger.error(f"[ERROR] Connection failed: {e}")
            self.is_connected = False
            return False

    def setup_market_data(self):
        """Set up market data with error handling"""
        try:
            if not self.is_connected:
                logger.error("[ERROR] Cannot setup market data - not connected to IB")
                return False
            
            self.contracts.clear()
            self.tickers.clear()
            
            for api_currency, ib_currency in self.currency_pairs.items():
                try:
                    # Try standard symbol format first, fallback to base/quote
                    try:
                        contract = Forex(ib_currency)
                    except:
                        # Fallback to base/quote format
                        if ib_currency == 'EURUSD':
                            contract = Forex('EUR', 'USD')
                        elif ib_currency == 'USDCAD':
                            contract = Forex('USD', 'CAD')
                        elif ib_currency == 'GBPUSD':
                            contract = Forex('GBP', 'USD')
                        else:
                            raise Exception(f"Unknown currency pair: {ib_currency}")
                    
                    self.contracts[api_currency] = contract
                    ticker = self.ib.reqMktData(contract, '', False, False)
                    self.tickers[api_currency] = ticker
                    
                    logger.info(f"[OK] Market data: {api_currency}")
                    time.sleep(0.2)  # Reduced from 0.5s to speed up setup
                    
                except Exception as contract_error:
                    logger.error(f"[ERROR] Failed to setup {api_currency}: {contract_error}")
                    return False
            
            logger.info("[OK] Market data setup complete")
            return True
            
        except Exception as e:
            logger.error(f"[ERROR] Market data setup failed: {e}")
            return False

    def on_disconnected(self):
        """Handle disconnection with automatic reconnection"""
        logger.warning("[RECONNECT] IB disconnected - starting reconnection")
        self.is_connected = False
        
        if not self.shutdown_requested:
            threading.Thread(target=self.handle_reconnection, daemon=True).start()

    def handle_reconnection(self):
        """Automatic reconnection with exponential backoff"""
        while (not self.is_connected and 
               not self.shutdown_requested and 
               self.reconnection_attempts < self.max_reconnection_attempts):
            
            self.reconnection_attempts += 1
            wait_time = min(5 * (2 ** (self.reconnection_attempts - 1)), 300)
            
            logger.info(f"[RECONNECT] Attempt {self.reconnection_attempts}/{self.max_reconnection_attempts} in {wait_time}s")
            time.sleep(wait_time)
            
            try:
                if self.connect_to_ib():
                    time.sleep(2)
                    if self.setup_market_data():
                        logger.info("[OK] Reconnection successful!")
                        return
                    else:
                        self.is_connected = False
            except Exception as e:
                logger.error(f"[ERROR] Reconnection failed: {e}")
        
        if self.reconnection_attempts >= self.max_reconnection_attempts:
            logger.error("[ERROR] Max reconnection attempts reached")
            logger.error("[ERROR] Automatic reconnection stopped. Use 'RECONNECT' command to manually reconnect.")
            logger.error("[ERROR] Send command via: python send_command.py RECONNECT")

    def on_error(self, reqId, errorCode, errorString, contract):
        """Enhanced error handling"""
        critical_errors = [502, 504, 1100, 1101, 1102]
        market_data_errors = [2103, 2104, 2106, 2119, 2158]
        
        if errorCode in critical_errors:
            logger.error(f"[ERROR] Critical Error {errorCode}: {errorString}")
            self.is_connected = False
            if not self.shutdown_requested:
                threading.Thread(target=self.handle_reconnection, daemon=True).start()
                
        elif errorCode in market_data_errors:
            if errorCode == 2103:
                logger.error(f"[ERROR] Market data broken: {errorString}")
                threading.Thread(target=self.recover_market_data, daemon=True).start()
            else:
                logger.info(f"[INFO] Market data: {errorString}")
        else:
            logger.info(f"[INFO] IB Message {errorCode}: {errorString}")

    def get_midpoint_price(self, ticker) -> Optional[float]:
        """Calculate midpoint with validation"""
        try:
            if (ticker.bid and ticker.ask and 
                not util.isNan(ticker.bid) and not util.isNan(ticker.ask) and
                ticker.bid > 0 and ticker.ask > 0 and ticker.ask > ticker.bid):
                return (ticker.bid + ticker.ask) / 2.0
            elif ticker.last and not util.isNan(ticker.last) and ticker.last > 0:
                return ticker.last
            return None
        except Exception as e:
            logger.warning(f"Price calculation error: {e}")
            return None

    def on_pending_tickers(self, tickers):
        """Handle market data with heartbeat tracking"""
        current_time = datetime.datetime.now()
        self.last_heartbeat = current_time
        
        for ticker in tickers:
            currency = None
            for api_curr, stored_ticker in self.tickers.items():
                if stored_ticker.contract.symbol == ticker.contract.symbol:
                    currency = api_curr
                    break
            
            if currency is None:
                continue
            
            self.last_tick_times[currency] = current_time
            
            price = self.get_midpoint_price(ticker)
            if price is None:
                continue
                
            if not self.is_price_reasonable(currency, price):
                logger.warning(f"[PRICE] {currency} - Unusual price: {price:.5f}")
                continue
            
            # Store recent price for RECONNECT status
            if currency not in self.recent_prices:
                self.recent_prices[currency] = []
            self.recent_prices[currency].append({
                'price': price,
                'time': current_time,
                'bid': ticker.bid,
                'ask': ticker.ask
            })
            # Keep only last 10 prices
            if len(self.recent_prices[currency]) > 10:
                self.recent_prices[currency].pop(0)
            
            # THROTTLE: Only send price updates at most 2 per second per currency
            current_time = datetime.datetime.now()
            time_since_last = (current_time - self.last_price_send_times[currency]).total_seconds()
            
            if time_since_last >= self.price_throttle_seconds:
                # Log price data occasionally (every 20th send to avoid spam)
                if not hasattr(self, '_price_log_counter'):
                    self._price_log_counter = {}
                if currency not in self._price_log_counter:
                    self._price_log_counter[currency] = 0
                
                self._price_log_counter[currency] += 1
                if self._price_log_counter[currency] % 20 == 0:
                    logger.info(f"[PRICE] {currency}: {price:.5f} (bid: {ticker.bid}, ask: {ticker.ask})")
                
                # Send to algorithm and update last send time
                self.send_to_algorithm_with_retry(currency, price, ticker.time)
                self.last_price_send_times[currency] = current_time

    def is_price_reasonable(self, currency: str, price: float) -> bool:
        """Basic price validation"""
        ranges = {
            'EUR.USD': (0.9, 1.3),
            'USD.CAD': (1.2, 1.5),
            'GBP.USD': (1.0, 1.6)
        }
        if currency in ranges:
            min_price, max_price = ranges[currency]
            return min_price <= price <= max_price
        return True

    def send_to_algorithm_with_retry(self, currency: str, price: float, timestamp, max_retries=3):
        """Send to API with retry logic"""
        for attempt in range(max_retries):
            try:
                time_str = timestamp.strftime("%Y-%m-%d %H:%M:%S.%f") if timestamp else datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                
                payload = {
                    "data": {"Price": price, "Time": time_str},
                    "currency": currency
                }
                
                response = requests.post(self.api_url, json=payload, timeout=10)
                
                if response.status_code == 200:
                    result = response.json()
                    signal = result.get("signal", "hold")
                    returned_currency = result.get("currency", currency)
                    
                    if signal != "hold" and signal != self.last_signals.get(returned_currency, "hold"):
                        logger.info(f"[SIGNAL] {signal} for {returned_currency} at {price}")
                        self.handle_signal(returned_currency, signal, price)
                        self.last_signals[returned_currency] = signal
                    elif signal == "hold":
                        self.last_signals[returned_currency] = "hold"
                    return
                else:
                    logger.error(f"[ERROR] API error {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    logger.error(f"[ERROR] API failed after {max_retries} attempts: {e}")

    def handle_signal(self, currency: str, signal: str, current_price: float):
        """Handle trade signals with position management"""
        with self.position_lock:
            try:
                current_position = self.positions.get(currency)
                
                if signal == "buy":
                    if current_position is None:
                        self.place_order_with_retry(currency, "BUY", current_price)
                        self.positions[currency] = "long"
                    elif current_position == "short":
                        self.place_order_with_retry(currency, "BUY", current_price)
                        self.positions[currency] = None
                        
                elif signal == "sell":
                    if current_position is None:
                        self.place_order_with_retry(currency, "SELL", current_price)
                        self.positions[currency] = "short"
                    elif current_position == "long":
                        self.place_order_with_retry(currency, "SELL", current_price)
                        self.positions[currency] = None
                        
                elif signal == "close":
                    if current_position == "long":
                        self.place_order_with_retry(currency, "SELL", current_price)
                        self.positions[currency] = None
                    elif current_position == "short":
                        self.place_order_with_retry(currency, "BUY", current_price)
                        self.positions[currency] = None
                        
            except Exception as e:
                logger.error(f"[ERROR] Signal handling error: {e}")

    def place_order_with_retry(self, currency: str, action: str, intended_price: float, max_retries=3):
        """Place order with retry logic"""
        for attempt in range(max_retries):
            try:
                if not self.is_connected:
                    logger.error(f"[ERROR] Cannot place order - not connected")
                    return
                
                contract = self.contracts.get(currency)
                if not contract:
                    logger.error(f"[ERROR] No contract for {currency}")
                    return
                
                order = MarketOrder(action, self.position_size)
                trade = self.ib.placeOrder(contract, order)
                
                self.pending_orders[trade.order.orderId] = {
                    'currency': currency,
                    'action': action,
                    'intended_price': intended_price,
                    'quantity': self.position_size,
                    'timestamp': datetime.datetime.now()
                }
                
                logger.info(f"[ORDER] Order placed: {action} {currency} at {intended_price}")
                return
                
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    logger.error(f"[ERROR] Order failed after {max_retries} attempts: {e}")

    def on_order_status(self, trade):
        """Handle order status updates"""
        order_id = trade.order.orderId
        status = trade.orderStatus.status
        
        if order_id in self.pending_orders:
            order_info = self.pending_orders[order_id]
            logger.info(f"[ORDER] Order {order_id} ({order_info['currency']}): {status}")
            
            if status in ['Filled', 'Cancelled', 'ApiCancelled']:
                del self.pending_orders[order_id]

    def on_execution(self, trade, fill):
        """Handle order fills with price logging"""
        order_id = trade.order.orderId
        
        if order_id in self.pending_orders:
            order_info = self.pending_orders[order_id]
            
            price_log = {
                'currency': order_info['currency'],
                'action': order_info['action'],
                'intended_price': order_info['intended_price'],
                'actual_price': fill.execution.price,
                'quantity': fill.execution.shares,
                'timestamp': datetime.datetime.now(),
                'commission': fill.commissionReport.commission if fill.commissionReport else 0
            }
            
            self.price_logs.append(price_log)
            
            diff_pips = (fill.execution.price - order_info['intended_price']) * 10000
            
            logger.info(f"[FILL] {order_info['currency']} {order_info['action']} | "
                       f"Intended: {order_info['intended_price']:.5f} | "
                       f"Actual: {fill.execution.price:.5f} | "
                       f"Slippage: {diff_pips:+.1f} pips")

    # FREQUENCY-OPTIMIZED MONITORING
    
    def start_dual_frequency_monitoring(self):
        """Start optimized monitoring with two frequencies"""
        
        # Fast thread: Data monitoring (30s)
        fast_thread = threading.Thread(target=self.fast_data_monitoring, daemon=True)
        fast_thread.start()
        
        # Slow thread: Market context (5min)
        slow_thread = threading.Thread(target=self.slow_context_updates, daemon=True)
        slow_thread.start()
        
        logger.info("[OK] Started dual-frequency monitoring")

    def fast_data_monitoring(self):
        """HIGH FREQUENCY (30s): Quick data checks using cached context"""
        while not self.shutdown_requested:
            try:
                # Quick data flow check
                has_recent_data = self.quick_data_check()
                
                # Get cached market context
                market_context = self.get_cached_context()
                
                # HYBRID DETECTION LOGIC
                if has_recent_data:
                    # Data flowing
                    self.handle_data_flowing()
                elif not market_context.get('timezone_says_open', False):
                    # Markets closed
                    self.handle_markets_closed()
                else:
                    # Connection issue!
                    self.handle_connection_issue(market_context)
                
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"[ERROR] Fast monitoring error: {e}")
                time.sleep(30)

    def slow_context_updates(self):
        """LOW FREQUENCY (5min): Heavy timezone calculations"""
        while not self.shutdown_requested:
            try:
                # Heavy timezone calculations
                new_context = self.calculate_market_context()
                
                # Update cache
                self.market_context_cache.update(new_context)
                self.market_context_cache['last_updated'] = datetime.datetime.now()
                
                # Only log context changes, not every update
                active_sessions = [s['name'] for s in new_context.get('active_sessions', [])]
                if not hasattr(self, '_last_active_sessions') or self._last_active_sessions != active_sessions:
                    logger.info(f"[MARKET] Active sessions: {', '.join(active_sessions) or 'All closed'}")
                    self._last_active_sessions = active_sessions
                
                time.sleep(300)  # 5 minutes
                
            except Exception as e:
                logger.error(f"[ERROR] Context update error: {e}")
                time.sleep(300)

    def quick_data_check(self):
        """FAST: Check if any currency has recent data"""
        current_time = datetime.datetime.now()
        for currency, last_tick in self.last_tick_times.items():
            if last_tick and (current_time - last_tick).total_seconds() < 300:
                return True
        return False

    def get_cached_context(self):
        """FAST: Get cached market context"""
        cache = self.market_context_cache
        if (cache.get('last_updated') and 
            (datetime.datetime.now() - cache['last_updated']).total_seconds() < 600):
            return cache
        return cache  # Use stale cache rather than recalculate

    def calculate_market_context(self):
        """SLOW: Calculate full market context with timezones"""
        try:
            utc_now = datetime.datetime.now(pytz.UTC)
            
            sessions = [
                ('Sydney', 'Australia/Sydney', datetime.time(7, 0), datetime.time(17, 0)),
                ('Tokyo', 'Asia/Tokyo', datetime.time(9, 0), datetime.time(17, 0)),
                ('London', 'Europe/London', datetime.time(8, 0), datetime.time(16, 0)),
                ('New_York', 'America/New_York', datetime.time(9, 0), datetime.time(16, 0))
            ]
            
            active_sessions = []
            for session_name, tz_name, start_time, end_time in sessions:
                tz = pytz.timezone(tz_name)
                local_time = utc_now.astimezone(tz)
                
                if (local_time.weekday() < 5 and 
                    start_time <= local_time.time() <= end_time):
                    active_sessions.append({'name': session_name})
            
            return {
                'timezone_says_open': len(active_sessions) > 0,
                'active_sessions': active_sessions
            }
            
        except Exception as e:
            logger.warning(f"Market context calculation error: {e}")
            return {'timezone_says_open': True, 'active_sessions': []}

    def handle_data_flowing(self):
        """Handle normal data flow"""
        self._detailed_check_counter += 1
        if self._detailed_check_counter >= 10:  # Every 5 minutes
            self.detailed_currency_check()
            self._detailed_check_counter = 0

    def handle_markets_closed(self):
        """Handle markets closed"""
        # Only log this occasionally to avoid spam
        if not hasattr(self, '_last_market_closed_log'):
            self._last_market_closed_log = datetime.datetime.min
        
        now = datetime.datetime.now()
        if (now - self._last_market_closed_log).total_seconds() > 1800:  # Every 30 minutes
            logger.info("[MARKET] Markets closed")
            self._last_market_closed_log = now

    def handle_connection_issue(self, market_context):
        """Handle connection issue with rate limiting"""
        now = datetime.datetime.now()
        if (now - self._last_recovery_attempt).total_seconds() > 120:  # 2 minutes
            active_sessions = [s['name'] for s in market_context.get('active_sessions', [])]
            logger.error(f"[CONNECTION] CONNECTION ISSUE: Sessions {active_sessions} active but no data!")
            
            self.trigger_recovery()
            self._last_recovery_attempt = now

    def detailed_currency_check(self):
        """Detailed per-currency analysis"""
        current_time = datetime.datetime.now()
        stale_currencies = []
        
        for currency, last_tick in self.last_tick_times.items():
            if last_tick and (current_time - last_tick).total_seconds() > 120:
                stale_currencies.append(currency)
        
        # Only log if there are actually stale currencies
        if stale_currencies:
            logger.warning(f"[STALE] No recent data: {', '.join(stale_currencies)}")

    def trigger_recovery(self):
        """Trigger appropriate recovery"""
        if not self.ib.isConnected():
            threading.Thread(target=self.handle_reconnection, daemon=True).start()
        else:
            threading.Thread(target=self.recover_market_data, daemon=True).start()

    def handle_manual_reconnect(self):
        """Handle manual RECONNECT command with status reporting"""
        try:
            logger.info("[RECONNECT] Starting manual reconnection process...")
            
            # Reset reconnection attempts counter
            self.reconnection_attempts = 0
            
            # Clear recent prices to ensure we get fresh data
            for currency in self.recent_prices:
                self.recent_prices[currency] = []
            
            # Check current connection state
            if self.ib.isConnected():
                logger.info("[RECONNECT] Already connected - refreshing market data...")
                # Just refresh market data (fast mode for manual reconnect)
                self.recover_market_data(fast_mode=True)
                time.sleep(2)  # Reduced wait for market data to stabilize
            else:
                logger.info("[RECONNECT] Not connected - attempting full reconnection...")
                # Attempt full reconnection
                if not self.connect_to_ib():
                    return "[RECONNECT] Failed to connect to IB. Check TWS/Gateway is running."
                
                time.sleep(2)
                
                if not self.setup_market_data():
                    return "[RECONNECT] Connected to IB but failed to setup market data."
                
                logger.info("[RECONNECT] Connection established, waiting for price data...")
                time.sleep(3)  # Reduced wait time for manual reconnect
            
            # Check if we're getting data and build status report
            status_lines = []
            status_lines.append("\n[RECONNECT STATUS REPORT]")
            status_lines.append("=" * 50)
            
            # Connection status
            if self.is_connected:
                status_lines.append("✅ Connection Status: CONNECTED")
            else:
                status_lines.append("❌ Connection Status: DISCONNECTED")
                status_lines.append("\nReconnection failed. Check TWS/Gateway connection.")
                return "\n".join(status_lines)
            
            # Check data flow
            data_flowing = False
            current_time = datetime.datetime.now()
            
            status_lines.append("\n📊 Market Data Status:")
            for currency in self.currency_pairs.keys():
                last_tick = self.last_tick_times.get(currency)
                if last_tick and (current_time - last_tick).total_seconds() < 60:
                    data_flowing = True
                    status_lines.append(f"  {currency}: ✅ Active")
                else:
                    status_lines.append(f"  {currency}: ⚠️  No recent data")
            
            if data_flowing:
                status_lines.append("\n💰 Latest Prices:")
                # Show most recent price for each currency
                for currency in self.currency_pairs.keys():
                    if self.recent_prices[currency]:
                        latest = self.recent_prices[currency][-1]
                        price_time = latest['time'].strftime("%H:%M:%S")
                        status_lines.append(
                            f"  {currency}: {latest['price']:.5f} "
                            f"(Bid: {latest['bid']:.5f}, Ask: {latest['ask']:.5f}) "
                            f"@ {price_time}"
                        )
                    else:
                        status_lines.append(f"  {currency}: Waiting for data...")
                
                status_lines.append("\n✅ Reconnection successful! Algorithm will continue from last state.")
            else:
                status_lines.append("\n⚠️  Connected but no market data flowing yet.")
                status_lines.append("This could mean markets are closed or data subscriptions need time.")
                status_lines.append("Try again in 30 seconds if markets should be open.")
            
            status_lines.append("=" * 50)
            
            result = "\n".join(status_lines)
            logger.info(result)
            return result
            
        except Exception as e:
            error_msg = f"[RECONNECT] Error during manual reconnection: {e}"
            logger.error(error_msg, exc_info=True)
            return error_msg

    def recover_market_data(self, fast_mode=False):
        """Recover market data subscriptions"""
        logger.info("[RECOVER] Recovering market data...")
        try:
            # Reduce wait times in fast mode (manual RECONNECT)
            if not fast_mode:
                time.sleep(5)  # Full wait for automatic recovery
            else:
                time.sleep(1)  # Quick wait for manual recovery
                
            if self.is_connected:
                for currency, ticker in list(self.tickers.items()):
                    try:
                        self.ib.cancelMktData(ticker.contract)
                    except:
                        pass
                
                self.tickers.clear()
                
                if not fast_mode:
                    time.sleep(2)  # Full wait for automatic recovery
                else:
                    time.sleep(0.5)  # Quick wait for manual recovery
                
                if self.setup_market_data():
                    logger.info("[OK] Market data recovered")
                    self.last_heartbeat = datetime.datetime.now()
                    
        except Exception as e:
            logger.error(f"[ERROR] Market data recovery failed: {e}")

    def start_command_listener(self):
        """Start command listener thread for runtime commands"""
        command_thread = threading.Thread(target=self.command_listener_loop, daemon=True)
        command_thread.start()
        logger.info("[OK] Command listener started - available commands:")
        logger.info("  CLOSE_EURUSD/USDCAD/GBPUSD - Close position for currency")
        logger.info("  TWS_CLOSED_EURUSD/USDCAD/GBPUSD - Mark position as closed in TWS")
        logger.info("  RECONNECT - Force reconnection to IB")
        logger.info("  SKIP_WARMUP - Skip warmup on next restart")
        logger.info("  RESTART - Restart the server")
        logger.info("  STATUS - Show current positions and connection status")

    def command_listener_loop(self):
        """Listen for commands from stdin"""
        import sys
        
        while not self.shutdown_requested:
            try:
                # Check if stdin is available (not redirected/closed)
                if sys.stdin.isatty():
                    command = input().strip().upper()
                    
                    if command.startswith("CLOSE_"):
                        # Extract currency and convert format (EURUSD -> EUR.USD)
                        currency_part = command.replace("CLOSE_", "")
                        currency = None
                        
                        # Map command format to internal format
                        if currency_part == "EURUSD":
                            currency = "EUR.USD"
                        elif currency_part == "USDCAD":
                            currency = "USD.CAD"
                        elif currency_part == "GBPUSD":
                            currency = "GBP.USD"
                        
                        if currency and currency in self.currency_pairs:
                            with self.position_lock:
                                current_position = self.positions.get(currency)
                                if current_position:
                                    logger.info(f"[COMMAND] Closing {current_position} position for {currency}")
                                    # Get current price for logging
                                    ticker = self.tickers.get(currency)
                                    current_price = self.get_midpoint_price(ticker) if ticker else 0
                                    self.handle_signal(currency, "close", current_price)
                                else:
                                    logger.info(f"[COMMAND] No position to close for {currency}")
                        else:
                            logger.warning(f"[COMMAND] Unknown currency in command: {command}")
                            
                    elif command.startswith("TWS_CLOSED_"):
                        # Mark position as closed (for manual TWS closes)
                        currency_part = command.replace("TWS_CLOSED_", "")
                        currency = None
                        
                        if currency_part == "EURUSD":
                            currency = "EUR.USD"
                        elif currency_part == "USDCAD":
                            currency = "USD.CAD"
                        elif currency_part == "GBPUSD":
                            currency = "GBP.USD"
                        
                        if currency and currency in self.currency_pairs:
                            with self.position_lock:
                                old_position = self.positions.get(currency)
                                self.positions[currency] = None
                                logger.info(f"[COMMAND] Marked {currency} as closed in TWS (was: {old_position})")
                        else:
                            logger.warning(f"[COMMAND] Unknown currency in command: {command}")
                            
                    elif command == "RECONNECT":
                        # Manual reconnection attempt
                        logger.info("[COMMAND] RECONNECT requested - attempting manual reconnection")
                        self.handle_manual_reconnect()
                            
                    elif command == "SKIP_WARMUP":
                        # Create a flag file for server.py to detect on next restart
                        try:
                            with open('skip_warmup.flag', 'w') as f:
                                f.write('1')
                            logger.info("[COMMAND] SKIP_WARMUP flag set for next restart")
                            logger.info("[COMMAND] Delete skip_warmup.flag to re-enable warmup")
                        except Exception as e:
                            logger.error(f"[COMMAND] Failed to create skip flag: {e}")
                            
                    elif command == "RESTART":
                        # Create restart flag and shutdown gracefully
                        logger.info("[COMMAND] RESTART requested - creating restart flag")
                        try:
                            with open('restart_requested.flag', 'w') as f:
                                f.write('1')
                            logger.info("[COMMAND] Restart flag created - initiating shutdown")
                            # Signal shutdown
                            self.shutdown_requested = True
                            # Disconnect from IB to trigger clean exit
                            if self.ib.isConnected():
                                self.ib.disconnect()
                        except Exception as e:
                            logger.error(f"[COMMAND] Failed to create restart flag: {e}")
                            
                    elif command == "STATUS":
                        # Show current status
                        logger.info("[COMMAND] Current Status:")
                        logger.info(f"  Connected to IB: {self.is_connected}")
                        logger.info(f"  Positions:")
                        with self.position_lock:
                            for currency, position in self.positions.items():
                                if position:
                                    logger.info(f"    {currency}: {position}")
                                else:
                                    logger.info(f"    {currency}: flat")
                        logger.info(f"  Pending orders: {len(self.pending_orders)}")
                        
                    elif command == "HELP":
                        logger.info("[COMMAND] Available commands:")
                        logger.info("  CLOSE_EURUSD/USDCAD/GBPUSD - Close position")
                        logger.info("  TWS_CLOSED_EURUSD/USDCAD/GBPUSD - Mark as closed") 
                        logger.info("  RECONNECT - Force reconnection to IB")
                        logger.info("  SKIP_WARMUP - Skip warmup on next restart")
                        logger.info("  RESTART - Restart the server")
                        logger.info("  STATUS - Show positions and connection")
                        logger.info("  HELP - Show this help")
                        
                    elif command:
                        logger.warning(f"[COMMAND] Unknown command: {command}")
                        logger.info("[COMMAND] Type HELP for available commands")
                        
                else:
                    # If stdin is not a TTY (e.g., running in background), just sleep
                    time.sleep(1)
                    
            except EOFError:
                # stdin closed, exit gracefully
                logger.info("[COMMAND] Command listener: stdin closed")
                break
            except KeyboardInterrupt:
                # Ctrl+C pressed
                break
            except Exception as e:
                logger.error(f"[ERROR] Command processing error: {e}")
                time.sleep(1)  # Prevent tight loop on errors

    def execute_command(self, command):
        """Execute a command and return result (for HTTP endpoint)"""
        try:
            command = command.strip().upper()
            
            if command.startswith("CLOSE_"):
                # Extract currency and convert format (EURUSD -> EUR.USD)
                currency_part = command.replace("CLOSE_", "")
                currency = None
                
                # Map command format to internal format
                if currency_part == "EURUSD":
                    currency = "EUR.USD"
                elif currency_part == "USDCAD":
                    currency = "USD.CAD"
                elif currency_part == "GBPUSD":
                    currency = "GBP.USD"
                
                if currency and currency in self.currency_pairs:
                    with self.position_lock:
                        current_position = self.positions.get(currency)
                        if current_position:
                            logger.info(f"[HTTP-COMMAND] Closing {current_position} position for {currency}")
                            # Get current price for logging
                            ticker = self.tickers.get(currency)
                            current_price = self.get_midpoint_price(ticker) if ticker else 0
                            self.handle_signal(currency, "close", current_price)
                            return f"Closing {current_position} position for {currency}"
                        else:
                            return f"No position to close for {currency}"
                else:
                    return f"Unknown currency in command: {command}"
                    
            elif command.startswith("TWS_CLOSED_"):
                # Mark position as closed (for manual TWS closes)
                currency_part = command.replace("TWS_CLOSED_", "")
                currency = None
                
                if currency_part == "EURUSD":
                    currency = "EUR.USD"
                elif currency_part == "USDCAD":
                    currency = "USD.CAD"
                elif currency_part == "GBPUSD":
                    currency = "GBP.USD"
                
                if currency and currency in self.currency_pairs:
                    with self.position_lock:
                        old_position = self.positions.get(currency)
                        self.positions[currency] = None
                        logger.info(f"[HTTP-COMMAND] Marked {currency} as closed in TWS (was: {old_position})")
                        return f"Marked {currency} as closed in TWS (was: {old_position})"
                else:
                    return f"Unknown currency in command: {command}"
                    
            elif command == "RECONNECT":
                # Manual reconnection attempt
                logger.info("[HTTP-COMMAND] RECONNECT requested - attempting manual reconnection")
                result = self.handle_manual_reconnect()
                return result
                    
            elif command == "SKIP_WARMUP":
                # Create a flag file for server.py to detect on next restart
                try:
                    with open('skip_warmup.flag', 'w') as f:
                        f.write('1')
                    logger.info("[HTTP-COMMAND] SKIP_WARMUP flag set for next restart")
                    return "SKIP_WARMUP flag set for next restart"
                except Exception as e:
                    logger.error(f"[HTTP-COMMAND] Failed to create skip flag: {e}")
                    return f"Failed to create skip flag: {e}"
                    
            elif command == "RESTART":
                # Create restart flag and shutdown gracefully
                logger.info("[HTTP-COMMAND] RESTART requested - creating restart flag")
                try:
                    with open('restart_requested.flag', 'w') as f:
                        f.write('1')
                    logger.info("[HTTP-COMMAND] Restart flag created - initiating shutdown")
                    # Signal shutdown
                    self.shutdown_requested = True
                    # Disconnect from IB to trigger clean exit
                    if self.ib.isConnected():
                        self.ib.disconnect()
                    return "Restart flag created - initiating shutdown"
                except Exception as e:
                    logger.error(f"[HTTP-COMMAND] Failed to create restart flag: {e}")
                    return f"Failed to create restart flag: {e}"
                    
            elif command == "STATUS":
                # Show current status
                status_lines = []
                status_lines.append(f"Connected to IB: {self.is_connected}")
                status_lines.append("Positions:")
                with self.position_lock:
                    for currency, position in self.positions.items():
                        if position:
                            status_lines.append(f"  {currency}: {position}")
                        else:
                            status_lines.append(f"  {currency}: flat")
                status_lines.append(f"Pending orders: {len(self.pending_orders)}")
                
                status_text = "\n".join(status_lines)
                logger.info(f"[HTTP-COMMAND] Status requested:\n{status_text}")
                return status_text
                
            elif command == "HELP":
                help_text = """Available commands:
  CLOSE_EURUSD/USDCAD/GBPUSD - Close position
  TWS_CLOSED_EURUSD/USDCAD/GBPUSD - Mark as closed
  RECONNECT - Force reconnection to IB
  SKIP_WARMUP - Skip warmup on next restart
  RESTART - Restart the server
  STATUS - Show positions and connection
  HELP - Show this help"""
                logger.info("[HTTP-COMMAND] Help requested")
                return help_text
                
            else:
                return f"Unknown command: {command}. Type HELP for available commands."
                
        except Exception as e:
            logger.error(f"[ERROR] HTTP command processing error: {e}")
            return f"Error processing command: {e}"

    def run(self):
        """Main run loop"""
        logger.info("[START] Starting Enhanced IB Trading Bot")
        
        # Start HTTP API server first (so it's available even during connection)
        self.start_http_server()
        
        if not self.connect_to_ib():
            logger.error("[ERROR] Initial connection failed")
            return
        
        if not self.setup_market_data():
            logger.error("[ERROR] Market data setup failed")
            return
        
        # Start monitoring
        self.start_dual_frequency_monitoring()
        
        # Start command listener
        self.start_command_listener()
        
        logger.info("[OK] Bot running with enhanced monitoring")
        
        try:
            # Status logging
            def status_logger():
                last_positions = {}
                while not self.shutdown_requested:
                    time.sleep(300)  # Check every 5 minutes
                    current_positions = dict(self.positions)
                    
                    # Only log if positions have changed
                    if current_positions != last_positions:
                        active_positions = {k: v for k, v in current_positions.items() if v is not None}
                        if active_positions:
                            logger.info(f"[POSITIONS] Active: {active_positions}")
                        else:
                            logger.info("[POSITIONS] All flat")
                        last_positions = current_positions
            
            threading.Thread(target=status_logger, daemon=True).start()
            self.ib.run()
            
        except KeyboardInterrupt:
            logger.info("[STOP] Shutdown requested")
        finally:
            self.shutdown()

    def shutdown(self):
        """Clean shutdown"""
        logger.info("[SHUTDOWN] Shutting down...")
        self.shutdown_requested = True
        
        try:
            if self.pending_orders:
                for order_id in list(self.pending_orders.keys()):
                    try:
                        self.ib.cancelOrder(order_id)
                    except:
                        pass
            
            if self.ib.isConnected():
                self.ib.disconnect()
            
            logger.info("[OK] Shutdown complete")
            
        except Exception as e:
            logger.error(f"[ERROR] Shutdown error: {e}")

if __name__ == "__main__":
    import os
    
    # Log environment variables for debugging
    logger = logging.getLogger(__name__)
    logger.info(f"IB_HOST environment variable: {os.environ.get('IB_HOST', 'NOT SET')}")
    logger.info(f"IB_PORT environment variable: {os.environ.get('IB_PORT', 'NOT SET')}")
    logger.info(f"Running in Docker: {'Yes' if os.environ.get('IB_HOST') == 'host.docker.internal' else 'No'}")
    
    # Read configuration from environment variables (for Docker)
    # Falls back to defaults if not in Docker
    ib_host = os.environ.get('IB_HOST', '127.0.0.1')
    ib_port = int(os.environ.get('IB_PORT', '7497'))
    client_id = int(os.environ.get('IB_CLIENT_ID', '6969'))
    
    logger.info(f"Creating bot with host={ib_host}, port={ib_port}, clientId={client_id}")
    
    bot = EnhancedIBTradingBot(
        ib_host=ib_host,
        ib_port=ib_port,
        client_id=client_id,
        api_url=os.environ.get('API_URL', 'http://localhost:5000/trade_signal'),
        position_size=int(os.environ.get('POSITION_SIZE', '100000'))
    )
    
    bot.run()