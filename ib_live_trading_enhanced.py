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
import requests
import logging
import datetime
import time
import threading
import pytz
from collections import defaultdict
from typing import Dict, Optional
from datetime import timedelta

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
        
        # Thread lock
        self.position_lock = threading.Lock()
        
        logger.info(f"[INIT] Enhanced IB Trading Bot initialized for {len(self.currency_pairs)} currencies")

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
                    ticker = util.run(self.ib.reqMktData(contract, '', False, False))
                    self.tickers[api_currency] = ticker
                    
                    logger.info(f"[OK] Market data: {api_currency}")
                    time.sleep(0.5)
                    
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
            
            # Log price data occasionally (every 20th tick to avoid spam)
            if not hasattr(self, '_price_log_counter'):
                self._price_log_counter = {}
            if currency not in self._price_log_counter:
                self._price_log_counter[currency] = 0
            
            self._price_log_counter[currency] += 1
            if self._price_log_counter[currency] % 20 == 0:
                logger.info(f"[PRICE] {currency}: {price:.5f} (bid: {ticker.bid}, ask: {ticker.ask})")
            
            self.send_to_algorithm_with_retry(currency, price, ticker.time)

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

    def recover_market_data(self):
        """Recover market data subscriptions"""
        logger.info("[RECOVER] Recovering market data...")
        try:
            time.sleep(5)
            if self.is_connected:
                for currency, ticker in list(self.tickers.items()):
                    try:
                        util.run(self.ib.cancelMktData(ticker.contract))
                    except:
                        pass
                
                self.tickers.clear()
                time.sleep(2)
                
                if self.setup_market_data():
                    logger.info("[OK] Market data recovered")
                    self.last_heartbeat = datetime.datetime.now()
                    
        except Exception as e:
            logger.error(f"[ERROR] Market data recovery failed: {e}")

    def run(self):
        """Main run loop"""
        logger.info("[START] Starting Enhanced IB Trading Bot")
        
        if not self.connect_to_ib():
            logger.error("[ERROR] Initial connection failed")
            return
        
        if not self.setup_market_data():
            logger.error("[ERROR] Market data setup failed")
            return
        
        # Start monitoring
        self.start_dual_frequency_monitoring()
        
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
                        util.run(self.ib.cancelOrder(order_id))
                    except:
                        pass
            
            if self.ib.isConnected():
                util.run(self.ib.disconnect())
            
            logger.info("[OK] Shutdown complete")
            
        except Exception as e:
            logger.error(f"[ERROR] Shutdown error: {e}")

if __name__ == "__main__":
    import os
    
    # Read configuration from environment variables (for Docker)
    # Falls back to defaults if not in Docker
    bot = EnhancedIBTradingBot(
        ib_host=os.environ.get('IB_HOST', '127.0.0.1'),
        ib_port=int(os.environ.get('IB_PORT', '7497')),
        client_id=int(os.environ.get('IB_CLIENT_ID', '6969')),
        api_url=os.environ.get('API_URL', 'http://localhost:5000/trade_signal'),
        position_size=int(os.environ.get('POSITION_SIZE', '100000'))
    )
    
    bot.run() 