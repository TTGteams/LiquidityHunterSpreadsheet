from flask import Flask, request, jsonify
import pandas as pd
import algorithm
import threading
import logging
from logging.handlers import RotatingFileHandler
from waitress import serve
import pytz
import datetime
import requests
import time
import sys
import signal
import subprocess
import atexit
import os

app = Flask(__name__)

# Global variables for skip settings
SKIP_MODE2 = False
SKIP_MODE3 = False

# IB bot HTTP API configuration
IB_API_PORT = os.environ.get('IB_API_PORT', '5001')

trade_logger = logging.getLogger("trade_logger")
debug_logger = logging.getLogger("debug_logger")

trade_logger.setLevel(logging.INFO)
debug_logger.setLevel(logging.WARNING)

trade_logger.propagate = False
debug_logger.propagate = False

# Custom formatter class to handle MST timezone
class MSTFormatter(logging.Formatter):
    def converter(self, timestamp):
        dt = datetime.datetime.fromtimestamp(timestamp)
        mst_tz = pytz.timezone('MST')
        return dt.astimezone(mst_tz)
    
    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f %Z')

# Keep existing size-based rotation but update formatter
trade_handler = RotatingFileHandler(
    'trade_log.log',
    maxBytes=100000000,  # 100MB per file
    backupCount=5
)
trade_handler.setLevel(logging.INFO)
trade_formatter = MSTFormatter('%(asctime)s - %(levelname)s - %(message)s')
trade_handler.setFormatter(trade_formatter)
trade_logger.addHandler(trade_handler)

debug_handler = RotatingFileHandler(
    'debug_log.log',
    maxBytes=10000000,  # 10MB per file
    backupCount=2
)
debug_handler.setLevel(logging.DEBUG)
debug_formatter = MSTFormatter('%(asctime)s - %(levelname)s - %(message)s')
debug_handler.setFormatter(debug_formatter)
debug_logger.addHandler(debug_handler)

# Create a lock for thread safety
state_lock = threading.Lock()

@app.route('/trade_signal', methods=['POST'])
def trade_signal():
    try:
        content = request.json
        data = content['data']
        
        # Extract currency from request, default to EUR.USD if not provided
        currency = content.get('currency', 'EUR.USD')
        
        # Check if this is a forwarded signal from batch processing
        is_forwarded = content.get('source') == 'batch_forward'
        
        # Validate currency is in supported list
        if currency not in algorithm.SUPPORTED_CURRENCIES:
            error_msg = f"Unsupported currency: {currency}"
            trade_logger.error(error_msg)
            return jsonify({'error': error_msg}), 400
        
        # Validate input data
        required_fields = ['Time', 'Price']
        for field in required_fields:
            if field not in data:
                error_msg = f"Missing field: {field}"
                trade_logger.error(error_msg)
                return jsonify({'error': error_msg}), 400

        # For forwarded signals, just log and return (avoid double processing)
        if is_forwarded:
            trade_logger.info(f"Forwarded signal received: {currency} at {data['Price']}")
            # Return a placeholder response for monitoring systems
            return jsonify({
                'signal': 'forwarded', 
                'currency': currency, 
                'source': 'batch_processing'
            }), 200

        # Convert the data to a DataFrame (only for non-forwarded signals)
        df = pd.DataFrame([data])
        
        # Add Currency column if provided in the request
        if 'currency' in content:
            df['Currency'] = content['currency']
        
        # Parse the time field - FIXED VERSION
        try:
            # First try direct conversion without format specification
            df['Time'] = pd.to_datetime(df['Time'])
        except:
            # If that fails, try specific formats
            time_parsed = False
            formats_to_try = [
                '%Y-%m-%d %H:%M:%S',      # Standard without ms
                '%Y-%m-%dT%H:%M:%S',      # ISO format without ms
                '%Y-%m-%d %H:%M:%S.%f',   # Standard with ms
                '%Y-%m-%dT%H:%M:%S.%f',   # ISO format with ms
                '%Y-%m-%d %H:%M'          # Minutes only
            ]
            
            for fmt in formats_to_try:
                try:
                    df['Time'] = pd.to_datetime(df['Time'], format=fmt)
                    time_parsed = True
                    break
                except:
                    continue
            
            if not time_parsed:
                error_msg = "Could not parse time in any supported format"
                trade_logger.error(error_msg)
                return jsonify({'error': error_msg}), 400
        
        # Set 'Time' as the index
        df.set_index('Time', inplace=True)
        
        # Process the new data point through the algorithm
        with state_lock:
            signal, processed_currency = algorithm.process_market_data(df, currency)
            
        # Only log if signal is not 'hold'
        if signal != 'hold':
            trade_logger.info(f"Generated {processed_currency} signal: {signal}")
            
        # Always return the signal and currency, even if it's 'hold'
        return jsonify({'signal': signal, 'currency': processed_currency}), 200
        
    except Exception as e:
        debug_logger.error(f"Error occurred: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400

@app.route('/command', methods=['POST'])
def execute_command():
    """Execute commands via HTTP endpoint"""
    try:
        content = request.json
        command = content.get('command', '').strip().upper()
        
        if not command:
            return jsonify({'error': 'No command provided'}), 400
        
        # Handle server-level commands that don't require IB bot
        if command == 'SKIP_WARMUP':
            try:
                # Create skip_warmup.flag file
                with open('skip_warmup.flag', 'w') as f:
                    f.write('skip')
                debug_logger.info("Created skip_warmup.flag file")
                trade_logger.info("SKIP_WARMUP command received - will skip warmup on next restart")
                
                return jsonify({
                    'command': command,
                    'result': 'Skip warmup flag set. Will skip warmup on next restart.',
                    'timestamp': datetime.datetime.now().isoformat()
                }), 200
                
            except Exception as e:
                debug_logger.error(f"Error creating skip_warmup.flag: {e}")
                return jsonify({'error': f'Failed to set skip warmup flag: {str(e)}'}), 500
                
        elif command == 'RESTART':
            try:
                # Create restart_requested.flag file
                with open('restart_requested.flag', 'w') as f:
                    f.write('restart')
                debug_logger.info("Created restart_requested.flag file")
                trade_logger.info("RESTART command received - initiating server restart")
                
                return jsonify({
                    'command': command,
                    'result': 'Restart initiated. Server will restart momentarily.',
                    'timestamp': datetime.datetime.now().isoformat()
                }), 200
                
            except Exception as e:
                debug_logger.error(f"Error creating restart_requested.flag: {e}")
                return jsonify({'error': f'Failed to initiate restart: {str(e)}'}), 500
        
        # For all other commands, forward to IB bot via HTTP API
        try:
            # Get IB bot API port from environment or use default
            ib_api_port = os.environ.get('IB_API_PORT', '5001')
            ib_api_url = f"http://localhost:{ib_api_port}/command"
            
            # Forward command to IB bot
            response = requests.post(ib_api_url, json={'command': command}, timeout=5)
            
            if response.status_code == 200:
                result_data = response.json()
                return jsonify({
                    'command': command,
                    'result': result_data.get('result', 'Command executed'),
                    'timestamp': datetime.datetime.now().isoformat()
                }), 200
            else:
                # IB bot API returned an error
                error_data = response.json() if response.headers.get('content-type') == 'application/json' else {}
                return jsonify({'error': error_data.get('error', f'IB bot returned status {response.status_code}')}), response.status_code
                
        except requests.exceptions.ConnectionError:
            # IB bot is not running or not reachable
            return jsonify({'error': 'IB trading not yet started'}), 503
        except requests.exceptions.Timeout:
            return jsonify({'error': 'IB bot command timeout'}), 504
        except Exception as e:
            debug_logger.error(f"Error forwarding command to IB bot: {e}")
            return jsonify({'error': f'Failed to forward command: {str(e)}'}), 500
        
    except Exception as e:
        debug_logger.error(f"Error executing command: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/algorithm_state', methods=['GET'])
def get_algorithm_state():
    """Get the current state of the algorithm including zones and bars for all currencies"""
    try:
        import algorithm
        
        # Prepare state data for all currencies
        state_data = {}
        
        for currency in algorithm.SUPPORTED_CURRENCIES:
            # Get zones for this currency
            zones = {}
            if currency in algorithm.current_valid_zones_dict:
                # Convert zone tuples to strings for JSON serialization
                for zone_id, zone_data in algorithm.current_valid_zones_dict[currency].items():
                    zone_key = f"{zone_id[0]:.6f}_{zone_id[1]:.6f}" if isinstance(zone_id, tuple) else str(zone_id)
                    zones[zone_key] = {
                        'start_price': zone_data.get('start_price'),
                        'end_price': zone_data.get('end_price'),
                        'zone_size': zone_data.get('zone_size'),
                        'zone_type': zone_data.get('zone_type'),
                        'confirmation_time': zone_data.get('confirmation_time').isoformat() if zone_data.get('confirmation_time') else None
                    }
            
            # Get bars for this currency (last 384 for indicators)
            bars_data = []
            if currency in algorithm.bars and not algorithm.bars[currency].empty:
                recent_bars = algorithm.bars[currency].tail(384)
                for idx, row in recent_bars.iterrows():
                    bar = {
                        'time': idx.isoformat(),
                        'open': float(row['open']),
                        'high': float(row['high']),
                        'low': float(row['low']),
                        'close': float(row['close'])
                    }
                    # Add indicators if available
                    if 'RSI' in row and not pd.isna(row['RSI']):
                        bar['RSI'] = float(row['RSI'])
                    if 'MACD_Line' in row and not pd.isna(row['MACD_Line']):
                        bar['MACD_Line'] = float(row['MACD_Line'])
                    if 'Signal_Line' in row and not pd.isna(row['Signal_Line']):
                        bar['Signal_Line'] = float(row['Signal_Line'])
                    bars_data.append(bar)
            
            state_data[currency] = {
                'zones': zones,
                'zones_count': len(zones),
                'bars': bars_data,
                'bars_count': len(bars_data)
            }
        
        return jsonify({
            'status': 'success',
            'timestamp': datetime.datetime.now().isoformat(),
            'currencies': state_data
        }), 200
        
    except Exception as e:
        debug_logger.error(f"Error getting algorithm state: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/trade_signal_batch', methods=['POST'])
def trade_signal_batch():
    """
    Batch endpoint for processing multiple price ticks sequentially.
    Used by testing scripts to speed up historical data processing.
    Maintains tick-by-tick algorithm behavior.
    
    Note: Signal forwarding has been disabled to prevent connection exhaustion during backtesting.
    """
    try:
        content = request.json
        data_points = content['data']  # Expecting array of {Time, Price} objects
        currency = content.get('currency', 'EUR.USD')
        
        # Validate currency
        if currency not in algorithm.SUPPORTED_CURRENCIES:
            error_msg = f"Unsupported currency: {currency}"
            trade_logger.error(error_msg)
            return jsonify({'error': error_msg}), 400
        
        # Validate data_points is a list
        if not isinstance(data_points, list):
            error_msg = "Data must be an array of price points"
            trade_logger.error(error_msg)
            return jsonify({'error': error_msg}), 400
        
        results = []
        signals_count = {'buy': 0, 'sell': 0, 'close': 0, 'hold': 0}
        
        # Process each data point sequentially
        for i, data_point in enumerate(data_points):
            try:
                # Validate required fields
                if 'Time' not in data_point or 'Price' not in data_point:
                    results.append({'error': 'Missing Time or Price field', 'index': i})
                    continue
                
                # Create DataFrame for single data point
                df = pd.DataFrame([data_point])
                
                # Parse time using same logic as single endpoint
                try:
                    df['Time'] = pd.to_datetime(df['Time'])
                except:
                    # Try specific formats
                    time_parsed = False
                    formats_to_try = [
                        '%Y-%m-%d %H:%M:%S',
                        '%Y-%m-%dT%H:%M:%S',
                        '%Y-%m-%d %H:%M:%S.%f',
                        '%Y-%m-%dT%H:%M:%S.%f',
                        '%Y-%m-%d %H:%M'
                    ]
                    
                    for fmt in formats_to_try:
                        try:
                            df['Time'] = pd.to_datetime(df['Time'], format=fmt)
                            time_parsed = True
                            break
                        except:
                            continue
                    
                    if not time_parsed:
                        results.append({'error': 'Could not parse time', 'index': i})
                        continue
                
                # Set index
                df.set_index('Time', inplace=True)
                
                # Process through algorithm
                with state_lock:
                    signal, processed_currency = algorithm.process_market_data(df, currency)
                
                # Track signal counts
                signals_count[signal] = signals_count.get(signal, 0) + 1
                
                # Add to results
                signal_result = {
                    'index': i,
                    'signal': signal,
                    'currency': processed_currency,
                    'time': data_point['Time'],
                    'price': data_point['Price']
                }
                results.append(signal_result)
                
                # Forward non-hold signals to /trade_signal endpoint for external monitoring
                # COMMENTED OUT: This causes connection exhaustion during high-volume backtesting
                # if signal != 'hold':
                #     forward_signal_to_trade_endpoint(signal_result)
                
                # Log non-hold signals only once (removed duplicate logging)
                # Logging is now handled by the notification endpoint
                
            except Exception as e:
                results.append({'error': str(e), 'index': i})
                debug_logger.error(f"Error processing batch item {i}: {e}")
        
        # Return summary
        return jsonify({
            'results': results,
            'summary': {
                'total_processed': len(data_points),
                'successful': len([r for r in results if 'signal' in r]),
                'failed': len([r for r in results if 'error' in r]),
                'signals_count': signals_count
            }
        }), 200
        
    except Exception as e:
        debug_logger.error(f"Error in batch processing: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400

# Global variable to track IB process
ib_process = None

def start_ib_live_trading():
    """
    Start the IB live trading script as a subprocess.
    """
    global ib_process
    
    try:
        import subprocess
        import os
        
        debug_logger.info("Starting IB Live Trading...")
        
        # Pass environment variables to subprocess
        env = os.environ.copy()
        
        # Log critical environment variables for debugging
        debug_logger.info(f"IB_HOST from environment: {env.get('IB_HOST', 'NOT SET')}")
        debug_logger.info(f"IB_PORT from environment: {env.get('IB_PORT', 'NOT SET')}")
        debug_logger.info(f"ALGO_INSTANCE from environment: {env.get('ALGO_INSTANCE', '1')}")
        debug_logger.info(f"IB_API_PORT from environment: {env.get('IB_API_PORT', '5001')}")
        
        # Start the IB trading script with environment
        ib_process = subprocess.Popen(
            [sys.executable, 'ib_live_trading_enhanced.py'],
            cwd=os.getcwd(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
            env=env  # Pass the environment variables
        )
        
        debug_logger.info(f"IB Live Trading started with PID: {ib_process.pid}")
        trade_logger.info("IB Live Trading system activated")
        
        # Start a thread to monitor the IB process
        monitor_thread = threading.Thread(target=monitor_ib_process, daemon=True)
        monitor_thread.start()
        
    except Exception as e:
        debug_logger.error(f"Failed to start IB Live Trading: {e}", exc_info=True)

def monitor_ib_process():
    """
    Monitor the IB process and restart it if it crashes.
    """
    global ib_process
    
    while ib_process is not None:
        try:
            # Check if process is still running
            if ib_process.poll() is not None:
                debug_logger.warning("IB Live Trading process has stopped")
                
                # Wait a moment before restarting
                time.sleep(5)
                
                debug_logger.info("Restarting IB Live Trading...")
                start_ib_live_trading()
                break
            
            # Read and log output from IB process
            try:
                line = ib_process.stdout.readline()
                if line:
                    debug_logger.info(f"IB: {line.strip()}")
            except:
                pass
            
            time.sleep(1)
            
        except Exception as e:
            debug_logger.error(f"Error monitoring IB process: {e}")
            time.sleep(5)

def stop_ib_live_trading():
    """
    Stop the IB live trading process gracefully.
    """
    global ib_process
    
    if ib_process is not None:
        try:
            debug_logger.info("Stopping IB Live Trading...")
            ib_process.terminate()
            
            # Wait for graceful shutdown
            try:
                ib_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                debug_logger.warning("IB process didn't stop gracefully, forcing shutdown...")
                ib_process.kill()
                ib_process.wait()
            
            ib_process = None
            debug_logger.info("IB Live Trading stopped")
            
        except Exception as e:
            debug_logger.error(f"Error stopping IB process: {e}")

def run_post_startup_warmup():
    """
    Run the complete warmup sequence after server startup, then start IB live trading.
    This runs in a background thread to avoid blocking the server.
    """
    global SKIP_MODE2, SKIP_MODE3
    
    try:
        # Wait a moment for server to be fully ready
        debug_logger.info("Waiting 2 seconds for server to be ready...")
        time.sleep(2)
        
        # Check for skip_warmup.flag file (set by IB command listener)
        skip_file_exists = False
        try:
            import os
            if os.path.exists('skip_warmup.flag'):
                skip_file_exists = True
                debug_logger.info("Found skip_warmup.flag - will skip warmup modes")
                # Delete the flag so it only applies once
                try:
                    os.remove('skip_warmup.flag')
                    debug_logger.info("Deleted skip_warmup.flag")
                except Exception as e:
                    debug_logger.warning(f"Could not delete skip_warmup.flag: {e}")
        except Exception as e:
            debug_logger.error(f"Error checking for skip_warmup.flag: {e}")
        
        # Apply skip flag if it existed
        if skip_file_exists:
            SKIP_MODE2 = True
            SKIP_MODE3 = True
        
        if SKIP_MODE2 or SKIP_MODE3:
            skipped_modes = []
            if SKIP_MODE2:
                skipped_modes.append("Mode 2 (Historical)")
            if SKIP_MODE3:
                skipped_modes.append("Mode 3 (Warmup)")
            debug_logger.info(f"Starting post-startup warmup sequence... SKIPPING: {', '.join(skipped_modes)}")
        else:
            debug_logger.info("Starting post-startup warmup sequence...")
        
        # Import and run the complete warmup sequence
        try:
            from run_test import warmup_data
            
            # Run warmup_data with skip parameters
            debug_logger.info("Calling warmup_data function...")
            result = warmup_data(skip_mode2=SKIP_MODE2, skip_mode3=SKIP_MODE3)
            
            # If we skipped warmup modes, load recent zones from database
            if SKIP_MODE2 and SKIP_MODE3:
                debug_logger.info("Loading recent zones from database due to SKIP_WARMUP...")
                try:
                    # Load the most recent 10 zones per currency from database
                    recent_zones = algorithm.load_recent_zones_from_database(limit=10)
                    
                    # Apply the loaded zones to the algorithm state
                    for currency, zones in recent_zones.items():
                        if zones:
                            algorithm.load_preexisting_zones(zones, currency)
                            debug_logger.warning(
                                f"Populated {currency} with {len(zones)} recent zones from database"
                            )
                    
                    trade_logger.info("Quick startup complete - loaded recent zones from database")
                    
                except Exception as e:
                    debug_logger.error(f"Error loading recent zones from database: {e}", exc_info=True)
                    debug_logger.warning("Continuing without recent zones")
            
            debug_logger.info("Post-startup warmup completed successfully")
            trade_logger.info("Algorithm warmup complete - all zones and bars loaded")
            
            # Now start IB live trading
            start_ib_live_trading()
            
        except Exception as e:
            debug_logger.error(f"Error during post-startup warmup: {e}", exc_info=True)
            debug_logger.warning("Server will continue with empty algorithm state")
            
    except Exception as e:
        debug_logger.error(f"Fatal error in post-startup warmup: {e}", exc_info=True)

# Add signal forwarding function
# COMMENTED OUT: This function caused connection exhaustion during high-volume backtesting
# Keeping for reference in case needed for live trading scenarios
# def forward_signal_to_trade_endpoint(signal_data):
#     """
#     Forward signals to the original /trade_signal endpoint for external monitoring.
#     This is async and lightweight - just forwards the original tick data, not reprocessing.
#     """
#     def do_forward():
#         try:
#             # Forward the original tick data to /trade_signal (same format as individual requests)
#             forward_payload = {
#                 "data": {
#                     "Time": signal_data['time'], 
#                     "Price": signal_data['price']
#                 },
#                 "currency": signal_data['currency'],
#                 "source": "batch_forward"  # Flag to indicate this is a forward, not original
#             }
#             
#             # Quick forward with short timeout
#             response = requests.post("http://localhost:5000/trade_signal", 
#                                    json=forward_payload, 
#                                    timeout=2)  # Short timeout for forwarding
#             
#             if response.status_code == 200:
#                 debug_logger.info(f"Successfully forwarded {signal_data['signal']} to /trade_signal")
#             else:
#                 debug_logger.warning(f"Failed to forward signal: HTTP {response.status_code}")
#                 
#         except Exception as e:
#             debug_logger.warning(f"Error forwarding to /trade_signal: {e}")
#             # Don't raise - forwarding failures shouldn't break batch processing
#     
#     # Run in background thread to avoid blocking
#     import threading
#     threading.Thread(target=do_forward, daemon=True).start()

# Removed /signal_notification endpoint - all signals now forwarded to /trade_signal 
# for better compatibility with external monitoring systems

# Signal handling for graceful shutdown
def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    debug_logger.info(f"Received signal {signum}, shutting down gracefully...")
    stop_ib_live_trading()
    sys.exit(0)

def check_restart_flag():
    """Monitor for restart flag and initiate graceful shutdown"""
    import os
    while True:
        try:
            if os.path.exists('restart_requested.flag'):
                debug_logger.info("Restart flag detected - initiating graceful shutdown")
                trade_logger.info("RESTART command received - shutting down for restart")
                
                # Stop IB trading first
                stop_ib_live_trading()
                
                # Give it a moment to clean up
                time.sleep(2)
                
                # Remove the restart flag
                try:
                    os.remove('restart_requested.flag')
                except:
                    pass
                
                # Restart the current process with same arguments
                debug_logger.info("Executing restart...")
                python = sys.executable
                os.execl(python, python, *sys.argv)
                
            time.sleep(1)  # Check every second
            
        except Exception as e:
            debug_logger.error(f"Error checking restart flag: {e}")
            time.sleep(1)

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# Register cleanup function for normal exit
atexit.register(stop_ib_live_trading)

if __name__ == '__main__':
    # Parse command line arguments for skip options
    if len(sys.argv) > 1:
        if sys.argv[1].lower() == "skip":
            SKIP_MODE2 = True
            SKIP_MODE3 = True
            print("Server started with SKIP mode - will skip Modes 2 and 3 during startup")
        elif sys.argv[1].lower() == "help" or sys.argv[1].lower() == "--help":
            print("Usage: python server.py [skip]")
            print("  skip: Skip Modes 2 and 3 during startup warmup (faster startup)")
            print("  (no argument): Run all modes during startup (normal operation)")
            sys.exit(0)
        else:
            print(f"Unknown argument: {sys.argv[1]}")
            print("Usage: python server.py [skip]")
            print("  skip: Skip Modes 2 and 3 during startup warmup")
            print("  (no argument): Run all modes during startup")
            sys.exit(1)
    
    # Get algorithm instance from environment variable
    import os
    ALGO_INSTANCE = int(os.environ.get('ALGO_INSTANCE', '1'))
    
    # Show immediate startup message
    print(f"Starting Algorithm Trading Server - Instance #{ALGO_INSTANCE}...")
    if SKIP_MODE2 and SKIP_MODE3:
        print("SKIP MODE: Will skip historical backtest and warmup preparation")
    print("Initializing database connections...")
    
    # Import algorithm but DON'T run warmup during startup (avoid circular dependency)
    import algorithm
    
    # Set the instance ID in the algorithm module
    algorithm.ALGO_INSTANCE = ALGO_INSTANCE
    
    # Initialize database and basic algorithm state
    debug_logger.info(f"Initializing algorithm database connections for instance {ALGO_INSTANCE}...")
    algorithm.initialize_database()
    
    print("Starting Flask server on port 5000...")
    debug_logger.info("Starting server...")
    
    # Start the warmup sequence in a background thread AFTER server starts
    print("Starting background warmup process...")
    if SKIP_MODE2 and SKIP_MODE3:
        print("   (SKIP MODE: Only signal test will run - much faster startup)")
    else:
        print("   (This will take 5-10 minutes to complete)")
    print("   Server will be ready for API calls immediately")
    print("")
    
    warmup_thread = threading.Thread(target=run_post_startup_warmup, daemon=True)
    warmup_thread.start()
    
    # Start restart monitoring thread
    restart_monitor = threading.Thread(target=check_restart_flag, daemon=True)
    restart_monitor.start()
    debug_logger.info("Restart monitor started")
    
    # Start the server (this will block and keep the server running)
    debug_logger.info("Server starting on http://0.0.0.0:5000")
    print("Server is now running and accepting requests!")
    if SKIP_MODE2 and SKIP_MODE3:
        print("Warmup process running in background (SKIP MODE - faster)...")
    else:
        print("Warmup process running in background...")
    print("API available at: http://0.0.0.0:5000 (accessible from any IP)")
    print("")
    print("COMMAND LISTENER: After IB Live Trading starts, you can use these commands:")
    print("  CLOSE_EURUSD/USDCAD/GBPUSD - Close position for currency")
    print("  TWS_CLOSED_EURUSD/USDCAD/GBPUSD - Mark position as closed in TWS")
    print("  RECONNECT - Force reconnection to IB (use after max attempts reached)")
    print("  SKIP_WARMUP - Skip warmup on next restart")
    print("  RESTART - Restart the server")
    print("  STATUS - Show current positions")
    print("  HELP - Show all commands")
    print("")
    print("Fast restart: SKIP_WARMUP then RESTART")
    print("")
    # Increase worker threads to handle concurrent requests better
    serve(app, host='0.0.0.0', port=5000, threads=32)  # 32 threads - high capacity without lock contention risk
