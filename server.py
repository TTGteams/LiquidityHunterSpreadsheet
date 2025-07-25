from flask import Flask, request, jsonify
import pandas as pd
import algorithm
import threading
import logging
from logging.handlers import RotatingFileHandler
from waitress import serve
import pytz
import datetime
import time
import sys
import signal
import os

app = Flask(__name__)

# Global variables for skip settings
SKIP_MODE2 = False
SKIP_MODE3 = False

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
    """Execute commands via HTTP endpoint - simplified for external app usage"""
    try:
        content = request.json
        command = content.get('command', '').strip().upper()
        
        if not command:
            return jsonify({'error': 'No command provided'}), 400
        
        # Handle server-level commands only
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
        
        elif command == 'FULL_RESTART':
            try:
                # Enable live trading mode and save current signal state before restart
                algorithm.is_live_trading_mode = True
                algorithm.save_algorithm_signal_state()
                
                # Create full restart flag - performs complete warmup
                with open('restart_requested.flag', 'w') as f:
                    f.write('full')
                debug_logger.info("Created full restart flag")
                trade_logger.info("FULL_RESTART command received - initiating full restart with complete warmup")
                
                return jsonify({
                    'command': command,
                    'result': 'Full restart initiated - will perform complete warmup sequence.',
                    'timestamp': datetime.datetime.now().isoformat()
                }), 200
                
            except Exception as e:
                debug_logger.error(f"Error creating restart_requested.flag: {e}")
                return jsonify({'error': f'Failed to initiate full restart: {str(e)}'}), 500
        
        elif command == 'RESTART':
            try:
                # Enable live trading mode and save current signal state before restart
                algorithm.is_live_trading_mode = True
                algorithm.save_algorithm_signal_state()
                
                # Create smart restart flag - skips warmup and loads recent data
                with open('restart_requested.flag', 'w') as f:
                    f.write('smart')
                debug_logger.info("Created smart restart flag")
                trade_logger.info("RESTART command received - initiating smart restart (skip warmup, load recent data)")
                
                return jsonify({
                    'command': command,
                    'result': 'Smart restart initiated - will skip warmup and load recent data from database.',
                    'timestamp': datetime.datetime.now().isoformat()
                }), 200
                
            except Exception as e:
                debug_logger.error(f"Error creating restart_requested.flag: {e}")
                return jsonify({'error': f'Failed to initiate restart: {str(e)}'}), 500
        
        elif command == 'HELP':
            help_text = """Available commands for external trading app:
  RESTART - Smart restart (skip warmup, load recent data from database)
  FULL_RESTART - Full restart with complete warmup sequence
  SKIP_WARMUP - Skip warmup on next restart
  SHOW_PRICES - Show recent price data and algorithm state
  HELP - Show this help message
  
Note: Connection management, position tracking, and order execution
are handled by your external trading application."""
            
            return jsonify({
                'command': command,
                'result': help_text,
                'timestamp': datetime.datetime.now().isoformat()
            }), 200
        
        elif command == 'SHOW_PRICES':
            try:
                # Show recent prices and algorithm state
                price_info = []
                current_time = datetime.datetime.now()
                
                for currency in algorithm.SUPPORTED_CURRENCIES:
                    # Get recent bars if available
                    if currency in algorithm.bars and not algorithm.bars[currency].empty:
                        recent_bars = algorithm.bars[currency].tail(5)
                        for idx, row in recent_bars.iterrows():
                            price_info.append({
                                'currency': currency,
                                'time': idx.isoformat(),
                                'price': f"{row['close']:.5f}",
                                'type': 'bar_close'
                            })
                    
                    # Get zone count
                    zone_count = len(algorithm.current_valid_zones_dict.get(currency, {}))
                    if zone_count > 0:
                        price_info.append({
                            'currency': currency,
                            'zones': zone_count,
                            'type': 'zone_info'
                        })
                
                result = {
                    'timestamp': current_time.isoformat(),
                    'price_data': price_info,
                    'total_zones': sum(len(zones) for zones in algorithm.current_valid_zones_dict.values()),
                    'supported_currencies': algorithm.SUPPORTED_CURRENCIES
                }
                
                return jsonify({
                    'command': command,
                    'result': result,
                    'timestamp': datetime.datetime.now().isoformat()
                }), 200
                
            except Exception as e:
                debug_logger.error(f"Error getting price info: {e}")
                return jsonify({'error': f'Failed to get price information: {str(e)}'}), 500
        
        else:
            return jsonify({'error': f'Unknown command: {command}. Use HELP for available commands.'}), 400
        
    except Exception as e:
        debug_logger.error(f"Error executing command: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint"""
    return jsonify({
        'status': 'running',
        'timestamp': datetime.datetime.now().isoformat(),
        'supported_currencies': algorithm.SUPPORTED_CURRENCIES
    }), 200

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

def run_post_startup_warmup():
    """
    Run the complete warmup sequence after server startup.
    This runs in a background thread to avoid blocking the server.
    """
    global SKIP_MODE2, SKIP_MODE3
    
    try:
        # Wait a moment for server to be fully ready
        debug_logger.info("Waiting 2 seconds for server to be ready...")
        time.sleep(2)
        
        # Check for skip_warmup.flag file
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
            
            # If we skipped warmup modes, load recent zones AND bars with indicators from database
            if SKIP_MODE2 and SKIP_MODE3:
                debug_logger.info("Loading recent zones and bars with indicators from database (HYBRID APPROACH)...")
                try:
                    # Load the most recent 10 zones per currency from database
                    recent_zones = algorithm.load_recent_zones_from_database(limit=10)
                    
                    # Load the most recent 100 bars with indicators per currency from database
                    recent_bars = algorithm.load_recent_bars_with_indicators_from_database(limit=100)
                    
                    # Apply the loaded zones to the algorithm state
                    zones_loaded = 0
                    bars_loaded = 0
                    
                    for currency in algorithm.SUPPORTED_CURRENCIES:
                        # Load zones
                        if currency in recent_zones and recent_zones[currency]:
                            algorithm.load_preexisting_zones(recent_zones[currency], currency)
                            zones_loaded += len(recent_zones[currency])
                            debug_logger.warning(
                                f"Populated {currency} with {len(recent_zones[currency])} recent zones from database"
                            )
                        
                        # Load bars with indicators
                        if currency in recent_bars and not recent_bars[currency].empty:
                            algorithm.load_preexisting_bars_and_indicators(recent_bars[currency], currency)
                            bars_loaded += len(recent_bars[currency])
                            debug_logger.warning(
                                f"Populated {currency} with {len(recent_bars[currency])} recent bars with indicators from database"
                            )
                    
                    trade_logger.info(
                        f"HYBRID SMART RESTART complete - loaded {zones_loaded} zones and {bars_loaded} bars with indicators from database"
                    )
                    
                except Exception as e:
                    debug_logger.error(f"Error loading recent data from database: {e}", exc_info=True)
                    debug_logger.warning("Continuing without recent zones and bars")
            
            debug_logger.info("Post-startup warmup completed successfully")
            trade_logger.info("Algorithm warmup complete - all zones and bars loaded")
            
            # Reset trading state to start fresh for live trading
            # This ensures no backtest trades affect live trading
            debug_logger.info("Resetting trading state for fresh live trading start...")
            algorithm.reset_trading_state_for_live_trading()
            
            trade_logger.info("Trading algorithm ready - waiting for external app to send price data")
            
        except Exception as e:
            debug_logger.error(f"Error during post-startup warmup: {e}", exc_info=True)
            debug_logger.warning("Server will continue with empty algorithm state")
            
    except Exception as e:
        debug_logger.error(f"Fatal error in post-startup warmup: {e}", exc_info=True)

# Signal handling for graceful shutdown
def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    debug_logger.info(f"Received signal {signum}, shutting down gracefully...")
    sys.exit(0)

def check_restart_flag():
    """Monitor for restart flag and initiate graceful shutdown"""
    import os
    while True:
        try:
            if os.path.exists('restart_requested.flag'):
                # Read the restart type from the flag file
                restart_type = 'smart'  # default
                try:
                    with open('restart_requested.flag', 'r') as f:
                        restart_type = f.read().strip()
                except:
                    pass
                
                debug_logger.info(f"Restart flag detected - type: {restart_type}")
                
                if restart_type == 'smart':
                    trade_logger.info("RESTART command received - smart restart (skip warmup, load recent zones)")
                elif restart_type == 'full':
                    trade_logger.info("FULL_RESTART command received - full restart with complete warmup")
                else:
                    trade_logger.info("RESTART command received - shutting down for restart")
                
                # Give it a moment to clean up
                time.sleep(2)
                
                # Remove the restart flag
                try:
                    os.remove('restart_requested.flag')
                except:
                    pass
                
                # Restart the current process with restart type argument
                debug_logger.info(f"Executing {restart_type} restart...")
                python = sys.executable
                
                # Build new arguments list
                new_args = [python] + sys.argv
                if restart_type == 'smart':
                    new_args.append('smart_restart')
                elif restart_type == 'full':
                    new_args.append('full_restart')
                
                os.execl(python, *new_args)
                
            time.sleep(1)  # Check every second
            
        except Exception as e:
            debug_logger.error(f"Error checking restart flag: {e}")
            time.sleep(1)

# Register signal handlers
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

if __name__ == '__main__':
    # Parse command line arguments for skip options and restart types
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        
        if arg == "skip":
            SKIP_MODE2 = True
            SKIP_MODE3 = True
            print("Server started with SKIP mode - will skip Modes 2 and 3 during startup")
        elif arg == "smart_restart":
            SKIP_MODE2 = True
            SKIP_MODE3 = True
            print("Server started with SMART RESTART - will skip warmup and load recent zones from database")
        elif arg == "full_restart":
            SKIP_MODE2 = False
            SKIP_MODE3 = False
            print("Server started with FULL RESTART - will perform complete warmup sequence")
        elif arg == "help" or arg == "--help":
            print("Usage: python server.py [option]")
            print("  skip: Skip Modes 2 and 3 during startup warmup (faster startup)")
            print("  smart_restart: Skip warmup and load recent zones (used by RESTART command)")
            print("  full_restart: Perform complete warmup (used by FULL_RESTART command)")
            print("  (no argument): Run all modes during startup (normal operation)")
            sys.exit(0)
        else:
            print(f"Unknown argument: {arg}")
            print("Usage: python server.py [option]")
            print("  skip: Skip Modes 2 and 3 during startup warmup")
            print("  smart_restart: Skip warmup and load recent zones")
            print("  full_restart: Perform complete warmup")
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
    
    print("Starting Flask server on port 6000...")
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
    print("EXTERNAL APP INTEGRATION:")
    print("  Send price data to: POST /trade_signal")
    print("  Payload: {'data': {'Time': '2024-01-01 12:00:00', 'Price': 1.0523}, 'currency': 'EUR.USD'}")
    print("  Response: {'signal': 'buy|sell|close|hold', 'currency': 'EUR.USD'}")
    print("")
    print("AVAILABLE COMMANDS (via POST /command):")
    print("  RESTART - Smart restart (skip warmup, load recent data)")
    print("  FULL_RESTART - Full restart with complete warmup")
    print("  SKIP_WARMUP - Skip warmup on next restart")
    print("  SHOW_PRICES - Show recent price data and algorithm state")
    print("  HELP - Show available commands")
    print("")
    # Increase worker threads to handle concurrent requests better
    serve(app, host='0.0.0.0', port=5000, threads=32)  # 32 threads - high capacity without lock contention risk