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

@app.route('/trade_signal_batch', methods=['POST'])
def trade_signal_batch():
    """
    Batch endpoint for processing multiple price ticks sequentially.
    Used by testing scripts to speed up historical data processing.
    Maintains tick-by-tick algorithm behavior.
    
    Also forwards non-hold signals to /trade_signal endpoint for external monitoring.
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
                if signal != 'hold':
                    forward_signal_to_trade_endpoint(signal_result)
                
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
        
        # Start the IB trading script
        ib_process = subprocess.Popen(
            [sys.executable, 'ib_live_trading_enhanced.py'],
            cwd=os.getcwd(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
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
            result = warmup_data()
            
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
def forward_signal_to_trade_endpoint(signal_data):
    """
    Forward signals to the original /trade_signal endpoint for external monitoring.
    This is async and lightweight - just forwards the original tick data, not reprocessing.
    """
    def do_forward():
        try:
            # Forward the original tick data to /trade_signal (same format as individual requests)
            forward_payload = {
                "data": {
                    "Time": signal_data['time'], 
                    "Price": signal_data['price']
                },
                "currency": signal_data['currency'],
                "source": "batch_forward"  # Flag to indicate this is a forward, not original
            }
            
            # Quick forward with short timeout
            response = requests.post("http://localhost:5000/trade_signal", 
                                   json=forward_payload, 
                                   timeout=2)  # Short timeout for forwarding
            
            if response.status_code == 200:
                debug_logger.info(f"Successfully forwarded {signal_data['signal']} to /trade_signal")
            else:
                debug_logger.warning(f"Failed to forward signal: HTTP {response.status_code}")
                
        except Exception as e:
            debug_logger.warning(f"Error forwarding to /trade_signal: {e}")
            # Don't raise - forwarding failures shouldn't break batch processing
    
    # Run in background thread to avoid blocking
    import threading
    threading.Thread(target=do_forward, daemon=True).start()

# Removed /signal_notification endpoint - all signals now forwarded to /trade_signal 
# for better compatibility with external monitoring systems

# Signal handling for graceful shutdown
def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    debug_logger.info(f"Received signal {signum}, shutting down gracefully...")
    stop_ib_live_trading()
    sys.exit(0)

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
    
    # Show immediate startup message
    print("Starting Algorithm Trading Server...")
    if SKIP_MODE2 and SKIP_MODE3:
        print("SKIP MODE: Will skip historical backtest and warmup preparation")
    print("Initializing database connections...")
    
    # Import algorithm but DON'T run warmup during startup (avoid circular dependency)
    import algorithm
    
    # Initialize database and basic algorithm state
    debug_logger.info("Initializing algorithm database connections...")
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
    
    # Start the server (this will block and keep the server running)
    debug_logger.info("Server starting on http://0.0.0.0:5000")
    print("Server is now running and accepting requests!")
    if SKIP_MODE2 and SKIP_MODE3:
        print("Warmup process running in background (SKIP MODE - faster)...")
    else:
        print("Warmup process running in background...")
    print("API available at: http://0.0.0.0:5000 (accessible from any IP)")
    print("")
    serve(app, host='0.0.0.0', port=5000)