from flask import Flask, request, jsonify
import pandas as pd
import algorithm
import threading
import logging
from logging.handlers import RotatingFileHandler
from waitress import serve

app = Flask(__name__)

trade_logger = logging.getLogger("trade_logger")
debug_logger = logging.getLogger("debug_logger")

trade_logger.setLevel(logging.INFO)
# CHANGED from INFO to WARNING:
debug_logger.setLevel(logging.WARNING)

trade_logger.propagate = False
debug_logger.propagate = False

trade_handler = RotatingFileHandler(
    'trade_log.log',
    maxBytes=100000000,  # 100MB per file
    backupCount=5
)
trade_handler.setLevel(logging.INFO)
trade_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
trade_handler.setFormatter(trade_formatter)
trade_logger.addHandler(trade_handler)

debug_handler = RotatingFileHandler(
    'debug_log.log',
    # 10MB per file
    maxBytes=10000000,
    backupCount=2
)

# Capture DEBUG and above, but the logger itself is set to WARNING,
# so effectively we only see WARNING/ERROR/CRITICAL messages.
debug_handler.setLevel(logging.DEBUG)
debug_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
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

        # Convert the data to a DataFrame
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

if __name__ == '__main__':
    # Warm up the main algorithm before starting the server.py script
    import algorithm
    algorithm.main()  # Calls warmup_data() via algorithm.main(), so bars & zones are ready

    # Run the app with waitress or your preferred WSGI server
    serve(app, host='0.0.0.0', port=5000)
