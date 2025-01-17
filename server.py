from flask import Flask, request, jsonify
import pandas as pd
from algorithm import process_market_data  # Unchanged import for the main algo function
import threading
import logging
from logging.handlers import RotatingFileHandler
from waitress import serve

app = Flask(__name__)

trade_logger = logging.getLogger("trade_logger")
debug_logger = logging.getLogger("debug_logger")

trade_logger.setLevel(logging.INFO)
debug_logger.setLevel(logging.INFO)  # Change to DEBUG if detailed logs are needed

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

# Capture DEBUG and above
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
        trade_logger.info(f"Received data: {content}")  # Use trade_logger instead of root logger
        data = content['data']

        # Validate input data
        required_fields = ['Time', 'Price']
        for field in required_fields:
            if field not in data:
                error_msg = f"Missing field: {field}"
                trade_logger.error(error_msg)
                return jsonify({'error': error_msg}), 400

        # Convert the data to a DataFrame
        df = pd.DataFrame([data])

        # Parse the timestamp with millisecond precision
        df['Time'] = pd.to_datetime(df['Time'], format='%Y-%m-%d %H:%M:%S.%f')

        # Set 'Time' as the index
        df.set_index('Time', inplace=True)

        # Process the new data point through the algorithm
        with state_lock:
            signal = process_market_data(df)
        trade_logger.info(f"Generated signal: {signal}")

        # Return the signal
        return jsonify({'signal': signal}), 200

    except Exception as e:
        debug_logger.error(f"Error occurred: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    # NEW: Warm up the main algorithm before starting the server.py script
    import algorithm
    algorithm.main()  # Calls warmup_data() via algorithm.main(), so bars & zones are ready

    # Run the app with waitress or your preferred WSGI server
    serve(app, host='0.0.0.0', port=5001)
