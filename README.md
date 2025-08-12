# Diego FX Momentum Trading Algorithm

A sophisticated algorithmic trading system for forex markets, designed for **external app integration**. The system provides pure signal generation based on liquidity zones, technical indicators, and momentum analysis.

## 🎯 **External App Integration Model**

This system acts as a **signal generation API** - your external trading application handles:
- **TWS/Broker connectivity** (e.g., Interactive Brokers, MetaTrader, etc.)
- **Market data feeds** 
- **Order execution**
- **Position management**
- **Risk management**

The algorithm provides **intelligent trading signals** based on sophisticated analysis.

## 🏗️ **Architecture Overview**

```
Your Trading App → HTTP API → Trading Algorithm → Trading Signals → Your Trading App
       ↓                              ↓
  Order Execution             Database Storage
```

### **Core Components**
- **`algorithm.py`** - Core trading logic (zones, indicators, signals)
- **`server.py`** - Flask API server for external app integration
- `run_test.py` remains for reference/historical backtesting only. The server no longer uses warmup.
- **Database** - SQL Server for persistence and analysis
- **Docker** - Containerized deployment

## 🚀 **Quick Start**

### **1. Start the Algorithm API**
```bash
# Using Docker (recommended)
docker-compose up

# Or locally
pip install -r requirements.txt
python server.py
```

### **2. Test API Connectivity**
```bash
curl -X POST http://localhost:5000/trade_signal \
  -H "Content-Type: application/json" \
  -d '{"data": {"Time": "2024-01-01 12:00:00", "Price": 1.0523}, "currency": "EUR.USD"}'
```

Expected response:
```json
{"signal": "hold", "currency": "EUR.USD"}
```

### **3. Try the Example Integration**
```bash
python external_app_example.py
```

## 🔌 **API Integration Guide**

### **Core Endpoint: `/trade_signal`**
Send price ticks and receive trading signals:

```python
import requests

# Send price update
payload = {
    "data": {
        "Time": "2024-01-01 12:00:00.123456",
        "Price": 1.0523
    },
    "currency": "EUR.USD"
}

response = requests.post("http://localhost:5000/trade_signal", json=payload)
result = response.json()

# Result: {"signal": "buy|sell|hold", "currency": "EUR.USD"}
```

### **Available Signals**
- **`buy`** - Open long position OR close short position
- **`sell`** - Open short position OR close long position  
- **`hold`** - No action required

**Position Logic:**
- When opening a long trade → sends `buy`
- When closing a long trade → sends `sell` 
- When opening a short trade → sends `sell`
- When closing a short trade → sends `buy`

### **Supported Currency Pairs**
- `EUR.USD`
- `USD.CAD`
- `GBP.USD`

### **Additional Endpoints**
- **`GET /health`** - Check API health
- **`GET /algorithm_state`** - Get zones and bars data
- **`POST /command`** - Send management commands

## 🛠️ **Management Commands**

Send commands via POST to `/command`:

```python
commands = {
    "RESTART": "Smart restart",
    "FULL_RESTART": "Full restart",
    "SKIP_WARMUP": "No-op; warmup removed", 
    "SHOW_PRICES": "Show recent price data and algorithm state",
    "HELP": "Show available commands"
}
```

## 📊 **Algorithm Features**

### **Liquidity Zone Detection**
- Identifies demand/supply zones from price action
- Validates zones using technical indicators
- Dynamic zone invalidation based on price violations

### **Technical Analysis**
- **RSI (14)** - Momentum confirmation
- **MACD (12,26,9)** - Trend confirmation  
- **ATR-based thresholds** - Adaptive to market volatility

### **Multi-Currency Support**
- Simultaneous analysis of multiple pairs
- Currency-specific zone management
- Independent signal generation per pair

### **Database Persistence**
- All bars, zones, and signals stored in SQL Server
- Historical analysis and backtesting data
- Restart recovery from database state

## 🔧 **Configuration**

### **Environment Variables**
```bash
# Algorithm Configuration
ALGO_INSTANCE=1                    # Algorithm instance ID
DB_SERVER=192.168.50.100          # SQL Server host
DB_DATABASE=FXStrat               # Database name
DB_USERNAME=djaime                # Database user
DB_PASSWORD=***                   # Database password
```

### **Docker Configuration**
The `docker-compose.yml` includes:
- Flask API on port 5000
- Persistent logging and data volumes
- Health checks for monitoring
- Resource limits for stability

## 📈 **Integration Examples**

### **Basic Price Feed Integration**
```python
from external_app_example import TradingAlgorithmClient

client = TradingAlgorithmClient()

# Send price update
result = client.send_price_update("EUR.USD", 1.0523)

if result['signal'] != 'hold':
    # Execute trade via your broker API
    execute_trade(result['currency'], result['signal'])
```

### **Advanced Integration Pattern**
```python
class MyTradingApp:
    def __init__(self):
        self.algo_client = TradingAlgorithmClient()
        self.broker = MyBrokerAPI()  # Your broker integration
        
    def on_price_tick(self, currency, price, timestamp):
        # Send to algorithm
        signal = self.algo_client.send_price_update(currency, price, timestamp)
        
        # Handle signal changes
        if signal['signal'] != 'hold':
            self.execute_trade_signal(currency, signal['signal'], price)
    
    def execute_trade_signal(self, currency, signal, price):
        current_position = self.broker.get_position(currency)
        
        if signal == 'buy':
            if current_position == 'short':
                # Close short position first
                self.broker.close_position(currency)
            if current_position != 'long':
                # Open long position
                self.broker.place_buy_order(currency, self.calculate_position_size())
        elif signal == 'sell':
            if current_position == 'long':
                # Close long position first  
                self.broker.close_position(currency)
            if current_position != 'short':
                # Open short position
                self.broker.place_sell_order(currency, self.calculate_position_size())
```

## 📋 **System Requirements**

### **Runtime Requirements**
- Python 3.12+
- SQL Server (local or remote)
- 4GB RAM minimum
- Docker (optional but recommended)

### **Python Dependencies**
- Flask, Waitress (API server)
- pandas, numpy (data processing)
- pandas_ta (technical analysis)
- pyodbc (database connectivity)
- requests (HTTP client for examples)

## 🔍 **Monitoring & Debugging**

### **Logs**
- **`trade_log.log`** - Trading signals and important events
- **`debug_log.log`** - Detailed algorithm behavior
- Both logs rotate automatically (100MB trade, 10MB debug)

### **Health Monitoring**
```bash
# Check API health
curl http://localhost:5000/health

# Get algorithm state
curl http://localhost:5000/algorithm_state

# View recent prices
curl -X POST http://localhost:5000/command \
  -H "Content-Type: application/json" \
  -d '{"command": "SHOW_PRICES"}'
```

## 🧪 **Testing & Development**

### **Test API with Simulated Data**
```bash
python external_app_example.py
```

### **Skip Warmup for Faster Development**
```bash
# Start with recent data from database (much faster)
python server.py smart_restart

# Or via command
curl -X POST http://localhost:5000/command \
  -d '{"command": "SKIP_WARMUP"}'
```

## 🎯 **Production Deployment**

### **Docker Deployment (Recommended)**
```bash
# Production start
docker-compose up -d

# Monitor logs
docker-compose logs -f

# Restart algorithm
curl -X POST http://localhost:5000/command \
  -d '{"command": "RESTART"}'
```

### **Local Deployment**
```bash
# Install dependencies
pip install -r requirements.txt

# Start algorithm API
python server.py

# Your trading app connects to http://localhost:5000
```

## 💡 **Integration Tips**

1. **Error Handling** - Always handle API timeouts and network errors
2. **Signal Deduplication** - Algorithm prevents duplicate signals automatically
3. **Health Monitoring** - Regularly check `/health` endpoint
4. **Restart Recovery** - Algorithm preserves signal state across restarts
5. **Multi-Currency** - Send price updates for all pairs simultaneously
6. **Position Sync** - Your app manages positions; algorithm generates signals

## 🤝 **Support**

For integration questions or issues:
1. Check logs in `trade_log.log` and `debug_log.log`
2. Use `SHOW_PRICES` command to verify algorithm state
3. Test with `external_app_example.py` to isolate issues
4. Monitor `/health` endpoint for API connectivity

---

**Ready to integrate?** Start with `external_app_example.py` to see the API in action!