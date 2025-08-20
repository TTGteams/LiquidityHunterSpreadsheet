# CRITICAL FIXES FOR SIGNAL DISCREPANCY ISSUES
# Apply these changes to algorithm.py

# 1. FIX PRICE VALIDATION RANGES - Make them more realistic
VALID_PRICE_RANGES = {
    "EUR.USD": (0.95, 1.25),    # Tighter range: 0.95-1.25 instead of 0.5-1.5
    "USD.CAD": (1.25, 1.45),   # Tighter range: 1.25-1.45 instead of 1.2-1.5  
    "GBP.USD": (1.15, 1.35)    # Tighter range: 1.15-1.35 instead of 0.8-1.8
}

# 2. ADD ADDITIONAL PRICE VALIDATION
def validate_price_realistically(currency, price, previous_price=None):
    """
    Enhanced price validation with realistic bounds and change rate limits
    """
    # Basic range check
    min_price, max_price = VALID_PRICE_RANGES.get(currency, (0.5, 2.0))
    if price < min_price or price > max_price:
        return False, f"Price {price} outside realistic range [{min_price}, {max_price}]"
    
    # Rate of change validation (if we have previous price)
    if previous_price is not None:
        change_rate = abs(price - previous_price) / previous_price
        if change_rate > 0.05:  # 5% change limit
            return False, f"Price change too large: {change_rate*100:.1f}% from {previous_price} to {price}"
    
    return True, "Valid"

# 3. MODIFY DUPLICATE PREVENTION - Make it less aggressive
def save_signal_to_database_fixed(signal_type, price, signal_time=None, currency="EUR.USD", signal_intent=None):
    """
    Fixed version with less aggressive duplicate prevention
    """
    if signal_type == 'hold':
        return True
    
    if not signal_intent:
        debug_logger.error(f"Cannot save {currency} signal without SignalIntent")
        return False
        
    if signal_time is None:
        signal_time = datetime.datetime.now()
    
    price = round(price, 5)
    
    conn = None
    cursor = None    
    try:
        conn = get_db_connection()
        if conn is None:
            debug_logger.error(f"Failed to save {currency} signal - no database connection")
            return False
        
        cursor = conn.cursor()
        
        # MODIFIED: Only check for duplicate SignalIntent within last 5 minutes (not indefinitely)
        cursor.execute("""
        SELECT TOP 1 SignalIntent, SignalTime, Price
        FROM FXStrat_TradeSignalsSent 
        WHERE Currency = ?
        AND AlgoInstance = ?
        AND SignalTime >= DATEADD(minute, -5, ?)  -- Only check last 5 minutes
        ORDER BY SignalTime DESC
        """, currency, ALGO_INSTANCE, signal_time)
        
        recent_signal = cursor.fetchone()
        
        # Check for duplicate SignalIntent only in last 5 minutes
        if recent_signal and recent_signal[0] == signal_intent:
            time_diff = (signal_time - recent_signal[1]).total_seconds()
            debug_logger.info(
                f"Prevented duplicate {currency} SignalIntent: {signal_intent} "
                f"(last sent {time_diff:.0f}s ago at {recent_signal[2]:.5f})"
            )
            return True
        
        # REMOVED: Rapid-fire prevention - this was too aggressive
        # The 15-minute rapid-fire check has been removed
        
        # Insert the new signal
        cursor.execute("""
        INSERT INTO FXStrat_TradeSignalsSent 
        (SignalTime, SignalType, Price, Currency, SignalIntent, AlgoInstance)
        VALUES (?, ?, ?, ?, ?, ?)
        """, 
        signal_time, signal_type, price, currency, signal_intent, ALGO_INSTANCE)
        
        conn.commit()
        
        debug_logger.warning(
            f"✓ {currency} {signal_type.upper()} signal saved: {signal_intent} at {price:.5f}"
        )
        
        return True
    
    except Exception as e:
        debug_logger.error(f"Error saving {currency} signal to database: {e}", exc_info=True)
        if conn:
            try:
                conn.rollback()
            except:
                pass
        return False
    
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass

# 4. ADD SIGNAL AUDIT LOGGING
def audit_signal_decision(currency, raw_signal, signal_intent, tick_price, tick_time, saved_to_db):
    """
    Comprehensive logging for signal decisions to help debug discrepancies
    """
    audit_logger = logging.getLogger("signal_audit")
    
    audit_info = {
        'timestamp': tick_time.isoformat(),
        'currency': currency,
        'signal': raw_signal,
        'intent': signal_intent,
        'price': tick_price,
        'saved_to_db': saved_to_db,
        'trades_count': len(trades.get(currency, [])),
        'open_trades': len([t for t in trades.get(currency, []) if t.get('status') == 'open'])
    }
    
    audit_logger.info(f"SIGNAL_AUDIT: {json.dumps(audit_info)}")
    
    # Also log to debug for immediate visibility
    debug_logger.warning(
        f"🔍 SIGNAL AUDIT {currency}: {raw_signal} -> {signal_intent} at {tick_price:.5f} | "
        f"DB_SAVED: {saved_to_db} | TRADES: {audit_info['trades_count']} | OPEN: {audit_info['open_trades']}"
    )

# 5. CURRENCY VALIDATION ENHANCEMENT
def validate_currency_consistency(request_currency, data_currency=None):
    """
    Ensure currency consistency between request and data
    """
    if data_currency and data_currency != request_currency:
        debug_logger.warning(
            f"Currency mismatch: Request={request_currency}, Data={data_currency}. "
            f"Using request currency: {request_currency}"
        )
    
    if request_currency not in SUPPORTED_CURRENCIES:
        raise ValueError(f"Unsupported currency: {request_currency}")
    
    return request_currency
