#!/usr/bin/env python3
"""
Signal Monitoring and Audit Script
Helps track discrepancies between algorithm signals and database records
"""

import sqlite3
import pyodbc
import pandas as pd
import json
from datetime import datetime, timedelta
import logging

# Database configuration (update with your settings)
DB_CONFIG = {
    'server': '192.168.50.100',
    'database': 'FXStrat',
    'username': 'djaime',
    'password': 'Enrique30072000!3',
    'driver': 'ODBC Driver 17 for SQL Server'
}

def get_db_connection():
    """Get database connection"""
    conn_str = (
        f"Driver={{{DB_CONFIG['driver']}}};"
        f"Server={DB_CONFIG['server']};"
        f"Database={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        f"Connection Timeout=30;"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

def check_recent_signals(hours=24):
    """
    Check all signals from the last N hours and identify potential issues
    """
    print(f"\n🔍 SIGNAL AUDIT REPORT - Last {hours} hours")
    print("=" * 60)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all recent signals
        cursor.execute("""
        SELECT 
            SignalTime,
            SignalType, 
            Price,
            Currency,
            SignalIntent,
            AlgoInstance
        FROM FXStrat_TradeSignalsSent 
        WHERE SignalTime >= DATEADD(hour, -?, GETDATE())
        ORDER BY SignalTime DESC
        """, hours)
        
        signals = cursor.fetchall()
        
        if not signals:
            print("❌ No signals found in the specified time period")
            return
        
        print(f"✅ Found {len(signals)} signals")
        print("\n📊 SIGNAL BREAKDOWN:")
        
        # Group by currency
        currency_stats = {}
        intent_stats = {}
        price_issues = []
        
        for signal in signals:
            signal_time, signal_type, price, currency, intent, instance = signal
            
            # Currency stats
            if currency not in currency_stats:
                currency_stats[currency] = {'buy': 0, 'sell': 0, 'total': 0}
            currency_stats[currency][signal_type] += 1
            currency_stats[currency]['total'] += 1
            
            # Intent stats  
            if intent:
                intent_stats[intent] = intent_stats.get(intent, 0) + 1
            
            # Price validation
            price_valid = validate_price_range(currency, price)
            if not price_valid:
                price_issues.append({
                    'time': signal_time,
                    'currency': currency,
                    'price': price,
                    'signal': signal_type,
                    'intent': intent
                })
        
        # Display currency breakdown
        for currency, stats in currency_stats.items():
            print(f"  {currency}: {stats['total']} signals ({stats['buy']} buy, {stats['sell']} sell)")
        
        # Display intent breakdown
        print(f"\n🎯 SIGNAL INTENTS:")
        for intent, count in intent_stats.items():
            print(f"  {intent}: {count}")
        
        # Display price issues
        if price_issues:
            print(f"\n⚠️  PRICE VALIDATION ISSUES ({len(price_issues)}):")
            for issue in price_issues:
                print(f"  {issue['time']}: {issue['currency']} {issue['signal']} at {issue['price']:.5f} ({issue['intent']})")
        else:
            print(f"\n✅ No price validation issues found")
            
        # Check for missing signals (gaps in time)
        print(f"\n⏰ TIME GAP ANALYSIS:")
        check_signal_gaps(signals)
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error checking signals: {e}")

def validate_price_range(currency, price):
    """Validate if price is within realistic range"""
    ranges = {
        "EUR.USD": (0.95, 1.25),
        "USD.CAD": (1.25, 1.45),  
        "GBP.USD": (1.15, 1.35)
    }
    
    if currency not in ranges:
        return True  # Unknown currency, assume valid
    
    min_price, max_price = ranges[currency]
    return min_price <= price <= max_price

def check_signal_gaps(signals):
    """Check for unusual gaps in signal timing"""
    if len(signals) < 2:
        return
    
    gaps = []
    for i in range(len(signals) - 1):
        current_time = signals[i][0]  # SignalTime
        next_time = signals[i + 1][0]
        
        # Calculate gap in minutes
        gap_minutes = abs((current_time - next_time).total_seconds() / 60)
        
        # Flag gaps > 2 hours during trading hours
        if gap_minutes > 120:
            gaps.append({
                'start': next_time,
                'end': current_time, 
                'gap_minutes': gap_minutes
            })
    
    if gaps:
        print(f"  Found {len(gaps)} significant gaps:")
        for gap in gaps[:5]:  # Show first 5
            print(f"    {gap['start']} to {gap['end']}: {gap['gap_minutes']:.0f} minutes")
    else:
        print("  No significant time gaps detected")

def compare_with_expected_pattern():
    """
    Compare recent signals with expected trading patterns
    """
    print(f"\n📈 TRADING PATTERN ANALYSIS:")
    print("-" * 40)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check for balanced open/close signals per currency
        cursor.execute("""
        SELECT 
            Currency,
            SignalIntent,
            COUNT(*) as Count
        FROM FXStrat_TradeSignalsSent 
        WHERE SignalTime >= DATEADD(day, -1, GETDATE())
        AND SignalIntent IS NOT NULL
        GROUP BY Currency, SignalIntent
        ORDER BY Currency, SignalIntent
        """)
        
        results = cursor.fetchall()
        
        # Group by currency
        currency_intents = {}
        for row in results:
            currency, intent, count = row
            if currency not in currency_intents:
                currency_intents[currency] = {}
            currency_intents[currency][intent] = count
        
        # Analyze balance
        for currency, intents in currency_intents.items():
            opens = intents.get('OPEN_LONG', 0) + intents.get('OPEN_SHORT', 0)
            closes = intents.get('CLOSE_LONG', 0) + intents.get('CLOSE_SHORT', 0)
            
            print(f"  {currency}: {opens} opens, {closes} closes", end="")
            
            if opens != closes:
                imbalance = abs(opens - closes)
                print(f" ⚠️  IMBALANCE: {imbalance}")
            else:
                print(f" ✅ BALANCED")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error in pattern analysis: {e}")

def generate_audit_report():
    """Generate comprehensive audit report"""
    print("\n" + "="*60)
    print("🔍 COMPREHENSIVE SIGNAL AUDIT REPORT")
    print("="*60)
    
    check_recent_signals(24)  # Last 24 hours
    compare_with_expected_pattern()
    
    print(f"\n💡 RECOMMENDATIONS:")
    print("  1. Check external app logs for signals sent to TWS")
    print("  2. Verify currency parameter in API calls")
    print("  3. Monitor price validation alerts")
    print("  4. Check for database connection issues")
    print("  5. Review duplicate prevention settings")

def monitor_live_signals():
    """
    Real-time monitoring mode (run this during trading hours)
    """
    print("🔴 LIVE SIGNAL MONITORING MODE")
    print("Checking for new signals every 30 seconds...")
    print("Press Ctrl+C to stop")
    
    import time
    last_check = datetime.now() - timedelta(minutes=5)
    
    try:
        while True:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Check for new signals since last check
            cursor.execute("""
            SELECT 
                SignalTime,
                SignalType, 
                Price,
                Currency,
                SignalIntent
            FROM FXStrat_TradeSignalsSent 
            WHERE SignalTime > ?
            ORDER BY SignalTime DESC
            """, last_check)
            
            new_signals = cursor.fetchall()
            
            if new_signals:
                print(f"\n🆕 {len(new_signals)} new signals:")
                for signal in new_signals:
                    signal_time, signal_type, price, currency, intent = signal
                    print(f"  {signal_time}: {currency} {signal_type.upper()} at {price:.5f} ({intent})")
                
                last_check = new_signals[0][0]  # Update last check time
            
            conn.close()
            time.sleep(30)  # Check every 30 seconds
            
    except KeyboardInterrupt:
        print("\n✋ Monitoring stopped")
    except Exception as e:
        print(f"❌ Error in live monitoring: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "live":
        monitor_live_signals()
    else:
        generate_audit_report()
