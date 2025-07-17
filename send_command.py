#!/usr/bin/env python3
"""
Command sender script for the Enhanced IB Trading System
Sends commands via HTTP API to both server and IB bot
"""

import requests
import sys
import json
import time

def send_command(command, host='localhost', port=5000):
    """Send a command to the trading system"""
    try:
        url = f"http://{host}:{port}/command"
        payload = {"command": command}
        
        print(f"Sending command: {command}")
        print(f"Target: {url}")
        print("-" * 50)
        
        # Set longer timeout for RECONNECT, SWITCH, and RESTART commands
        timeout = 30 if command in ['RECONNECT', 'SWITCH_LIVE', 'SWITCH_PAPER', 'RESTART', 'FULL_RESTART'] else 10
        
        response = requests.post(url, json=payload, timeout=timeout)
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Success!")
            print(f"Command: {result.get('command', command)}")
            print(f"Result: {result.get('result', 'No result')}")
            if 'timestamp' in result:
                print(f"Timestamp: {result['timestamp']}")
        else:
            print(f"❌ Error: HTTP {response.status_code}")
            try:
                error_data = response.json()
                print(f"Error: {error_data.get('error', 'Unknown error')}")
            except:
                print(f"Error: {response.text}")
        
    except requests.exceptions.ConnectionError:
        print("❌ Connection Error: Trading system is not running or not reachable")
        print(f"Make sure the server is running on {host}:{port}")
    except requests.exceptions.Timeout:
        print("❌ Timeout Error: Command took too long to execute")
        print("This can happen with RECONNECT or SWITCH commands - check logs")
    except Exception as e:
        print(f"❌ Error: {e}")

def show_help():
    """Show available commands"""
    print("Enhanced IB Trading System - Command Sender")
    print("=" * 50)
    print("Available commands:")
    print("")
    print("Position Management:")
    print("  CLOSE_EURUSD      - Close EURUSD position")
    print("  CLOSE_USDCAD      - Close USDCAD position")
    print("  CLOSE_GBPUSD      - Close GBPUSD position")
    print("  TWS_CLOSED_EURUSD - Mark EURUSD as closed in TWS")
    print("  TWS_CLOSED_USDCAD - Mark USDCAD as closed in TWS")
    print("  TWS_CLOSED_GBPUSD - Mark GBPUSD as closed in TWS")
    print("")
    print("Trading Configuration:")
    print("  SWITCH_LIVE       - Switch to live trading (instance 1 only)")
    print("  SWITCH_PAPER      - Switch to paper trading")
    print("  SET_ORDER_SIZE 50000 - Set position size (example: 50,000)")
    print("")
    print("Connection Management:")
    print("  RECONNECT         - Force reconnection to IB")
    print("  STATUS            - Show current status")
    print("")
    print("System Management:")
    print("  SKIP_WARMUP       - Skip warmup on next restart")
    print("  RESTART           - Smart restart (auto-saves positions, skip warmup, load recent zones)")
    print("  FULL_RESTART      - Full restart (complete warmup sequence)")
    print("  REMEMBER_POSITIONS - Save current positions to remembered_positions.json")
    print("  CLEAR_REMEMBERED_POSITIONS - Delete remembered_positions.json")
    print("  HELP              - Show this help")
    print("")
    print("Usage:")
    print("  python send_command.py <command>")
    print("  python send_command.py STATUS")
    print("  python send_command.py SWITCH_LIVE")
    print("  python send_command.py SET_ORDER_SIZE 75000")
    print("  python send_command.py RESTART")
    print("  python send_command.py REMEMBER_POSITIONS")
    print("  python send_command.py CLEAR_REMEMBERED_POSITIONS")
    print("")
    print("Examples:")
    print("  python send_command.py STATUS")
    print("  python send_command.py SWITCH_LIVE")
    print("  python send_command.py SET_ORDER_SIZE 25000")
    print("  python send_command.py CLOSE_EURUSD")
    print("  python send_command.py RESTART")
    print("  python send_command.py FULL_RESTART")
    print("  python send_command.py REMEMBER_POSITIONS")
    print("  python send_command.py CLEAR_REMEMBERED_POSITIONS")

def main():
    if len(sys.argv) < 2:
        show_help()
        return
    
    command = " ".join(sys.argv[1:]).upper()
    
    if command == "HELP" or command == "--HELP":
        show_help()
        return
    
    # Show what we're about to do
    print(f"Enhanced IB Trading System - Command: {command}")
    print("=" * 50)
    
    # Send the command
    send_command(command)
    
    # Show quick status for certain commands
    if command in ['SWITCH_LIVE', 'SWITCH_PAPER', 'RECONNECT', 'RESTART', 'FULL_RESTART']:
        print("\n" + "=" * 50)
        print("Getting updated status...")
        time.sleep(2)  # Give it a moment to process
        send_command('STATUS')

if __name__ == "__main__":
    main() 