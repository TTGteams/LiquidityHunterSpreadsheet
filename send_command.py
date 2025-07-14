#!/usr/bin/env python3
"""
Python script to send commands to the forex trading system via HTTP.
Replaces send_command.sh with a cleaner Python-native approach.
"""

import requests
import json
import sys
import time

def send_command(command, base_url="http://localhost:5000"):
    """Send a command to the trading system via HTTP"""
    try:
        # Use longer timeout for RECONNECT command
        timeout = 20 if command.upper() == "RECONNECT" else 10
        
        response = requests.post(
            f"{base_url}/command",
            json={"command": command},
            timeout=timeout
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"Command: {result['command']}")
            print(f"Result: {result['result']}")
            print(f"Timestamp: {result['timestamp']}")
            return True
        else:
            print(f"Error: HTTP {response.status_code}")
            try:
                error_data = response.json()
                print(f"Details: {error_data.get('error', 'Unknown error')}")
            except:
                print(f"Response: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to trading system")
        print("Make sure the container is running and accessible on port 5000")
        return False
    except requests.exceptions.Timeout:
        print("Error: Request timed out")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: python send_command.py <command>")
        print("")
        print("Available commands:")
        print("  CLOSE_EURUSD/USDCAD/GBPUSD - Close position for currency")
        print("  TWS_CLOSED_EURUSD/USDCAD/GBPUSD - Mark position as closed in TWS")
        print("  RECONNECT - Force reconnection to IB (use after max attempts reached)")
        print("  SKIP_WARMUP - Skip warmup on next restart")
        print("  RESTART - Restart the server")
        print("  STATUS - Show current positions")
        print("  HELP - Show all commands")
        print("")
        print("Examples:")
        print("  python send_command.py STATUS")
        print("  python send_command.py CLOSE_EURUSD")
        print("  python send_command.py RECONNECT")
        print("  python send_command.py SKIP_WARMUP")
        print("  python send_command.py RESTART")
        sys.exit(1)
    
    command = sys.argv[1]
    
    # Check if we're running in Docker and need to use container IP
    base_url = "http://localhost:5000"
    if len(sys.argv) > 2 and sys.argv[2] == "--docker":
        base_url = "http://192.168.50.100:5000"  # Adjust IP as needed
    
    print(f"Sending command: {command}")
    print(f"Target: {base_url}")
    print("-" * 50)
    
    success = send_command(command, base_url)
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main() 