#!/usr/bin/env python3
from ib_insync import IB
import sys

print("Testing IB connection from container...")
print("Host: host.docker.internal")
print("Port: 7497")
print("Client ID: 6969")

ib = IB()
try:
    print("Attempting connection...")
    ib.connect('host.docker.internal', 7497, 6969, timeout=10)
    
    if ib.isConnected():
        print("✅ Connected successfully!")
        print(f"Connection status: {ib.isConnected()}")
        
        # Try to get server time
        try:
            server_time = ib.reqCurrentTime()
            print(f"✅ Server time: {server_time}")
        except Exception as e:
            print(f"❌ Failed to get server time: {e}")
        
        ib.disconnect()
        print("✅ Connection test PASSED")
    else:
        print("❌ Connection failed - not connected")
        sys.exit(1)
        
except Exception as e:
    print(f"❌ Connection error: {e}")
    sys.exit(1) 