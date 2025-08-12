#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "requests>=2.32.4",
#     "ping3>=4.0.4",
# ]
# ///

import time
import logging
from config import get_ollama_cluster

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_reconnection():
    """Test the automatic reconnection functionality."""
    cluster = get_ollama_cluster()
    
    print("🔄 Testing Automatic Reconnection System")
    print("=" * 50)
    
    # Initial health check
    print("\n1️⃣ Initial Health Check:")
    cluster.health_check_all()
    status = cluster.get_server_status()
    print(f"Active servers: {status['active_servers']}/{status['total_servers']}")
    
    for server in status['servers']:
        status_icon = "🟢" if server['is_active'] else "🔴"
        print(f"  {status_icon} {server['name']} - {'Active' if server['is_active'] else 'Inactive'}")
    
    # Simulate server going down (by marking it inactive)
    if cluster.servers:
        test_server = cluster.servers[0]
        print(f"\n2️⃣ Simulating server {test_server.name} going offline...")
        test_server.is_active = False
        test_server.error_count = 5
        print(f"  🔴 {test_server.name} marked as inactive")
    
    # Show status after "failure"
    print("\n3️⃣ Status after simulated failure:")
    status = cluster.get_server_status()
    print(f"Active servers: {status['active_servers']}/{status['total_servers']}")
    
    # Test automatic reconnection
    print("\n4️⃣ Testing automatic reconnection...")
    print("   (This will check if the server is back online)")
    
    # Force a reconnection check
    cluster.force_reconnect_check()
    
    # Show final status
    print("\n5️⃣ Final status after reconnection attempt:")
    status = cluster.get_server_status()
    print(f"Active servers: {status['active_servers']}/{status['total_servers']}")
    
    for server in status['servers']:
        status_icon = "🟢" if server['is_active'] else "🔴"
        response_time = f"{server['response_time']:.0f}ms" if server['response_time'] else "N/A"
        print(f"  {status_icon} {server['name']} - {response_time}")
    
    print("\n✅ Reconnection test complete!")
    print("\n💡 Key Features:")
    print("   • Automatic health checks every 30 seconds")
    print("   • Manual reconnection via 'Force Reconnect' button")
    print("   • Server reactivation when they come back online")
    print("   • Error tracking and automatic failover")

if __name__ == "__main__":
    test_reconnection() 