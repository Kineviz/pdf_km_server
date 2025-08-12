#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "requests>=2.32.4",
# ]
# ///

import requests
import time

def test_server_health():
    """Test if the server is running and responding."""
    try:
        response = requests.get("http://localhost:7860", timeout=5)
        if response.status_code == 200:
            print("✅ Server is running and responding!")
            return True
        else:
            print(f"❌ Server responded with status code: {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ Could not connect to server. Is it running on port 7860?")
        return False
    except Exception as e:
        print(f"❌ Error testing server: {e}")
        return False

def test_server_info():
    """Get server information."""
    try:
        response = requests.get("http://localhost:7860", timeout=5)
        if response.status_code == 200:
            print("📊 Server Information:")
            print(f"   - Status: Running")
            print(f"   - URL: http://localhost:7860")
            print(f"   - Response size: {len(response.content)} bytes")
            return True
    except Exception as e:
        print(f"❌ Error getting server info: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Testing PDF to Knowledge Map Web Service")
    print("=" * 50)
    
    # Test server health
    if test_server_health():
        test_server_info()
        print("\n🎉 Server is ready for use!")
        print("\n📝 Next steps:")
        print("1. Open your browser to http://localhost:7860")
        print("2. Upload a PDF file")
        print("3. Choose an AI model (Gemma3 recommended)")
        print("4. Start processing")
        print("5. Monitor job status and download results")
    else:
        print("\n❌ Server is not ready. Please check:")
        print("1. Is Ollama running? (ollama run gemma3)")
        print("2. Is the server running? (./server.py)")
        print("3. Is port 7860 available?") 