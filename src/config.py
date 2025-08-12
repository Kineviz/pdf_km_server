#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "requests>=2.32.4",
#     "ping3>=4.0.4",
# ]
# ///

import json
import requests
import time
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass
from ping3 import ping

logger = logging.getLogger(__name__)

@dataclass
class OllamaServer:
    """Represents an Ollama server configuration."""
    name: str
    url: str
    model: str = "gemma3"
    timeout: int = 30
    max_retries: int = 3
    is_active: bool = True
    last_check: Optional[float] = None
    response_time: Optional[float] = None
    error_count: int = 0
    max_errors: int = 5

class OllamaCluster:
    """Manages multiple Ollama servers with health checks and load balancing."""
    
    def __init__(self, config_file: str = "ollama_servers.json"):
        self.config_file = config_file
        self.servers: List[OllamaServer] = []
        self.current_server_index = 0
        self.last_health_check = 0
        self.health_check_interval = 30  # Check every 30 seconds
        self.load_config()
    
    def load_config(self):
        """Load server configuration from JSON file."""
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)
            
            self.servers = []
            for server_config in config.get('servers', []):
                server = OllamaServer(
                    name=server_config['name'],
                    url=server_config['url'],
                    model=server_config.get('model', 'gemma3'),
                    timeout=server_config.get('timeout', 30),
                    max_retries=server_config.get('max_retries', 3)
                )
                self.servers.append(server)
            
            logger.info(f"Loaded {len(self.servers)} Ollama servers from config")
            
        except FileNotFoundError:
            logger.warning(f"Config file {self.config_file} not found. Creating default config.")
            self.create_default_config()
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            self.create_default_config()
    
    def create_default_config(self):
        """Create a default configuration file."""
        default_config = {
            "servers": [
                {
                    "name": "local",
                    "url": "http://localhost:11434",
                    "model": "gemma3",
                    "timeout": 30,
                    "max_retries": 3
                }
            ]
        }
        
        with open(self.config_file, 'w') as f:
            json.dump(default_config, f, indent=2)
        
        self.servers = [OllamaServer(**default_config['servers'][0])]
        logger.info(f"Created default config with local server")
    
    def health_check_server(self, server: OllamaServer) -> bool:
        """Check if a server is healthy and responsive."""
        try:
            # First try ping
            ping_result = ping(server.url.replace('http://', '').replace('https://', '').split(':')[0])
            if ping_result is None:
                server.is_active = False
                server.error_count += 1
                logger.warning(f"Server {server.name} is not reachable via ping")
                return False
            
            # Then try HTTP health check
            health_url = f"{server.url}/api/tags"
            start_time = time.time()
            response = requests.get(health_url, timeout=5)
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                # If server was previously inactive, log that it's back online
                was_inactive = not server.is_active
                server.is_active = True
                server.response_time = response_time
                server.error_count = 0
                server.last_check = time.time()
                
                if was_inactive:
                    logger.info(f"🟢 Server {server.name} is back online! (response time: {response_time:.2f}s)")
                else:
                    logger.info(f"Server {server.name} is healthy (response time: {response_time:.2f}s)")
                return True
            else:
                server.is_active = False
                server.error_count += 1
                logger.warning(f"Server {server.name} returned status {response.status_code}")
                return False
                
        except Exception as e:
            server.is_active = False
            server.error_count += 1
            logger.warning(f"Health check failed for {server.name}: {e}")
            return False
    
    def health_check_all(self):
        """Check health of all servers."""
        logger.info("Starting health check of all servers...")
        for server in self.servers:
            self.health_check_server(server)
        
        active_servers = [s for s in self.servers if s.is_active]
        logger.info(f"Health check complete. {len(active_servers)}/{len(self.servers)} servers active")
        self.last_health_check = time.time()
    
    def health_check_inactive_servers(self):
        """Check only inactive servers to see if they're back online."""
        inactive_servers = [s for s in self.servers if not s.is_active]
        if not inactive_servers:
            return
        
        logger.info(f"Checking {len(inactive_servers)} inactive servers for reconnection...")
        reactivated_count = 0
        
        for server in inactive_servers:
            if self.health_check_server(server):
                reactivated_count += 1
        
        if reactivated_count > 0:
            logger.info(f"🟢 Reactivated {reactivated_count} servers!")
        else:
            logger.debug("No servers reactivated")
    
    def periodic_health_check(self):
        """Perform health check if enough time has passed since last check."""
        current_time = time.time()
        if current_time - self.last_health_check > self.health_check_interval:
            self.health_check_inactive_servers()
    
    def get_next_available_server(self) -> Optional[OllamaServer]:
        """Get the next available server using round-robin load balancing."""
        # Perform periodic health check before selecting server
        self.periodic_health_check()
        
        active_servers = [s for s in self.servers if s.is_active]
        
        if not active_servers:
            logger.error("No active servers available")
            return None
        
        # Use round-robin selection
        server = active_servers[self.current_server_index % len(active_servers)]
        self.current_server_index += 1
        
        return server
    
    def send_request_with_retry(self, prompt: str, model: str = "gemma3", max_retries: int = 3) -> Optional[str]:
        """Send a request to Ollama with retry logic and server failover."""
        for attempt in range(max_retries):
            server = self.get_next_available_server()
            if not server:
                logger.error("No available servers for request")
                return None
            
            try:
                logger.info(f"Sending request to {server.name} (attempt {attempt + 1})")
                
                # Prepare the request using chat format with JSON structure
                request_data = {
                    "model": model,
                    "messages": [
                        {
                            "role": "system", 
                            "content": "Extract observations from the text. An observation is a natural language statement that contains one or more entities and describes relationships or facts about them. For each observation, identify the most important entities mentioned in it and provide a single word that best describes the key relationship or fact. Try to limit to 2 entities per observation, but you may include more if multiple people's names are listed together or if the observation requires more entities to be meaningful. Use these standardized categories: Person, Organization, Object, Location, Event, Date, Concept, Trait, Role, Animal, Technology, Product. The label should be the actual name of the entity (e.g., 'Bruce Lee' for a person, 'IBM' for an organization, 'New York' for a location)."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "stream": False,
                    "temperature": 0,
                    "top_p": 1.0,
                    "top_k": 1,
                    "repeat_penalty": 1.0,
                    "seed": 42,
                    "format": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "observation": {
                                    "type": "string",
                                    "description": "A natural language statement that describes relationships or facts about entities"
                                },
                                "relationship": {
                                    "type": "string",
                                    "description": "A single word that best describes the key relationship or fact (e.g., 'lives', 'born', 'helps', 'protects', 'loves')"
                                },
                                "entities": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "label": {
                                                "type": "string",
                                                "description": "The actual name of the entity (e.g., 'Bruce Lee', 'IBM', 'New York')"
                                            },
                                            "category": {
                                                "type": "string",
                                                "description": "One of: Person, Organization, Object, Location, Event, Date, Concept, Trait, Role, Animal, Technology, Product"
                                            }
                                        },
                                        "required": ["label", "category"]
                                    },
                                    "description": "List of entities mentioned in the observation"
                                }
                            },
                            "required": ["observation", "relationship", "entities"]
                        }
                    }
                }
                
                start_time = time.time()
                response = requests.post(
                    f"{server.url}/api/chat",
                    json=request_data,
                    timeout=server.timeout
                )
                response_time = time.time() - start_time
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"Request successful on {server.name} (response time: {response_time:.2f}s)")
                    return result.get('message', {}).get('content', '')
                else:
                    logger.warning(f"Request failed on {server.name} with status {response.status_code}")
                    server.error_count += 1
                    if server.error_count >= server.max_errors:
                        server.is_active = False
                        logger.warning(f"Server {server.name} marked as inactive due to too many errors")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"Request timeout on {server.name}")
                server.error_count += 1
                if server.error_count >= server.max_errors:
                    server.is_active = False
                    
            except Exception as e:
                logger.warning(f"Request error on {server.name}: {e}")
                server.error_count += 1
                if server.error_count >= server.max_errors:
                    server.is_active = False
        
        logger.error(f"All retry attempts failed for request")
        return None
    
    def get_server_status(self) -> Dict:
        """Get status of all servers."""
        # Perform a quick health check of inactive servers before reporting status
        self.health_check_inactive_servers()
        
        status = {
            "total_servers": len(self.servers),
            "active_servers": len([s for s in self.servers if s.is_active]),
            "last_health_check": self.last_health_check,
            "health_check_interval": self.health_check_interval,
            "servers": []
        }
        
        for server in self.servers:
            server_info = {
                "name": server.name,
                "url": server.url,
                "model": server.model,
                "is_active": server.is_active,
                "error_count": server.error_count,
                "response_time": server.response_time,
                "last_check": server.last_check,
                "max_errors": server.max_errors
            }
            status["servers"].append(server_info)
        
        return status
    
    def force_reconnect_check(self):
        """Manually trigger a reconnection check of all inactive servers."""
        logger.info("🔄 Manual reconnection check triggered")
        self.health_check_inactive_servers()
        return self.get_server_status()

# Global cluster instance
ollama_cluster = OllamaCluster()

def get_ollama_cluster() -> OllamaCluster:
    """Get the global Ollama cluster instance."""
    return ollama_cluster

if __name__ == "__main__":
    # Test the cluster
    cluster = OllamaCluster()
    cluster.health_check_all()
    
    print("\nServer Status:")
    status = cluster.get_server_status()
    print(json.dumps(status, indent=2))
    
    # Test a simple request
    print("\nTesting request...")
    result = cluster.send_request_with_retry("Hello, how are you?")
    if result:
        print(f"Response: {result[:100]}...")
    else:
        print("No response received") 