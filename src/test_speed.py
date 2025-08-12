#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "requests>=2.32.4",
#     "ping3>=4.0.4",
#     "tqdm>=4.66.0"
# ]
# ///

import time
import json
import logging
from typing import Dict, List
from config import get_ollama_cluster

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_server_performance(server_name: str, test_prompts: List[str]) -> Dict:
    """Test performance of a specific server with multiple prompts."""
    cluster = get_ollama_cluster()
    
    # Find the specific server
    target_server = None
    for server in cluster.servers:
        if server.name == server_name:
            target_server = server
            break
    
    if not target_server:
        logger.error(f"Server '{server_name}' not found in configuration")
        return None
    
    logger.info(f"Testing server: {server_name} ({target_server.url})")
    
    # Health check the server first
    is_healthy = cluster.health_check_server(target_server)
    if not is_healthy:
        logger.error(f"Server {server_name} is not healthy")
        return None
    
    results = {
        "server_name": server_name,
        "url": target_server.url,
        "model": target_server.model,
        "is_healthy": is_healthy,
        "response_time": target_server.response_time,
        "requests": []
    }
    
    # Test with each prompt
    for i, prompt in enumerate(test_prompts):
        logger.info(f"Testing prompt {i+1}/{len(test_prompts)} on {server_name}")
        
        start_time = time.time()
        try:
            # Temporarily set this as the only active server for testing
            original_servers = cluster.servers.copy()
            cluster.servers = [target_server]
            cluster.current_server_index = 0
            
            response = cluster.send_request_with_retry(prompt, target_server.model, max_retries=1)
            
            end_time = time.time()
            request_time = end_time - start_time
            
            if response:
                results["requests"].append({
                    "prompt_index": i,
                    "prompt_length": len(prompt),
                    "response_length": len(response),
                    "request_time": request_time,
                    "success": True,
                    "response_preview": response[:100] + "..." if len(response) > 100 else response
                })
                logger.info(f"✅ Request {i+1} successful in {request_time:.2f}s")
            else:
                results["requests"].append({
                    "prompt_index": i,
                    "prompt_length": len(prompt),
                    "request_time": request_time,
                    "success": False,
                    "error": "No response received"
                })
                logger.error(f"❌ Request {i+1} failed")
                
        except Exception as e:
            end_time = time.time()
            request_time = end_time - start_time
            results["requests"].append({
                "prompt_index": i,
                "prompt_length": len(prompt),
                "request_time": request_time,
                "success": False,
                "error": str(e)
            })
            logger.error(f"❌ Request {i+1} failed with error: {e}")
        
        finally:
            # Restore original server list
            cluster.servers = original_servers
    
    return results

def calculate_performance_metrics(results: Dict) -> Dict:
    """Calculate performance metrics from test results."""
    if not results or not results["requests"]:
        return None
    
    successful_requests = [r for r in results["requests"] if r["success"]]
    failed_requests = [r for r in results["requests"] if not r["success"]]
    
    if not successful_requests:
        return {
            "server_name": results["server_name"],
            "success_rate": 0.0,
            "avg_response_time": None,
            "min_response_time": None,
            "max_response_time": None,
            "total_requests": len(results["requests"]),
            "successful_requests": 0,
            "failed_requests": len(failed_requests)
        }
    
    response_times = [r["request_time"] for r in successful_requests]
    
    return {
        "server_name": results["server_name"],
        "success_rate": len(successful_requests) / len(results["requests"]),
        "avg_response_time": sum(response_times) / len(response_times),
        "min_response_time": min(response_times),
        "max_response_time": max(response_times),
        "total_requests": len(results["requests"]),
        "successful_requests": len(successful_requests),
        "failed_requests": len(failed_requests),
        "health_check_response_time": results.get("response_time")
    }

def run_speed_comparison():
    """Run speed comparison between M4 MBP and Mac Mini."""
    
    # Test prompts that simulate our PDF processing workload
    test_prompts = [
        "Extract observations from this text about Bruce Lee: Bruce Lee was a martial artist and actor who founded Jeet Kune Do. He was born in San Francisco in 1940 and became famous for his roles in films like Enter the Dragon.",
        
        "Extract observations from this text about technology: Apple Inc. was founded by Steve Jobs and Steve Wozniak in 1976. The company is headquartered in Cupertino, California and is known for products like the iPhone, iPad, and Mac computers.",
        
        "Extract observations from this text about science: Albert Einstein developed the theory of relativity while working at the Swiss Patent Office. He was born in Germany in 1879 and later became a professor at Princeton University.",
        
        "Extract observations from this text about business: Microsoft Corporation was founded by Bill Gates and Paul Allen in 1975. The company is based in Redmond, Washington and is a leader in software development and cloud computing services.",
        
        "Extract observations from this text about sports: Michael Jordan played basketball for the Chicago Bulls and won six NBA championships. He was born in Brooklyn, New York and is considered one of the greatest basketball players of all time."
    ]
    
    # Load servers from configuration file
    try:
        with open('ollama_servers.json', 'r') as f:
            config = json.load(f)
            servers_to_test = [server['name'] for server in config['servers']]
            logger.info(f"Loaded {len(servers_to_test)} servers from ollama_servers.json")
    except FileNotFoundError:
        logger.error("ollama_servers.json not found, using default servers")
        servers_to_test = ["local"]
    except Exception as e:
        logger.error(f"Error loading ollama_servers.json: {e}")
        servers_to_test = ["local"]
    
    logger.info("🚀 Starting speed comparison test...")
    logger.info(f"Testing servers: {', '.join(servers_to_test)}")
    logger.info(f"Number of test prompts: {len(test_prompts)}")
    
    all_results = {}
    all_metrics = {}
    
    for server_name in servers_to_test:
        logger.info(f"\n{'='*50}")
        logger.info(f"Testing server: {server_name}")
        logger.info(f"{'='*50}")
        
        results = test_server_performance(server_name, test_prompts)
        if results:
            all_results[server_name] = results
            metrics = calculate_performance_metrics(results)
            all_metrics[server_name] = metrics
            
            logger.info(f"✅ Completed testing for {server_name}")
        else:
            logger.error(f"❌ Failed to test {server_name}")
    
    # Print comparison results
    print("\n" + "="*80)
    print("🏁 SPEED COMPARISON RESULTS")
    print("="*80)
    
    for server_name, metrics in all_metrics.items():
        if metrics:
            print(f"\n📊 {server_name}:")
            print(f"   Success Rate: {metrics['success_rate']:.1%}")
            print(f"   Avg Response Time: {metrics['avg_response_time']:.2f}s" if metrics['avg_response_time'] else "   Avg Response Time: N/A")
            print(f"   Min Response Time: {metrics['min_response_time']:.2f}s" if metrics['min_response_time'] else "   Min Response Time: N/A")
            print(f"   Max Response Time: {metrics['max_response_time']:.2f}s" if metrics['max_response_time'] else "   Max Response Time: N/A")
            print(f"   Health Check Response: {metrics['health_check_response_time']:.2f}s" if metrics['health_check_response_time'] else "   Health Check Response: N/A")
            print(f"   Successful Requests: {metrics['successful_requests']}/{metrics['total_requests']}")
        else:
            print(f"\n❌ {server_name}: No metrics available")
    
    # Determine winner
    if len(all_metrics) >= 2:
        valid_metrics = {k: v for k, v in all_metrics.items() if v and v['avg_response_time']}
        if len(valid_metrics) >= 2:
            fastest_server = min(valid_metrics.items(), key=lambda x: x[1]['avg_response_time'])
            print(f"\n🏆 WINNER: {fastest_server[0]} (avg: {fastest_server[1]['avg_response_time']:.2f}s)")
            
            # Calculate speed difference
            sorted_servers = sorted(valid_metrics.items(), key=lambda x: x[1]['avg_response_time'])
            if len(sorted_servers) >= 2:
                fastest_time = sorted_servers[0][1]['avg_response_time']
                slowest_time = sorted_servers[-1][1]['avg_response_time']
                speed_improvement = ((slowest_time - fastest_time) / slowest_time) * 100
                print(f"⚡ Speed improvement: {speed_improvement:.1f}% faster")
    
    # Save detailed results to file
    output_file = "speed_test_results.json"
    with open(output_file, 'w') as f:
        json.dump({
            "test_timestamp": time.time(),
            "results": all_results,
            "metrics": all_metrics
        }, f, indent=2)
    
    print(f"\n📄 Detailed results saved to: {output_file}")
    print("="*80)

if __name__ == "__main__":
    run_speed_comparison() 