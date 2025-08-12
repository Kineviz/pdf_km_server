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
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import get_ollama_cluster

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_sequential_processing():
    """Test sequential processing (current implementation)."""
    logger.info("🔄 Testing SEQUENTIAL processing...")
    
    cluster = get_ollama_cluster()
    cluster.health_check_all()
    
    test_prompts = [
        "Extract observations from this text about Bruce Lee: Bruce Lee was a martial artist and actor who founded Jeet Kune Do. He was born in San Francisco in 1940 and became famous for his roles in films like Enter the Dragon.",
        "Extract observations from this text about technology: Apple Inc. was founded by Steve Jobs and Steve Wozniak in 1976. The company is headquartered in Cupertino, California and is known for products like the iPhone, iPad, and Mac computers.",
        "Extract observations from this text about science: Albert Einstein developed the theory of relativity while working at the Swiss Patent Office. He was born in Germany in 1879 and later became a professor at Princeton University.",
        "Extract observations from this text about business: Microsoft Corporation was founded by Bill Gates and Paul Allen in 1975. The company is based in Redmond, Washington and is a leader in software development and cloud computing services.",
        "Extract observations from this text about sports: Michael Jordan played basketball for the Chicago Bulls and won six NBA championships. He was born in Brooklyn, New York and is considered one of the greatest basketball players of all time."
    ]
    
    start_time = time.time()
    results = []
    
    for i, prompt in enumerate(test_prompts):
        logger.info(f"Processing prompt {i+1}/{len(test_prompts)}")
        prompt_start = time.time()
        
        response = cluster.send_request_with_retry(prompt, "gemma3")
        
        prompt_end = time.time()
        prompt_time = prompt_end - prompt_start
        
        results.append({
            "prompt_index": i,
            "response_length": len(response) if response else 0,
            "processing_time": prompt_time,
            "success": response is not None
        })
        
        logger.info(f"Prompt {i+1} completed in {prompt_time:.2f}s")
    
    total_time = time.time() - start_time
    logger.info(f"✅ Sequential processing completed in {total_time:.2f}s")
    
    return results, total_time

def test_parallel_processing():
    """Test parallel processing with ThreadPoolExecutor."""
    logger.info("⚡ Testing PARALLEL processing...")
    
    cluster = get_ollama_cluster()
    cluster.health_check_all()
    
    test_prompts = [
        "Extract observations from this text about Bruce Lee: Bruce Lee was a martial artist and actor who founded Jeet Kune Do. He was born in San Francisco in 1940 and became famous for his roles in films like Enter the Dragon.",
        "Extract observations from this text about technology: Apple Inc. was founded by Steve Jobs and Steve Wozniak in 1976. The company is headquartered in Cupertino, California and is known for products like the iPhone, iPad, and Mac computers.",
        "Extract observations from this text about science: Albert Einstein developed the theory of relativity while working at the Swiss Patent Office. He was born in Germany in 1879 and later became a professor at Princeton University.",
        "Extract observations from this text about business: Microsoft Corporation was founded by Bill Gates and Paul Allen in 1975. The company is based in Redmond, Washington and is a leader in software development and cloud computing services.",
        "Extract observations from this text about sports: Michael Jordan played basketball for the Chicago Bulls and won six NBA championships. He was born in Brooklyn, New York and is considered one of the greatest basketball players of all time."
    ]
    
    def process_prompt(prompt_data):
        prompt_index, prompt = prompt_data
        logger.info(f"Starting parallel processing of prompt {prompt_index+1}")
        prompt_start = time.time()
        
        response = cluster.send_request_with_retry(prompt, "gemma3")
        
        prompt_end = time.time()
        prompt_time = prompt_end - prompt_start
        
        logger.info(f"Parallel prompt {prompt_index+1} completed in {prompt_time:.2f}s")
        
        return {
            "prompt_index": prompt_index,
            "response_length": len(response) if response else 0,
            "processing_time": prompt_time,
            "success": response is not None
        }
    
    start_time = time.time()
    results = []
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=len(cluster.servers)) as executor:
        # Submit all tasks
        future_to_prompt = {
            executor.submit(process_prompt, (i, prompt)): i 
            for i, prompt in enumerate(test_prompts)
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_prompt):
            result = future.result()
            results.append(result)
    
    total_time = time.time() - start_time
    logger.info(f"✅ Parallel processing completed in {total_time:.2f}s")
    
    return results, total_time

def compare_processing_methods():
    """Compare sequential vs parallel processing."""
    logger.info("🚀 Starting processing method comparison...")
    
    # Test sequential processing
    sequential_results, sequential_time = test_sequential_processing()
    
    # Wait a bit between tests
    time.sleep(2)
    
    # Test parallel processing
    parallel_results, parallel_time = test_parallel_processing()
    
    # Calculate statistics
    sequential_avg = sum(r['processing_time'] for r in sequential_results) / len(sequential_results)
    parallel_avg = sum(r['processing_time'] for r in parallel_results) / len(parallel_results)
    
    speedup = sequential_time / parallel_time if parallel_time > 0 else 0
    
    print("\n" + "="*80)
    print("📊 PROCESSING METHOD COMPARISON")
    print("="*80)
    
    print(f"\n🔄 Sequential Processing:")
    print(f"   Total Time: {sequential_time:.2f}s")
    print(f"   Average Time per Prompt: {sequential_avg:.2f}s")
    print(f"   Success Rate: {sum(1 for r in sequential_results if r['success'])}/{len(sequential_results)}")
    
    print(f"\n⚡ Parallel Processing:")
    print(f"   Total Time: {parallel_time:.2f}s")
    print(f"   Average Time per Prompt: {parallel_avg:.2f}s")
    print(f"   Success Rate: {sum(1 for r in parallel_results if r['success'])}/{len(parallel_results)}")
    
    print(f"\n🏆 Performance Comparison:")
    print(f"   Speedup: {speedup:.2f}x faster")
    print(f"   Time Saved: {sequential_time - parallel_time:.2f}s")
    print(f"   Efficiency Improvement: {((sequential_time - parallel_time) / sequential_time * 100):.1f}%")
    
    if speedup > 1:
        print(f"   ✅ Parallel processing is {speedup:.2f}x faster!")
    else:
        print(f"   ⚠️ Sequential processing was faster (possible network overhead)")
    
    print("="*80)

if __name__ == "__main__":
    compare_processing_methods() 