#!/usr/bin/env python3
"""
Step 3: Observation Extraction
Extracts observations and entities from text chunks using AI models.
"""

import json
import logging
import requests
from typing import List, Dict, Any
from tqdm import tqdm
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add the parent directory to the path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import get_ollama_cluster


def extract_observations_from_chunks(chunks: List[str], model: str, ollama_url: str = None, progress_callback=None) -> List[Dict[str, Any]]:
    """Extract observations from all chunks with parallel processing using Ollama cluster."""
    all_observations = []
    cluster = get_ollama_cluster()
    
    # Perform health check before starting
    cluster.health_check_all()
    
    def process_chunk(chunk_data):
        """Process a single chunk with metadata."""
        chunk_index, chunk = chunk_data
        try:
            logging.info(f"Processing chunk {chunk_index+1}/{len(chunks)}. Model: {model}")
            chunk_observations = extract_observations_with_cluster(chunk, model, cluster)
            
            # Add chunk metadata to each observation
            for obs in chunk_observations:
                obs['chunk_index'] = chunk_index
                
                # Try to find the observation text within the chunk for position tracking
                observation_text = obs['observation']
                chunk_text = chunk
                
                # Find the position of the observation within the chunk
                start_pos = chunk_text.find(observation_text)
                if start_pos != -1:
                    obs['chunk_start_pos'] = start_pos
                    obs['chunk_end_pos'] = start_pos + len(observation_text)
                else:
                    # If exact match not found, use approximate positions
                    obs['chunk_start_pos'] = 0
                    obs['chunk_end_pos'] = len(chunk)
                    obs['position_approximate'] = True
            
            logging.info(f"Chunk {chunk_index+1}: extracted {len(chunk_observations)} observations")
            return chunk_observations
            
        except Exception as e:
            logging.error(f"Error processing chunk {chunk_index+1}: {e}")
            return []
    
    # Use ThreadPoolExecutor for parallel processing
    # Use number of servers as max_workers for optimal distribution
    max_workers = min(len(cluster.servers), len(chunks))
    logging.info(f"Starting parallel processing with {max_workers} workers for {len(chunks)} chunks")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all chunk processing tasks
        future_to_chunk = {
            executor.submit(process_chunk, (i, chunk)): i 
            for i, chunk in enumerate(chunks)
        }
        
        # Collect results as they complete with progress tracking
        completed_chunks = 0
        for future in as_completed(future_to_chunk):
            chunk_observations = future.result()
            all_observations.extend(chunk_observations)
            completed_chunks += 1
            
            # Update progress if callback is provided
            if progress_callback:
                chunk_progress = (completed_chunks / len(chunks)) * 100
                progress_callback(chunk_progress, f"Processed {completed_chunks}/{len(chunks)} chunks")
    
    logging.info(f"Parallel processing completed. Total observations: {len(all_observations)}")
    return all_observations


def extract_observations_with_cluster(chunk: str, model: str, cluster):
    """Extract observations from a single chunk using Ollama cluster with failover."""
    
    # Prepare the prompt
    system_prompt = "Extract observations from the text. An observation is a natural language statement that contains one or more entities and describes relationships or facts about them. For each observation, identify the most important entities mentioned in it and provide a single word that best describes the key relationship or fact. Try to limit to 2 entities per observation, but you may include more if multiple people's names are listed together or if the observation requires more entities to be meaningful. Use these standardized categories: Person, Organization, Object, Location, Event, Date, Concept, Trait, Role, Animal, Technology, Product. The label should be the actual name of the entity (e.g., 'Bruce Lee' for a person, 'IBM' for an organization, 'New York' for a location)."
    
    user_prompt = f"Extract observations from this text:\n\n{chunk}"
    
    # Use the cluster to send the request with retry and failover
    response = cluster.send_request_with_retry(user_prompt, model)
    
    if response is None:
        logging.error("Failed to get response from any server in the cluster")
        return []
    
    try:
        # Parse the JSON response
        logging.info(f"Raw response length: {len(response)}")
        logging.debug(f"Raw response: {response[:500]}...")
        observations = json.loads(response)
        logging.info(f"Successfully parsed {len(observations)} observations")
        return observations
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON response: {e}")
        logging.error(f"Raw response: {response}")
        return []


def extract_observations(chunk: str, model: str, ollama_url: str):
    """Extract observations from a single chunk using AI (legacy function for backward compatibility).
    
    Uses deterministic parameters for consistent results across multiple runs:
    - temperature=0: Always choose most likely token
    - top_p=1.0: No nucleus sampling
    - top_k=1: Only consider top token
    - repeat_penalty=1.0: No repetition penalty
    - seed=42: Fixed random seed
    """
    response = requests.post(
        f"{ollama_url}/api/chat",
        json={
            "model": model,
            "messages": [
                {
                    "role": "system", 
                    "content": "Extract observations from the text. An observation is a natural language statement that contains one or more entities and describes relationships or facts about them. For each observation, identify the most important entities mentioned in it and provide a single word that best describes the key relationship or fact. Try to limit to 2 entities per observation, but you may include more if multiple people's names are listed together or if the observation requires more entities to be meaningful. Use these standardized categories: Person, Organization, Object, Location, Event, Date, Concept, Trait, Role, Animal, Technology, Product. The label should be the actual name of the entity (e.g., 'Bruce Lee' for a person, 'IBM' for an organization, 'New York' for a location)."
                },
                {
                    "role": "user",
                    "content": chunk
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
        },
    )
    return json.loads(response.json()['message']['content']) 