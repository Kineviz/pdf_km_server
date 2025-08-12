#!/usr/bin/env python3
"""
Step 4: Kuzu Database Integration
Loads observations and entities into Kuzu graph database.
"""

import logging
import kuzu
import os
from typing import List, Dict, Any


def load_observations_to_kuzu(observations: List[Dict[str, Any]], kuzu_path: str, text_content: str = None, chunks_with_metadata: List[Dict[str, Any]] = None, input_file: str = None):
    """Load observations and entities into Kuzu database."""
    logger = logging.getLogger(__name__)
    logger.info("Loading observations and entities into Kuzu database")
    
    # Create or connect to Kuzu database
    db = kuzu.Database(kuzu_path)
    conn = kuzu.Connection(db)
    
    # Create schema if it doesn't exist
    create_kuzu_schema(conn)
    
    # Load PDF and chunks if provided
    if text_content and chunks_with_metadata and input_file:
        load_pdf_and_chunks_to_kuzu(conn, input_file, text_content, chunks_with_metadata)
    
    # Extract all unique entities from observations
    entities = extract_unique_entities(observations)
    logger.info(f"Found {len(entities)} unique entities")
    
    # Load entities into database
    load_entities_to_kuzu(conn, entities)
    
    # Load observations into database
    load_observations_to_kuzu_db(conn, observations, input_file)
    
    logger.info("Successfully loaded observations and entities into Kuzu database")


def create_kuzu_schema(conn):
    """Create the Kuzu database schema."""
    try:
        # Create PDF node table
        conn.execute("""
            CREATE NODE TABLE IF NOT EXISTS PDF(
                path STRING PRIMARY KEY,
                filename STRING,
                text STRING
            )
        """)
    except Exception as e:
        # Table might already exist, continue
        pass
    
    try:
        # Create Chunk node table
        # id is pdf filename + chunk index
        conn.execute("""
            CREATE NODE TABLE IF NOT EXISTS Chunk(
                id STRING PRIMARY KEY,
                text STRING,
                index INT64,
                start_pos INT64,
                end_pos INT64,
                pdf_path STRING
            )
        """)
    except Exception as e:
        # Table might already exist, continue
        pass
    
    try:
        # Create Observation node table
        # id is current injection timestamp + index
        conn.execute("""
            CREATE NODE TABLE IF NOT EXISTS Observation(
                id STRING PRIMARY KEY,
                text STRING,
                relationship STRING,
                chunk_index INT64,
                chunk_start_pos INT64,
                chunk_end_pos INT64,
                pdf_path STRING
            )
        """)
    except Exception as e:
        # Table might already exist, continue
        pass
    
    try:
        # Create Entity node table
        conn.execute("""
            CREATE NODE TABLE IF NOT EXISTS Entity(
                id STRING PRIMARY KEY,
                label STRING,
                category STRING
            )
        """)
    except Exception as e:
        # Table might already exist, continue
        pass
    
    try:
        # Create HAS_CHUNK relationship table
        conn.execute("""
            CREATE REL TABLE IF NOT EXISTS HAS_CHUNK(
                FROM PDF TO Chunk
            )
        """)
    except Exception as e:
        # Table might already exist, continue
        pass
    
    try:
        # Create REFERENCE_CHUNK relationship table
        conn.execute("""
            CREATE REL TABLE IF NOT EXISTS REFERENCE_CHUNK(
                FROM Observation TO Chunk
            )
        """)
    except Exception as e:
        # Table might already exist, continue
        pass
    
    try:
        # Create MENTION relationship table
        conn.execute("""
            CREATE REL TABLE IF NOT EXISTS MENTION(
                FROM Observation TO Entity
            )
        """)
    except Exception as e:
        # Table might already exist, continue
        pass


def extract_unique_entities(observations: List[Dict[str, Any]]) -> Dict[str, str]:
    """Extract unique entities from observations."""
    entities = {}
    for obs in observations:
        for entity in obs['entities']:
            label = entity['label']
            category = entity['category']
            entities[label] = category
    return entities


def load_entities_to_kuzu(conn, entities: Dict[str, str]):
    """Load entities into Kuzu database."""
    logger = logging.getLogger(__name__)
    logger.info(f"Loading {len(entities)} entities into database")
    
    for label, category in entities.items():
        try:
            entity_id = f"{category}_{label}".lower()
            conn.execute(
                "MERGE (e:Entity {id: $id, label: $label, category: $category})",
                {
                    "id": entity_id,
                    "label": label,
                    "category": category
                }
            )
        except Exception as e:
            logger.warning(f"Failed to merge entity '{label}': {e}")


def load_pdf_and_chunks_to_kuzu(conn, input_file: str, text_content: str, chunks_with_metadata: List[Dict[str, Any]]):
    """Load PDF and chunks into Kuzu database."""
    logger = logging.getLogger(__name__)
    logger.info(f"Loading PDF and {len(chunks_with_metadata)} chunks into database")
    
    try:
        # Extract filename from path using helper function
        from steps.step1_text_extraction import get_pdf_filename
        pdf_filename = get_pdf_filename(input_file)
        
        # Create PDF node with separate path and filename
        conn.execute(
            "MERGE (p:PDF {path: $path, filename: $filename, text: $text})",
            {
                "path": input_file,
                "filename": pdf_filename,
                "text": text_content
            }
        )
        
        # Create chunk nodes and HAS_CHUNK relationships
        pdf_name = os.path.basename(input_file).replace('.pdf', '')
        for chunk in chunks_with_metadata:
            chunk_id = f"{pdf_name}_chunk_{chunk['index']}"
            conn.execute(
                """
                MERGE (c:Chunk {
                    id: $id,
                    text: $text,
                    index: $index,
                    start_pos: $start_pos,
                    end_pos: $end_pos,
                    pdf_path: $pdf_path
                })
                """,
                {
                    "id": chunk_id,
                    "text": chunk['text'],
                    "index": chunk['index'],
                    "start_pos": chunk['start_pos'],
                    "end_pos": chunk['end_pos'],
                    "pdf_path": input_file
                }
            )
            
            # Create HAS_CHUNK relationship
            conn.execute(
                """
                MATCH (p:PDF {path: $path})
                MATCH (c:Chunk {id: $chunk_id})
                MERGE (p)-[r:HAS_CHUNK]->(c)
                """,
                {
                    "path": input_file,
                    "chunk_id": chunk_id
                }
            )
            
    except Exception as e:
        logger.warning(f"Failed to load PDF and chunks: {e}")


def load_observations_to_kuzu_db(conn, observations: List[Dict[str, Any]], input_file: str):
    """Load observations into Kuzu database."""
    logger = logging.getLogger(__name__)
    logger.info(f"Loading {len(observations)} observations into database")
    
    import time
    timestamp = int(time.time())
    
    for i, obs in enumerate(observations):
        try:
            # Create observation node with timestamp to avoid conflicts
            obs_id = f"obs_{timestamp}_{i}"
            conn.execute(
                """
                MERGE (o:Observation {
                    id: $id,
                    text: $text,
                    relationship: $relationship,
                    chunk_index: $chunk_index,
                    chunk_start_pos: $chunk_start_pos,
                    chunk_end_pos: $chunk_end_pos,
                    pdf_path: $pdf_path
                })
                """,
                {
                    "id": obs_id,
                    "text": obs['observation'],
                    "relationship": obs.get('relationship', ''),
                    "chunk_index": obs['chunk_index'],
                    "chunk_start_pos": obs['chunk_start_pos'],
                    "chunk_end_pos": obs['chunk_end_pos'],
                    "pdf_path": input_file
                }
            )
            
            # Create REFERENCE_CHUNK relationship
            pdf_name = os.path.basename(input_file).replace('.pdf', '')
            chunk_id = f"{pdf_name}_chunk_{obs['chunk_index']}"
            conn.execute(
                """
                MATCH (o:Observation {id: $obs_id})
                MATCH (c:Chunk {id: $chunk_id})
                MERGE (o)-[r:REFERENCE_CHUNK]->(c)
                """,
                {
                    "obs_id": obs_id,
                    "chunk_id": chunk_id
                }
            )
            
            # Create MENTION relationships to entities
            for entity in obs['entities']:
                conn.execute(
                    """
                    MATCH (o:Observation {id: $obs_id})
                    MATCH (e:Entity {label: $entity_label})
                    MERGE (o)-[r:MENTION]->(e)
                    """,
                    {
                        "obs_id": obs_id,
                        "entity_label": entity['label']
                    }
                )
                
        except Exception as e:
            logger.warning(f"Failed to load observation {i}: {e}") 