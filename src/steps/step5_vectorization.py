#!/usr/bin/env python3
"""
Step 5: Vectorization
Vectorizes observation text using sentence-transformers and stores in Kuzu database.
"""

import logging
import kuzu
import os
from typing import List, Dict, Any
from sentence_transformers import SentenceTransformer
logger = logging.getLogger(__name__)


def vectorize_observations(kuzu_path: str):
    """Vectorize all observations in the database and store vectors."""
    logger.info("Vectorizing observations in Kuzu database")
    
    # Create or connect to Kuzu database
    db = kuzu.Database(kuzu_path)
    conn = kuzu.Connection(db)
    
    # Install and load vector extension
    try:
        conn.execute("INSTALL vector")
        conn.execute("LOAD vector")
        logger.info("Vector extension loaded successfully")
    except Exception as e:
        logger.warning(f"Vector extension already loaded or failed to load: {e}")
    
    # Create vector schema if it doesn't exist
    create_vector_schema(conn)
    
    # Load the sentence transformer model
    try:
        model = SentenceTransformer("all-MiniLM-L6-v2")
        logger.info("Sentence transformer model loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load sentence transformer model: {e}")
        return
    
    # Get all observations from the database
    observations = conn.execute("MATCH (o:Observation) RETURN o.id, o.text")
    
    # Vectorize each observation and store in database
    while observations.has_next():
        obs = observations.get_next()
        obs_id = obs[0]
        obs_text = obs[1]
        
        try:
            # Generate embedding
            embedding = model.encode(obs_text).tolist()
            
            # Store vector in ObservationTextVector table
            print(f"Vectorizing observation {obs_id}")
            conn.execute(
                """
                MERGE (v:ObservationTextVector {
                    id: $obs_id,
                    vector: $vector
                })
                """,
                {
                    "obs_id": obs_id,
                    "vector": embedding
                }
            )
            
            # Create relationship between Observation and ObservationTextVector
            conn.execute(
                """
                MATCH (o:Observation {id: $obs_id})
                MATCH (v:ObservationTextVector {id: $obs_id})
                MERGE (o)-[:OBSERVATION_TEXT_VECTOR]->(v)
                """,
                {
                    "obs_id": obs_id
                }
            )
            
        except Exception as e:
            logger.warning(f"Failed to vectorize observation {obs_id}: {e}")
    
    # Create vector index
    try:
        conn.execute("""
            CALL CREATE_VECTOR_INDEX(
                'ObservationTextVector',
                'observation_text_vector_index',
                'vector',
                metric := 'cosine'
            )
        """)
        logger.info("Vector index created successfully")
    except Exception as e:
        logger.warning(f"Vector index creation failed (might already exist): {e}")
    
    logger.info("Successfully vectorized all observations")


def create_vector_schema(conn):
    """Create the vector-related database schema."""
    try:
        # Create ObservationTextVector node table
        conn.execute("""
            CREATE NODE TABLE IF NOT EXISTS ObservationTextVector(
                id STRING PRIMARY KEY,
                vector FLOAT[384]
            )
        """)
        logger.info("ObservationTextVector table created/verified")
    except Exception as e:
        # Table might already exist, continue
        logger.debug(f"ObservationTextVector table creation: {e}")
    
    try:
        # Create OBSERVATION_TEXT_VECTOR relationship table
        conn.execute("""
            CREATE REL TABLE IF NOT EXISTS OBSERVATION_TEXT_VECTOR(
                FROM Observation TO ObservationTextVector
            )
        """)
        logger.info("OBSERVATION_TEXT_VECTOR relationship table created/verified")
    except Exception as e:
        # Table might already exist, continue
        logger.debug(f"OBSERVATION_TEXT_VECTOR relationship table creation: {e}")


def semantic_search(kuzu_path: str, query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Perform semantic search on observations."""
    logger = logging.getLogger(__name__)
    logger.info(f"Performing semantic search for query: {query}")
    
    # Create or connect to Kuzu database
    db = kuzu.Database(kuzu_path)
    conn = kuzu.Connection(db)
    
    # Install and load vector extension
    try:
        conn.execute("INSTALL vector")
        conn.execute("LOAD vector")
    except Exception as e:
        logger.warning(f"Vector extension already loaded or failed to load: {e}")
    
    # Load the sentence transformer model
    try:
        model = SentenceTransformer("all-MiniLM-L6-v2")
    except Exception as e:
        logger.error(f"Failed to load sentence transformer model: {e}")
        return []
    
    # Generate query vector
    try:
        query_vector = model.encode(query).tolist()
    except Exception as e:
        logger.error(f"Failed to encode query: {e}")
        return []
    
    # Perform semantic search
    try:
        result = conn.execute(
            """
            CALL QUERY_VECTOR_INDEX(
                'ObservationTextVector',
                'observation_text_vector_index',
                $query_vector,
                $limit,
                efs := 500
            )
            WITH node AS n, distance
            MATCH (n)<-[:OBSERVATION_TEXT_VECTOR]-(o:Observation)
            RETURN o, distance
            ORDER BY distance
            LIMIT $limit
            """,
            {
                "query_vector": query_vector,
                "limit": limit
            }
        )
        
        # Convert to list of dictionaries
        results = []
        while result.has_next():
            row = result.get_next()
            results.append({"node": row[0], "distance": row[1]})
        
        return results
        
    except Exception as e:
        logger.error(f"Semantic search failed: {e}")
        return []
