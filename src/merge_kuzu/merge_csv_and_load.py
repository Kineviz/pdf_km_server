#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "kuzu==0.10.1",
#     "pandas>=2.0.0",
# ]
# ///

import os
import pandas as pd
import kuzu
from typing import List, Dict, Any

def merge_csv_files(csv_dir: str) -> Dict[str, pd.DataFrame]:
    """Merge CSV files from multiple databases."""
    print("🔄 Merging CSV files...")
    
    merged_data = {}
    
    # Get all CSV files
    csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
    
    # Group files by type (entities, observations, chunks, etc.)
    file_groups = {}
    for file in csv_files:
        # Extract type from filename, handling relationship types properly
        if 'obs_chunk_relationships' in file:
            file_type = 'obs_chunk_relationships'
        elif 'chunk_relationships' in file:
            file_type = 'chunk_relationships'
        elif 'entity_mentions' in file:
            file_type = 'entity_mentions'
        else:
            # Extract type from filename (e.g., km_kuzu_bell_lab_entities.csv -> entities)
            parts = file.replace('.csv', '').split('_')
            file_type = parts[-1]  # Last part is the type
        
        if file_type not in file_groups:
            file_groups[file_type] = []
        file_groups[file_type].append(file)
    
    # Merge each type
    for file_type, files in file_groups.items():
        print(f"📊 Merging {file_type} files: {files}")
        
        dfs = []
        for file in files:
            df = pd.read_csv(os.path.join(csv_dir, file))
            dfs.append(df)
        
        # Concatenate all dataframes
        if dfs:
            merged_df = pd.concat(dfs, ignore_index=True)
            
            # Remove duplicates based on primary key
            if file_type == 'entities':
                merged_df = merged_df.drop_duplicates(subset=['id'])
            elif file_type == 'observations':
                merged_df = merged_df.drop_duplicates(subset=['id'])
            elif file_type == 'chunks':
                merged_df = merged_df.drop_duplicates(subset=['id'])
            elif file_type == 'pdfs':
                merged_df = merged_df.drop_duplicates(subset=['path'])
            else:
                # For relationship files, remove exact duplicates
                merged_df = merged_df.drop_duplicates()
            
            merged_data[file_type] = merged_df
            print(f"✅ Merged {len(merged_df)} {file_type}")
    
    return merged_data

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
        conn.execute("""
            CREATE NODE TABLE IF NOT EXISTS Chunk(
                id STRING PRIMARY KEY,
                text STRING,
                index INT64,
                pdf_path STRING
            )
        """)
    except Exception as e:
        # Table might already exist, continue
        pass
    
    try:
        # Create Observation node table
        conn.execute("""
            CREATE NODE TABLE IF NOT EXISTS Observation(
                id STRING PRIMARY KEY,
                text STRING,
                relationship STRING,
                chunk_index INT64,
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

def load_data_to_kuzu(merged_data: Dict[str, pd.DataFrame], kuzu_path: str):
    """Load merged data into KuzuDB."""
    print(f"📥 Loading data into KuzuDB: {kuzu_path}")
    
    # Create or connect to Kuzu database
    db = kuzu.Database(kuzu_path)
    conn = kuzu.Connection(db)
    
    # Create schema
    create_kuzu_schema(conn)
    
    # Load PDFs
    if 'pdfs' in merged_data:
        print(f"📄 Loading {len(merged_data['pdfs'])} PDFs...")
        for _, row in merged_data['pdfs'].iterrows():
            conn.execute(
                "MERGE (p:PDF {path: $path, filename: $filename, text: $text})",
                {
                    "path": row['path'],
                    "filename": row['filename'],
                    "text": row['text']
                }
            )
    
    # Load Entities
    if 'entities' in merged_data:
        print(f"🏷️  Loading {len(merged_data['entities'])} entities...")
        for _, row in merged_data['entities'].iterrows():
            conn.execute(
                "MERGE (e:Entity {id: $id, label: $label, category: $category})",
                {
                    "id": row['id'],
                    "label": row['label'],
                    "category": row['category']
                }
            )
    
    # Load Chunks
    if 'chunks' in merged_data:
        print(f"📝 Loading {len(merged_data['chunks'])} chunks...")
        for _, row in merged_data['chunks'].iterrows():
            conn.execute(
                "MERGE (c:Chunk {id: $id, text: $text, index: $index, pdf_path: $pdf_path})",
                {
                    "id": row['id'],
                    "text": row['text'],
                    "index": row['index'],
                    "pdf_path": row['pdf_path']
                }
            )
    
    # Load Observations
    if 'observations' in merged_data:
        print(f"👁️  Loading {len(merged_data['observations'])} observations...")
        for _, row in merged_data['observations'].iterrows():
            conn.execute(
                "MERGE (o:Observation {id: $id, text: $text, relationship: $relationship, chunk_index: $chunk_index, pdf_path: $pdf_path})",
                {
                    "id": row['id'],
                    "text": row['text'],
                    "relationship": row['relationship'],
                    "chunk_index": row['chunk_index'],
                    "pdf_path": row['pdf_path']
                }
            )
    
    # Load relationships
    print(f"🔍 Available relationship types: {list(merged_data.keys())}")
    
    # Load entity mentions
    if 'entity_mentions' in merged_data:
        print(f"🔗 Loading {len(merged_data['entity_mentions'])} entity mentions...")
        loaded_count = 0
        for _, row in merged_data['entity_mentions'].iterrows():
            try:
                conn.execute(
                    """
                    MATCH (o:Observation {id: $observation_id})
                    MATCH (e:Entity {id: $entity_id})
                    MERGE (o)-[r:MENTION]->(e)
                    """,
                    {
                        "observation_id": row['observation_id'],
                        "entity_id": row['entity_id']
                    }
                )
                loaded_count += 1
            except Exception as e:
                print(f"⚠️  Failed to load relationship {row['observation_id']} -> {row['entity_id']}: {e}")
        print(f"✅ Loaded {loaded_count} entity mentions")
    
    # Load chunk relationships (PDF to Chunk)
    if 'chunk_relationships' in merged_data:
        print(f"🔗 Loading {len(merged_data['chunk_relationships'])} chunk relationships...")
        loaded_count = 0
        for _, row in merged_data['chunk_relationships'].iterrows():
            try:
                conn.execute(
                    """
                    MATCH (p:PDF {path: $pdf_path})
                    MATCH (c:Chunk {id: $chunk_id})
                    MERGE (p)-[r:HAS_CHUNK]->(c)
                    """,
                    {
                        "pdf_path": row['pdf_path'],
                        "chunk_id": row['chunk_id']
                    }
                )
                loaded_count += 1
            except Exception as e:
                print(f"⚠️  Failed to load chunk relationship {row['pdf_path']} -> {row['chunk_id']}: {e}")
        print(f"✅ Loaded {loaded_count} chunk relationships")
    
    # Load observation-chunk relationships (Observation to Chunk)
    if 'obs_chunk_relationships' in merged_data:
        print(f"🔗 Loading {len(merged_data['obs_chunk_relationships'])} observation-chunk relationships...")
        loaded_count = 0
        for _, row in merged_data['obs_chunk_relationships'].iterrows():
            try:
                conn.execute(
                    """
                    MATCH (o:Observation {id: $observation_id})
                    MATCH (c:Chunk {id: $chunk_id})
                    MERGE (o)-[r:REFERENCE_CHUNK]->(c)
                    """,
                    {
                        "observation_id": row['observation_id'],
                        "chunk_id": row['chunk_id']
                    }
                )
                loaded_count += 1
            except Exception as e:
                print(f"⚠️  Failed to load obs-chunk relationship {row['observation_id']} -> {row['chunk_id']}: {e}")
        print(f"✅ Loaded {loaded_count} observation-chunk relationships")
    
    conn.close()
    db.close()
    print("✅ Data loading completed!")

def main():
    """Merge CSV files and load into new KuzuDB."""
    csv_dir = "extracted_csv_data"
    kuzu_path = "merged_kuzu_db"
    
    if not os.path.exists(csv_dir):
        print(f"❌ CSV directory not found: {csv_dir}")
        return
    
    # Merge CSV files
    merged_data = merge_csv_files(csv_dir)
    
    # Load into KuzuDB
    load_data_to_kuzu(merged_data, kuzu_path)
    
    print(f"\n🎉 Merge completed! New database: {kuzu_path}")

if __name__ == "__main__":
    main() 