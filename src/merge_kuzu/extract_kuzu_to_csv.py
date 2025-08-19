#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "kuzu==0.10.0",
#     "pandas>=2.0.0",
# ]
# ///

import os
import tempfile
import zipfile
import csv
import kuzu
import pandas as pd
import gc
import time
from typing import List, Dict, Any

def extract_kuzu_to_csv(zip_file_path: str, output_dir: str):
    """Extract data from KuzuDB ZIP file to CSV format."""
    print(f"🔍 Extracting data from: {zip_file_path}")
    
    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    print(f"📁 Created temp dir: {temp_dir}")
    
    try:
        # Extract ZIP file
        print(f"📦 Extracting ZIP file...")
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
        print(f"✅ ZIP extraction completed")
        
        # Connect to database
        print(f"🔌 Connecting to database...")
        db = kuzu.Database(temp_dir)
        conn = kuzu.Connection(db)
        print(f"✅ Database connection successful")
        
        # Extract entities
        entities = []
        try:
            result = conn.execute("MATCH (e:Entity) RETURN e.id, e.label, e.category")
            while result.has_next():
                row = result.get_next()
                entities.append({
                    'id': row[0],
                    'label': row[1], 
                    'category': row[2]
                })
        except Exception as e:
            print(f"⚠️  No entities found: {e}")
        
        # Extract observations
        observations = []
        try:
            result = conn.execute("MATCH (o:Observation) RETURN o.id, o.text, o.relationship, o.chunk_index, o.pdf_path")
            while result.has_next():
                row = result.get_next()
                observations.append({
                    'id': row[0],
                    'text': row[1],
                    'relationship': row[2],
                    'chunk_index': row[3],
                    'pdf_path': row[4]
                })
        except Exception as e:
            print(f"⚠️  No observations found: {e}")
        
        # Extract chunks
        chunks = []
        try:
            result = conn.execute("MATCH (c:Chunk) RETURN c.id, c.text, c.index, c.pdf_path")
            while result.has_next():
                row = result.get_next()
                chunks.append({
                    'id': row[0],
                    'text': row[1],
                    'index': row[2],
                    'pdf_path': row[3]
                })
        except Exception as e:
            print(f"⚠️  No chunks found: {e}")
        
        # Extract PDFs
        pdfs = []
        try:
            result = conn.execute("MATCH (p:PDF) RETURN p.path, p.filename, p.text")
            while result.has_next():
                row = result.get_next()
                pdfs.append({
                    'path': row[0],
                    'filename': row[1],
                    'text': row[2]
                })
        except Exception as e:
            print(f"⚠️  No PDFs found: {e}")
        
        # Extract relationships (if they exist)
        relationships = []
        try:
            result = conn.execute("MATCH (source:Entity)-[r:RELATED_TO]->(target:Entity) RETURN source.id, target.id, r.explanation, r.relationship")
            while result.has_next():
                row = result.get_next()
                relationships.append({
                    'source_id': row[0],
                    'target_id': row[1],
                    'explanation': row[2],
                    'relationship': row[3]
                })
        except Exception as e:
            print(f"⚠️  No RELATED_TO relationships found: {e}")
        
        # Extract chunk relationships
        chunk_relationships = []
        try:
            result = conn.execute("MATCH (p:PDF)-[r:HAS_CHUNK]->(c:Chunk) RETURN p.path, c.id")
            while result.has_next():
                row = result.get_next()
                chunk_relationships.append({
                    'pdf_path': row[0],
                    'chunk_id': row[1]
                })
        except Exception as e:
            print(f"⚠️  No HAS_CHUNK relationships found: {e}")
        
        # Extract observation-chunk relationships
        obs_chunk_relationships = []
        try:
            result = conn.execute("MATCH (o:Observation)-[r:REFERENCE_CHUNK]->(c:Chunk) RETURN o.id, c.id")
            while result.has_next():
                row = result.get_next()
                obs_chunk_relationships.append({
                    'observation_id': row[0],
                    'chunk_id': row[1]
                })
        except Exception as e:
            print(f"⚠️  No REFERENCE_CHUNK relationships found: {e}")
        
        # Extract entity-mention relationships
        entity_mentions = []
        try:
            result = conn.execute("MATCH (o:Observation)-[r:MENTION]->(e:Entity) RETURN o.id, e.id")
            while result.has_next():
                row = result.get_next()
                entity_mentions.append({
                    'observation_id': row[0],
                    'entity_id': row[1]
                })
        except Exception as e:
            print(f"⚠️  No MENTION relationships found: {e}")
        
        conn.close()
        db.close()
        
        print(f"🔌 Database connection closed for {zip_file_path}")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Write CSV files
        base_name = os.path.splitext(os.path.basename(zip_file_path))[0]
        
        # Write nodes
        if entities:
            df = pd.DataFrame(entities)
            df.to_csv(f"{output_dir}/{base_name}_entities.csv", index=False)
            print(f"✅ Exported {len(entities)} entities to {base_name}_entities.csv")
        
        if observations:
            df = pd.DataFrame(observations)
            df.to_csv(f"{output_dir}/{base_name}_observations.csv", index=False)
            print(f"✅ Exported {len(observations)} observations to {base_name}_observations.csv")
        
        if chunks:
            df = pd.DataFrame(chunks)
            df.to_csv(f"{output_dir}/{base_name}_chunks.csv", index=False)
            print(f"✅ Exported {len(chunks)} chunks to {base_name}_chunks.csv")
        
        if pdfs:
            df = pd.DataFrame(pdfs)
            df.to_csv(f"{output_dir}/{base_name}_pdfs.csv", index=False)
            print(f"✅ Exported {len(pdfs)} PDFs to {base_name}_pdfs.csv")
        
        # Write edges
        if relationships:
            df = pd.DataFrame(relationships)
            df.to_csv(f"{output_dir}/{base_name}_relationships.csv", index=False)
            print(f"✅ Exported {len(relationships)} relationships to {base_name}_relationships.csv")
        
        if chunk_relationships:
            df = pd.DataFrame(chunk_relationships)
            df.to_csv(f"{output_dir}/{base_name}_chunk_relationships.csv", index=False)
            print(f"✅ Exported {len(chunk_relationships)} chunk relationships to {base_name}_chunk_relationships.csv")
        
        if obs_chunk_relationships:
            df = pd.DataFrame(obs_chunk_relationships)
            df.to_csv(f"{output_dir}/{base_name}_obs_chunk_relationships.csv", index=False)
            print(f"✅ Exported {len(obs_chunk_relationships)} observation-chunk relationships to {base_name}_obs_chunk_relationships.csv")
        
        if entity_mentions:
            df = pd.DataFrame(entity_mentions)
            df.to_csv(f"{output_dir}/{base_name}_entity_mentions.csv", index=False)
            print(f"✅ Exported {len(entity_mentions)} entity mentions to {base_name}_entity_mentions.csv")
        
    except Exception as e:
        print(f"❌ Error extracting {zip_file_path}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up
        try:
            import shutil
            shutil.rmtree(temp_dir)
            print(f"🧹 Cleaned up temp dir: {temp_dir}")
        except Exception as e:
            print(f"⚠️  Warning: Could not clean up temp dir {temp_dir}: {e}")
        
        print(f"✅ Completed processing {zip_file_path}")
        
        # Force garbage collection
        gc.collect()

def main():
    """Extract data from KuzuDB ZIP files to CSV."""
    # Create output directory
    output_dir = "extracted_csv_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract from both KuzuDB files
    kuzu_files = [
        "./fixtures/km_kuzu_story.zip",
        "./fixtures/km_kuzu_bell_lab.zip"
    ]
    
    print(f"📋 Processing {len(kuzu_files)} files: {kuzu_files}")
    for i, zip_file in enumerate(kuzu_files):
        print(f"🔍 Checking file {i+1}/{len(kuzu_files)}: {zip_file}")
        if os.path.exists(zip_file):
            print(f"✅ File exists, extracting data from: {zip_file}")
            try:
                extract_kuzu_to_csv(zip_file, output_dir)
                print(f"✅ Successfully processed: {zip_file}")
                print(f"🔄 Moving to next file...")
                time.sleep(1)  # Small delay between files
            except Exception as e:
                print(f"❌ Error processing {zip_file}: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"❌ File not found: {zip_file}")
    
    print(f"\n📁 All CSV files saved to: {output_dir}")

if __name__ == "__main__":
    main() 