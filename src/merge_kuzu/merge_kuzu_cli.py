#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "kuzu==0.11.2",
#     "pandas>=2.0.0",
# ]
# ///

import os
import sys
import argparse
import shutil
import traceback
import kuzu
import pandas as pd
    
from merge_csv_and_load import merge_csv_files, load_data_to_kuzu

def extract_single_kuzu(kuzu_path, output_dir):
    """Extract data from a single Kuzu file to CSV format."""
    print(f"🔌 Connecting to database...")
    db = kuzu.Database(kuzu_path)
    conn = kuzu.Connection(db)
    print(f"✅ Database connection successful")
    try:
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
            print(f"⚠️  No chunk relationships found: {e}")
        
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
        
        # Extract ObservationTextVector nodes
        observation_text_vectors = []
        try:
            print("🔍 Extracting ObservationTextVector nodes")
            result = conn.execute("MATCH (otv:ObservationTextVector) RETURN otv.id, otv.vector")
            while result.has_next():
                row = result.get_next()
                # Convert vector array to string representation for CSV
                vector_str = ','.join(map(str, row[1])) if row[1] else ''
                observation_text_vectors.append({
                    'id': row[0],
                    'vector': vector_str
                })
        except Exception as e:
            print(f"⚠️  No ObservationTextVector nodes found: {e}")
        
        # Extract Observation->ObservationTextVector relationships
        obs_text_vector_relationships = []
        try:
            result = conn.execute("MATCH (o:Observation)-[r:OBSERVATION_TEXT_VECTOR]->(otv:ObservationTextVector) RETURN o.id, otv.id")
            while result.has_next():
                row = result.get_next()
                obs_text_vector_relationships.append({
                    'observation_id': row[0],
                    'text_vector_id': row[1]
                })
        except Exception as e:
            print(f"⚠️  No OBSERVATION_TEXT_VECTOR relationships found: {e}")
        
        print(f"🔌 Database connection closed for {kuzu_path}")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        # Write CSV files
        base_name = os.path.splitext(os.path.basename(kuzu_path))[0]
        
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

        if observation_text_vectors:
            df = pd.DataFrame(observation_text_vectors)
            df.to_csv(f"{output_dir}/{base_name}_observation_text_vectors.csv", index=False)
            print(f"✅ Exported {len(observation_text_vectors)} ObservationTextVector nodes to {base_name}_observation_text_vectors.csv")

        if obs_text_vector_relationships:
            df = pd.DataFrame(obs_text_vector_relationships)
            df.to_csv(f"{output_dir}/{base_name}_obs_text_vector_relationships.csv", index=False)
            print(f"✅ Exported {len(obs_text_vector_relationships)} Observation->ObservationTextVector relationships to {base_name}_obs_text_vector_relationships.csv")

        print("✅ Finished extracting csv")
    except Exception as e:
        print(f"❌ Error extracting {kuzu_path}: {e}")
        traceback.print_exc()
    finally:
        print(f"✅ Completed processing {kuzu_path}")


def extract_kuzu_files(input_files, temp_dir):
    """Extract multiple KuzuDB files using the extract_single_kuzu function."""
    print(f"🔄 Extracting {len(input_files)} KuzuDB files...")
    
    # Extract each file using the extract_single_kuzu function
    print(f"🔍 Processing: {input_files}")
    for kuzu_file in input_files:
        print("Checking if file exists: ", kuzu_file)
        if not os.path.exists(kuzu_file):
            print(f"❌ File not found: {kuzu_file}")
            sys.exit(1)
        print(f"🔍 Processing: {kuzu_file}")
        
        # Call the extraction function directly
        try:
            extract_single_kuzu(kuzu_file, temp_dir)
            print(f"✅ Extraction completed for {kuzu_file}")
        except Exception as e:
            print(f"❌ Error during extraction for {kuzu_file}: {e}")
    
    print("✅ Extraction completed for all files")

def merge_and_load(output_db, temp_dir):
    """Merge CSV files and load into KuzuDB using existing merge script."""
    print(f"🔄 Merging CSV files and loading into: {output_db}")
    
    # Import the existing merge function
    sys.path.append('.')
    
    # Merge CSV files
    merged_data = merge_csv_files(temp_dir)
    
    # Load into new KuzuDB
    load_data_to_kuzu(merged_data, output_db)
    
    print(f"✅ Merge and load completed!")

def main():
    """Merge multiple KuzuDB ZIP files into a single database."""
    parser = argparse.ArgumentParser(description='Merge multiple Kuzu files into a single database')
    parser.add_argument('input_files', nargs='+', help='Kuzu files to merge')
    parser.add_argument('-o', '--output', default='merged_kuzu_db.kz', help='Output database path (default: merged_kuzu_db.kz)')
    parser.add_argument('--temp-dir', default='extracted_csv_data', help='Temporary directory for CSV files (default: extracted_csv_data)')
    parser.add_argument('--keep-csv', action='store_true', help='Keep CSV files after merge (default: delete them)')
    
    args = parser.parse_args()
    
    print(f"🔄 Merging {len(args.input_files)} KuzuDB files into: {args.output}")
    print(f"📁 Input files: {args.input_files}")
    
    try:
        # Extract each Kuzu file
        extract_kuzu_files(args.input_files, args.temp_dir)
        
        # Merge CSV files and load into KuzuDB at path args.output
        merge_and_load(args.output, args.temp_dir)
        
        # Clean up CSV files unless --keep-csv is specified
        if not args.keep_csv:
            if os.path.exists(args.temp_dir):
                shutil.rmtree(args.temp_dir)
                print(f"🧹 Cleaned up temporary CSV directory: {args.temp_dir}")
        
        if args.keep_csv:
            print(f"📁 CSV files saved to: {args.temp_dir}")
        
    except Exception as e:
        print(f"❌ Error during merge: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 