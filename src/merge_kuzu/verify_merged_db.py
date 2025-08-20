#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "kuzu==0.10.0",
# ]
# ///

import kuzu

def verify_merged_database(kuzu_path: str):
    """Verify the contents of the merged database."""
    print(f"🔍 Verifying merged database: {kuzu_path}")
    
    try:
        # Connect to database
        db = kuzu.Database(kuzu_path)
        conn = kuzu.Connection(db)
        
        # Count entities
        result = conn.execute("MATCH (e:Entity) RETURN count(e)")
        if result.has_next():
            count = result.get_next()[0]
            print(f"✅ Found {count} entities")
        
        # Count observations
        result = conn.execute("MATCH (o:Observation) RETURN count(o)")
        if result.has_next():
            count = result.get_next()[0]
            print(f"✅ Found {count} observations")
        
        # Count chunks
        result = conn.execute("MATCH (c:Chunk) RETURN count(c)")
        if result.has_next():
            count = result.get_next()[0]
            print(f"✅ Found {count} chunks")
        
        # Count PDFs
        result = conn.execute("MATCH (p:PDF) RETURN count(p)")
        if result.has_next():
            count = result.get_next()[0]
            print(f"✅ Found {count} PDFs")
        
        # Count ObservationTextVectors
        result = conn.execute("MATCH (otv:ObservationTextVector) RETURN count(otv)")
        if result.has_next():
            count = result.get_next()[0]
            print(f"✅ Found {count} ObservationTextVectors")
        
        # Count relationships
        result = conn.execute("MATCH ()-[r:HAS_CHUNK]->() RETURN count(r)")
        if result.has_next():
            count = result.get_next()[0]
            print(f"✅ Found {count} HAS_CHUNK relationships")
        
        result = conn.execute("MATCH ()-[r:REFERENCE_CHUNK]->() RETURN count(r)")
        if result.has_next():
            count = result.get_next()[0]
            print(f"✅ Found {count} REFERENCE_CHUNK relationships")
        
        result = conn.execute("MATCH ()-[r:MENTION]->() RETURN count(r)")
        if result.has_next():
            count = result.get_next()[0]
            print(f"✅ Found {count} MENTION relationships")
        
        result = conn.execute("MATCH ()-[r:OBSERVATION_TEXT_VECTOR]->() RETURN count(r)")
        if result.has_next():
            count = result.get_next()[0]
            print(f"✅ Found {count} OBSERVATION_TEXT_VECTOR relationships")
        
        # Show sample entities
        print(f"\n📋 Sample entities:")
        result = conn.execute("MATCH (e:Entity) RETURN e.id, e.label, e.category LIMIT 5")
        while result.has_next():
            row = result.get_next()
            print(f"  • {row[0]} ({row[1]} - {row[2]})")
        
        # Show PDFs
        print(f"\n📄 PDFs in database:")
        result = conn.execute("MATCH (p:PDF) RETURN p.path, p.filename")
        while result.has_next():
            row = result.get_next()
            print(f"  • {row[1]} ({row[0]})")
        
        # Show sample ObservationTextVectors
        print(f"\n🔢 Sample ObservationTextVectors:")
        result = conn.execute("MATCH (otv:ObservationTextVector) RETURN otv.id, length(otv.vector) LIMIT 5")
        while result.has_next():
            row = result.get_next()
            print(f"  • {row[0]} (vector length: {row[1]})")
        
        conn.close()
        db.close()
        
    except Exception as e:
        print(f"❌ Error verifying database: {e}")

if __name__ == "__main__":
    verify_merged_database("merged_kuzu_db") 