#!/usr/bin/env python3
"""
Step 2: Text Chunking
Divides text content into manageable chunks for AI processing.
"""

import logging
from chonky import ParagraphSplitter


def chunk_text(text: str) -> list:
    """Chunk text content into paragraphs."""
    logger = logging.getLogger(__name__)
    logger.info("Chunking document into paragraphs")
    
    splitter = ParagraphSplitter(device="cpu")
    chunks = [chunk for chunk in splitter(text)]
    
    logger.info(f"Created {len(chunks)} chunks from document")
    return chunks


def create_chunks_with_metadata(chunks: list) -> list:
    """Create chunks with indices and position metadata."""
    chunks_with_indices = []
    
    for i, chunk in enumerate(chunks):
        chunks_with_indices.append({
            "index": i,
            "text": chunk,
            "length": len(chunk),
        })
        
    
    return chunks_with_indices 