#!/usr/bin/env python3
"""
Step 1: Text Extraction from PDF
Extracts raw text content from PDF files using MarkItDown.
"""

import logging
from markitdown import MarkItDown


def extract_text_from_pdf(file_path: str) -> str:
    """Extract text content from a PDF file."""
    logger = logging.getLogger(__name__)
    logger.info(f"Extracting text from PDF: {file_path}")
    
    md = MarkItDown(enable_plugins=False)
    result = md.convert(file_path)
    text_content = result.text_content
    
    logger.info(f"Extracted {len(text_content)} characters of text content")
    return text_content


def get_pdf_filename(file_path: str) -> str:
    """Extract filename from full file path."""
    import os
    return os.path.basename(file_path) 