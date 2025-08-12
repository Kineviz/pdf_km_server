# PDF to Knowledge Map Server

A server that processes PDF documents to extract knowledge and create a graph database using AI-powered observation extraction and semantic search capabilities.

## Features

- **PDF Text Extraction**: Extract text content from PDF documents
- **AI-Powered Analysis**: Use Gemma3 model to extract observations and entities
- **Knowledge Graph Creation**: Build Kuzu graph database with entities and relationships
- **Semantic Search**: AI-powered vector search across observation text using sentence transformers
- **Database Merging**: Combine multiple knowledge graphs into consolidated databases
- **Web Interface**: Gradio-based UI for easy interaction

## Semantic Search

The server now includes advanced semantic search capabilities:

### How It Works

1. **Vectorization**: All observation text is automatically vectorized using the `all-MiniLM-L6-v2` model
2. **Vector Storage**: 384-dimensional vectors are stored in a dedicated `ObservationTextVector` table
3. **Indexing**: Vector index (`observation_text_vector_index`) enables fast similarity search
4. **Semantic Matching**: Uses cosine similarity to find semantically related content

### Search Features

- **Natural Language Queries**: Use plain English to search for concepts
- **Semantic Understanding**: Finds related content even without exact keyword matches
- **Relevance Ranking**: Results are ranked by semantic similarity
- **Context Preservation**: Search results include source PDF and relationship information

### Usage

1. Upload a KuzuDB ZIP file in the "🔍 Semantic Search" tab
2. Enter your search query in natural language
3. Adjust the number of results (1-50)
4. Click "🔍 Search" to find relevant observations

### Example Queries

- "machine learning algorithms"
- "climate change effects"
- "business strategy implementation"
- "scientific research methods"

## Installation

### Prerequisites

- Python 3.13+
- UV package manager
- Ollama with Gemma3 model

### Dependencies

The server automatically installs required dependencies including:
- `sentence-transformers>=2.5.1` for semantic search
- `kuzu==0.10.0` for graph database
- `torch>=2.7.1` for AI models
- `gradio>=5.39.0` for web interface

## Usage

### Starting the Server

```bash
cd src
uv run --script server.py
```

The server will be available at `http://localhost:7860`

### Processing PDFs

1. Go to the "Upload & Process" tab
2. Upload a PDF document
3. Click "Start Processing"
4. Wait for AI analysis to complete
5. Download the resulting knowledge graph

### Semantic Search

1. Go to the "🔍 Semantic Search" tab
2. Upload a KuzuDB ZIP file
3. Enter your search query
4. View semantically relevant results

## Architecture

### Processing Pipeline

1. **Text Extraction**: Extract text from PDF
2. **Chunking**: Split text into manageable segments
3. **AI Analysis**: Extract observations and entities using Gemma3
4. **Database Creation**: Build Kuzu graph database
5. **Vectorization**: Generate embeddings for semantic search

### Database Schema

- **PDF**: Document information and text content
- **Chunk**: Text segments with metadata
- **Observation**: AI-extracted knowledge statements
- **Entity**: Named entities and concepts
- **ObservationTextVector**: Vector embeddings for semantic search

### Relationships

- `PDF` → `HAS_CHUNK` → `Chunk`
- `Observation` → `REFERENCE_CHUNK` → `Chunk`
- `Observation` → `MENTION` → `Entity`
- `Observation` → `OBSERVATION_TEXT_VECTOR` → `ObservationTextVector`

## Performance

- **Processing Time**: 30-120 seconds per PDF (depending on size)
- **Search Speed**: 2-5 seconds for semantic queries
- **Vector Index**: Optimized for fast similarity search
- **Memory Usage**: Efficient vector storage with 384-dimensional embeddings

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

