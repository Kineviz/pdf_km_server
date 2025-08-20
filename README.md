# pdf_km_server

Convert PDF documents to Knowledge Maps, using Ollama and Kuzu.

What's a Knowledge Map? It's our (Kineviz) take on knowledge graph. Relationships between entities are stored as Observation nodes, which are connected to the Entities they reference. It's a holistic way to represent a document as a graph.

## Usage

### Prerequisites

- [`uv`](https://docs.astral.sh/uv/)
- [`ollama`](https://ollama.com/) with `gemma3` (or similar) installed

### Starting the Server

```bash
cd src
cp ollama_servers_example.json ollama_servers.json
# edit ollama_servers.json to add your ollama server
chmod +x server.py
./server.py
```

The server will be available at `http://localhost:7860`

### Processing PDFs

1. Go to the "Upload & Process" tab
2. Upload a PDF document
3. Click "Start Processing"
4. Wait for AI analysis to complete
5. Download the resulting Kuzu database.

### Merging Kuzu databases

When you have multiple PDFs, you can merge them into a single Kuzu database.

### Semantic Search

1. Go to the "🔍 Semantic Search" tab
2. Upload a Kuzu ZIP file
3. Enter your search query
4. View semantically relevant results

## Architecture

### Processing Pipeline

1. **Text Extraction**: Extract text from PDF
2. **Chunking**: Split text into manageable segments
3. **AI Analysis**: Extract observations and entities using Gemma3
4. **Database Creation**: Build Kuzu graph database
5. **Vectorization**: Generate embeddings for semantic search

### Node Tables

- **PDF**: Document information and text content
- **Chunk**: Text segments with metadata
- **Observation**: AI-extracted knowledge statements
- **Entity**: Named entities and concepts
- **ObservationTextVector**: Vector embeddings for semantic search

## License

This project is licensed under the MIT License - see the LICENSE file for details.

