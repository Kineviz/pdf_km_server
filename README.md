# 📚 PDF to Knowledge Map Server

A **distributed web service** that extracts knowledge from PDF documents and creates graph databases using AI models across multiple Ollama servers.

## 🚀 Features

- **📄 PDF Upload & Processing**: Upload PDF files and extract text content
- **🤖 AI-Powered Knowledge Extraction**: Use Gemma3 model to extract observations and entities
- **🔄 Distributed Processing**: Parallel processing across multiple Ollama servers
- **⚡ Load Balancing**: Automatic failover and round-robin distribution
- **📊 Real-Time Progress**: Live progress tracking with chunk-level updates
- **🗄️ Graph Database Generation**: Create KuzuDB knowledge graphs
- **🌐 Web Interface**: User-friendly Gradio web interface
- **👥 Job Queue Management**: Handle multiple concurrent users
- **📥 Download Results**: Download processed databases as ZIP files
- **⏱️ Time Estimation**: Estimated processing times based on file size
- **🏥 Health Monitoring**: Automatic server health checks and failover

## 🏗️ Architecture

### **Distributed Processing Pipeline**
1. **Text Extraction**: Extract raw text from PDF using MarkItDown
2. **Document Chunking**: Split text into manageable chunks
3. **Parallel AI Extraction**: Use multiple Ollama servers simultaneously
4. **Knowledge Graph Creation**: Build KuzuDB with nodes and relationships
5. **Database Export**: Create downloadable ZIP files

### **Ollama Cluster Management**
- **Multiple Servers**: Distribute load across multiple PCs
- **Health Monitoring**: Automatic ping and HTTP health checks
- **Load Balancing**: Round-robin distribution across active servers
- **Failover**: Automatic retry on different servers
- **Performance Tracking**: Response time and error monitoring

### **Data Model**
- **PDF**: Document metadata and content (path, filename, text)
- **Chunk**: Text segments with position information and PDF source
- **Observation**: Natural language statements with relationships and PDF source
- **Entity**: Named entities with categories (Person, Organization, etc.)
- **Relationships**: HAS_CHUNK, REFERENCE_CHUNK, MENTION

### **Multi-PDF Support**
- **PDF path tracking**: All chunks and observations track their source PDF
- **Data merging**: Multiple PDFs can be processed and merged into the same database
- **Source identification**: Easy to query which observations came from which PDF
- **Scalable architecture**: Supports processing multiple documents over time

## 🚀 Quick Start

### **Prerequisites**
- Python 3.13+
- Multiple Ollama servers with Gemma3 model
- Network access between servers

### **Installation**
```bash
# Clone the repository
git clone <repository-url>
cd pdf_km_server/src

# No manual installation needed - uv handles everything automatically
```

### **Configure Ollama Servers**

**Step 1: Copy Configuration Template**
```bash
cp ollama_servers_example.json ollama_servers.json
```

**Step 2: Edit Server Configuration**
Edit `ollama_servers.json` to add your servers:

```json
{
  "servers": [
    {
      "name": "Ollama Server 1",
      "url": "http://ollama_server_1:11434",
      "model": "gemma3",
      "timeout": 30,
      "max_retries": 3
    },
    {
      "name": "Ollama Server 2",
      "url": "http://ollama_server_2:11434",
      "model": "gemma3",
      "timeout": 30,
      "max_retries": 3
    }
  ]
}
```

**Configuration Fields:**
- **name**: Human-readable server identifier
- **url**: Ollama server URL (must be accessible from this machine)
- **model**: AI model name (recommended: `gemma3`)
- **timeout**: Request timeout in seconds
- **max_retries**: Number of retry attempts on failure

**Step 3: Test Server Connectivity**
```bash
# Test individual server speed
./test_speed.py

# Test parallel processing
./test_parallel.py
```

### **Start the Server**
```bash
./server.py
```

The server will start at `http://localhost:7860`

## 📖 Usage

### **1. Upload & Process**
1. Go to the "Upload & Process" tab
2. Upload a PDF document
3. Click "Start Processing"
4. **Watch real-time progress** with chunk-level updates
5. Download results when complete

### **2. Monitor Cluster Status**
- **Check Ollama Cluster Status**: See all server health and performance
- **View job status and progress**: Real-time updates
- **Check queue status**: Monitor multiple users

### **3. Download Results**
- Download processed KuzuDB as ZIP files
- Files are automatically created when processing completes

## ⚙️ Configuration

### **Ollama Server Configuration**
Each server in `ollama_servers.json` supports:

| Field | Description | Default |
|-------|-------------|---------|
| `name` | Server identifier | Required |
| `url` | Ollama server URL | Required |
| `model` | Model to use | `gemma3` |
| `timeout` | Request timeout (seconds) | `30` |
| `max_retries` | Max retries per request | `3` |

### **Health Check System**
- **Ping Check**: Tests basic network connectivity
- **HTTP Check**: Tests Ollama API endpoint (`/api/tags`)
- **Error Tracking**: Counts consecutive failures
- **Auto-Deactivation**: Marks servers inactive after 5 errors

## 📊 Performance Features

### **Parallel Processing**
- **True parallelism**: Multiple chunks processed simultaneously
- **Server utilization**: All active servers used efficiently
- **Load distribution**: Round-robin across available servers
- **Performance improvement**: ~1.46x faster than sequential processing

### **Real-Time Progress Tracking**
- **Chunk-level progress**: Shows "Processed 2/5 chunks"
- **Server utilization**: Indicates which servers are active
- **Time estimates**: Based on file size and server performance
- **Live updates**: Progress bar updates in real-time

## 📊 Job Status Types

- **queued**: Job is waiting to be processed
- **processing**: Job is currently being processed (with progress)
- **completed**: Job completed successfully
- **failed**: Job failed with an error

## 🔄 Processing Steps with Progress

1. **Text Extraction** (0-5%): Extract raw text from PDF
2. **Document Chunking** (5%): Split into manageable chunks
3. **AI Observation Extraction** (5-95%): 
   - **Real-time chunk progress**: "Processed 1/5 chunks" → "Processed 5/5 chunks"
   - **Parallel processing**: Multiple servers working simultaneously
   - **Load balancing**: Requests distributed across servers
4. **Knowledge Graph Creation** (95-100%): Build KuzuDB database

## ⏱️ Time Estimates

Processing time depends on:
- **PDF size**: Larger files take longer
- **Content complexity**: More text = more processing
- **Server performance**: Faster servers = faster processing
- **Parallel processing**: Multiple servers reduce total time

**Typical times with distributed processing:**
- Small PDFs (< 1MB): 20-40 seconds
- Medium PDFs (1-5MB): 40-80 seconds
- Large PDFs (> 5MB): 80+ seconds

## 📦 Output Format

### **KuzuDB Structure**
```
result_kuzudb/
└── kuzu_db_[job_id]/
    ├── catalog.kz
    ├── data.kz
    ├── metadata.kz
    └── [index files]
```

### **Downloadable ZIP**
- **Filename**: `km_kuzu_[job_id].zip`
- **Location**: `result_kuzudb/` directory
- **Contents**: Complete KuzuDB database

## 🔧 API Endpoints

The web interface provides these functions:
- **Upload & Process**: Handle PDF uploads and start processing
- **Check Status**: Monitor job progress and queue status
- **Check Ollama Cluster Status**: Monitor all server health and performance
- **Download Results**: Download completed databases

## 🛠️ Troubleshooting

### **Common Issues**

**Server Not Starting:**
```bash
# Check if port 7860 is available
lsof -i :7860

# Kill existing process if needed
pkill -f "server.py"
```

**Ollama Connection Issues:**
```bash
# Test Ollama server
curl http://localhost:11434/api/tags

# Check server configuration
cat ollama_servers.json

# Test cluster health
python config.py
```

**Processing Failures:**
- Check server logs for error messages
- Verify Ollama servers are running
- Ensure sufficient disk space for databases
- Check cluster health status in web interface

### **Performance Monitoring**
- **Cluster Status**: Monitor all server health in web interface
- **Response Times**: Track server performance
- **Error Counts**: Identify problematic servers
- **Load Distribution**: Ensure even distribution across servers

### **Logs**
- Check terminal output for detailed logs
- Look for error messages and warnings
- Monitor server health status
- Track parallel processing progress

## 🏗️ Architecture Details

### **Components**
- **Gradio Web Interface**: User interface and job management
- **Job Queue**: Thread-safe queue for managing multiple users
- **Processing Pipeline**: Modular text extraction and AI processing
- **Ollama Cluster**: Distributed AI model serving with health monitoring
- **KuzuDB**: Graph database for knowledge storage
- **Parallel Processing**: ThreadPoolExecutor for concurrent chunk processing

### **Data Flow**
1. **Upload** → PDF file uploaded to server
2. **Queue** → Job added to processing queue
3. **Extract** → Text extracted from PDF
4. **Chunk** → Text split into manageable segments
5. **Parallel Process** → AI extracts observations across multiple servers
6. **Store** → Data loaded into KuzuDB
7. **Export** → Database packaged as ZIP file
8. **Download** → User downloads results

### **Distributed Processing Flow**
```
PDF Upload → Text Extraction → Chunking → Parallel AI Processing
                                                      ↓
                                              Server 1: Chunk 1
                                              Server 2: Chunk 2
                                              Server 1: Chunk 3
                                              Server 2: Chunk 4
                                                      ↓
                                              Database Creation → ZIP Export
```

## 🔒 Security

- **Local Network Only**: Designed for trusted local networks
- **No Authentication**: Assumes secure local environment
- **File Handling**: Temporary files cleaned up automatically
- **Input Validation**: PDF files validated before processing
- **Network Security**: HTTP only for local network simplicity

## 📈 Performance Benefits

### **Parallel Processing**
- **Multiple servers** handle requests simultaneously
- **Reduced wait times** for large documents
- **Better resource utilization** across network

### **Reliability**
- **Automatic failover** ensures service continuity
- **Health monitoring** prevents using broken servers
- **Error recovery** handles temporary network issues

### **Scalability**
- **Easy to add servers** - just update config
- **Load distribution** prevents server overload
- **Horizontal scaling** for increased capacity

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

The MIT License is a permissive open source license that allows:
- ✅ Commercial use
- ✅ Modification
- ✅ Distribution
- ✅ Private use
- ✅ Patent use

The only requirement is that the license and copyright notice be included in all copies or substantial portions of the software.

