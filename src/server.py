#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "chonky>=0.1.6",
#     "click>=8.1.0",
#     "kuzu==0.10.0",
#     "markitdown[pdf]>=0.1.2",
#     "requests>=2.32.4",
#     "torch>=2.7.1",
#     "torchaudio>=2.7.1",
#     "torchvision>=0.22.1",
#     "tqdm>=4.66.0",
#     "gradio>=5.39.0",
#     "ping3>=4.0.4",
#     "sentence-transformers>=2.5.1",
#     "polars",
#     "pyarrow",
# ]
# ///

import gradio as gr
import os
import uuid
import time
import threading
import zipfile
import tempfile
import shutil
import subprocess
from datetime import datetime
from typing import Dict, List, Optional
import logging

# Import our existing modules
from steps.step1_text_extraction import extract_text_from_pdf
from steps.step2_chunking import chunk_text, create_chunks_with_metadata
from steps.step3_observation_extraction import extract_observations_from_chunks
from steps.step4_kuzu_integration import load_observations_to_kuzu
from steps.step5_vectorization import vectorize_observations
from config import get_ollama_cluster

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JobQueue:
    def __init__(self):
        self.jobs: Dict[str, Dict] = {}
        self.lock = threading.Lock()
    
    def add_job(self, job_id: str, pdf_file: str, model: str = "gemma3") -> Dict:
        """Add a new job to the queue."""
        with self.lock:
            job = {
                "id": job_id,
                "pdf_file": pdf_file,
                "model": model,
                "status": "queued",
                "progress": 0,
                "message": "Job queued",
                "created_at": datetime.now(),
                "started_at": None,
                "completed_at": None,
                "estimated_time": None,
                "kuzu_db_path": None,
                "observations_count": 0,
                "entities_count": 0,
                "word_count": 0,
                "estimated_pages": 0,
                "char_count": 0,
                "sentence_count": 0,
                "avg_words_per_sentence": 0,
                "chunks_count": 0,
                "chunks_processed": 0,
                "processing_time": 0
            }
            self.jobs[job_id] = job
            return job
    
    def update_job(self, job_id: str, **kwargs):
        """Update job status and progress."""
        with self.lock:
            if job_id in self.jobs:
                self.jobs[job_id].update(kwargs)
    
    def get_job(self, job_id: str) -> Optional[Dict]:
        """Get job by ID."""
        with self.lock:
            return self.jobs.get(job_id)
    
    def get_all_jobs(self) -> List[Dict]:
        """Get all jobs."""
        with self.lock:
            return list(self.jobs.values())

# Global job queue
job_queue = JobQueue()

def calculate_pdf_stats(text_content: str) -> dict:
    """Calculate PDF statistics."""
    words = text_content.split()
    word_count = len(words)
    estimated_pages = max(1.0, round(word_count / 500, 1))  # 500 words per page average, rounded to 1 decimal
    char_count = len(text_content)
    sentence_count = len([s for s in text_content.split('.') if s.strip()])
    avg_words_per_sentence = word_count / max(1, sentence_count)
    
    return {
        "word_count": word_count,
        "estimated_pages": estimated_pages,
        "char_count": char_count,
        "sentence_count": sentence_count,
        "avg_words_per_sentence": avg_words_per_sentence
    }

def estimate_processing_time(pdf_size_mb: float, model: str = "gemma3") -> int:
    """Estimate processing time in seconds based on PDF size and model."""
    # Base estimates (in seconds)
    base_time = 30  # Base time for small PDFs
    
    # Adjust for PDF size (rough estimate: 1MB = 30 seconds)
    size_factor = pdf_size_mb * 30
    
    # Model-specific adjustments
    if model == "phi4-mini":
        size_factor *= 0.6  # 40% faster
    
    estimated_time = base_time + size_factor
    return int(estimated_time)


def upload_and_process_pdf(pdf_file, model="gemma3:270m", progress=gr.Progress()):
    print(f"Uploading and processing {pdf_file} with model: {model}")
    """Handle PDF upload and start processing."""
    if pdf_file is None:
        return "Please upload a PDF file.", None, None, gr.update(visible=False)
    
    # Generate unique job ID
    job_id = str(uuid.uuid4())
    
    # Get file size for time estimation
    file_size_mb = os.path.getsize(pdf_file.name) / (1024 * 1024)
    estimated_time = estimate_processing_time(file_size_mb, model)
    
    # Add job to queue
    job = job_queue.add_job(job_id, pdf_file.name, model)
    job_queue.update_job(job_id, estimated_time=estimated_time)
    
    # Process directly (not in background thread) to use gr.Progress()
    try:
        job_queue.update_job(job_id, status="processing", started_at=datetime.now())
        progress(0, desc="Starting PDF processing...")
        
        # Step 1: Extract text
        job_queue.update_job(job_id, progress=5, message="Extracting text from PDF...")
        progress(0.05, desc="Extracting text from PDF...")
        text_content = extract_text_from_pdf(pdf_file.name)
        
        # Calculate PDF stats
        pdf_stats = calculate_pdf_stats(text_content)
        job_queue.update_job(job_id, 
                           word_count=pdf_stats["word_count"],
                           estimated_pages=pdf_stats["estimated_pages"],
                           char_count=pdf_stats["char_count"],
                           sentence_count=pdf_stats["sentence_count"],
                           avg_words_per_sentence=pdf_stats["avg_words_per_sentence"])
        
        # Step 2: Chunk document
        job_queue.update_job(job_id, progress=5, message="Chunking document...")
        progress(0.05, desc="Chunking document...")
        chunks = chunk_text(text_content)
        chunks_with_metadata = create_chunks_with_metadata(chunks)
        job_queue.update_job(job_id, chunks_count=len(chunks))
        
        # Step 3: Extract observations
        job_queue.update_job(job_id, progress=5, message="Extracting observations with AI...")
        progress(0.05, desc="Extracting observations with AI...")
        
        # Create progress callback for real-time chunk progress
        def update_chunk_progress(chunk_progress, message):
            # Map chunk progress (0-100) to overall progress (5-95)
            overall_progress = 5 + (chunk_progress * 0.9)
            # Extract current chunk number from message (e.g., "Processed 5/25 chunks")
            if "Processed" in message and "/" in message:
                try:
                    current_chunk = int(message.split("Processed ")[1].split("/")[0])
                    job_queue.update_job(job_id, chunks_processed=current_chunk)
                except:
                    pass
            progress(overall_progress / 100, desc=message)
        
        print(f"Extracting observations with model: {model}")
        observations = extract_observations_from_chunks(chunks, model, progress_callback=update_chunk_progress)
        
        # Step 4: Create Kuzu database
        job_queue.update_job(job_id, progress=95, message="Creating knowledge graph database...")
        progress(0.95, desc="Creating knowledge graph database...")
        
        # Create unique database path for this job
        kuzu_db_path = f"result_kuzudb/kuzu_db_{job_id}"
        if os.path.exists(kuzu_db_path):
            shutil.rmtree(kuzu_db_path)
        
        # Load into Kuzu database
        load_observations_to_kuzu(observations, kuzu_db_path, text_content, chunks_with_metadata, pdf_file.name)
        
        # Step 5: Vectorize observations
        job_queue.update_job(job_id, progress=97, message="Vectorizing observations...")
        progress(0.97, desc="Vectorizing observations...")
        vectorize_observations(kuzu_db_path)
        
        # Calculate processing time
        processing_time = (datetime.now() - job_queue.get_job(job_id)['started_at']).total_seconds()
        
        # Update job with results
        job_queue.update_job(
            job_id,
            status="completed",
            progress=100,
            message="Processing completed successfully!",
            completed_at=datetime.now(),
            kuzu_db_path=kuzu_db_path,
            observations_count=len(observations),
            entities_count=len(set(entity['label'] for obs in observations for entity in obs['entities'])),
            processing_time=processing_time
        )
        
        progress(1.0, desc="Processing completed successfully!")
        
        # Debug logging
        logger.info(f"Job {job_id} completed. Database path: {kuzu_db_path}")
        logger.info(f"Database exists: {os.path.exists(kuzu_db_path)}")
        if os.path.exists(kuzu_db_path):
            logger.info(f"Database contents: {os.listdir(kuzu_db_path)}")
        
    except Exception as e:
        logger.error(f"Error processing job {job_id}: {e}")
        job_queue.update_job(
            job_id,
            status="failed",
            message=f"Processing failed: {str(e)}"
        )
        progress(0, desc=f"Error: {str(e)}")
    
    # Get job stats for display
    job = job_queue.get_job(job_id)
    if job and job['status'] == 'completed':
        stats_text = f"""
Job completed! Job ID: {job_id}

📊 PDF Statistics:
• Words: {job['word_count']:,}
• Pages: {job['estimated_pages']}
• Chunks: {job['chunks_count']}

📈 Results:
• Observations: {job['observations_count']}
• Entities: {job['entities_count']}
• Time: {job['processing_time']:.1f}s
""".strip()
    else:
        stats_text = f"Job completed! Job ID: {job_id}\nEstimated time: {estimated_time} seconds"
    
    return stats_text, job_id, None, gr.update(visible=True)



def download_database(job_id):
    """Download the Kuzu database as a ZIP file."""
    if not job_id:
        return None, "Please enter a job ID."
    
    job = job_queue.get_job(job_id)
    if not job:
        return None, f"Job {job_id} not found."
    
    if job['status'] != 'completed':
        return None, f"Job {job_id} is not completed yet. Status: {job['status']}"
    
    kuzu_db_path = job['kuzu_db_path']
    if not os.path.exists(kuzu_db_path):
        return None, f"Database file not found for job {job_id}."
    
    # Create ZIP file
    zip_filename = f"result_kuzudb/km_kuzu_{job_id}.zip"
    with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(kuzu_db_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, kuzu_db_path)
                zipf.write(file_path, arcname)
    
    # Ensure the ZIP file is readable
    os.chmod(zip_filename, 0o644)
    
    return zip_filename, f"Database ready for download: {zip_filename}"





def get_ollama_server_status():
    """Get status of all Ollama servers in the cluster."""
    cluster = get_ollama_cluster()
    status = cluster.get_server_status()
    
    status_text = f"**🤖 LLM Servers:** {status['active_servers']}/{status['total_servers']} active\n\n"
    
    for server in status['servers']:
        status_icon = "🟢" if server['is_active'] else "🔴"
        response_time = f"{server['response_time']:.0f}ms" if server['response_time'] else "N/A"
        error_info = f" (errors: {server['error_count']}/{server['max_errors']})" if server['error_count'] > 0 else ""
        
        status_text += f"{status_icon} **{server['name']}** ({server['model']}) - {response_time}{error_info}\n"
    
    # Add reconnection info
    if status['active_servers'] < status['total_servers']:
        status_text += f"\n🔄 **Auto-reconnection:** Every {status['health_check_interval']} seconds\n"
        status_text += f"⏰ **Last check:** {time.time() - status['last_health_check']:.0f}s ago\n"
    
    return status_text

def force_ollama_reconnect():
    """Force a reconnection check of all inactive Ollama servers."""
    cluster = get_ollama_cluster()
    status = cluster.force_reconnect_check()
    
    status_text = f"**🔄 Manual Reconnection Check Complete**\n\n"
    status_text += f"**🤖 LLM Servers:** {status['active_servers']}/{status['total_servers']} active\n\n"
    
    for server in status['servers']:
        status_icon = "🟢" if server['is_active'] else "🔴"
        response_time = f"{server['response_time']:.0f}ms" if server['response_time'] else "N/A"
        error_info = f" (errors: {server['error_count']}/{server['max_errors']})" if server['error_count'] > 0 else ""
        
        status_text += f"{status_icon} **{server['name']}** ({server['model']}) - {response_time}{error_info}\n"
    
    return status_text


def auto_download_when_ready(job_id):
    """Download database when job is completed."""
    if not job_id:
        return None, "No job ID provided"
    
    job = job_queue.get_job(job_id)
    if not job:
        return None, "Job not found"
    
    if job['status'] == 'completed':
        # Create ZIP file
        kuzu_db_path = job['kuzu_db_path']
        logger.info(f"Checking database path: {kuzu_db_path}")
        logger.info(f"Database exists: {os.path.exists(kuzu_db_path)}")
        
        if not os.path.exists(kuzu_db_path):
            # Check if there are any kuzu_db directories
            import glob
            kuzu_dirs = glob.glob("result_kuzudb/kuzu_db_*")
            logger.info(f"Found kuzu directories: {kuzu_dirs}")
            return None, f"Database file not found at: {kuzu_db_path}. Found directories: {kuzu_dirs}"
        
        # List database contents
        try:
            db_contents = os.listdir(kuzu_db_path)
            logger.info(f"Database contents: {db_contents}")
        except Exception as e:
            logger.error(f"Error listing database contents: {e}")
            return None, f"Error accessing database: {str(e)}"
        
        zip_filename = f"result_kuzudb/km_kuzu_{job_id}.zip"
        try:
            with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for root, dirs, files in os.walk(kuzu_db_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        arcname = os.path.relpath(file_path, kuzu_db_path)
                        zipf.write(file_path, arcname)
            
            # Ensure the ZIP file is readable
            os.chmod(zip_filename, 0o644)
            
            logger.info(f"Created ZIP file: {zip_filename}")
            
            # Verify the ZIP file was created and is readable
            if os.path.exists(zip_filename):
                file_size = os.path.getsize(zip_filename)
                logger.info(f"ZIP file size: {file_size} bytes")
                logger.info(f"Returning file path: {zip_filename}")
                logger.info(f"File is readable: {os.access(zip_filename, os.R_OK)}")
                # Return the file path for Gradio to serve (same format as download_database)
                return zip_filename, f"✅ Database ready for download! ({file_size} bytes)"
            else:
                logger.error(f"ZIP file was not created: {zip_filename}")
                return None, f"❌ Error: ZIP file was not created"
        except Exception as e:
            logger.error(f"Error creating ZIP file: {e}")
            return None, f"❌ Error creating ZIP file: {str(e)}"
    elif job['status'] == 'failed':
        return None, f"❌ Processing failed: {job.get('message', 'Unknown error')}"
    else:
        return None, f"⏳ Still processing... {job.get('message', 'Unknown status')}"

def merge_kuzu_databases(kuzu_files, progress=gr.Progress()):
    """Merge multiple KuzuDB files into a single database."""
    if not kuzu_files:
        return None, "❌ No KuzuDB files provided"
    
    try:
        # Create temporary directory for the merge
        temp_dir = tempfile.mkdtemp()
        merge_output_dir = os.path.join(temp_dir, "merged_kuzu_db")
        
        # Save uploaded files to temp directory
        saved_files = []
        for i, file in enumerate(kuzu_files):
            if file is None:
                continue
            
            # Generate a unique filename
            file_ext = os.path.splitext(file.name)[1]
            temp_filename = f"kuzu_file_{i}{file_ext}"
            temp_path = os.path.join(temp_dir, temp_filename)
            
            # Copy the uploaded file
            shutil.copy2(file.name, temp_path)
            saved_files.append(temp_path)
        
        if not saved_files:
            return None, "❌ No valid KuzuDB files found"
        
        progress(0.1, desc="📦 Preparing merge...")
        
        # Run the merge using our CLI tool
        merge_script = os.path.join(os.path.dirname(__file__), "merge_kuzu", "merge_kuzu_cli.py")
        
        if not os.path.exists(merge_script):
            return None, "❌ Merge script not found. Please ensure merge_kuzu folder exists."
        
        # Construct the command
        cmd = ["uv", "run", "--script", merge_script, "-o", merge_output_dir, "--result-dir", temp_dir, "--temp-dir", os.path.join(temp_dir, "csv_data")]
        cmd.extend(saved_files)
        
        progress(0.2, desc="🔄 Starting merge process...")
        
        # Run the merge command
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.path.join(os.path.dirname(__file__), "merge_kuzu"))
        
        progress(0.8, desc="📦 Creating download package...")
        
        if result.returncode != 0:
            error_msg = result.stderr if result.stderr else "Unknown error"
            return None, f"❌ Merge failed: {error_msg}"
        
        # Find the created ZIP file
        zip_files = [f for f in os.listdir(temp_dir) if f.endswith('.zip')]
        if not zip_files:
            return None, "❌ No merged database ZIP file found"
        
        zip_path = os.path.join(temp_dir, zip_files[0])
        
        # Create a unique filename for download in a persistent location
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        download_filename = f"merged_kuzu_db_{timestamp}.zip"
        
        # Ensure result_kuzudb directory exists
        result_dir = "result_kuzudb"
        os.makedirs(result_dir, exist_ok=True)
        
        download_path = os.path.join(result_dir, download_filename)
        
        # Copy to persistent download path
        shutil.copy2(zip_path, download_path)
        
        # Ensure the file is readable
        os.chmod(download_path, 0o644)
        
        progress(1.0, desc="✅ Merge completed!")
        
        # Clean up temporary files
        try:
            for file in saved_files:
                if os.path.exists(file):
                    os.remove(file)
            # Also clean up the temporary directory
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception as e:
            logger.warning(f"Could not clean up temporary files: {e}")
        
        # Verify the file exists and is readable
        if os.path.exists(download_path):
            file_size = os.path.getsize(download_path)
            logger.info(f"Created merged database: {download_path} ({file_size} bytes)")
            logger.info(f"File is readable: {os.access(download_path, os.R_OK)}")
            logger.info(f"Returning file path for Gradio: {download_path}")
            return download_path, f"✅ Successfully merged {len(saved_files)} KuzuDB files! Ready for download ({file_size} bytes)."
        else:
            logger.error(f"File does not exist: {download_path}")
            return None, "❌ Error: Merged database file was not created properly."
        
    except Exception as e:
        logger.error(f"Error in merge_kuzu_databases: {e}")
        return None, f"❌ Error during merge: {str(e)}"


def perform_semantic_search(kuzu_zip_file, query: str, limit: int):
    """Perform semantic search on a KuzuDB file."""
    if not kuzu_zip_file:
        return "❌ Please upload a KuzuDB ZIP file"
    
    if not query or not query.strip():
        return "❌ Please enter a search query"
    
    try:
        # Create temporary directory for extraction
        temp_dir = tempfile.mkdtemp()
        extract_dir = os.path.join(temp_dir, "extracted_kuzu")
        
        # Extract the ZIP file
        with zipfile.ZipFile(kuzu_zip_file.name, 'r') as zipf:
            zipf.extractall(extract_dir)
        
        kuzu_db_path = extract_dir
        
        # Import the semantic search function
        from steps.step5_vectorization import semantic_search
        
        # Perform the search
        results = semantic_search(kuzu_db_path, query.strip(), limit)
        
        # Clean up temporary files
        shutil.rmtree(temp_dir, ignore_errors=True)
        
        if not results:
            return f"🔍 No results found for query: '{query}'"
        
        # Format results
        output = f"🔍 Found {len(results)} results for query: '{query}'\n\n"
        
        for i, result in enumerate(results, 1):
            # Extract filename from path
            node = result['node']
            filename = os.path.basename(node['pdf_path']) if node['pdf_path'] else "Unknown"
            distance = result['distance']
            text = node['text'][:200] + "..." if len(node['text']) > 200 else node['text']
            
            output += f"**{i}. Result (Similarity: {1 - distance:.3f})**\n"
            output += f"📄 Source: {filename}\n"
            output += f"🔗 Relationship: {node['relationship']}\n"
            output += f"📝 Text: {text}\n\n"
        
        return output
        
    except Exception as e:
        logger.error(f"Error in semantic search: {e}")
        return f"❌ Error during search: {str(e)}"


# Create Gradio interface
with gr.Blocks(title="PDF to Knowledge Map Server", theme=gr.themes.Soft()) as demo:
    gr.Markdown("# 📚 PDF to Knowledge Map Server")
    gr.Markdown("Upload a PDF document to extract knowledge and create a graph database.")
    
    with gr.Tab("Upload & Process"):
        with gr.Row():
            with gr.Column(scale=2):
                pdf_input = gr.File(label="Upload PDF Document", file_types=[".pdf"])
                # Hidden model selection - always use gemma3
                model_select = gr.Dropdown(
                    choices=["gemma3"],
                    value="gemma3",
                    label="AI Model",
                    visible=False
                )
                process_btn = gr.Button("Start Processing", variant="primary")
                upload_output = gr.Textbox(label="Upload Result", lines=3)
                # Hidden state to store job_id for triggering download preparation
                job_id_state = gr.State()
                
                # Progress tracking - handled automatically by gr.Progress()
                
                # Auto-download section
                auto_download_btn = gr.Button("📥 Prepare Download", visible=False, variant="primary")
                auto_download_output = gr.Textbox(label="Download Status", interactive=False, visible=False)
                auto_download_file = gr.File(label="Download Database", visible=True)
            
            with gr.Column(scale=1):
                gr.Markdown("### Processing Steps")
                
                gr.Markdown("""
                1. **Text extraction** from PDF
                2. **Document chunking** into segments
                3. **AI observation extraction** (Gemma3)
                4. **Knowledge graph creation**
                5. **Database generation**
                
                **Time:** 30-120 seconds
                """)
                
                gr.Markdown("### 🤖 LLM Server Health")
                llm_health_output = gr.Markdown(label="LLM Server Status")
                with gr.Row():
                    refresh_llm_btn = gr.Button("🔄 Refresh LLM Status", size="sm")
                    reconnect_llm_btn = gr.Button("🔌 Force Reconnect", size="sm", variant="secondary")
    

    
    with gr.Tab("Download Results"):
        with gr.Row():
            with gr.Column():
                download_job_id = gr.Textbox(label="Enter Job ID")
                download_btn = gr.Button("Download Database")
                download_output = gr.Textbox(label="Download Status")
            
            with gr.Column():
                download_file = gr.File(label="Downloaded Database", visible=False)
    
    with gr.Tab("Merge KuzuDB"):
        with gr.Row():
            with gr.Column(scale=2):
                kuzu_files_input = gr.File(
                    label="Upload KuzuDB Files", 
                    file_types=[".zip"],
                    file_count="multiple"
                )
                merge_btn = gr.Button("🔄 Merge Databases", variant="primary")
                merge_output = gr.Textbox(label="Merge Status", lines=3)
                merged_file = gr.File(label="Download Merged Database", visible=True)
            
            with gr.Column(scale=1):
                gr.Markdown("### Merge Process")
                gr.Markdown("""
                1. **Upload multiple KuzuDB ZIP files**
                2. **Extract data** from each database
                3. **Merge and deduplicate** entities and relationships
                4. **Create consolidated** KuzuDB
                5. **Download merged** database
                
                **Time:** 10-30 seconds
                """)
                
                gr.Markdown("### 📋 Supported Files")
                gr.Markdown("""
                - KuzuDB ZIP files (`.zip`) created by this server
                - Multiple files can be uploaded
                - Each file must be from a different PDF
                - Automatic deduplication
                - Preserves all relationships
                """)
    
    with gr.Tab("🔍 Semantic Search"):
        with gr.Row():
            with gr.Column(scale=2):
                search_kuzu_input = gr.File(
                    label="Upload KuzuDB File", 
                    file_types=[".zip"],
                    file_count="single"
                )
                search_query = gr.Textbox(
                    label="Search Query",
                    placeholder="Enter your search query here...",
                    lines=2
                )
                search_limit = gr.Slider(
                    minimum=1,
                    maximum=50,
                    value=10,
                    step=1,
                    label="Number of Results",
                    info="Maximum number of results to return"
                )
                search_btn = gr.Button("🔍 Search", variant="primary")
                search_output = gr.Textbox(label="Search Results", lines=10)
            
            with gr.Column(scale=1):
                gr.Markdown("### Semantic Search")
                gr.Markdown("""
                1. **Upload a KuzuDB ZIP file**
                2. **Enter your search query**
                3. **AI-powered semantic search** finds relevant observations
                4. **Results ranked by relevance** using vector similarity
                5. **View matching observations** with context
                
                **Time:** 2-5 seconds
                """)
                
                gr.Markdown("### 🔍 How It Works")
                gr.Markdown("""
                - Uses **all-MiniLM-L6-v2** embedding model
                - **384-dimensional vectors** for observation text
                - **Cosine similarity** for relevance ranking
                - **Vector index** for fast search performance
                - Finds **semantically similar** content, not just exact matches
                """)
                
                gr.Markdown("### 💡 Search Tips")
                gr.Markdown("""
                - Use **natural language** queries
                - **Synonyms and related terms** work well
                - **Longer queries** often give better results
                - Results include **observation text** and **source PDF**
                """)
    
    # Event handlers
    process_btn.click(
        upload_and_process_pdf,
        inputs=[pdf_input, model_select],
        outputs=[upload_output, job_id_state, download_file, auto_download_btn]
    )
    
    # Merge KuzuDB event handler
    merge_btn.click(
        merge_kuzu_databases,
        inputs=[kuzu_files_input],
        outputs=[merged_file, merge_output]
    )
    
    # Semantic Search event handler
    search_btn.click(
        perform_semantic_search,
        inputs=[search_kuzu_input, search_query, search_limit],
        outputs=[search_output]
    )
    
    # Also trigger download when job completes
    job_id_state.change(
        auto_download_when_ready,
        inputs=[job_id_state],
        outputs=[auto_download_file, auto_download_output],
        show_progress=False
    )
    
    download_btn.click(
        download_database,
        inputs=[download_job_id],
        outputs=[download_file, download_output]
    )
    
    # LLM Health refresh
    refresh_llm_btn.click(
        get_ollama_server_status,
        inputs=[],
        outputs=[llm_health_output]
    )
    
    # LLM Force reconnect
    reconnect_llm_btn.click(
        force_ollama_reconnect,
        inputs=[],
        outputs=[llm_health_output]
    )
    
    # Auto-refresh LLM health every 30 seconds
    demo.load(
        get_ollama_server_status,
        inputs=[],
        outputs=[llm_health_output]
    )

if __name__ == "__main__":
    demo.queue()  # Enable queue for concurrent users
    demo.launch(server_name="0.0.0.0", server_port=7860, share=False) 
