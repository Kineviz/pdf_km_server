# KuzuDB Merge Tools

This folder contains tools for merging multiple KuzuDB ZIP files into a single consolidated database.

## Files

### Main Tools

- **`merge_kuzu_cli.py`** - Main command-line interface for merging multiple KuzuDB files
- **`merge_csv_and_load.py`** - Core merge functionality (CSV merging and KuzuDB loading)
- **`verify_merged_db.py`** - Verify the contents of a merged KuzuDB

## Usage

### Quick Start

```bash
# Merge multiple KuzuDB files
./merge_kuzu_cli.py file1.zip file2.zip file3.zip -o merged_db

# Keep CSV files for inspection
./merge_kuzu_cli.py file1.zip file2.zip -o merged_db --keep-csv

# Specify custom directories
./merge_kuzu_cli.py file1.zip file2.zip -o merged_db --result-dir my_results --temp-dir my_csvs
```

### Command Line Options

- `input_files` - KuzuDB ZIP files to merge (required)
- `-o, --output` - Output database name (default: merged_kuzu_db)
- `--temp-dir` - Temporary directory for CSV files (default: extracted_csv_data)
- `--result-dir` - Result directory for zipped database (default: result_kuzudb)
- `--keep-csv` - Keep CSV files after merge (default: delete them)

### Workflow

1. **Extract** data from multiple KuzuDB ZIP files to CSV format
2. **Merge** CSV files with deduplication based on primary keys
3. **Load** merged data into a new KuzuDB
4. **Zip** the resulting database
5. **Clean up** temporary files and directories

### Output

- Creates a zipped KuzuDB in the result directory
- Removes temporary CSV files and merged database directory
- Preserves all relationships (HAS_CHUNK, REFERENCE_CHUNK, MENTION)

## Example

```bash
# Merge story and bell_lab databases
./merge_kuzu_cli.py fixtures/km_kuzu_story.zip fixtures/km_kuzu_bell_lab.zip -o merged_kuzu_db

# Result: result_kuzudb/merged_kuzu_db.zip
```

## Architecture

The tools use a CSV-based merge approach for reliability:

1. **Extraction**: KuzuDB → CSV files (nodes and relationships)
2. **Merging**: CSV files → Consolidated CSV files (with deduplication)
3. **Loading**: Consolidated CSV files → New KuzuDB
4. **Packaging**: New KuzuDB → ZIP file

This approach ensures data integrity and handles complex relationship preservation. 