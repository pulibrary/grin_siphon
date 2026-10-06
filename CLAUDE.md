# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

The project uses PDM (Python Dependency Manager) for package management and includes these key commands:

```bash
# Run the main orchestrator
pdm orchestrator

# Run the token log viewer
pdm viewer

# Lint with ruff
pdm lint

# Run tests with pytest
pdm test

# Install dependencies
pdm install

# Install with dev dependencies
pdm install -G dev
```

## Architecture Overview

This is a **Kanban-style asynchronous pipeline** for processing encrypted tarballs containing digitized books. The pipeline implements a resilient, loosely coupled workflow where stages communicate via tokens (JSON files) moved through a sequence of directories (buckets).

### Core Components

1. **Pipeline Structure**: Each processing stage watches an input directory for tokens, processes them, and moves them to the next stage's input directory.

2. **Key Classes**:
   - `Manager` (`src/pipeline/manager.py`): High-level coordinator that manages the token bag, ledger, and staging operations
   - `Orchestrator` (`src/pipeline/orchestrator.py`): Launches and manages filter processes
   - `Pipeline` (`src/pipeline/plumbing.py`): Manages bucket directories and token flow
   - `Token` (`src/pipeline/plumbing.py`): Represents a processing unit with barcode and metadata
   - `Secretary` (`src/pipeline/secretary.py`): Manages book selection from the ledger into the token bag
   - `Stager` (`src/pipeline/stager.py`): Moves tokens from bag to pipeline start
   - `BookLedger` (`src/pipeline/book_ledger.py`): Tracks available books and processing status
   - `TokenBag` (`src/pipeline/token_bag.py`): Holds tokens ready for processing

3. **Filter Architecture**: Individual filters (in `src/pipeline/filters/`) are standalone processes that can be run independently. Each filter implements a specific transformation:
   - `Requester`: Initiates conversion requests
   - `Downloader`: Downloads converted files
   - `Decryptor`: Decrypts downloaded tarballs
   - `Uploader`: Uploads processed files to object storage
   - `Cleaner`: Final cleanup operations

4. **Client Modules** (`src/clients/`):
   - Google authentication and Drive API integration
   - GRIN service client for book conversion requests
   - Object storage clients for AWS/Google Cloud

### Configuration

The system is configured via YAML files (see `config-template.yml`):
- **Buckets**: Define the pipeline directory structure
- **Filters**: Configure each processing stage with input/output buckets
- **Global settings**: Polling intervals, credentials, file paths

### Token Flow

1. Books are selected from the ledger into the token bag (`Manager.fill_token_bag()`)
2. Tokens are staged from bag to pipeline start (`Manager.stage()`)
3. Tokens flow through buckets: start → requested → converted → downloaded → decrypted → stored → done
4. Each filter processes tokens from its input bucket and moves them to its output bucket

### Important Patterns

- **Asynchronous Processing**: Each filter runs as an independent process
- **Fault Tolerance**: Pipeline can be restarted at any stage by checking bucket contents
- **Observable State**: Pipeline status can be monitored by examining bucket directories
- **Token-based Communication**: All data passing between stages uses JSON token files

## Testing

Tests are located in the `tests/` directory. The pytest configuration (`pytest.ini`) sets the Python path to `src/` for proper imports.

Run individual test files:
```bash
pytest tests/test_token_bag.py
pytest tests/test_stager.py
```

## Environment Setup

The project expects certain environment variables and configuration files:
- `PIPELINE_CONFIG`: Path to configuration YAML (defaults to `config.yml`)
- Various authentication files for Google services and AWS credentials
- Bucket directories must exist before pipeline execution

## Code Organization

- `src/pipeline/`: Core pipeline logic and orchestration
- `src/pipeline/filters/`: Individual processing stage implementations
- `src/clients/`: External service integrations
- `src/reporters/`: Status reporting and monitoring utilities
- `tests/`: Test suite with test data in `tests/data/`