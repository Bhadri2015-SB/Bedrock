# AWS Bedrock Knowledge Base Toolkit

A toolkit for working with AWS Bedrock Knowledge Bases.

## Setup

1. Install requirements:
   ```
   pip install -r requirements.txt
   ```

2. Configure AWS credentials (either run `setup_aws_credentials.py` or use AWS CLI)
   ```
   python setup_aws_credentials.py
   ```

3. Copy `config.env` to `.env` and update with your values
   ```
   cp config.env .env
   ```

## Key Components

- **S3 Document Upload**: `s3_uploader.py`
- **Knowledge Base Ingestion**: `kb_ingestion.py`
- **Knowledge Base Querying**: `kb_query.py`
- **Complete Manager**: `bedrock_kb_manager.py`

## Document Upload to S3

```bash
# Upload a single file
python s3_uploader.py --bucket YOUR-BUCKET --path /path/to/file.txt

# Upload a directory recursively
python s3_uploader.py --bucket YOUR-BUCKET --path /path/to/directory --recursive

# List files in bucket
python s3_uploader.py --bucket YOUR-BUCKET --list
```

## Knowledge Base Ingestion

```bash
# Create a new knowledge base
python kb_ingestion.py create --name "My KB" --bucket YOUR-BUCKET

# List knowledge bases
python kb_ingestion.py list

# Ingest documents from S3
python kb_ingestion.py quickingest --kb-id YOUR-KB-ID --bucket YOUR-BUCKET --wait
```

## Knowledge Base Querying

```bash
# Query with LLM-powered answers
python kb_query.py --kb-id YOUR-KB-ID "What are the key features of AWS Bedrock?"

# Interactive query mode
python kb_query.py --kb-id YOUR-KB-ID --interactive

# Raw document retrieval without LLM
python kb_query.py --kb-id YOUR-KB-ID --raw "What are the key features of AWS Bedrock?"
```

## All-in-One Manager

```bash
# Complete workflow: upload, ingest, and query
python bedrock_kb_manager.py workflow --bucket YOUR-BUCKET --path /path/to/docs --kb-id YOUR-KB-ID --wait

# Create a new KB and ingest documents
python bedrock_kb_manager.py workflow --bucket YOUR-BUCKET --path /path/to/docs --create-kb --kb-name "New KB" --wait

# Interactive query mode
python bedrock_kb_manager.py query --kb-id YOUR-KB-ID --interactive
```

## Example Workflow

1. Upload documents:
   ```bash
   python bedrock_kb_manager.py upload --bucket YOUR-BUCKET --path ./documents --recursive
   ```

2. Create a knowledge base:
   ```bash
   python bedrock_kb_manager.py create --name "My Knowledge Base" --bucket YOUR-BUCKET
   ```

3. Ingest documents:
   ```bash
   python bedrock_kb_manager.py ingest --kb-id YOUR-KB-ID --bucket YOUR-BUCKET --wait
   ```

4. Query the knowledge base:
   ```bash
   python bedrock_kb_manager.py query --kb-id YOUR-KB-ID --interactive
   ```
