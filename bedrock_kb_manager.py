"""
Main script for AWS Bedrock Knowledge Base Operations

This script provides a unified interface for:
1. Uploading documents to S3
2. Ingesting documents into a knowledge base
3. Querying the knowledge base
"""
import os
import argparse
from pathlib import Path
from dotenv import load_dotenv
import time

# Import our modules
from s3_uploader import S3Uploader, verify_aws_credentials, setup_aws_credentials
from kb_ingestion import KnowledgeBaseIngestor
from kb_query import KnowledgeBaseQuerier, interactive_query_mode

# Load environment variables
load_dotenv()


def main():
    """Main function to parse arguments and perform operations"""
    parser = argparse.ArgumentParser(description="AWS Bedrock Knowledge Base Operations")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Upload documents command
    upload_parser = subparsers.add_parser("upload", help="Upload documents to S3")
    upload_parser.add_argument("--bucket", required=True, help="S3 bucket name")
    upload_parser.add_argument("--prefix", default="documents/", help="S3 prefix/folder")
    upload_parser.add_argument("--path", required=True, help="Local file or directory path to upload")
    upload_parser.add_argument("--recursive", action="store_true", help="Upload directory recursively")
    upload_parser.add_argument("--list", action="store_true", help="List existing files in bucket")
    
    # Create knowledge base command
    create_parser = subparsers.add_parser("create", help="Create a new knowledge base")
    create_parser.add_argument("--name", required=True, help="Knowledge base name")
    create_parser.add_argument("--bucket", required=True, help="S3 bucket name")
    create_parser.add_argument("--prefix", default="documents/", help="S3 prefix/folder")
    
    # List knowledge bases command
    list_kb_parser = subparsers.add_parser("list-kb", help="List knowledge bases")
    
    # Ingest documents command
    ingest_parser = subparsers.add_parser("ingest", help="Ingest documents into knowledge base")
    ingest_parser.add_argument("--kb-id", required=True, help="Knowledge base ID")
    ingest_parser.add_argument("--bucket", required=True, help="S3 bucket name")
    ingest_parser.add_argument("--prefix", default="documents/", help="S3 prefix/folder")
    ingest_parser.add_argument("--wait", action="store_true", help="Wait for ingestion to complete")
    
    # Query knowledge base command
    query_parser = subparsers.add_parser("query", help="Query knowledge base")
    query_parser.add_argument("--kb-id", required=True, help="Knowledge base ID")
    query_parser.add_argument("question", nargs="?", help="Question to ask")
    query_parser.add_argument("--raw", action="store_true", help="Return raw documents without LLM generation")
    query_parser.add_argument("--max-results", type=int, default=3, help="Maximum number of results to retrieve")
    query_parser.add_argument("--interactive", action="store_true", help="Run in interactive query mode")
    
    # Complete workflow command (upload, ingest, and query in one)
    workflow_parser = subparsers.add_parser("workflow", help="Complete workflow: upload, ingest, and query")
    workflow_parser.add_argument("--kb-id", help="Knowledge base ID (required if not creating a new KB)")
    workflow_parser.add_argument("--bucket", required=True, help="S3 bucket name")
    workflow_parser.add_argument("--path", required=True, help="Local file or directory path to upload")
    workflow_parser.add_argument("--create-kb", action="store_true", help="Create a new knowledge base")
    workflow_parser.add_argument("--kb-name", help="Name for the new knowledge base")
    workflow_parser.add_argument("--question", help="Question to ask after ingestion")
    workflow_parser.add_argument("--wait", action="store_true", help="Wait for ingestion to complete")
    
    # Setup command
    setup_parser = subparsers.add_parser("setup", help="Set up AWS credentials")
    
    # Global arguments
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"), help="AWS region")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Check if command specified
    if not args.command:
        parser.print_help()
        return
    
    # Handle setup command
    if args.command == "setup":
        success = setup_aws_credentials()
        if success:
            print("AWS credentials set up successfully")
        else:
            print("Failed to set up AWS credentials")
        return
    
    # Verify credentials for all other commands
    if args.command != "setup":
        verified, _ = verify_aws_credentials(args.region)
        if not verified:
            setup = input("Would you like to set up AWS credentials now? (y/n): ")
            if setup.lower() == 'y':
                if not setup_aws_credentials():
                    print("Failed to set up AWS credentials. Exiting.")
                    return
            else:
                return
    
    # Process commands
    if args.command == "upload":
        handle_upload_command(args)
    elif args.command == "create":
        handle_create_command(args)
    elif args.command == "list-kb":
        handle_list_kb_command(args)
    elif args.command == "ingest":
        handle_ingest_command(args)
    elif args.command == "query":
        handle_query_command(args)
    elif args.command == "workflow":
        handle_workflow_command(args)


def handle_upload_command(args):
    """Handle upload command"""
    uploader = S3Uploader(args.bucket, args.prefix, args.region)
    
    if args.list:
        files = uploader.list_bucket_contents()
        print(f"\nFiles in s3://{args.bucket}/{args.prefix}:")
        for file in files:
            print(f" - {file}")
    
    if args.path:
        path = Path(args.path)
        if path.is_dir():
            print(f"Uploading directory {path} to S3...")
            uploaded = uploader.upload_directory(path, recursive=args.recursive)
            print(f"Uploaded {len(uploaded)} files to S3")
        elif path.is_file():
            print(f"Uploading file {path} to S3...")
            success, s3_key = uploader.upload_file(path)
            if success:
                print(f"Successfully uploaded to {s3_key}")
            else:
                print(f"Failed to upload {path}")
        else:
            print(f"Error: Path {path} not found")


def handle_create_command(args):
    """Handle create knowledge base command"""
    ingestor = KnowledgeBaseIngestor(args.region)
    
    kb_id = ingestor.create_knowledge_base(
        kb_name=args.name,
        s3_bucket=args.bucket,
        s3_prefix=args.prefix
    )
    
    print(f"\n✅ Knowledge Base created successfully!")
    print(f"📝 Knowledge Base ID: {kb_id}")
    print(f"📁 S3 Bucket: {args.bucket}/{args.prefix}")
    print(f"🔗 Add this to your .env file:")
    print(f"AWS_KNOWLEDGE_BASE_ID={kb_id}")
    print(f"AWS_S3_BUCKET={args.bucket}")


def handle_list_kb_command(args):
    """Handle list knowledge bases command"""
    ingestor = KnowledgeBaseIngestor(args.region)
    kbs = ingestor.list_knowledge_bases()
    
    if not kbs:
        print("No knowledge bases found in this account/region.")
        return
    
    print(f"Found {len(kbs)} knowledge bases:")
    for i, kb in enumerate(kbs, 1):
        print(f"\n{i}. ID: {kb['knowledgeBaseId']}")
        print(f"   Name: {kb['name']}")
        print(f"   Status: {kb['status']}")
        if 'createdAt' in kb:
            print(f"   Created: {kb['createdAt'].strftime('%Y-%m-%d %H:%M:%S')}")


def handle_ingest_command(args):
    """Handle ingest command"""
    ingestor = KnowledgeBaseIngestor(args.region)
    
    # Create data source name with timestamp
    ds_name = f"s3-source-{int(time.time())}"
    
    print(f"Adding data source for S3 bucket {args.bucket}/{args.prefix}")
    ds_id = ingestor.add_s3_data_source(
        kb_id=args.kb_id,
        name=ds_name,
        s3_bucket=args.bucket,
        s3_prefix=args.prefix
    )
    print(f"Created data source with ID: {ds_id}")
    
    print("Starting ingestion job...")
    job_id = ingestor.start_ingestion(args.kb_id, ds_id)
    print(f"Ingestion job started with ID: {job_id}")
    
    if args.wait:
        ingestor.wait_for_ingestion(args.kb_id, ds_id, job_id)
    else:
        print("\nIngestion job is running in the background.")
        print("To monitor progress, run:")
        print(f"python kb_ingestion.py monitor --kb-id {args.kb_id} --ds-id {ds_id} --job-id {job_id}")


def handle_query_command(args):
    """Handle query command"""
    if args.interactive:
        interactive_query_mode(args.kb_id, args.region)
        return
    
    if not args.question:
        print("Error: Please provide a question or use --interactive mode")
        return
    
    querier = KnowledgeBaseQuerier(args.kb_id, args.region)
    
    try:
        if args.raw:
            # Just retrieve relevant documents
            print("Retrieving documents...")
            result = querier.query_knowledge_base(args.question, args.max_results)
            
            if not result.get('results'):
                print("No relevant documents found for this question.")
                return
            
            print(f"\nFound {len(result['results'])} relevant documents:")
            for i, doc in enumerate(result['results'], 1):
                source = doc.get('location', {}).get('s3Location', {}).get('uri', 'Unknown')
                file_name = source.split('/')[-1] if '/' in source else source
                content = doc.get('content', {}).get('text', '')
                
                print(f"\n[{i}] {file_name}")
                print(f"Score: {doc.get('score', 0):.4f}")
                print(f"Content snippet: {content[:200]}...")
        else:
            # Generate answer with LLM
            print("Querying knowledge base and generating answer...")
            result = querier.query_with_llm_generation(args.question, args.max_results)
            
            print("\n" + "=" * 80)
            print(f"Question: {result['question']}")
            print("-" * 80)
            print(f"Answer: {result['generated_answer']}")
            print("-" * 80)
            print(f"Confidence: {result['confidence_score']:.4f}")
            
            if result.get('source_documents'):
                print(f"\nBased on {len(result['source_documents'])} documents:")
                for i, doc in enumerate(result['source_documents'][:3], 1):
                    source = doc.get('location', {}).get('s3Location', {}).get('uri', 'Unknown')
                    file_name = source.split('/')[-1] if '/' in source else source
                    print(f"  {i}. {file_name}")
    
    except Exception as e:
        print(f"Error querying knowledge base: {str(e)}")


def handle_workflow_command(args):
    """Handle complete workflow command"""
    # Step 1: Upload documents
    print("Step 1: Uploading documents...")
    uploader = S3Uploader(args.bucket, "documents/", args.region)
    
    path = Path(args.path)
    if path.is_dir():
        print(f"Uploading directory {path} to S3...")
        uploaded = uploader.upload_directory(path, recursive=True)
        print(f"Uploaded {len(uploaded)} files to S3")
    elif path.is_file():
        print(f"Uploading file {path} to S3...")
        success, s3_key = uploader.upload_file(path)
        if success:
            print(f"Successfully uploaded to {s3_key}")
        else:
            print(f"Failed to upload {path}")
            return
    else:
        print(f"Error: Path {path} not found")
        return
    
    # Step 2: Create KB or use existing
    kb_id = args.kb_id
    ingestor = KnowledgeBaseIngestor(args.region)
    
    if args.create_kb:
        print("\nStep 2: Creating knowledge base...")
        kb_name = args.kb_name or f"KB-{int(time.time())}"
        kb_id = ingestor.create_knowledge_base(
            kb_name=kb_name,
            s3_bucket=args.bucket,
            s3_prefix="documents/"
        )
        print(f"Created knowledge base with ID: {kb_id}")
    else:
        if not kb_id:
            kb_id = os.environ.get("AWS_KNOWLEDGE_BASE_ID")
            
        if not kb_id:
            print("Error: No knowledge base ID provided. Use --kb-id or --create-kb")
            return
            
        print(f"\nStep 2: Using existing knowledge base: {kb_id}")
    
    # Step 3: Ingest documents
    print("\nStep 3: Ingesting documents...")
    ds_name = f"s3-source-{int(time.time())}"
    ds_id = ingestor.add_s3_data_source(
        kb_id=kb_id,
        name=ds_name,
        s3_bucket=args.bucket,
        s3_prefix="documents/"
    )
    print(f"Created data source with ID: {ds_id}")
    
    job_id = ingestor.start_ingestion(kb_id, ds_id)
    print(f"Started ingestion job with ID: {job_id}")
    
    if args.wait:
        ingestor.wait_for_ingestion(kb_id, ds_id, job_id)
        if args.question:
            # Step 4: Query KB
            print("\nStep 4: Querying knowledge base...")
            querier = KnowledgeBaseQuerier(kb_id, args.region)
            result = querier.query_with_llm_generation(args.question)
            
            print("\n" + "=" * 80)
            print(f"Question: {result['question']}")
            print("-" * 80)
            print(f"Answer: {result['generated_answer']}")
            print("-" * 80)
            print(f"Confidence: {result['confidence_score']:.4f}")
    else:
        print("\nIngestion job is running in the background.")
        print("To monitor progress, run:")
        print(f"python kb_ingestion.py monitor --kb-id {kb_id} --ds-id {ds_id} --job-id {job_id}")
        
    # Final output
    print("\n✅ Workflow completed!")
    print(f"Knowledge Base ID: {kb_id}")
    print(f"S3 Bucket: {args.bucket}")
    print("To query, run:")
    print(f"python bedrock_kb_manager.py query --kb-id {kb_id} --interactive")


if __name__ == "__main__":
    main()
