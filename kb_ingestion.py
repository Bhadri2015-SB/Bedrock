"""
Knowledge Base Ingestion Module

This module provides functions for ingesting documents into an AWS Bedrock Knowledge Base.
"""
import boto3
import json
import time
import os
from typing import Dict, List, Any, Optional, Union
from dotenv import load_dotenv
from s3_uploader import verify_aws_credentials, setup_aws_credentials

# Load environment variables
load_dotenv()


class KnowledgeBaseIngestor:
    """Class for ingesting documents into AWS Bedrock Knowledge Base"""
    
    def __init__(self, region=None):
        """
        Initialize with AWS region
        
        Args:
            region: AWS region
        """
        self.region = region or os.environ.get('AWS_REGION', 'us-east-1')
        self.bedrock_agent = boto3.client('bedrock-agent', region_name=self.region)
        self.s3 = boto3.client('s3', region_name=self.region)
        
    def create_knowledge_base(self, 
                            kb_name: str,
                            s3_bucket: str,
                            s3_prefix: str = "documents/",
                            vector_store_name: str = None,
                            embedding_model: str = "amazon.titan-embed-text-v1") -> str:
        """
        Create a new AWS Bedrock Knowledge Base
        
        Args:
            kb_name: Name for the knowledge base
            s3_bucket: S3 bucket containing documents
            s3_prefix: Prefix/folder in S3 bucket
            vector_store_name: Optional name for vector store
            embedding_model: Embedding model to use
            
        Returns:
            Knowledge Base ID
        """
        
        print(f"Creating knowledge base: {kb_name}")
        print(f"Using S3 bucket: {s3_bucket}/{s3_prefix}")
        
        if not vector_store_name:
            vector_store_name = f"{kb_name}-vectors"
        
        # Create vector collection for storage
        collection_arn = self._create_vector_collection(vector_store_name)
        print(f"Created vector collection: {vector_store_name}")
        
        # Get or create IAM role
        role_arn = self._get_or_create_kb_role()
        print(f"Using IAM role: {role_arn}")
        
        # Configuration for Knowledge Base
        kb_config = {
            "name": kb_name,
            "description": f"Knowledge Base for {kb_name} documents",
            "roleArn": role_arn,
            "knowledgeBaseConfiguration": {
                "type": "VECTOR",
                "vectorKnowledgeBaseConfiguration": {
                    "embeddingModelArn": f"arn:aws:bedrock:{self.region}::foundation-model/{embedding_model}"
                }
            },
            "storageConfiguration": {
                "type": "OPENSEARCH_SERVERLESS",
                "opensearchServerlessConfiguration": {
                    "collectionArn": collection_arn,
                    "vectorIndexName": f"{kb_name}-index",
                    "fieldMapping": {
                        "vectorField": "vector",
                        "textField": "text",
                        "metadataField": "metadata"
                    }
                }
            }
        }
        
        try:
            response = self.bedrock_agent.create_knowledge_base(**kb_config)
            kb_id = response['knowledgeBase']['knowledgeBaseId']
            print(f"Knowledge Base created with ID: {kb_id}")
            
            return kb_id
            
        except Exception as e:
            print(f"Error creating Knowledge Base: {str(e)}")
            raise
    
    def list_knowledge_bases(self) -> List[Dict]:
        """
        List all knowledge bases in the account
        
        Returns:
            List of knowledge base details
        """
        try:
            response = self.bedrock_agent.list_knowledge_bases()
            return response.get('knowledgeBaseSummaries', [])
        except Exception as e:
            print(f"Error listing knowledge bases: {str(e)}")
            return []
    
    def get_knowledge_base(self, kb_id: str) -> Dict:
        """
        Get details about a specific knowledge base
        
        Args:
            kb_id: Knowledge base ID
            
        Returns:
            Knowledge base details
        """
        try:
            response = self.bedrock_agent.get_knowledge_base(knowledgeBaseId=kb_id)
            return response.get('knowledgeBase', {})
        except Exception as e:
            print(f"Error getting knowledge base details: {str(e)}")
            return {}
    
    def add_s3_data_source(self, kb_id: str, name: str, s3_bucket: str, s3_prefix: str = "documents/") -> str:
        """
        Add S3 data source to knowledge base
        
        Args:
            kb_id: Knowledge base ID
            name: Data source name
            s3_bucket: S3 bucket name
            s3_prefix: S3 prefix/folder
            
        Returns:
            Data source ID
        """
        # Ensure prefix ends with /
        if not s3_prefix.endswith('/'):
            s3_prefix += '/'
        
        data_source_config = {
            "knowledgeBaseId": kb_id,
            "name": f"{name}-s3-source",
            "description": f"S3 data source for {name}",
            "dataSourceConfiguration": {
                "type": "S3",
                "s3Configuration": {
                    "bucketArn": f"arn:aws:s3:::{s3_bucket}",
                    "inclusionPrefixes": [s3_prefix]
                }
            }
        }
        
        try:
            response = self.bedrock_agent.create_data_source(**data_source_config)
            ds_id = response['dataSource']['dataSourceId']
            return ds_id
            
        except Exception as e:
            print(f"Error creating data source: {str(e)}")
            raise
    
    def start_ingestion(self, kb_id: str, ds_id: str) -> str:
        """
        Start ingestion job for a data source
        
        Args:
            kb_id: Knowledge base ID
            ds_id: Data source ID
            
        Returns:
            Ingestion job ID
        """
        try:
            response = self.bedrock_agent.start_ingestion_job(
                knowledgeBaseId=kb_id,
                dataSourceId=ds_id
            )
            
            job_id = response['ingestionJob']['ingestionJobId']
            return job_id
                
        except Exception as e:
            print(f"Error starting ingestion job: {str(e)}")
            raise
    
    def get_ingestion_job_status(self, kb_id: str, ds_id: str, job_id: str) -> Dict:
        """
        Get status of an ingestion job
        
        Args:
            kb_id: Knowledge base ID
            ds_id: Data source ID
            job_id: Ingestion job ID
            
        Returns:
            Job status details
        """
        try:
            response = self.bedrock_agent.get_ingestion_job(
                knowledgeBaseId=kb_id,
                dataSourceId=ds_id,
                ingestionJobId=job_id
            )
            return response.get('ingestionJob', {})
        except Exception as e:
            print(f"Error getting ingestion job status: {str(e)}")
            return {}
    
    def list_data_sources(self, kb_id: str) -> List[Dict]:
        """
        List all data sources for a knowledge base
        
        Args:
            kb_id: Knowledge base ID
            
        Returns:
            List of data sources
        """
        try:
            response = self.bedrock_agent.list_data_sources(knowledgeBaseId=kb_id)
            return response.get('dataSourceSummaries', [])
        except Exception as e:
            print(f"Error listing data sources: {str(e)}")
            return []
    
    def wait_for_ingestion(self, kb_id: str, ds_id: str, job_id: str, max_wait_seconds=600) -> Dict:
        """
        Wait for ingestion job to complete
        
        Args:
            kb_id: Knowledge base ID
            ds_id: Data source ID
            job_id: Ingestion job ID
            max_wait_seconds: Maximum wait time in seconds
            
        Returns:
            Final job status
        """
        start_time = time.time()
        previous_status = None
        
        while True:
            job_info = self.get_ingestion_job_status(kb_id, ds_id, job_id)
            status = job_info.get("status")
            
            if not status:
                print("Error: Could not retrieve job status")
                return {"status": "ERROR"}
            
            # Only print if status changed
            if status != previous_status:
                elapsed_time = time.time() - start_time
                print(f"[{int(elapsed_time)}s] Ingestion status: {status}")
                previous_status = status
            
            # Print statistics if available
            if "statistics" in job_info:
                stats = job_info["statistics"]
                processed = stats.get("numberOfDocumentsProcessed", 0)
                failed = stats.get("numberOfDocumentsFailed", 0)
                pending = stats.get("numberOfDocumentsPending", 0)
                
                print(f"  Documents: {processed} processed, {failed} failed, {pending} pending")
            
            # Check if finished
            if status in ["COMPLETE", "FAILED", "STOPPED"]:
                if status == "COMPLETE":
                    print("\n✅ Ingestion job completed successfully!")
                elif status == "FAILED":
                    print("\n❌ Ingestion job failed!")
                    if "failureReasons" in job_info:
                        print("Failure reasons:")
                        for reason in job_info["failureReasons"]:
                            print(f"  - {reason}")
                else:
                    print("\n⚠️ Ingestion job was stopped.")
                
                return job_info
            
            # Check timeout
            if (time.time() - start_time) > max_wait_seconds:
                print(f"\n⚠️ Reached maximum wait time of {max_wait_seconds} seconds")
                print("Ingestion job is still running in the background")
                return job_info
            
            # Wait before checking again
            time.sleep(10)
    
    def _create_vector_collection(self, collection_name: str) -> str:
        """
        Create OpenSearch Serverless collection for vector storage
        
        Args:
            collection_name: Name for the collection
            
        Returns:
            Collection ARN
        """
        opensearch = boto3.client('opensearchserverless', region_name=self.region)
        
        try:
            # Check if collection already exists
            try:
                existing = opensearch.describe_collection(id=collection_name)
                return existing['collectionDetail']['arn']
            except:
                pass  # Collection doesn't exist, create it
                
            response = opensearch.create_collection(
                name=collection_name,
                type='VECTORSEARCH',
                description=f"Vector collection for {collection_name}"
            )
            
            collection_arn = response['createCollectionDetail']['arn']
            
            print("Waiting for OpenSearch Serverless collection to be active...")
            waiter_time = 0
            while True:
                status = opensearch.describe_collection(id=collection_name)
                current_status = status['collectionDetail']['status']
                print(f"Collection status: {current_status} (waited {waiter_time}s)")
                
                if current_status == 'ACTIVE':
                    break
                    
                time.sleep(30)
                waiter_time += 30
                
                # Timeout after 20 minutes
                if waiter_time > 1200:  # 20 minutes in seconds
                    print("Warning: Timed out waiting for collection to be active")
                    break
            
            return collection_arn
            
        except Exception as e:
            print(f"Error creating vector collection: {str(e)}")
            raise
    
    def _get_or_create_kb_role(self) -> str:
        """
        Get or create IAM role for Knowledge Base
        
        Returns:
            IAM role ARN
        """
        iam = boto3.client('iam')
        role_name = "AmazonBedrockExecutionRoleForKnowledgeBase"
        
        try:
            response = iam.get_role(RoleName=role_name)
            return response['Role']['Arn']
        except:
            # Create role if doesn't exist
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "bedrock.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
            
            role_response = iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="Execution role for Amazon Bedrock Knowledge Base"
            )
            
            # Attach necessary policies
            iam.attach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/AmazonBedrockFullAccess'
            )
            
            # Also attach S3 read access
            iam.attach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
            )
            
            return role_response['Role']['Arn']


def main():
    """Command-line interface for KB ingestion"""
    # Load environment variables
    load_dotenv()
    
    import argparse
    parser = argparse.ArgumentParser(description="AWS Bedrock Knowledge Base Ingestion")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Create KB command
    create_parser = subparsers.add_parser("create", help="Create a new knowledge base")
    create_parser.add_argument("--name", required=True, help="Knowledge base name")
    create_parser.add_argument("--bucket", required=True, help="S3 bucket name")
    create_parser.add_argument("--prefix", default="documents/", help="S3 prefix/folder")
    
    # List KBs command
    list_parser = subparsers.add_parser("list", help="List knowledge bases")
    
    # Add data source command
    datasource_parser = subparsers.add_parser("add-source", help="Add S3 data source")
    datasource_parser.add_argument("--kb-id", required=True, help="Knowledge base ID")
    datasource_parser.add_argument("--name", required=True, help="Data source name")
    datasource_parser.add_argument("--bucket", required=True, help="S3 bucket name")
    datasource_parser.add_argument("--prefix", default="documents/", help="S3 prefix/folder")
    
    # Ingest command
    ingest_parser = subparsers.add_parser("ingest", help="Start ingestion job")
    ingest_parser.add_argument("--kb-id", required=True, help="Knowledge base ID")
    ingest_parser.add_argument("--ds-id", required=True, help="Data source ID")
    ingest_parser.add_argument("--wait", action="store_true", help="Wait for ingestion to complete")
    
    # Monitor ingestion command
    monitor_parser = subparsers.add_parser("monitor", help="Monitor ingestion job")
    monitor_parser.add_argument("--kb-id", required=True, help="Knowledge base ID")
    monitor_parser.add_argument("--ds-id", required=True, help="Data source ID")
    monitor_parser.add_argument("--job-id", required=True, help="Ingestion job ID")
    monitor_parser.add_argument("--timeout", type=int, default=600, help="Maximum wait time in seconds")
    
    # Quick ingest command (all in one)
    quickingest_parser = subparsers.add_parser("quickingest", help="Quick ingest from S3")
    quickingest_parser.add_argument("--kb-id", required=True, help="Knowledge base ID")
    quickingest_parser.add_argument("--bucket", required=True, help="S3 bucket name")
    quickingest_parser.add_argument("--prefix", default="documents/", help="S3 prefix/folder")
    quickingest_parser.add_argument("--wait", action="store_true", help="Wait for ingestion to complete")
    
    # Global arguments
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"), help="AWS region")
    
    args = parser.parse_args()
    
    # Check if command specified
    if not args.command:
        parser.print_help()
        return
    
    # Verify credentials
    verified, _ = verify_aws_credentials(args.region)
    if not verified:
        setup = input("Would you like to set up AWS credentials now? (y/n): ")
        if setup.lower() == 'y':
            if not setup_aws_credentials():
                print("Failed to set up AWS credentials. Exiting.")
                return
        else:
            return
    
    # Create ingestor object
    ingestor = KnowledgeBaseIngestor(args.region)
    
    # Process commands
    if args.command == "create":
        kb_id = ingestor.create_knowledge_base(
            kb_name=args.name,
            s3_bucket=args.bucket,
            s3_prefix=args.prefix
        )
        print(f"\nKnowledge Base created: {kb_id}")
        print("Add this to your .env file:")
        print(f"AWS_KNOWLEDGE_BASE_ID={kb_id}")
        
    elif args.command == "list":
        kbs = ingestor.list_knowledge_bases()
        print(f"Found {len(kbs)} knowledge bases:")
        for kb in kbs:
            print(f"ID: {kb['knowledgeBaseId']} - Name: {kb['name']} - Status: {kb['status']}")
            
    elif args.command == "add-source":
        ds_id = ingestor.add_s3_data_source(
            kb_id=args.kb_id,
            name=args.name,
            s3_bucket=args.bucket,
            s3_prefix=args.prefix
        )
        print(f"Created data source with ID: {ds_id}")
        print(f"To start ingestion, run:")
        print(f"python kb_ingestion.py ingest --kb-id {args.kb_id} --ds-id {ds_id}")
        
    elif args.command == "ingest":
        job_id = ingestor.start_ingestion(args.kb_id, args.ds_id)
        print(f"Started ingestion job: {job_id}")
        
        if args.wait:
            ingestor.wait_for_ingestion(args.kb_id, args.ds_id, job_id)
        else:
            print(f"To monitor progress, run:")
            print(f"python kb_ingestion.py monitor --kb-id {args.kb_id} --ds-id {args.ds_id} --job-id {job_id}")
            
    elif args.command == "monitor":
        ingestor.wait_for_ingestion(args.kb_id, args.ds_id, args.job_id, args.timeout)
        
    elif args.command == "quickingest":
        print(f"Quick ingest from S3 bucket {args.bucket}/{args.prefix} into KB {args.kb_id}")
        
        # Generate a unique data source name
        ds_name = f"s3-source-{int(time.time())}"
        
        # Add data source
        ds_id = ingestor.add_s3_data_source(
            kb_id=args.kb_id,
            name=ds_name,
            s3_bucket=args.bucket,
            s3_prefix=args.prefix
        )
        print(f"Created data source with ID: {ds_id}")
        
        # Start ingestion
        job_id = ingestor.start_ingestion(args.kb_id, ds_id)
        print(f"Started ingestion job: {job_id}")
        
        # Wait if requested
        if args.wait:
            ingestor.wait_for_ingestion(args.kb_id, ds_id, job_id)
        else:
            print(f"To monitor progress, run:")
            print(f"python kb_ingestion.py monitor --kb-id {args.kb_id} --ds-id {ds_id} --job-id {job_id}")


if __name__ == "__main__":
    main()
