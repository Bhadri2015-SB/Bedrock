"""
S3 Document Uploader

This module provides functions for uploading documents to an S3 bucket.
"""
import boto3
import os
import mimetypes
from pathlib import Path
import sys
from dotenv import load_dotenv


class S3Uploader:
    def __init__(self, bucket_name, prefix="documents/", region="us-east-1"):
        """Initialize the document uploader with your bucket configuration"""
        self.s3 = boto3.client("s3", region_name=region)
        self.bucket_name = bucket_name
        self.prefix = prefix
        if not self.prefix.endswith('/'):
            self.prefix += '/'

    def upload_file(self, file_path, content_type=None):
        """
        Upload a single file to S3
        
        Args:
            file_path: Path to the file to upload
            content_type: Content type of the file
            
        Returns:
            Tuple of (success, s3_key)
        """
        try:
            file_path = Path(file_path)
            
            # If content type not provided, guess it
            if content_type is None:
                content_type, _ = mimetypes.guess_type(file_path)
                if content_type is None:
                    content_type = "application/octet-stream"
            
            # Create the S3 key (path within the bucket)
            s3_key = f"{self.prefix}{file_path.name}"
            
            # Upload with metadata
            print(f"Uploading {file_path} to s3://{self.bucket_name}/{s3_key}")
            
            # Extra args for content type
            extra_args = {
                "ContentType": content_type,
                "Metadata": {
                    "filename": file_path.name
                }
            }
            
            self.s3.upload_file(
                Filename=str(file_path),
                Bucket=self.bucket_name,
                Key=s3_key,
                ExtraArgs=extra_args
            )
            
            return True, s3_key
            
        except Exception as e:
            print(f"Error uploading {file_path}: {str(e)}")
            return False, None

    def upload_directory(self, dir_path, recursive=True):
        """
        Upload all files in a directory to S3
        
        Args:
            dir_path: Directory path to upload
            recursive: Whether to upload subdirectories
            
        Returns:
            List of uploaded S3 keys
        """
        dir_path = Path(dir_path)
        if not dir_path.is_dir():
            print(f"Error: {dir_path} is not a directory")
            return []
        
        uploaded_files = []
        
        # Walk through the directory
        if recursive:
            all_files = dir_path.rglob('*')
        else:
            all_files = dir_path.glob('*')
            
        # Filter only files (not directories)
        all_files = [f for f in all_files if f.is_file()]
        
        for file_path in all_files:
            success, s3_key = self.upload_file(file_path)
            if success:
                uploaded_files.append(s3_key)
                
        print(f"\nUploaded {len(uploaded_files)} files to S3")
        return uploaded_files
    
    def list_bucket_contents(self):
        """
        List all objects in the bucket with the given prefix
        
        Returns:
            List of S3 keys
        """
        try:
            response = self.s3.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.prefix
            )
            
            if 'Contents' in response:
                files = [item['Key'] for item in response['Contents']]
                return files
            else:
                return []
                
        except Exception as e:
            print(f"Error listing bucket contents: {str(e)}")
            return []


def verify_aws_credentials(region="us-east-1"):
    """
    Verify that AWS credentials are properly configured
    
    Args:
        region: AWS region
        
    Returns:
        Tuple of (success, identity_info)
    """
    try:
        # Try to get caller identity - this will fail if credentials are not set up
        sts = boto3.client('sts', region_name=region)
        identity = sts.get_caller_identity()
        print(f"AWS credentials verified - logged in as: {identity['Arn']}")
        return True, identity
    except Exception as e:
        print("AWS credential error:", str(e))
        print("\nPlease make sure your AWS credentials are properly configured.")
        print("You can set up credentials using one of these methods:")
        print("1. Run 'aws configure' to set up credentials")
        print("2. Set environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        print("3. Create a .env file with AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        print("4. Configure IAM role if running on AWS infrastructure")
        return False, None


def setup_aws_credentials():
    """
    Interactive setup for AWS credentials
    
    Returns:
        Boolean indicating success
    """
    print("Setting up AWS credentials...")
    
    aws_access_key = input("AWS Access Key ID: ")
    aws_secret_key = input("AWS Secret Access Key: ")
    aws_region = input("AWS Region (default: us-east-1): ") or "us-east-1"
    
    # Set environment variables
    os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_key
    os.environ["AWS_REGION"] = aws_region
    
    # Also update or create .env file
    env_file_path = ".env"
    
    try:
        # Read existing .env file
        env_content = {}
        if os.path.exists(env_file_path):
            with open(env_file_path, "r") as f:
                for line in f:
                    if "=" in line and not line.startswith("#"):
                        key, value = line.strip().split("=", 1)
                        env_content[key] = value
        
        # Update with new credentials
        env_content["AWS_ACCESS_KEY_ID"] = aws_access_key
        env_content["AWS_SECRET_ACCESS_KEY"] = aws_secret_key
        env_content["AWS_REGION"] = aws_region
        
        # Write back to .env file
        with open(env_file_path, "w") as f:
            for key, value in env_content.items():
                f.write(f"{key}={value}\n")
        
        print(f"Credentials saved to {env_file_path}")
        
        # Verify credentials
        return verify_aws_credentials(aws_region)[0]
        
    except Exception as e:
        print(f"Error saving credentials: {str(e)}")
        return False


def main():
    """Command-line interface for uploading documents to S3"""
    # Load environment variables from .env file if present
    load_dotenv()
    
    import argparse
    parser = argparse.ArgumentParser(description="Upload documents to S3 for AWS Bedrock Knowledge Base")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--prefix", default="documents/", help="S3 prefix/folder")
    parser.add_argument("--path", required=True, help="Local file or directory path to upload")
    parser.add_argument("--recursive", action="store_true", help="Upload directory recursively")
    parser.add_argument("--list", action="store_true", help="List existing files in bucket")
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"), help="AWS region name")
    parser.add_argument("--setup", action="store_true", help="Set up AWS credentials")
    
    args = parser.parse_args()
    
    # Handle setup request
    if args.setup:
        if setup_aws_credentials():
            print("AWS credentials set up successfully")
        else:
            print("Failed to set up AWS credentials")
            sys.exit(1)
    
    # Verify AWS credentials before proceeding
    verified, _ = verify_aws_credentials(args.region)
    if not verified:
        setup = input("Would you like to set up AWS credentials now? (y/n): ")
        if setup.lower() == 'y':
            if not setup_aws_credentials():
                print("Failed to set up AWS credentials. Exiting.")
                sys.exit(1)
        else:
            sys.exit(1)
    
    try:
        print(f"Using AWS region: {args.region}")
        uploader = S3Uploader(args.bucket, args.prefix, args.region)
        
        if args.list:
            files = uploader.list_bucket_contents()
            print(f"\nFiles in s3://{args.bucket}/{args.prefix}:")
            for file in files:
                print(f" - {file}")
        
        if args.path:
            path = Path(args.path)
            if path.is_dir():
                uploader.upload_directory(path, recursive=args.recursive)
            elif path.is_file():
                uploader.upload_file(path)
            else:
                print(f"Error: Path {path} not found")
    
    except boto3.exceptions.NoCredentialsError:
        print("Error: AWS credentials not found")
        sys.exit(1)
    except boto3.exceptions.NoRegionError:
        print("Error: AWS region not specified")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
