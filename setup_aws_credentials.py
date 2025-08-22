import os
import boto3
import sys
from dotenv import load_dotenv

def setup_aws_credentials():
    """
    Check and verify AWS credentials.
    Returns True if valid credentials are found.
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Check if credentials are in environment variables
    aws_access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_region = os.environ.get("AWS_REGION")
    
    if not aws_access_key or not aws_secret_key:
        print("AWS credentials not found in environment variables.")
        
        # Prompt user for credentials
        print("\nPlease enter your AWS credentials:")
        aws_access_key = input("AWS Access Key ID: ")
        aws_secret_key = input("AWS Secret Access Key: ")
        aws_region = input("AWS Region (default: us-east-1): ") or "us-east-1"
        
        # Save to .env file
        with open(".env", "a") as env_file:
            env_file.write(f"\n# AWS Credentials\n")
            env_file.write(f"AWS_ACCESS_KEY_ID={aws_access_key}\n")
            env_file.write(f"AWS_SECRET_ACCESS_KEY={aws_secret_key}\n")
            env_file.write(f"AWS_REGION={aws_region}\n")
        
        print("Credentials saved to .env file")
        
        # Set environment variables for current session
        os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_key
        os.environ["AWS_REGION"] = aws_region
    
    # Try to verify the credentials
    try:
        sts = boto3.client('sts', region_name=aws_region or "us-east-1")
        identity = sts.get_caller_identity()
        account_id = identity['Account']
        user_arn = identity['Arn']
        
        print("\nAWS Credentials Verified Successfully!")
        print(f"Account ID: {account_id}")
        print(f"User/Role: {user_arn}")
        
        # Test S3 access - list buckets
        try:
            s3 = boto3.client('s3')
            buckets = s3.list_buckets()
            print(f"\nS3 Access Verified - You have access to {len(buckets['Buckets'])} buckets")
        except Exception as e:
            print(f"\nWarning: Could not list S3 buckets: {str(e)}")
        
        return True
    
    except Exception as e:
        print(f"\nError verifying AWS credentials: {str(e)}")
        print("\nPlease check that your credentials are correct and try again.")
        return False

if __name__ == "__main__":
    print("AWS Credential Setup and Verification")
    print("=====================================")
    
    if setup_aws_credentials():
        print("\nCredentials setup completed successfully!")
    else:
        print("\nCredential setup failed.")
        sys.exit(1)
