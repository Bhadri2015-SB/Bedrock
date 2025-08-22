"""
Knowledge Base Query Module

This module provides functions for querying an AWS Bedrock Knowledge Base.
"""
import boto3
import json
import os
from typing import Dict, List, Any, Optional, Union
from dotenv import load_dotenv
from s3_uploader import verify_aws_credentials, setup_aws_credentials

# Load environment variables
load_dotenv()


class KnowledgeBaseQuerier:
    """Class for querying AWS Bedrock Knowledge Base"""
    
    def __init__(self, kb_id: str, region=None, verify_ssl=True):
        """
        Initialize with knowledge base ID
        
        Args:
            kb_id: Knowledge base ID
            region: AWS region
            verify_ssl: Whether to verify SSL certificates (set to False to bypass SSL validation errors)
        """
        self.kb_id = kb_id
        self.region = region or os.environ.get('AWS_REGION', 'us-east-1')
        
        # Create session with optional SSL verification
        session = boto3.Session(region_name=self.region)
        self.bedrock_agent_runtime = session.client('bedrock-agent-runtime', 
                                                   region_name=self.region, 
                                                   verify=verify_ssl)
        self.bedrock_runtime = session.client('bedrock-runtime', 
                                             region_name=self.region, 
                                             verify=verify_ssl)
    
    def query_knowledge_base(self, question: str, max_results: int = 5) -> Dict[str, Any]:
        """
        Query the knowledge base for relevant documents
        
        Args:
            question: User question
            max_results: Maximum number of documents to retrieve
            
        Returns:
            Dictionary with question and retrieval results
        """
        
        try:
            response = self.bedrock_agent_runtime.retrieve(
                knowledgeBaseId=self.kb_id,
                retrievalQuery={
                    'text': question
                },
                retrievalConfiguration={
                    'vectorSearchConfiguration': {
                        'numberOfResults': max_results
                    }
                }
            )
            
            return {
                'question': question,
                'results': response['retrievalResults']
            }
            
        except Exception as e:
            print(f"Error querying knowledge base: {str(e)}")
            raise
    
    def query_with_llm_generation(self, 
                              question: str, 
                              max_results: int = 3, 
                              model_id: str = None) -> Dict[str, Any]:
        """
        Query KB and generate response using Llama3.2 70B
        
        Args:
            question: User question
            max_results: Maximum number of documents to retrieve
            model_id: Model ID to use (defaults to Meta Llama3.2 70B)
            
        Returns:
            Dictionary with question, answer, sources, and confidence
        """
        
        # First, retrieve relevant documents
        kb_results = self.query_knowledge_base(question, max_results)
        
        # Check if we have any results
        if not kb_results.get('results'):
            return {
                'question': question,
                'generated_answer': "I couldn't find any relevant information in the knowledge base to answer your question.",
                'source_documents': [],
                'confidence_score': 0.0
            }
        
        # Prepare context from retrieved documents
        context = self._prepare_context(kb_results['results'])
        
        # Generate response using LLM
        llm_response = self._generate_with_llm(question, context, model_id)
        
        # Calculate confidence
        confidence = self._calculate_confidence(kb_results['results'])
        
        return {
            'question': question,
            'generated_answer': llm_response,
            'source_documents': kb_results['results'],
            'confidence_score': confidence
        }
    
    def _prepare_context(self, retrieval_results: List[Dict]) -> str:
        """
        Prepare context from retrieved documents
        
        Args:
            retrieval_results: List of retrieved documents
            
        Returns:
            Formatted context string
        """
        context_parts = []
        
        for i, result in enumerate(retrieval_results, 1):
            content = result['content']['text']
            source = result.get('location', {}).get('s3Location', {}).get('uri', 'Unknown')
            file_name = source.split('/')[-1] if '/' in source else source
            
            context_parts.append(f"Document {i} [Source: {file_name}]\n{content}\n")
        
        return "\n".join(context_parts)
    
    def _generate_with_llm(self, question: str, context: str, model_id: str = None) -> str:
        """
        Generate answer using an LLM model
        
        Args:
            question: User question
            context: Document context
            model_id: Model ID to use (defaults to Meta Llama3.2 70B)
            
        Returns:
            Generated answer
        """
        
        prompt = f"""You are a helpful assistant that answers questions based only on the provided context. 
Be concise and factual. If you don't know the answer or if the information isn't in the context, say so.

Context:
{context}

Question: {question}

Answer:"""
        
        body = {
            "prompt": prompt,
            "max_gen_len": 1000,
            "temperature": 0.1,
            "top_p": 0.9
        }
        
        try:
            # Default model ID
            if not model_id:
                model_id = "meta.llama3-1-70b-instruct-v1:0"
            
            # Check if alternate model ID is specified in environment
            alt_model = os.environ.get("AWS_BEDROCK_LLM_MODEL")
            if alt_model:
                model_id = alt_model
            
            response = self.bedrock_runtime.invoke_model(
                modelId=model_id,
                body=json.dumps(body),
                contentType="application/json"
            )
            
            response_body = json.loads(response['body'].read())
            return response_body.get('generation', '').strip()
            
        except Exception as e:
            print(f"Error generating with LLM: {str(e)}")
            return f"Error generating response: {str(e)}"
    
    def _calculate_confidence(self, results: List[Dict]) -> float:
        """
        Calculate confidence score based on retrieval results
        
        Args:
            results: List of retrieval results
            
        Returns:
            Confidence score between 0 and 1
        """
        if not results:
            return 0.0
        
        # Simple confidence calculation based on scores
        scores = [result.get('score', 0) for result in results]
        
        if not scores:
            return 0.0
            
        # Normalize to 0-1 range
        avg_score = sum(scores) / len(scores)
        return min(max(avg_score, 0.0), 1.0)

    def get_available_models(self) -> List[Dict]:
        """
        Get list of available Bedrock models
        
        Returns:
            List of model information
        """
        try:
            bedrock = boto3.client('bedrock', region_name=self.region)
            response = bedrock.list_foundation_models()
            return response.get('modelSummaries', [])
        except Exception as e:
            print(f"Error getting model list: {str(e)}")
            return []


def interactive_query_mode(kb_id: str, region: str = None, verify_ssl: bool = True):
    """
    Run interactive query mode
    
    Args:
        kb_id: Knowledge base ID
        region: AWS region
        verify_ssl: Whether to verify SSL certificates
    """
    querier = KnowledgeBaseQuerier(kb_id, region, verify_ssl)
    
    print(f"\nInteractive Query Mode (Knowledge Base: {kb_id})")
    print("Type your questions and press Enter. Type 'exit' to quit.")
    print("-" * 80)
    
    while True:
        question = input("\nYour question: ")
        
        if question.lower() in ["exit", "quit", "q"]:
            print("Exiting interactive mode...")
            break
            
        if not question.strip():
            continue
            
        try:
            print("Generating answer...")
            result = querier.query_with_llm_generation(question)
            
            print("\n" + "=" * 80)
            print("ANSWER:")
            print("-" * 80)
            print(result["generated_answer"])
            print("-" * 80)
            
            # Show sources if available
            if result.get("source_documents"):
                print("\nSOURCES:")
                for i, doc in enumerate(result["source_documents"][:3], 1):
                    source = doc.get("location", {}).get("s3Location", {}).get("uri", "Unknown")
                    file_name = source.split("/")[-1] if "/" in source else source
                    score = doc.get("score", 0)
                    print(f"{i}. {file_name} (score: {score:.4f})")
                    
            print(f"\nConfidence Score: {result['confidence_score']:.4f}")
            
        except Exception as e:
            print(f"Error: {str(e)}")


def main():
    """Command-line interface for KB querying"""
    # Load environment variables
    load_dotenv()
    
    import argparse
    parser = argparse.ArgumentParser(description="AWS Bedrock Knowledge Base Query Tool")
    
    # Required arguments
    parser.add_argument("--kb-id", required=True, help="Knowledge base ID")
    
    # Query options
    parser.add_argument("question", nargs="?", help="Question to ask (if not in interactive mode)")
    parser.add_argument("--raw", action="store_true", help="Return raw documents without LLM generation")
    parser.add_argument("--max-results", type=int, default=3, help="Maximum number of results to retrieve")
    parser.add_argument("--interactive", action="store_true", help="Run in interactive query mode")
    parser.add_argument("--model", help="Model ID to use for generation")
    parser.add_argument("--no-verify-ssl", action="store_true", help="Disable SSL certificate verification")
    
    # Global arguments
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"), help="AWS region")
    
    args = parser.parse_args()
    
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
    
    # Create querier object
    verify_ssl = not args.no_verify_ssl
    querier = KnowledgeBaseQuerier(args.kb_id, args.region, verify_ssl)
    
    # Handle interactive mode
    if args.interactive:
        interactive_query_mode(args.kb_id, args.region, verify_ssl)
        return
    
    # Handle single question
    if not args.question:
        print("Error: Please provide a question or use --interactive mode")
        parser.print_help()
        return
    
    # Process query
    try:
        if args.raw:
            # Just return raw documents
            result = querier.query_knowledge_base(args.question, args.max_results)
            print(f"\nFound {len(result['results'])} relevant documents for: {args.question}")
            
            for i, doc in enumerate(result['results'], 1):
                source = doc.get('location', {}).get('s3Location', {}).get('uri', 'Unknown')
                file_name = source.split('/')[-1] if '/' in source else source
                score = doc.get('score', 0)
                content = doc.get('content', {}).get('text', '')
                
                print(f"\n[{i}] {file_name} (Score: {score:.4f})")
                print("-" * 40)
                # Print first 200 characters of content
                print(f"{content[:200]}..." if len(content) > 200 else content)
                
        else:
            # Generate answer with LLM
            result = querier.query_with_llm_generation(
                args.question, 
                args.max_results,
                args.model
            )
            
            print("\nQUESTION:")
            print(result["question"])
            
            print("\nANSWER:")
            print(result["generated_answer"])
            
            print(f"\nConfidence Score: {result['confidence_score']:.4f}")
            
            if result.get("source_documents"):
                print("\nSOURCES:")
                for i, doc in enumerate(result["source_documents"][:3], 1):
                    source = doc.get("location", {}).get("s3Location", {}).get("uri", "Unknown")
                    file_name = source.split("/")[-1] if "/" in source else source
                    score = doc.get("score", 0)
                    print(f"{i}. {file_name} (score: {score:.4f})")
            
    except Exception as e:
        print(f"Error querying knowledge base: {str(e)}")


if __name__ == "__main__":
    main()
