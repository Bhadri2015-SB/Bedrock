import boto3
import os
from dotenv import load_dotenv

load_dotenv()

kb_id = os.getenv("AWS_KNOWLEDGE_BASE_ID")
model_arn = os.getenv("AWS_MODEL_ARN")

client = boto3.client("bedrock-agent-runtime",
                      region_name=os.getenv("AWS_REGION"),
                      aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                      aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))

def retrieve_answer(question):
    response = client.retrieve_and_generate(
        input={
            "text": question
        },
        retrieveAndGenerateConfiguration={
            "type":"KNOWLEDGE_BASE",
            "knowledgeBaseConfiguration":{
                "knowledgeBaseId":kb_id,
                "modelArn":model_arn,
                "retrievalConfiguration":{
                    "vectorSearchConfiguration":{
                        "numberOfResults":5
                    }
                },
                "generationConfiguration":{
                    "inferenceConfig":{
                        "textInferenceConfig":{
                            "temperature":0.5,
                            "topP":0.9,
                            "maxTokens":512
                        }
                    }
                },
                "orchestrationConfiguration":{
                    "inferenceConfig":{
                        "textInferenceConfig":{
                            "temperature":0.5,
                            "topP":0.9,
                            "maxTokens":512
                        }
                    }
                }
            }
        }
    )

    return response

response = retrieve_answer("What are the products of troudz?")
print(response['output']['text'])

print("--------------------------------------------------")

print(response)