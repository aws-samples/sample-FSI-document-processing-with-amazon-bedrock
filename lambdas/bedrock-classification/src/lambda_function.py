
import json
import boto3
import os

s3 = boto3.client('s3')

# Bedrock Runtime client used to invoke and question the models
client = boto3.client("bedrock-runtime", region_name="us-east-1")

model_Id = "amazon.nova-lite-v1:0"
 

def lambda_handler(event, context):
    # Retrieve the S3 bucket name and folder (prefix) from the event
    bucket_name = event['bucket_name']
    prefix = event['prefix']
    
    # List all objects in the specified S3 folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = response.get('Contents', [])
    files = [file for file in files if file['Key'] != prefix]

    classification_results = []

    for obj in files:
        key = obj['Key']        
        if key.endswith('.json'):
            continue

        # Get the content of the file
        file_obj = s3.get_object(Bucket=bucket_name, Key=key)
        file_content = file_obj['Body'].read().decode('utf-8')
        print(file_content)
        
        # Define your system prompt(s).
        system_list = [{"text": "Your function is to read the contents of a PDF file, and determine if the file is an Auto Insurance Document. Answer with True or False"}]

        message_list = [{"role": "user", "content": [{"text": f'{file_content}'}]}]
        print(message_list)

        inf_params = {"maxTokens": 500, "topP": 0.9, "topK": 20, "temperature": 0.7}

        request_body = {
            "schemaVersion": "messages-v1",
            "messages": message_list,
            "system": system_list,
            "inferenceConfig": inf_params
        }

        try:
            response = client.invoke_model(
                modelId=model_Id, body=json.dumps(request_body))
            response_body = json.loads(response.get('body').read())
            print("Response body:", json.dumps(response_body, indent=2))  # Add this line for debugging
        except Exception as e:
            print(f"Error invoking model: {e}")
            continue

        # Update this section to handle potential changes in response structure
        if 'output' in response_body and 'message' in response_body['output']:
            answer = response_body['output']['message']['content'][0]['text']
        elif 'completions' in response_body and response_body['completions']:
            answer = response_body['completions'][0].get('data', {}).get('text', '')
        else:
            print("Unexpected response structure:", json.dumps(response_body, indent=2))
            continue

        is_claims_document = False
                    
        if answer and "true" in answer.lower():
            is_claims_document = True


        # Collect the result
        classification_results.append({
            'file': key,
            'is_claims_document': is_claims_document
        })

    return {
        'statusCode': 200,
        'classificationResults': classification_results
    }
