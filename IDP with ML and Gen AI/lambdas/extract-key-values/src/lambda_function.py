import json
import boto3
import re
import os

s3 = boto3.client('s3')

# Initialize the DynamoDB client
dynamodb = boto3.resource('dynamodb')

# List of keywords to search for and their corresponding DynamoDB attributes
attribute_mapping = {
    "INSURED": "policyHolder",
    "CLAIM #": "claimNumber",
    "POLICY #": "policyID",
    "DATE OF ACCIDENT": "date",
    "DEDUCTIBLE": "deductible"
}

def lambda_handler(event, context):
    secret_name = os.environ["SECRET_NAME"]
    region_name = os.environ.get("AWS_REGION", "us-east-1")
    client = boto3.client("secretsmanager", region_name=region_name)
    

    try:
        # Retrieve the secret value
        response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response["SecretString"])
        table_name = secret["table_name"]
        
        print(f"Retrieved secret")
        
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise e

    table = dynamodb.Table(table_name)


    text_bucket = event['scanning_text_bucket']
    scanning_bucket = event['scanning_in_process_bucket']

    destination_prefix = 'archived_docs'

    archive_bucket = event['archive_bucket']

    # Parse the event input from the state machine
    classification_result = event.get("classificationResult", {})
    print(classification_result)

    # Get the full file path for the S3 object
    file_path = classification_result.get("file")
    print(f"File path: {file_path}")

    prefix = os.path.dirname(file_path)
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    if file_path is None:

        return {
            'statusCode': 400,
            'body': "Missing 'file' key in classificationResult"
        }
    
    object_name = os.path.basename(file_path)
    cleaned_object_name = object_name.split('.pdf')[0] + '.pdf'
    print(cleaned_object_name)

    # List all objects within the specified prefix
    response = s3.list_objects_v2(Bucket=text_bucket, Prefix=prefix)
    
    # Initialize the JSON object to be rCeturned
    result = {}
    
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.json'):
                try:
                    # Retrieve the content of the JSON file
                    file_obj = s3.get_object(Bucket=text_bucket, Key=key)
                    file_content = json.load(file_obj['Body'])

                    # Extract the ClaimNumber from the JSON content
                    claim_number = None
                    if 'CLAIM #' in file_content:
                        claim_number = str(file_content['CLAIM #']).strip()
                    
                    file_name = os.path.basename(key)
                    cleaned_file_name = file_name.split('.pdf')[0]

                    # Construct the DynamoDB item
                    dynamodb_item = {
                        'claimNumber': claim_number,  # Partition key
                        'fileName': cleaned_file_name #sort key
                    }

                    # Map matching keys to predefined attributes
                    for json_key, json_value in file_content.items():
                        json_key_cleaned = json_key.strip().upper()
                        print(f"Processing key: {json_key_cleaned}, value: {json_value}")

                        for keyword, ddb_attribute in attribute_mapping.items():
                            if json_key_cleaned == keyword.upper():
                                print(f"Exact match found for keyword: -> {ddb_attribute}")

                                # Extract the value and clean it
                                if isinstance(json_value, list) and len(json_value) > 0:
                                    value = str(json_value[0]).strip()
                                else:
                                    value = str(json_value).strip()

                                # Map the value to the DynamoDB attribute
                                dynamodb_item[ddb_attribute] = value
                                print(f"Mapped value: {ddb_attribute} = {value}")


                    # Store the item in DynamoDB
                    if len(dynamodb_item) > 1:  # Ensure we have more than just the FileName
                        table.put_item(Item=dynamodb_item)
                        print(f"Stored in DynamoDB: {dynamodb_item}")

                except Exception as e:
                    print(f"Error processing file {key}: {e}")

        # Delete the original object from the source bucket (scanning in process)
        source_key = f"{prefix}{cleaned_object_name}"
        destination_key = f"{destination_prefix}/{source_key}" if destination_prefix else source_key

        s3.copy_object(
            CopySource={'Bucket': scanning_bucket, 'Key': source_key},
            Bucket=archive_bucket,
            Key=destination_key
        )

        # Delete the  object from the text bucket
        txtfile = f"{cleaned_object_name}.txt"
        jsonfile = f"{cleaned_object_name}.json"

        delete_source_keys = [
            {"Key": f"{prefix}{txtfile}"},
            {"Key": f"{prefix}{jsonfile}"}
        ]

        # Batch delete objects
        s3.delete_objects(
            Bucket=text_bucket,
            Delete={"Objects": delete_source_keys}
        )


    return {
        'statusCode': 200,
        'status': "Processing complete."
    }