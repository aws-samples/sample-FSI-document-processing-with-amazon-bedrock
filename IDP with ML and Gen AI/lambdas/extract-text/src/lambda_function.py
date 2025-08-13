import boto3
import time
import sys
import re
import json
from collections import defaultdict
from datetime import datetime, timedelta

def get_kv_relationship(key_map, value_map, block_map):
    kvs = defaultdict(list)
    for block_id, key_block in key_map.items():
        value_block = find_value_block(key_block, value_map)
        if value_block:
            key = get_text(key_block, block_map)
            val = get_text(value_block, block_map)
            if key and val:
                kvs[key].append(val)
    return kvs

def find_value_block(key_block, value_map):
    if 'Relationships' not in key_block:
        return None
    for relationship in key_block['Relationships']:
        if relationship['Type'] == 'VALUE':
            for value_id in relationship['Ids']:
                return value_map.get(value_id)
    return None

def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map.get(child_id)
                    if not word:
                        continue
                    if word['BlockType'] == 'WORD':
                        text += word.get('Text', '') + ' '
                    elif word['BlockType'] == 'SELECTION_ELEMENT':
                        if word.get('SelectionStatus') == 'SELECTED':
                            text += 'X '
    return text.strip()

# def print_kvs(kvs):
#     for key, value in kvs.items():
#         print(f"{key} : {value}")


def move_skipped_files_to_s3(source_bucket, s3_folder_name, destination_bucket, skipped_files, s3_client, destination_folder='skipped'):
    deleted_folders = set()  # Track processed folders
    
    for file in skipped_files:
        try:
            print(f"Processing file: {file}")

            # Construct the destination key
            destination_key = f"{destination_folder}/{file}"
            print(f"Copying {file} to {destination_key}...")

            # Copy the file
            s3_client.copy_object(CopySource={'Bucket': source_bucket, 'Key': file}, Bucket=destination_bucket, Key=destination_key)

            # Delete the file
            s3_client.delete_object(Bucket=source_bucket, Key=file)
            print(f"Deleted: {file}")

            # Extract folder prefix
            folder_prefix = "/".join(file.split("/")[:-1]) + "/"

            # Store folder for later deletion attempt
            deleted_folders.add(folder_prefix)

        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"File not found: {file}")
            else:
                print(f"An error occurred while processing {file}: {e}")

    # Attempt to delete folder prefixes at the end
    for folder in deleted_folders:
        try:
            s3_client.delete_object(Bucket=source_bucket, Key=folder)
            print(f"Deleted empty folder prefix: {folder}")
        except s3_client.exceptions.ClientError as e:
            print(f"Could not delete folder {folder}: {e}")



def lambda_handler(event, context):
    source_bucket = event['BatchInput']['source_bucket']
    destination_bucket = event['BatchInput']['human_review_bucket']
    text_bucket = event['BatchInput']['text_bucket']
    s3_client = boto3.client('s3')
    textract_client = boto3.client('textract')
    s3_folder_name = event['Items'][0]['Prefix']

    skipped_files = []
    pdf_found = False
    
    # List objects in the specified folder
    response = s3_client.list_objects_v2(
        Bucket=source_bucket,
        Prefix=s3_folder_name
    )  
    if 'Contents' in response:
        for obj in response['Contents']:
            # Get the last modified timestamp of the object
            last_modified = obj['LastModified']
            
            # Calculate the age of the object
            current_time = datetime.now(last_modified.tzinfo)
            age = current_time - last_modified

            # Extract the object key
            key = obj['Key']
            
            # Check if the object is a folder (prefix) and not a file
            if key.endswith('/'):
                # Create an empty object (which will act as the "folder")
                s3_client.put_object(Bucket=text_bucket, Key=key)
                continue
            
            # Check if the object is a PDF file
            if key.lower().endswith('.pdf'):
                pdf_found = True
                try:
                    # Start Textract analysis on the PDF file
                    response = textract_client.start_document_analysis(
                        DocumentLocation={
                            'S3Object': {
                                'Bucket': source_bucket, 
                                'Name': key
                            }
                        },
                        FeatureTypes=["FORMS"],
                    )
                    
                    # Get the JobId for the Textract analysis
                    job_id = response['JobId']
                    print(f"Started Textract job with JobId: {job_id} for file: {key}")
                    
                    # Wait for Textract analysis to complete
                    status = wait_for_textract_completion(textract_client, job_id)
                    
                    if status == 'SUCCEEDED':
                        # Retrieve all blocks from Textract
                        blocks = get_all_document_analysis(textract_client, job_id)
                        
                        # Extract text lines
                        text = ''
                        for block in blocks:
                            if block['BlockType'] == "LINE":
                                # print(block['Text'])
                                text += block['Text'] + ' '
                        
                        # Upload the extracted text to S3
                        key_name = key + ".txt"
                        s3_client.put_object(Bucket=text_bucket, Key=key_name, Body=text)
                        print(f"Uploaded extracted text to {key_name}")
                        
                        # Build key-value maps
                        key_map = {}
                        value_map = {}
                        block_map = {}
                        for block in blocks:
                            block_id = block['Id']
                            block_map[block_id] = block
                            if block['BlockType'] == "KEY_VALUE_SET":
                                if 'KEY' in block.get('EntityTypes', []):
                                    key_map[block_id] = block
                                elif 'VALUE' in block.get('EntityTypes', []):
                                    value_map[block_id] = block
                        
                        # Get Key-Value relationships
                        kvs = get_kv_relationship(key_map, value_map, block_map)
                        body = json.dumps(kvs, indent=4)
                        folder_name = key + ".json"
                        s3_client.put_object(Bucket=text_bucket, Key=folder_name, Body=body)
                        print(f"Uploaded key-value pairs to {folder_name}")
                    else:
                        print(f"Textract job {job_id} failed for file: {key}")
                
                except Exception as e:
                    print(f"Error processing file {key}: {str(e)}")
                    continue  # Proceed to the next file
            
            else:
                # If not a PDF, skip the file and add to skipped list
                print(f"Skipping {key} as it is not a PDF file")
                skipped_files.append(key)
                s3_client.delete_object(Bucket=text_bucket,Key=s3_folder_name)
            
        # If no PDF files were found, return an error
        if not pdf_found:
            move_skipped_files_to_s3(source_bucket, s3_folder_name, destination_bucket, skipped_files, s3_client)
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'No PDF files found in the specified folder.',
                    'skipped_files': skipped_files
                })
            }
        if skipped_files:
            move_skipped_files_to_s3(source_bucket, s3_folder_name, destination_bucket, skipped_files, s3_client)
            
            return {'statusCode': 200,
            'body': s3_folder_name,
            'message': 'Process successful - but some files were skipped (moved to Human Review).',
            'skipped_files': skipped_files
            }
        
        return {
            'statusCode': 200,
            'body': s3_folder_name,
            'message': 'All files successfuly processed!'
        }

def wait_for_textract_completion(textract_client, job_id, max_retries=30, delay=10):

    retries = 0
    while retries < max_retries:
        try:
            response = textract_client.get_document_analysis(JobId=job_id)
            status = response['JobStatus']
            print(f"Job {job_id} status: {status}")
            
            if status in ['SUCCEEDED', 'FAILED']:
                return status
        except Exception as e:
            print(f"Error fetching job status for {job_id}: {str(e)}")
        
        retries += 1
        time.sleep(delay)
    
    raise TimeoutError(f"Textract job {job_id} did not complete within the expected time.")

def get_all_document_analysis(textract_client, job_id):

    blocks = []
    next_token = None
    
    while True:
        if next_token:
            response = textract_client.get_document_analysis(JobId=job_id, NextToken=next_token)
        else:
            response = textract_client.get_document_analysis(JobId=job_id)
        
        blocks.extend(response.get('Blocks', []))

        next_token = response.get('NextToken')
        if not next_token:
            break
    
    return blocks



