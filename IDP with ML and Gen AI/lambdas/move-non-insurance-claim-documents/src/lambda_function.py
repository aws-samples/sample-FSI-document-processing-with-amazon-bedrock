
import boto3
import os

# Create an S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):

    # Extract input parameters from the event
    source_bucket = event['scanning_in_process_bucket']
    destination_bucket = event['destination_bucket']
    destination_prefix = 'review_docs'

    text_bucket = event['scanning_text_bucket']

    # Parse the event input from the state machine
    classification_result = event.get("classificationResult", {})
    print(classification_result)

    # Get the full file path for the S3 object
    file_path = classification_result.get("file")
    print(f"File path: {file_path}")

    if file_path is None:

        return {
            'statusCode': 400,
            'body': "Missing 'file' key in classificationResult"
        }

    #Break down the file path into the prefix (source folder) and object name
    source_folder = os.path.dirname(file_path)
    if source_folder and not source_folder.endswith("/"):
        source_folder += "/"

    object_name = os.path.basename(file_path)
    cleaned_object_name = object_name.split('.pdf')[0] + '.pdf'

    if not source_bucket or not destination_bucket or not cleaned_object_name:
        return {
            'statusCode': 400,
            'body': 'Missing required parameters: source_bucket, destination_bucket, object_name'
        }

    # Construct source and destination keys
    source_key = f"{source_folder}{cleaned_object_name}" if source_folder else cleaned_object_name
    destination_key = f"{destination_prefix}/{source_key}" if destination_prefix else source_key

    try:
        # Copy the object to the destination
        s3.copy_object(
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Bucket=destination_bucket,
            Key=destination_key
        )

        # Delete the original object from the source bucket (scanning-staging)
        s3.delete_object(Bucket=source_bucket, Key=source_key)

        txtfile = f"{cleaned_object_name}.txt"
        jsonfile = f"{cleaned_object_name}.json"

        delete_source_keys = [
            {"Key": f"{source_folder}{txtfile}"},
            {"Key": f"{source_folder}{jsonfile}"}
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

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f"Error moving object: {str(e)}"
        }
