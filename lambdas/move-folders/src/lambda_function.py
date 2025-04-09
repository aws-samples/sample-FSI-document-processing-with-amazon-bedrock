import boto3

def lambda_handler(event, context):
    source_bucket = event['source_bucket']
    destination_bucket = event['destination_bucket']
    folder_key = event['folder_key']
    additional_folder= ''


    print("This is my event object: ", event)
    
    move_folder(source_bucket, destination_bucket, folder_key)
    
    #Check if additional folder variable is empty.
    if event['additional_folder'] != '':
        additional_folder= event['additional_folder']
        additional_key= event['folder_key'] 
        delete_additional_folder(additional_folder, additional_key)

    return {
        'statusCode': 200,
        'prefix': folder_key,
        'body': 'Folder moved successfully'
    }

def move_folder(source_bucket, destination_bucket, folder_key):

    # Create S3 client
    s3 = boto3.client('s3')

    # List objects in the source folder
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=folder_key)

    # Move each object to the destination bucket
    for obj in response.get('Contents', []):
        source_key = obj['Key']
        s3.copy_object(
            Bucket=destination_bucket,
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Key=source_key
        )
        s3.delete_object(Bucket=source_bucket, Key=source_key)
        
def delete_additional_folder(additional_folder , additional_key):
    
    # Create S3 client
    s3 = boto3.client('s3')

    # List objects in the source folder
    response = s3.list_objects_v2(Bucket=additional_folder, Prefix=additional_key)

    # Move each object to the destination bucket
    for obj in response.get('Contents', []):
        source_key = obj['Key']
        s3.delete_object(Bucket=additional_folder, Key=source_key)