import json
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # Extract parameters from the event
    bucket_name = event['bucket_name']
    parent_folder = event['parent_folder']
    folders = event['folders']  # List of folder paths to check
    filename = event['filename']  # Filename to check for (e.g., '.csv')
    
    any_folder_empty = False
    
    for folder in folders:
        full_path = f"{parent_folder}/{folder}"
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=full_path)
        
        # Debug: Print the response
        print(f"Checking folder: {full_path}")
        print(response)
        
        # Check if 'Contents' key exists and if it contains the specified file
        file_found = False
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith(filename):
                    print(f"File found in {full_path}")
                    file_found = True
                    break
        
        if not file_found:
            print(f"No file found in {full_path}")
            any_folder_empty = True
    
    return {
        'statusCode': 200,
        'body': json.dumps({'is_empty': any_folder_empty})
    }