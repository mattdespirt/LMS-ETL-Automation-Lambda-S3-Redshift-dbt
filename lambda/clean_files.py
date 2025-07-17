import boto3
import numpy as np
import pandas as pd
from io import StringIO
import botocore

# Initialize S3 client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    print('Event: ' + str(event))
    
    strParms = str(event)
    bucket_name = ""
    source_prefix = ""
    target_prefix = ""
    filemask = ""
    skiprows = ""
    
    #print(bucket_name.replace("s3://","").split('/'))
    
    if strParms.find('bucket_name') > 0: bucket_name = event['bucket_name']
    if strParms.find('source_prefix') > 0:  source_prefix = event['source_prefix']
    if strParms.find('target_prefix') > 0: target_prefix = event['target_prefix']
    if strParms.find('filemask') > 0: filemask = event['filemask']
    if strParms.find('skiprows') > 0: skiprows = event['skiprows']

    print(bucket_name)
    print(source_prefix)
    print(target_prefix)
    print(source_prefix + target_prefix)
    print(filemask)
    
    # List all CSV files in the specified folder
    response = s3_client.list_objects(Bucket=bucket_name, Prefix=source_prefix)
    if 'Contents' in response:
        for item in response['Contents']:
            file_key = item['Key']
           # if (file_key.endswith('.csv') or file_key.endswith('.xlsx')) and "cleaned" not in file_key and filemask in file_key:
            if file_key.endswith('.csv') and "cleaned" not in file_key and filemask in file_key:
                # Process each CSV file
    #            print(file_key)
    #            print(bucket_name)
    #            print(skiprows) 
                #process_csv_file(bucket_name, file_key, source_prefix + target_prefix, int(skiprows))
                if file_key.endswith('/'):
                    continue
                #if filemask in file_key:
                process_csv_file(bucket_name, file_key,   target_prefix, int(skiprows))
    #            print("hello")
                s3_client.delete_object(Bucket=bucket_name, Key=file_key)
                #print(bucket_name)
                #print(file_key)


                
def process_csv_file(bucket, key, target_prefix, skiprows):
    # Get the CSV file from S3
    
    #print (bucket)
    #print (key)
    #print (skiprows)
    
    
    #rowstoskip = int(skiprows)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    file_content = response['Body'].read().decode('utf-8', errors='ignore')
    
    print(skiprows)
    #print(rowstoskip)
    

#    print('about to read to df')
    # Read the file content into a pandas DataFrame
    df = pd.read_csv(StringIO(file_content),skiprows=skiprows)

    print('read complete')   
    #df.drop(df.index[:skiprows], inplace=True)
    #df.drop(df.index[:rowstoskip], inplace=True)
    
    print('2')
    # Remove line feeds from the DataFrame
    #df = df.replace('\n', '', regex=True)
    for col in df.columns:
        df[col] = df[col].astype(str).replace("\n", " ", regex=True)
        df[col] = df[col].astype(str).replace("\r", " ", regex=True)
        df[col] = df[col].astype(str).replace('"', " ", regex=True)  

    print('3')
    # Convert the DataFrame back to CSV
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    print('4')
    # Define the new key for the cleaned CSV file
    new_key = f"{target_prefix}{key.split('/')[-1]}"

    #print(bucket)
    #print( new_key)
    #print('5')
    # Write the modified CSV to the 'cleaned' subfolder in S3
    s3_client.put_object(Bucket=bucket, Key=new_key, Body=csv_buffer.getvalue())
    print('6')
    
# Note: This function needs to be triggered by an event or manually for execution.