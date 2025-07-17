"""
    Glue Script: convert_csv_parquet.py
    -- this script reads a source file, converts it to parquet and moves it to a target directory
"""


#from msilib.schema import File
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import *
from pyspark.sql.types import StructType, StringType, DecimalType, IntegerType, StructField, LongType, TimestampType, BooleanType, DateType, _parse_datatype_string
from pyspark.sql import functions as F
from datetime import datetime
import operator
import boto3

# function allows us to pull out optional job parameters
def get_glue_env_var(key, default="none"):
    if f'--{key}' in sys.argv:
        return getResolvedOptions(sys.argv, [key])[key]
    else:
        return default
        
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','db_name','table_name','src_uri','tgt_uri','partition_keys','file_delimiter'])
## optional parameters
retention_in_hours = int(get_glue_env_var('retention_hours', '-1'))
## defaults to zero. If the value is 1 then the file is deleted instead of archived. 
delete_source_file = int(get_glue_env_var('delete_source_file', '0'))
## appends the filename to the dataset so that if multiple files are in the directory you can still sort by the filename.
add_filename_to_ds = int(get_glue_env_var('add_filename_to_ds', '0'))

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

##variables for arguments
db_name = args['db_name']
table_name = args['table_name']
src_uri = args['src_uri']
tgt_uri = args['tgt_uri']
file_delimiter = args['file_delimiter']

if args['partition_keys'] != 'None':
    partition_keys = args['partition_keys'].split(',')
else:
    partition_keys = None

src_uri_split = src_uri.replace("s3://","").split('/')
src_bucket = src_uri_split.pop(0)
src_key = "/".join(src_uri_split)
archive_key = "Archive/"+src_key

tgt_uri_split = tgt_uri.replace("s3://","").split('/')
tgt_bucket = tgt_uri_split.pop(0)
tgt_key = "/".join(tgt_uri_split)

##Client and Resource 
glue_client = boto3.client('glue',region_name='us-east-2')
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

## get table schema from glue
get_table = glue_client.get_table(
    DatabaseName=args['db_name'],
    Name=args['table_name'])

tableschema = get_table['Table']['StorageDescriptor']['Columns']

## Create Dataframe with Schema
file_schema = StructType()

for Dict in tableschema:
    dtype = _parse_datatype_string(Dict['Type'])
    file_schema.add(Dict['Name'], dtype, True)

df = spark.createDataFrame((),file_schema)

##Get list of files from src_prefix
FileList = s3_client.list_objects_v2(
    Bucket=src_bucket,
    Prefix=src_key
)['Contents']
FileList = sorted(FileList, key=operator.itemgetter('Key','LastModified'))

# read the files and write to parquet
if file_delimiter == ',':
    if add_filename_to_ds == 1:
        data_source = spark.read.csv(path=src_uri,schema=file_schema,header='True',enforceSchema='True',ignoreLeadingWhiteSpace='True',ignoreTrailingWhiteSpace='True').withColumn("filename", F.input_file_name())
    else:
        data_source = spark.read.csv(path=src_uri,schema=file_schema,header='True',enforceSchema='True',ignoreLeadingWhiteSpace='True',ignoreTrailingWhiteSpace='True')
else:
    if add_filename_to_ds == 1:
        print("add filename")
        data_source = spark.read.options(delimiter=file_delimiter).csv(path=src_uri,schema=file_schema,header='True',enforceSchema='True',ignoreLeadingWhiteSpace='True',ignoreTrailingWhiteSpace='True').withColumn("filename", F.input_file_name())
    else:
        print("do not add filename")
        data_source = spark.read.options(delimiter=file_delimiter).csv(path=src_uri,schema=file_schema,header='True',enforceSchema='True',ignoreLeadingWhiteSpace='True',ignoreTrailingWhiteSpace='True')
    
data_source.write.parquet(path=tgt_uri + datetime.now().strftime("%Y%m%d%H%M%S%s") + '/', mode= 'append', partitionBy=partition_keys, compression= 'snappy')

if data_source.rdd.isEmpty():
    print("🚨 WARNING: No rows read from CSV. Check delimiter, schema, source files.")
else:
    print(f"✅ Successfully read {data_source.count()} records.")
    data_source.show(5)

# move the files to archive
for key in FileList:
    if not key['Key'].endswith('/'):

        ## Iterate through each csv file: Copy, Delete.
        archive_Key = archive_key+key['Key'].split("/")[-1]
        if delete_source_file == 1:
            s3_resource.Object(src_bucket, key['Key']).delete()
        else:
            s3_resource.Object(src_bucket, archive_Key).copy_from(CopySource={'Bucket': src_bucket, 'Key': key['Key']})
            s3_resource.Object(src_bucket, key['Key']).delete()

# run purge if retention_in_hours > 0
if retention_in_hours > 0:
    print('Purge hours: ' + str(retention_in_hours))
    glueContext.purge_table(db_name, table_name, {"retentionPeriod": retention_in_hours})

job.commit()
