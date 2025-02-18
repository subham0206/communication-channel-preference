#!/usr/bin/env python
# coding: utf-8

### This is a data input and output utility script
import pandas as pd
import numpy as np
import io
import math
from io import StringIO
import datetime as dt
from scripts.sql_functions import  get_query_attentive_data, get_query_bluecore_data, get_query_sendgrid_data, get_query_epsilon_data



def get_most_recent_s3_object(bucket_name, prefix):

    '''
    Creates connection to S3 bucket and returns latest uploaded file in the bucket.
    Input: bucket_name (str), prefix(string)
    bucket_name -> The Name of the Bucket
    prefix -> the path to the folder within the S3 bucket.    
    '''

    s3 = boto3.client('s3')
    paginator = s3.get_paginator( "list_objects_v2" ) #makes sure more it includes buckets with more than 1000 objects and not only in page 1.
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    latest = None
    for page in page_iterator:
        if "Contents" in page:
            latest2 = max(page['Contents'], key=lambda x: x['LastModified'])
            if latest is None or latest2['LastModified'] > latest['LastModified']:
                latest = latest2
    return latest

def sf_connection():
   
    from utils import ssm_cnx
    sf_cnx = ssm_cnx.get_snowflake_connection()
    return sf_cnx

sf_cnx = sf_connection()
cur =  sf_cnx.cursor()

def snow_flake_execute(stmt, verbose=False, include_ending_semicolon=False):
    df = read_data_from_snow_flake(stmt, verbose, include_ending_semicolon)
    return df


def read_data_from_snow_flake(stmt, verbose=False, include_ending_semicolon=False):
    #sf_cnx = sf_connection()
    #cur =  sf_cnx.cursor()
    stmts = split_stmt(stmt, include_ending_semicolon)
    for stmt in stmts:
        if verbose:
            print('Executing statement\n----------')
            print(stmt)
            print('----------\n')
        cur.execute(stmt)
    #dta = cur.fetchall()
    #df = pd.DataFrame(dta, columns=[i[0] for i in cur.description])
    #column_names = [i[0] for i in cur.description]
    df = cur.fetch_pandas_all()
    #df.columns = column_names
    df.columns = df.columns.str.lower()
    #df = df.drop_duplicates()
    #cur.close()
    #sf_cnx.close()
    return df


# Function to execute the query and return results as a DataFrame
def read_data_from_snow_flake_in_batch(cursor, query):
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        df.columns = [col.upper() for col in df.columns]  # Adjust column names as needed
        df = df.astype(str)  # Ensure all columns are of type string
        return df
    except Exception as e:
        print(f"SQL Error: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of an error

def process_batch(cursor, emails_batch, brand, query_type):
    emails_str = "', '".join(emails_batch)
    
    # Select the correct query based on query_type
    if query_type == 'attentive':
        query = get_query_attentive_data(f"'{emails_str}'", brand)
    elif query_type == 'bluecore':
        query = get_query_bluecore_data(f"'{emails_str}'", brand)
    elif query_type == 'sendgrid':
        query = get_query_sendgrid_data(f"'{emails_str}'", brand)
    elif query_type == 'epsilon':
        query = get_query_epsilon_data(f"'{emails_str}'", brand)
    elif query_type == 'customer_vintage':
        query = get_query_customer_vintage(f"'{emails_str}'", brand)
    elif query_type == 'customer_rfm':
        query = get_query_customer_rfm(f"'{emails_str}'", brand)
    elif query_type == 'customer_behaviour':
        query = get_query_customer_behaviour(f"'{emails_str}'", brand)
    elif query_type == 'customer_item_features':
        query = get_query_customer_item_features(f"'{emails_str}'", brand)
    else:
        raise ValueError(f"Unknown query type: {query_type}")
    
    # Execute the query
    batch_data = read_data_from_snow_flake_in_batch(cursor, query)
    return batch_data






def read_data_in_batches(df, batch_size, cursor, brand, query_type,  use_first_column):
    # Choose email addresses based on the use_first_column flag
    email_addresses = df.iloc[:, 0].unique() if use_first_column else df.iloc[:, 1].unique()
    total_emails = len(email_addresses)
    num_batches = math.ceil(total_emails / batch_size)
    
    all_data = []
    
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min(start_idx + batch_size, total_emails)
        emails_batch = email_addresses[start_idx:end_idx]
        
        # Process each batch of emails
        batch_data = process_batch(cursor, emails_batch, brand, query_type)
        all_data.append(batch_data)
        print(f"Processed batch {i + 1} of {num_batches}")

    # Concatenate all batches into a single DataFrame
    result_df = pd.concat(all_data, ignore_index=True)
    return result_df

'''
def process_large_csv(file_path, batch_size, brand, query_type, use_first_column=False):
    sf_cnx = sf_connection()
    cursor =  sf_cnx.cursor()
    final_data = []
    chunksize = 10**5  # Read in chunks of 100,000 rows
    for chunk in pd.read_csv(file_path, chunksize=chunksize):
        final_data.append(read_data_in_batches(chunk, batch_size, cursor, brand, use_first_column))   
    final_df = pd.concat(final_data, ignore_index=True)
    return final_df

def process_large_csv(file_path, batch_size, brand, query_type, use_first_column=False):
    # Initialize the Snowflake connection and cursor
    sf_cnx = sf_connection()
    cursor = sf_cnx.cursor()

    final_data = []
    chunksize = 10**5  # Read in chunks of 100,000 rows

    # Set up s3fs with requester_pays=True for requester-pays buckets
    if file_path.startswith("s3://"):
        fs = s3fs.S3FileSystem(requester_pays=True)
        with fs.open(file_path, "r") as f:
            for chunk in pd.read_csv(f, chunksize=chunksize):
                # Process each chunk and append the result
                final_data.append(read_data_in_batches(chunk, batch_size, cursor, brand, use_first_column))
    else:
        # For local or other file systems, read directly
        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            final_data.append(read_data_in_batches(chunk, batch_size, cursor, brand, use_first_column))

    # Concatenate all processed data into a single DataFrame
    final_df = pd.concat(final_data, ignore_index=True)
    return final_df
    
'''

def process_large_csv(file_name, read_dir_name, batch_size, brand, query_type, bucket_name, use_first_column):
    print(f"file_name: {file_name}, read_dir_name: {read_dir_name}, batch_size: {batch_size}, brand: {brand}, query_type: {query_type}, use_first_column: {use_first_column}")

    sf_cnx = sf_connection()
    cursor = sf_cnx.cursor()
    final_data = []
    chunksize = 10**5  # Read in chunks of 100,000 rows
    
    # Read the CSV file from S3 using the existing function
    df = read_a_file_from_s3(file_name, read_dir_name, bucket_name)
    
    # Process the DataFrame in chunks
    for chunk in np.array_split(df, len(df) // chunksize + 1):
        final_data.append(read_data_in_batches(chunk, batch_size, cursor, brand, query_type, use_first_column))
    
    final_df = pd.concat(final_data, ignore_index=True)
    return final_df



def get_date_range():
    """Calculate the date range for the previous month and the same month a year ago.

    Returns:
        tuple: (start_month_name, start_year, end_month_name, end_year)
    """
    # Get the current date
    current_date = dt.datetime.now()

    # Calculate the end date (previous month)
    end_date = current_date.replace(day=1) - dt.timedelta(days=1)
    end_month_name = end_date.strftime('%b')  # Get the short name of the end month (e.g., Sep for October)
    end_year = end_date.strftime('%Y')  # Get the year of the end month

    # Calculate the start date (the same month last year)
    start_date = end_date.replace(year=end_date.year - 1)
    start_month_name = start_date.strftime('%b')  # Get the short name of the start month (e.g., Aug)
    start_year = start_date.strftime('%Y')  # Get the year of the start month

    return start_month_name, start_year, end_month_name, end_year

def create_new_table(stmt, verbose=False, include_ending_semicolon=False):
    #sf_cnx = sf_connection()
    #cur =  sf_cnx.cursor()
    stmts = split_stmt(stmt, include_ending_semicolon)
    for stmt in stmts:
        if verbose:
            print('Executing statement\n----------')
            print(stmt)
            print('----------\n')
        cur.execute(stmt)
    #dta = cur.fetchall()
    #df = pd.DataFrame(dta, columns=[i[0] for i in cur.description])
    #column_names = [i[0] for i in cur.description]
#     df = cur.fetch_pandas_all()
#     #df.columns = column_names
#     df.columns = df.columns.str.lower()
#     df = df.drop_duplicates()
#     #cur.close()
#     #sf_cnx.close()
#     return df
        # For DDL queries like CREATE TABLE, there's no need to fetch any data
    



def split_stmt(stmt, include_ending_semicolon=True):
    '''
    :type stmt: str
    :type include_ending_semicolon: bool
    '''
    import re
    patt = re.compile(r'''((?:[^;"']|"[^"]*"|'[^']*')+)''')
    stmt_list = []
    initial_parts = patt.split(stmt)
    for pt in initial_parts:
        pt = pt.strip()
        if pt in (';', ''): continue
        if include_ending_semicolon: pt += ';'
        stmt_list.append(pt)
    return stmt_list

#def upload_df_from_SageMaker_to_SF_Sandbox(DF,   table_name = 'temp'):
def upload_df_from_SageMaker_to_SF_Sandbox(DF,   table_name):
    bucket_nameX = 'nmg-analytics-ds-prod'
    save_dir_nameX = 'ds/Users/SnowFlake_upload/'
    file_nameX = table_name +'_file'
    
    column_names = ', '.join(str(c) +' VARCHAR' for c in DF.columns)
   
    save_flag = False
    save_flag = save_df_to_s3_in_zip_file(DF, bucket_nameX, save_dir_nameX, file_nameX)
    if save_flag == True:
        upload_df_from_S3_to_SF_Sandbox(column_names, table_name, save_dir_nameX,file_nameX )
    return True

def upload_df_from_S3_to_SF_Sandbox(column_names, table_name, save_dir_nameX,file_nameX ):
    snow_flake_execute("""drop table if exists nmedwprd_db.mktsand.{0};
                    create table nmedwprd_db.mktsand.{0} ({1});
                    copy into nmedwprd_db.mktsand.{0}
                        from @NMEDWPRD_DB.PUBLIC.AWS_DS_PRD_STG/{2}{3} 
                        file_format = (type = csv field_delimiter = '|') on_error = 'CONTINUE';
                        
                     select * from nmedwprd_db.mktsand.{0} limit 5
                        """.format(table_name, column_names,save_dir_nameX, file_nameX))
    # add here error catch...
    print("Data is uploaded!")
    return True

def upload_df_from_SageMaker_to_SF_Production(DF, table_name = 'temp'):
    
    bucket_nameX = 'nmg-analytics-ds-prod'
    save_dir_nameX = 'ds/prod/MLDM_tables/'
    file_nameX = table_name +'_file'
    save_flag = False
    save_flag = save_df_to_s3_in_zip_file(DF, bucket_nameX, save_dir_nameX, file_nameX)
    if save_flag == True:
        upload_df_from_S3_to_SF_Proudction(table_name, save_dir_nameX, file_nameX )
    return True


def upload_df_from_S3_to_SF_Proudction(table_name, save_dir_nameX, file_nameX ):
    
    database_schema_table = 'NMEDWPRD_DB.MLDM.'+ table_name
    snow_flake_execute("""DELETE FROM {0};
                   copy into {0}
                        from @NMEDWPRD_DB.PUBLIC.AWS_DS_PRD_STG/{1}{2}
                        file_format = (type = csv field_delimiter = '|') on_error = 'CONTINUE';
                        
                        select * from {0} limit 5;
                        """.format(database_schema_table, save_dir_nameX, file_nameX))
    print("Uploaded data to the production table")
    return True

def save_df_to_s3_in_zip_file(df_to_save, bucket_nameX, save_dir_nameX, file_nameX, headerX = False):
    """
    Input: 
    
    headerX should be True if you are savig data to S3 for uploading into Spark or other use
    headerX should be false if are saving data to S3 for uploading into Snowflake
    
    DF: data frame, file_nameX: name of commpressed file (e.g 'status_indicator')
    bucket_nameX = 'nmg-analytics-ds-prod'
    save_dir_nameX = 'Users/SnowFlake_upload/'

    Output: compressed file in gzip format in save_dir_named
    
    This function should be used before call function to upload to snowflake, e.g:
    upload_df_from_S3_to_SF_Production_Table(database_schema_table, save_dir_nameX, file_nameX )
    """
    import  boto3, gzip
    from io import BytesIO, TextIOWrapper
    
    gz_buffer = BytesIO()
    
    with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
        df_to_save.to_csv(TextIOWrapper(gz_file, 'utf8'), index=False, header=headerX, sep='|')
        boto3.resource('s3').Object(bucket_nameX,save_dir_nameX+file_nameX).put(Body=gz_buffer.getvalue())
        print ('saved in ', file_nameX)
    # add here error catch script    
    return True

'''
def save_df_to_s3(df_to_save, bucket_nameX, save_dir_nameX, file_nameX):
    import boto3
    from io import StringIO
    from datetime import date

    csv_buffer = StringIO()
    df_to_save.to_csv(csv_buffer,index=False)
    
    today = date.today()
    # file_nameX = file_nameX.split('.')[0]
    # file_nameX = file_nameX+'_{}'.format(today.strftime('%Y%m%d'))+'.csv'
    boto3.resource('s3').Object(bucket_nameX, save_dir_nameX+file_nameX).put(Body=csv_buffer.getvalue())
    print ('saved in ', file_nameX)
    return True
'''
def save_df_to_s3(df_to_save, bucket_nameX, save_dir_nameX, file_nameX):
    import boto3
    from io import StringIO
    from datetime import date

    try:
        # Prepare the CSV buffer
        csv_buffer = StringIO()
        df_to_save.to_csv(csv_buffer, index=False)

        # Handle file path formatting
        if not save_dir_nameX.endswith('/'):
            save_dir_nameX += '/'

        # Upload to S3
        boto3.resource('s3').Object(bucket_nameX, f"{save_dir_nameX}{file_nameX}").put(Body=csv_buffer.getvalue())

        # Log success (replace print with logger if available)
        print(f"File successfully saved to S3 at: s3://{bucket_nameX}/{save_dir_nameX}{file_nameX}")
    except Exception as e:
        # Log error (replace print with logger if available)
        print(f"Failed to save file to S3. Error: {str(e)}")
        raise


def save_pickle_to_s3(obj_to_save, bucket_nameX, save_dir_nameX, file_nameX):
    import boto3
    import pickle
    from io import BytesIO
    from datetime import date

    # Prepare binary buffer
    pickle_buffer = BytesIO()
    pickle.dump(obj_to_save, pickle_buffer)
    pickle_buffer.seek(0)  # Reset buffer position to the beginning

    # Include date in filename (optional)
    today = date.today()
    #file_nameX = file_nameX.split('.')[0] + '_{}'.format(today.strftime('%Y%m%d')) + '.pkl'
    file_nameX = file_nameX.split('.')[0] + '.pkl'

    # Upload the pickle file to S3
    boto3.resource('s3').Object(bucket_nameX, save_dir_nameX + file_nameX).put(Body=pickle_buffer.getvalue())
    print('Pickle file saved in S3 at:', save_dir_nameX + file_nameX)
    return True

'''
def save_df_to_s3(df_to_save, bucket_nameX, save_dir_nameX, file_nameX):
    import boto3
    from io import BytesIO
    from datetime import date

    s3_client = boto3.client('s3')
    today = date.today()
    #file_nameX = f"{file_nameX}_{today.strftime('%Y%m%d')}.csv"

    # Initiate the multipart upload
    multipart_upload = s3_client.create_multipart_upload(Bucket=bucket_nameX, Key=save_dir_nameX + file_nameX)
    part_number = 1
    parts = []
    max_part_size = 5 * 1024 * 1024  # 5 MB per part

    try:
        # Convert DataFrame to CSV and split into binary chunks
        for chunk in df_to_save.to_csv(index=False, chunksize=100000):
            csv_buffer = BytesIO()
            csv_buffer.write(chunk.encode('utf-8'))
            csv_buffer.seek(0)

            part = s3_client.upload_part(
                Bucket=bucket_nameX,
                Key=save_dir_nameX + file_nameX,
                PartNumber=part_number,
                UploadId=multipart_upload['UploadId'],
                Body=csv_buffer.getvalue()
            )
            parts.append({"ETag": part['ETag'], "PartNumber": part_number})
            part_number += 1

        # Complete the multipart upload
        s3_client.complete_multipart_upload(
            Bucket=bucket_nameX,
            Key=save_dir_nameX + file_nameX,
            UploadId=multipart_upload['UploadId'],
            MultipartUpload={"Parts": parts}
        )
        print(f'Successfully saved in {file_nameX}')
        return True

    except Exception as e:
        # Abort the multipart upload in case of error
        s3_client.abort_multipart_upload(
            Bucket=bucket_nameX,
            Key=save_dir_nameX + file_nameX,
            UploadId=multipart_upload['UploadId']
        )
        print("Upload failed:", e)
        return False

'''



def save_df_to_s3_ca(df_to_save, bucket_nameX, save_dir_nameX, file_nameX):
    import boto3
    from io import StringIO
    from datetime import date
    
    s3_client = boto3.client('s3')

    csv_buffer = StringIO()
    df_to_save.to_csv(csv_buffer,index=False)
    
    today = date.today()
    # file_nameX = file_nameX.split('.')[0]
    # file_nameX = file_nameX+'_{}'.format(today.strftime('%Y%m%d'))+'.csv'
#     boto3.resource('s3').Object(bucket_nameX, save_dir_nameX+file_nameX).put(Body=csv_buffer.getvalue())
    
    s3_client.put_object(Body=csv_buffer.getvalue(), ACL='bucket-owner-full-control', Bucket=bucket_nameX, Key=save_dir_nameX+file_nameX)
    
    print ('saved in ', file_nameX)
    return True


def save_df_to_s3_parquet(df, bucket_nameX, save_dir_nameX):
    import pyarrow.parquet as pq
    import s3fs
    s3 = s3fs.S3FileSystem()
    
    #df.to_parquet("s3a://"+bucket_nameX+'/'+save_dir_nameX,index=True)
    df.to_parquet("s3://"+bucket_nameX+'/'+save_dir_nameX,index=True)
    #df.to_parquet(self, fname, engine='auto', compression='snappy', index=None, partition_cols=None, **kwargs)
    #s3_url = 's3://bucket/folder/bucket.parquet.gzip'
    #df.to_parquet(s3_url, compression='gzip')
    print("file is saved")
    return

def save_spark_data_to_S3(df, prefix_name):
    # an example of a prefix_name = 'ds/dev/rec_engine/nm/output/implicit_rating/'
    df.coalesce(1).write.csv('s3://nmg-analytics-ds-prod/{0}'.format(prefix_name),mode='overwrite')
    return
              


def read_df_from_s3_parquet(file_nameX, read_dir_nameX, bucket_nameX='nmg-analytics-ds-prod'):
    import pyarrow.parquet as pq
    import s3fs
    import boto3
    import pandas as pd
    from io import StringIO

    s3 = s3fs.S3FileSystem()
    dir_nameX = f"{read_dir_nameX.rstrip('/')}/{file_nameX.lstrip('/')}"
    print(f"Attempting to read from bucket: {bucket_nameX}")  
    df = pq.ParquetDataset("s3://"+bucket_nameX+"/"+dir_nameX, filesystem=s3).read_pandas().to_pandas()
    df.columns = df.columns.str.lower()
    return df

'''
def read_a_file_from_s3(file_nameX, read_dir_nameX, bucket_nameX = 'nmg-analytics-ds-prod', column_namesX = False):
    import boto3
    from io import StringIO
    
    

    obj = boto3.client('s3').get_object(Bucket= bucket_nameX, Key= read_dir_nameX+file_nameX)
    
    if column_namesX == False:
        dfX = pd.read_csv(obj['Body'])
    else:
        #  usecols  = [4,6, 29,30,31,32,33]
        dfX = pd.read_csv(obj['Body'],usecols  = column_namesX)
    
    dfX.columns = dfX.columns.str.lower()
    return dfX
'''
def read_a_file_from_s3(file_nameX, read_dir_nameX, bucket_nameX='nmg-analytics-ds-prod', column_namesX=False):
    import boto3
    import pandas as pd
    from io import StringIO

    # Ensure proper key construction
    key = f"{read_dir_nameX.rstrip('/')}/{file_nameX.lstrip('/')}"
    print(f"Attempting to read from bucket: {bucket_nameX}, key: {key}")  # Debugging line
    
    try:
        obj = boto3.client('s3').get_object(Bucket=bucket_nameX, Key=key)
    except boto3.exceptions.S3UploadFailedError as e:
        print(f"Failed to get object: {e.response['Error']['Message']}")
        raise
    
    if not column_namesX:
        dfX = pd.read_csv(obj['Body'])
    else:
        dfX = pd.read_csv(obj['Body'], usecols=column_namesX)
    
    dfX.columns = dfX.columns.str.lower()
    return dfX



def read_spark_saved_data_from_S3(prefix_name, kernel_type = 'Spark_Magic'):
   # an example of a prefix_name = 'ds/dev/rec_engine/nm/output/implicit_rating/'
    
    bucket =resource.Bucket('nmg-analytics-ds-prod')
    objs = bucket.objects.filter(Prefix=prefix_name)
    
    for obj in objs:
        if '.csv' in obj.key:
            f=obj.key
    if kernel_type ==  'Spark_Magic':
        df = spark.read.csv('s3://nmg-analytics-ds-prod/{0}'.format(f))
    else:
        df = pd.read_csv('s3://nmg-analytics-ds-prod/{0}'.format(f),header=None,sep='|')
    return df


def contents_of_the_bucket(bucket = 'nmg-analytics-ds-prod'):
    import boto3  
    for key in boto3.client('s3').list_objects(Bucket=bucket)['Contents']:
        print(key['Key'])
    return True

def contents_of_a_dir_in_a_bucket(prefix, bucket = 'nmg-analytics-ds-prod'):
    import boto3
    
    my_bucket = boto3.resource('s3').Bucket('nmg-analytics-ds-prod')
    
    for my_bucket_object in my_bucket.objects.filter(Prefix = prefix):
        print(my_bucket_object)
        
    return True
               

def move_file_dirA_dirB(file_name, dir_A, bucket_A, dir_B, bucket_B):
    df = read_a_file_from_s3(file_name, dir_A, bucket_A)
    print(df.shape)
    print(df.head(2))
    save_df_to_s3(df, file_name, dir_B, bucket_B)
    return


def move_file_from_Win_Share_to_dirB(file_name, dir_A, dir_B, bucket_B):   
    df = read_a_file_from_s3(file_name, dir_A, 'nmg-analytics-da-share-prod')
    print(df.shape)
    print(df.head(2))
    save_df_to_s3(df, file_name, dir_B, bucket_B)
    return


def delete_all_files_from_a_S3_dir(dir_nameX, bucket_nameX):
    import boto3
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_nameX)
    
    objects_to_delete = []
    for obj in bucket.objects.filter(Prefix=dir_nameX):
        objects_to_delete.append({'Key': obj.key})
        
    bucket.delete_objects(
        Delete={'Objects': objects_to_delete})
    return True

def delete_a_single_file_from_a_S3_dir(file_nameX, dir_nameX, bucket_nameX):
    import boto3
    s3 = boto3.resource("s3")
    obj = s3.Object(bucket_nameX, dir_nameX+file_nameX)
    obj.delete()
    return True
   
def days_since_last_purchase(df):
    from datetime import date
    df["trans_date"] = pd.to_datetime(df["trans_date"])
    df["Days_Since_LPurchase"] = pd.to_datetime(date.today()) - df["trans_date"]
    df["Days_Since_LPurchase"] = df["Days_Since_LPurchase"].dt.days 
    df.drop_duplicates('curr_customer_id',inplace=True)
    return df

def remove_df_based_on_key(df_before,remove_key_cols):
    keys = list(remove_key_cols.columns.values)
    i1 = df_before.set_index(keys).index
    i2 = remove_key_cols.set_index(keys).index
    df_after = df_before[~i1.isin(i2)].reset_index(drop=True)
    return df_after


def apply_customer_exclusion(df_before):
    print("Customers before  : ", df_before['curr_customer_id'].nunique())
    suppress_df = read_data_from_snow_flake(""" 
    select curr_customer_id from NMEDWPRD_DB.pdwdm.sas_cmd_customer_v
                  where substr(SUPPRESSION_STRING,42,1) 
                        or substr(SUPPRESSION_STRING,1,1)
                        or substr(SUPPRESSION_STRING,3,1)
                        or substr(SUPPRESSION_STRING,5,1)
                        or substr(SUPPRESSION_STRING,6,1)
                        or substr(SUPPRESSION_STRING,7,1)""")

    suppress_df.columns = ['curr_customer_id']
    print('Excluded customers: ', suppress_df['curr_customer_id'].nunique())
    df_after = t.remove_df_based_on_key(df_before, suppress_df,['curr_customer_id'])
    print("Customers after   : ", df_after['curr_customer_id'].nunique())
    return df_after

def order_cluster(cluster_field_name, target_field_name,df,ascending):
    ## Rearrange the cluster label in order of highest value to lower value
    new_cluster_field_name = 'new_' + cluster_field_name
    df_new = df.groupby(cluster_field_name)[target_field_name].mean().reset_index()
    df_new = df_new.sort_values(by=target_field_name,ascending=ascending).reset_index(drop=True)
    df_new['index'] = df_new.index
    df_final = pd.merge(df,df_new[[cluster_field_name,'index']], on=cluster_field_name)
    df_final = df_final.drop([cluster_field_name],axis=1)
    df_final = df_final.rename(columns={"index":cluster_field_name})
    return df_final

def create_time_related_features(df):
    # input dataframe should only contain customer id and transaction date
    import pandas as pd
    
    # take only customer id and trans_data portion
    df = df[['customerid','trans_date']]
    df['trans_date'] = pd.to_datetime(df['trans_date'])
    df = df.sort_values(['customerid', 'trans_date']).drop_duplicates()

    # day related
    df['Year']  = df['trans_date'].dt.year
    df['Month'] = df['trans_date'].dt.month
    df['Week']  = df['trans_date'].dt.week
    df['Day']   = df['trans_date'].dt.dayofweek # Monday 0, Sunday 6
    
    #frequency (total number of trips)
    freq = df.groupby('customerid').trans_date.count().reset_index(name='Frequency')
    df =   pd.merge(df, freq, on='customerid')
    
    #interarrival_days
    
    df['Interarrival_Days'] = df.groupby('customerid')['trans_date'].diff().dt.days.fillna(0)
    
    # Add minimum and Maximum purchase dates and 
    first_purchase = df.groupby('customerid').trans_date.min().reset_index(name='MinPurchaseDate')
    last_purchase  = df.groupby('customerid').trans_date.max().reset_index(name = 'MaxPurchaseDate')
    first_last_purchase_dates = pd.merge(last_purchase,first_purchase,on='customerid',how='left')
    first_last_purchase_dates['Active_Duration'] = (first_last_purchase_dates['MaxPurchaseDate']-first_last_purchase_dates['MinPurchaseDate']).dt.days
    df = pd.merge(df, first_last_purchase_dates, on='customerid', how='inner')
    
    #Recency from today
    df ['Recency'] = (pd.Timestamp.today()- df['MaxPurchaseDate']).dt.days
    
    df = df[['customerid', 'trans_date', 'Year', 'Month', 'Week', 'Day', 'Frequency', 'Interarrival_Days', 'Recency', 'Active_Duration', 'MaxPurchaseDate', 'MinPurchaseDate']]
    return df
