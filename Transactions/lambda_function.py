import boto3
import json
import io
import re

import csv
from datetime import datetime

month = '202504'
bucket_name = 'monthly-transactions'

# Mapping from file2's headers to file1's headers
# Format: {file1_header: file2_header}
column_mapping = {
    'Card': 'Details',
    'Post Date': "Posting Date",
    'Description': 'Description',
    'Amount': 'Amount',
    'Type': 'Type'
}

s3 = boto3.client('s3')

def read_and_map(checkingFile, header_map, final_headers):

    reader = csv.DictReader(checkingFile)
    mapped_rows = []

    for row in reader:
        new_row = {}
        for header in final_headers:
            if header in header_map:
                old_key = header_map[header]
                new_row[header] = row.get(old_key, '')
            else:
                new_row[header] = ''  # Fill with empty string if not mapped
        mapped_rows.append(new_row)
    return mapped_rows


# Sort by Post Date
def parse_date(row):
    try:
        d = datetime.strptime(row['Post Date'], '%m/%d/%Y')  # Adjust format if needed
        return d
    except Exception:
        return datetime.min  # Fallback for invalid or missing dates

def str_starts_with(d, pattern):
    for k, v in d.items():
        if pattern.startswith(k):
            return v
    
    #if pattern == 'ORIG CO NAME:Secret Garden Ro       ORIG ID:4270465600 DESC DATE:       CO ENTRY DESCR:TRANSFER  SEC:CCD    TRACE#:111000029658910 EED:250602   IND ID:ST-L0J1W2N4J5P8              IND NAME:SECRET GARDEN ROSE INC TRN: 1539658910TC':
    #    print("NOT FOUND")
    return ''


def parse_csv_to_name_category_dict(bucket_name, objectKey, result):

    # Get the S3 object
    response = s3.get_object(Bucket=bucket_name, Key=objectKey)

    # Wrap the byte stream in a text wrapper
    body = response['Body']
    text_stream = io.TextIOWrapper(body, encoding='utf-8')

    # Use csv.reader to parse the file
    reader = csv.DictReader(text_stream)

    for row in reader:
        name = row.get("Description")
        category = row.get("Memo")
        if name is not None and category is not None and name not in result:
            result[name] = category

def add_category_column(input_file, name_to_category):

    with open(afile, mode='r', newline='', encoding='utf-8') as infile, \
        open(afile+".out.csv", mode='w', newline='', encoding='utf-8') as outfile:

        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames + ['category']  # Add new column

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            name = row.get('Description')
            row['category'] = name_to_category.get(name, '')  # Default to empty string
            writer.writerow(row)


def lambda_handler(event, context):

    name_category_dict = {}

    # Use paginator to handle large lists of objects
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix='TaggingInput')

    for page in pages:
        for obj in page.get('Contents', []):
            print(obj['Key'])  # This is the filename (key)
            parse_csv_to_name_category_dict(bucket_name, obj['Key'], name_category_dict)
            print(len(name_category_dict))
    
    # List matching objects
    responseChecking = s3.list_objects_v2(Bucket=bucket_name, Prefix=f'{month}/Chase3738')
    responseCC = s3.list_objects_v2(Bucket=bucket_name, Prefix=f'{month}/Chase8507')

    # Check if any file found
    if 'Contents' not in responseChecking or 'Contents' not in responseCC:
        return {"statusCode": 404, "body": "No matching file found."}

    # Access first matching file
    print(f"Reading file: {responseCC['Contents'][0]['Key']}", f"{responseChecking['Contents'][0]['Key']}")
    
    cc = s3.get_object(Bucket=bucket_name, Key=responseCC['Contents'][0]['Key'])
    checking = s3.get_object(Bucket=bucket_name, Key=responseChecking['Contents'][0]['Key'])

    ccbody = cc['Body'].read().decode('utf-8')  # decode bytes to string
    checkingbody = checking['Body'].read().decode('utf-8')  # decode bytes to string

    # Use csv.reader or csv.DictReader
    cc_file = io.StringIO(ccbody)
    checking_file = io.StringIO(checkingbody)

    reader = csv.DictReader(cc_file)
    file1_headers = reader.fieldnames
    
    data1 = list(reader)

    # Process file2
    data2 = read_and_map(checking_file, column_mapping, file1_headers)
    
    combined = data1 + data2
    
    combined_sorted = sorted(combined, key=parse_date)

    for row in combined_sorted:
        name = row.get('Description')
        row['Memo'] = str_starts_with(name_category_dict, name) # Default to empty string

    key = f"{month}/{month}-transactions.csv"

    # Create an in-memory text buffer
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=file1_headers)
    writer.writeheader()
    writer.writerows(combined_sorted)

    # Upload the CSV to S3
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
