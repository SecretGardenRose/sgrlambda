import boto3
from email import policy
from email.parser import BytesParser

s3 = boto3.client('s3')

def lambda_handler(event, context):

    # Step 1: Extract the message ID from SES event
    ses_record = event['Records'][0]['ses']
    message_id = ses_record['mail']['messageId']

    # Step 2: Build the S3 object path
    bucket_name = 'ses-sgrcredit'
    object_key = f'{message_id}'

    # Step 3: Read raw email content from S3
    s3_object = s3.get_object(Bucket=bucket_name, Key=object_key)
    raw_email = s3_object['Body'].read()

    # Step 4: Parse raw email using the built-in email parser
    msg = BytesParser(policy=policy.default).parsebytes(raw_email)

    # Extract metadata
    subject = msg['subject']
    sender = msg['from']
    recipient = msg['to']

    # Extract email body (plain text and HTML if present)
    plain_body = None
    html_body = None

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == 'text/plain':
                plain_body = part.get_content()
            elif content_type == 'text/html':
                html_body = part.get_content()
    else:
        content_type = msg.get_content_type()
        if content_type == 'text/plain':
            plain_body = msg.get_content()
        elif content_type == 'text/html':
            html_body = msg.get_content()

    # Debug output (only print first 200 characters for safety)
    print(f"From: {sender}")
    print(f"To: {recipient}")
    print(f"Subject: {subject}")
    print(f"Plain Body: {(plain_body or '')[:200]}")
    print(f"HTML Body: {(html_body or '')[:200]}")

    return {
        'statusCode': 200,
        'body': 'Email successfully parsed.'
    }
