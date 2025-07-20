import boto3

from email import policy
from email.parser import BytesParser


import io
import PyPDF2

s3 = boto3.client('s3')
textract = boto3.client('textract')
location = boto3.client("location")



def lambda_handler(event, context):

    print ("entered function 1")
    # Step 1: Extract the message ID from SES event
    ses_record = event['Records'][0]['ses']
    message_id = ses_record['mail']['messageId']
    
    #message_id = 'qtg8divbtknfuu2dtrvv6g8fukabvps2v4qqf681'
    #message_id = 'eblinuob0u6iqojngvbvf1lfcm22utbtq1ifdi01' #Amanda O

    # Step 2: Build the S3 object path
    bucket_name = 'ses-sgrcredit'
    object_key = f'{message_id}'

    print (object_key)
    # Step 3: Read raw email content from S3
    s3_object = s3.get_object(Bucket=bucket_name, Key=object_key)
    raw_email = s3_object['Body'].read()

    print ("hello")
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
                
            elif content_type == 'application/pdf':
                print (part.get_content_disposition())
                if part.get_content_disposition() == 'attachment' and part.get_filename().endswith('.pdf'):

                    print (part.get_filename())
                    print (part.get_content_type())
                    print (part.get_content_disposition())
                    bytes_data = part.get_payload(decode=True)

                    pdf_stream = io.BytesIO(bytes_data)
                    reader = PyPDF2.PdfReader(pdf_stream)

                    # Loop through all pages and extract text
                    for page_num, page in enumerate(reader.pages, start=1):
                        text = page.extract_text()
                        lines = text.splitlines()
                        
                        customer = None
                        phone = None
                        addr = None
                        date = None
                        item = None
                        for i in range(len(lines)):
                            if lines[i].startswith('Customer Order'):
                                print(lines[i])
                                
                                customer = lines[i+1]
                                phone = lines[i+2]
                                addr = lines[i+3] + lines[i+4].split(', USA')[0]
                                date = lines[i+4]
                                
                                # Search using your place index name
                                response = location.search_place_index_for_text(
                                    IndexName="MyLocationIndex",  # Use your actual index name
                                    Text=addr,
                                    MaxResults=1
                                )

                                if response['Results']:
                                    place = response['Results'][0]['Place']
                                    lat = place['Geometry']['Point'][1]
                                    lon = place['Geometry']['Point'][0]
                                    print(f"Latitude: {lat}, Longitude: {lon}")

                                print(customer) 
                                print(phone) 
                                print(addr)
                                print(date)

                            elif lines[i].startswith('Delivery Instructions:'):
                                deliveryInstructions = lines[i]
                                print(deliveryInstructions)

                            elif lines[i].startswith('Qty.'):
                                item = lines[i+1]
                                for idx in range(2,100):
                                    if lines[i+idx].startswith('~ End of Order'):
                                        break
                                    else:
                                        item = item + lines[i+idx]
                                print(item)
                            else:
                                print("unprocessed line", lines[i])
                        break


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

