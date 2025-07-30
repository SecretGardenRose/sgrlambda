import boto3

from email import policy
from email.parser import BytesParser

import urllib3
import io
import PyPDF2
import re
import json

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


    orderNumber = None
    first_name = None
    last_name = None
    phone = None
    addr = None
    date = None
    item = None
    lat = None
    lon = None
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
                        
 
                        for i in range(len(lines)):
                            if lines[i].startswith('Customer Order'):
                                print(lines[i])
                                
                                customer = lines[i+1].split()
                                first_name = customer[0]
                                last_name = customer[1]

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
                                print(first_name)
                                print(last_name)
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
                            
                            elif 'Order Number' in lines[i]:
                                orderNumber = lines[i].split()[-1].strip()
                                print(orderNumber)

                            else:
                                print("unprocessed line", lines[i])
                        break
                    
 
        http = urllib3.PoolManager()
        
        # API URL
        api_url = "https://www.hellosecretgarden.com/south-fast/mall/mallorder/save"
        
        data = {
            "orderNumber": orderNumber,
            "flowerPicture": "http://example.com/flower.jpg",
            "userEmail": "user@example.com",
            "externalId": orderNumber,
            "userPhone": phone,
            "orderStatus": "pending",
            "firstName": first_name,
            "lastName": last_name,
            "userZip": "10001",
            "note": deliveryInstructions,
            "city": "New York",
            "address": addr,
            "tags": "gift",
            "sgrCal": "some_value",
            "sgrCalValue": "some_value",
            "sgrInst": "some_instruction",
            "sgrInstValue": "some_instruction_value",
            "productName": "Rose Bouquet",
            "fulfillmentStatus": "unfulfilled",
            "delivery": "standard",
            "florist": "Local Florist",
            "billingPhone": "1234567890",
            "recipientPhone": "0987654321",
            "address2": "Apt 4B",
            "shopifyId": "SHOP123",
            "latitude": lat,
            "longitude": lon
        }
        
        # 发送请求
        response = http.request(
            'POST',
            api_url,
            body=json.dumps(data),
            headers={'Content-Type': 'application/json'}
        )

        print(f"响应结果: {response}")
        
        # 处理响应
        result = json.loads(response.data.decode('utf-8'))

        print(f"响应结果2: {result}")
        
        if response.status == 200 and result.get("code") == 0:
            return {"status": "success", "message": "操作成功"}
        else:
            return {"status": "error", "message": result}
            
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


